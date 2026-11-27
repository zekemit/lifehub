/**
 * src/goals/goalPipeline.ts — GoalExecutionWF's state machine, written against StepLike like every other pipeline (the code that runs in a Cloudflare
 * Workflow also runs, step for step, in the tests).
 *
 *   classify_and_debit        the tier fixes the price (micro_app 5 credits / $1.25, research_plan 3 / $0.75). Debit through src/billing/credits.ts,
 *                             subscription credits first; auto-recharge if opted in and within its monthly cap; otherwise INSUFFICIENT_CREDITS and
 *                             stop: after a top-up the goal restarts as instance <goal_id>~<n>.
 *   plan_dag                  household facts (after redact()) go to the chief model, which orders the task DAG. Code enforces the DAG.
 *   execute_with_self_repair  context_gather, schema_gen, code_gen, verify_bundle, smoke_test, package_install. Before EVERY model call Gate G8 (spend so
 *                             far + the call's pessimistic cost <= max_cost_usd) and the 90,000-token cliff are checked. A failed verify_bundle or
 *                             smoke_test increments goal_tasks.retries (at most 2), hands the exact diagnostics to the worker model, and re-verifies.
 *                             Gate G7 (no tax characterization) trips at once. Any failure refunds the full reservation, bucket for bucket.
 *   stage_r2_preview + wait   the verified bundle goes into R2 encrypted, the app is STAGED (preview only), the member is tapped (payload-less push) and
 *                             the goal waits for goal_approval:<goal_id>.
 *   publish_r3_live           the approval must carry the staged approval_hash; the stored bundle is re-hashed and re-verified; then the app is LIVE,
 *                             prod_url is set and a grounded hosted_micro_app fact is written. Declined, mismatched or timed out: the app is ARCHIVED,
 *                             the goal DECLINED, the credits refunded.
 * Nothing here is deployed to a member's own site: the install artifact is data the member applies. All network I/O goes through deps.fetch / the gateway.
 */

import { attemptAutoRecharge } from '../billing/autoRecharge';
import { debitCredit, refundCredit } from '../billing/credits';
import { stripeClient } from '../billing/stripe';
import { buildInstallArtifact, domainFromPrompt, type InstallArtifact } from '../builder/installPackager';
import { householdFacts, INSTALL_TOKEN_TTL_SECONDS, signAppToken } from '../builder/microAppRuntime';
import { referenceRecipeBundle } from '../builder/recipeTemplate';
import { formatDiagnostics, verifyBundle, type Bundle, type Diagnostic } from '../builder/sandboxVerifier';
import { smokeTestBundle, type SmokeResult } from '../builder/smokeTest';
import { getDecrypted, putEncrypted, sha256Hex } from '../crypto/vault';
import { writeFact } from '../graph/writeFact';
import type { StepLike } from '../ingest/steps';
import { WORKER_INPUT_TOKEN_CEILING, estimateTokens, resolveRoute, type Gateway } from '../llm/gateway';
import { costUsd } from '../llm/pricing';
import { TaxLanguageError, assertNoTaxLanguage } from '../missions/copy';
import { canonicalJson, type MissionDeps, type MissionEnv } from '../missions/runPipeline';
import { notifyHousehold } from '../notify/webPush';
import { redact } from '../redact';
import type { Env, GoalExecutionParams } from '../types';
import { GOAL_TIERS, classifyGoal, creditsFor, maxCostFor, type GoalTier } from './tiers';
import { verifyPlan, type ResearchPlan } from './planVerifier';

export const GOAL_APPROVAL_TIMEOUT_MS = 7 * 86_400_000;
export const MAX_REPAIRS = 2;
const STEP_RETRY = { retries: { limit: 2, delay: '10 seconds', backoff: 'exponential' as const } };

export type GoalEnv = MissionEnv & Pick<Env, 'SESSION_SECRET' | 'APP_NAME'>;

export interface GoalDeps extends Omit<MissionDeps, 'env'> {
  env: GoalEnv;
  /** The synthetic smoke test; production uses the real one. Tests substitute it to prove the repair loop covers smoke failures too. */
  smoke?: (bundle: Bundle) => Promise<SmokeResult>;
}

export type GoalStatus = 'PLANNING' | 'EXECUTING' | 'VERIFYING' | 'AWAITING_APPROVAL' | 'COMPLETED' | 'DECLINED' | 'FAILED' | 'INSUFFICIENT_CREDITS';
export type TaskKind = 'decompose' | 'context_gather' | 'schema_gen' | 'code_gen' | 'verify_bundle' | 'smoke_test' | 'package_install';

export interface GoalRow {
  goal_id: string;
  household_id: string;
  submitted_by_member_id: string | null;
  prompt_text: string;
  tier: GoalTier;
  status: GoalStatus;
  credits_reserved: number;
  max_cost_usd: number;
  cost_usd: number;
  approval_hash: string | null;
  preview_url: string | null;
  prod_url: string | null;
  install_artifact_json: string | null;
  result_summary_json: string | null;
  error_code: string | null;
  created_at: string;
  updated_at: string;
  attempt: number;
}

interface TaskRow {
  task_id: string;
  goal_id: string;
  seq_no: number;
  kind: TaskKind;
  spec_json: string;
  status: 'PENDING' | 'RUNNING' | 'PASSED' | 'FAILED';
  retries: number;
  output_json: string | null;
  error_text: string | null;
}

export interface GoalApprovalEvent {
  decision: 'approve' | 'decline';
  approval_hash?: string;
  member_id?: string;
}

export type GoalOutcome =
  | { status: 'insufficient_credits'; auto_recharge: string | null }
  | { status: 'failed'; code: string }
  | { status: 'declined'; reason: 'declined' | 'approval_hash_mismatch' | 'expired' }
  | { status: 'completed'; prod_url: string | null };

/** The DAG each tier runs, after the 'decompose' step (which is plan_dag itself). */
export const CANONICAL_DAG: Readonly<Record<GoalTier, readonly TaskKind[]>> = {
  micro_app: ['context_gather', 'schema_gen', 'code_gen', 'verify_bundle', 'smoke_test', 'package_install'],
  research_plan: ['context_gather', 'schema_gen', 'code_gen', 'verify_bundle'],
};

export class GoalGateError extends Error {
  override readonly name = 'GoalGateError';
  constructor(readonly code: 'G8_COST' | 'TOKEN_CLIFF' | 'G7_TAX_LANGUAGE' | 'REPAIR_EXHAUSTED' | 'PROMPT_QUARANTINED' | 'TIER_MISMATCH' | 'REGATE_FAILED', message: string) {
    super(message);
  }
}

/** The model answered, but not with usable JSON; the repair loop treats it as a failed attempt. */
class ModelOutputError extends Error {
  override readonly name = 'ModelOutputError';
}

const iso = (deps: GoalDeps): string => deps.now().toISOString();
export const appIdFor = async (goal_id: string): Promise<string> => `app_${(await sha256Hex(`app:${goal_id}`)).slice(0, 24)}`;
export const instanceIdFor = (goal_id: string, attempt: number): string => (attempt === 0 ? goal_id : `${goal_id}~${attempt}`);
const TERMINAL: readonly GoalStatus[] = ['COMPLETED', 'DECLINED', 'FAILED'];

// ─── rows ───────────────────────────────────────────────────────────────────

export async function loadGoal(db: D1Database, goal_id: string, household_id: string): Promise<GoalRow | null> {
  return db.prepare('SELECT * FROM goals WHERE goal_id = ? AND household_id = ?').bind(goal_id, household_id).first<GoalRow>();
}

const loadTasks = async (db: D1Database, goal_id: string): Promise<TaskRow[]> =>
  (await db.prepare('SELECT * FROM goal_tasks WHERE goal_id = ? ORDER BY seq_no').bind(goal_id).all<TaskRow>()).results;

async function moveTo(db: D1Database, goal: GoalRow, to: GoalStatus, from: readonly GoalStatus[], at: string, extra: { error_code?: string } = {}): Promise<void> {
  if (goal.status === to) return;
  const res = await db
    .prepare(`UPDATE goals SET status = ?, updated_at = ?, error_code = COALESCE(?, error_code) WHERE goal_id = ? AND household_id = ? AND status IN (${from.map(() => '?').join(', ')})`)
    .bind(to, at, extra.error_code ?? null, goal.goal_id, goal.household_id, ...from)
    .run();
  if ((res.meta.changes ?? 0) === 0) {
    const now = await loadGoal(db, goal.goal_id, goal.household_id);
    if (now?.status === to) return;
    throw new Error(`goal ${goal.goal_id} cannot move from ${now?.status ?? 'missing'} to ${to}`);
  }
  goal.status = to;
}

/** The ledger's run_id references mission_runs; an inert anchor row (state CANCELED, template goal_*) satisfies it and shows up nowhere. */
async function ensureAnchor(db: D1Database, goal: GoalRow, at: string): Promise<void> {
  await db
    .prepare(
      `INSERT OR IGNORE INTO mission_runs (run_id, household_id, template_key, template_version, origin, params_json, state, credit_cost, created_at, updated_at)
       VALUES (?, ?, ?, 1, 'user', '{"goal":true}', 'CANCELED', ?, ?, ?)`,
    )
    .bind(goal.goal_id, goal.household_id, `goal_${goal.tier}`, goal.credits_reserved, at, at)
    .run();
}

/** Ends a goal in failure: every unfinished task FAILED, the whole reservation refunded to the buckets it came from, the staged app archived. */
async function failGoal(db: D1Database, goal: GoalRow, code: string, at: string): Promise<void> {
  await db.prepare(`UPDATE goal_tasks SET status = 'FAILED', error_text = COALESCE(error_text, ?) WHERE goal_id = ? AND status IN ('PENDING', 'RUNNING')`).bind(code, goal.goal_id).run();
  await db.prepare(`UPDATE micro_apps SET status = 'ARCHIVED' WHERE goal_id = ? AND household_id = ? AND status = 'STAGED'`).bind(goal.goal_id, goal.household_id).run();
  await refundCredit(db, goal.household_id, goal.goal_id, at);
  await db.prepare(`UPDATE goals SET status = 'FAILED', error_code = ?, updated_at = ? WHERE goal_id = ? AND household_id = ? AND status NOT IN ('COMPLETED', 'DECLINED', 'FAILED')`).bind(code, at, goal.goal_id, goal.household_id).run();
  goal.status = 'FAILED';
}

async function setTask(db: D1Database, goal_id: string, seq_no: number, status: TaskRow['status'], o: { output?: unknown; error?: string | null; retry?: boolean } = {}): Promise<void> {
  await db
    .prepare(`UPDATE goal_tasks SET status = ?, output_json = COALESCE(?, output_json), error_text = ?, retries = retries + ? WHERE goal_id = ? AND seq_no = ? AND retries + ? <= ${MAX_REPAIRS}`)
    .bind(status, o.output === undefined ? null : JSON.stringify(o.output), o.error ?? null, o.retry === true ? 1 : 0, goal_id, seq_no, o.retry === true ? 1 : 0)
    .run();
}

// ─── model calls under the gates ────────────────────────────────────────────

const spentUsd = async (db: D1Database, goal_id: string): Promise<number> =>
  (await db.prepare('SELECT COALESCE(SUM(cost_usd), 0) AS c FROM llm_usage WHERE run_id = ?').bind(goal_id).first<{ c: number }>())?.c ?? 0;

/** What a call could cost at most: the pessimistic token estimate in, the whole output allowance out, at the dearer of the primary and failover model. */
export function estimateCallUsd(role: 'chief' | 'worker', env: Pick<MissionEnv, 'CHIEF_MODEL' | 'WORKER_MODEL'>, inputTokens: number, maxOutput: number): number {
  const route = resolveRoute(role, { chief: env.CHIEF_MODEL, worker: env.WORKER_MODEL });
  const use = { input_tokens: inputTokens, cached_tokens: 0, output_tokens: maxOutput };
  return Math.max(costUsd(route.primary, use), costUsd(route.failover, use));
}

async function callModel(
  deps: GoalDeps,
  goal: GoalRow,
  role: 'chief' | 'worker',
  req: { purpose: string; system: string; user: string; schema: Record<string, unknown>; maxOutput: number },
): Promise<unknown> {
  const db = deps.env.DB;
  const tokens = estimateTokens(req.system) + estimateTokens(req.user);
  if (tokens > WORKER_INPUT_TOKEN_CEILING) throw new GoalGateError('TOKEN_CLIFF', `${req.purpose} needs ~${tokens} input tokens; the ceiling is ${WORKER_INPUT_TOKEN_CEILING}`);
  const spent = await spentUsd(db, goal.goal_id);
  const estimate = estimateCallUsd(role, deps.env, tokens, req.maxOutput);
  if (spent + estimate > goal.max_cost_usd) {
    throw new GoalGateError('G8_COST', `${req.purpose}: $${spent.toFixed(4)} spent + up to $${estimate.toFixed(4)} would pass the $${goal.max_cost_usd.toFixed(2)} cap`);
  }
  const gateway: Gateway = deps.gateway;
  const res = await gateway[role]({ household_id: goal.household_id, run_id: goal.goal_id, purpose: req.purpose, system: req.system, user: req.user, json_schema: { name: req.purpose.replace(/\W+/g, '_'), schema: req.schema }, max_output_tokens: req.maxOutput });
  const total = await spentUsd(db, goal.goal_id);
  await db.prepare('UPDATE goals SET cost_usd = ?, updated_at = ? WHERE goal_id = ? AND household_id = ?').bind(total, iso(deps), goal.goal_id, goal.household_id).run();
  goal.cost_usd = total;
  try {
    return JSON.parse(res.text) as unknown;
  } catch {
    throw new ModelOutputError(`${req.purpose} did not return JSON`);
  }
}

function gateG7(...texts: string[]): void {
  try {
    assertNoTaxLanguage(...texts);
  } catch (err) {
    if (err instanceof TaxLanguageError) throw new GoalGateError('G7_TAX_LANGUAGE', err.message);
    throw err;
  }
}

// ─── context ────────────────────────────────────────────────────────────────

export interface GoalContext {
  members: string[];
  spouse: string | null;
  cuisines: string[];
  restrictions: string[];
  domain: string | null;
}

/** What the household's own record says that bears on the goal. Names are redacted like any text before a model sees them. */
export async function gatherContext(db: D1Database, goal: GoalRow): Promise<GoalContext> {
  const names = (await db.prepare('SELECT display_name FROM members WHERE household_id = ? AND display_name IS NOT NULL ORDER BY created_at, member_id LIMIT 6').bind(goal.household_id).all<{ display_name: string }>()).results.map((r) => r.display_name);
  const spouse = await db
    .prepare(`SELECT o.canonical_name AS n FROM facts_current f JOIN entities o ON o.entity_id = f.object_id WHERE f.household_id = ? AND f.predicate = 'spouse_of' ORDER BY f.fact_id LIMIT 1`)
    .bind(goal.household_id)
    .first<{ n: string }>();
  const facts = await householdFacts(db, goal.household_id);
  const safe = (s: string): string | null => {
    const r = redact(s);
    return r.redaction_status === 'quarantined' ? null : r.redacted_text;
  };
  const keep = (xs: string[]): string[] => xs.map(safe).filter((x): x is string => x !== null && x !== '');
  return { members: keep(names), spouse: spouse === null ? null : safe(spouse.n), cuisines: keep(facts.cuisines), restrictions: keep(facts.restrictions), domain: domainFromPrompt(goal.prompt_text) };
}

const contextText = (c: GoalContext): string =>
  [`Household members: ${c.members.join(', ') || 'unknown'}`, `Spouse or partner on record: ${c.spouse ?? 'none'}`, `Preferred cuisines: ${c.cuisines.join(', ') || 'none on record'}`, `Dietary restrictions: ${c.restrictions.join(', ') || 'none on record'}`, `The member's own site: ${c.domain ?? 'not named'}`].join('\n');

// ─── step 1: classify and debit ─────────────────────────────────────────────

type Started = { kind: 'ok' } | { kind: 'halted'; auto_recharge: string | null } | { kind: 'ended'; status: GoalStatus; code: string | null; prod_url: string | null };

async function classifyAndDebit(deps: GoalDeps, params: GoalExecutionParams): Promise<Started> {
  const db = deps.env.DB;
  const goal = await loadGoal(db, params.goal_id, params.household_id);
  if (goal === null) throw new Error(`goal ${params.goal_id} not found`);
  if (TERMINAL.includes(goal.status)) return { kind: 'ended', status: goal.status, code: goal.error_code, prod_url: goal.prod_url };
  const at = iso(deps);
  // The tier decides the price, deterministically, and the row is not trusted to say otherwise.
  if (!(goal.tier in GOAL_TIERS) || classifyGoal(goal.prompt_text) !== goal.tier || goal.credits_reserved !== creditsFor(goal.tier) || goal.max_cost_usd !== maxCostFor(goal.tier)) {
    await db.prepare(`UPDATE goals SET status = 'FAILED', error_code = 'TIER_MISMATCH', updated_at = ? WHERE goal_id = ? AND household_id = ?`).bind(at, goal.goal_id, goal.household_id).run();
    return { kind: 'ended', status: 'FAILED', code: 'TIER_MISMATCH', prod_url: null };
  }
  await ensureAnchor(db, goal, at);
  let res = await debitCredit(db, goal.household_id, goal.goal_id, goal.credits_reserved, at);
  let declined: string | null = null;
  if (!res.ok) {
    const recharge = await attemptAutoRecharge({ db, stripe: deps.stripe, now: deps.now }, goal.household_id, goal.goal_id, params.attempt);
    if (recharge.status === 'recharged') res = await debitCredit(db, goal.household_id, goal.goal_id, goal.credits_reserved, at);
    else declined = recharge.reason;
  }
  if (!res.ok) {
    await moveTo(db, goal, 'INSUFFICIENT_CREDITS', ['PLANNING', 'INSUFFICIENT_CREDITS'], at, { error_code: 'INSUFFICIENT_CREDITS' });
    return { kind: 'halted', auto_recharge: declined };
  }
  await db.prepare(`UPDATE goals SET status = 'PLANNING', error_code = NULL, updated_at = ? WHERE goal_id = ? AND household_id = ? AND status IN ('PLANNING', 'INSUFFICIENT_CREDITS')`).bind(at, goal.goal_id, goal.household_id).run();
  return { kind: 'ok' };
}

// ─── step 2: plan the DAG ───────────────────────────────────────────────────

export const slugOf = (s: string): string => {
  const slug = s.normalize('NFKD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40).replace(/-+$/, '');
  return slug.length >= 3 ? slug : 'household-app';
};

/** The chief's ordered tasks are used only if they are exactly the tier's DAG; otherwise the canonical DAG runs (and the plan says so). */
export function acceptDag(tier: GoalTier, proposed: unknown): { tasks: Array<{ kind: TaskKind; notes: string }>; source: 'chief' | 'canonical' } {
  const canonical = CANONICAL_DAG[tier];
  if (Array.isArray(proposed)) {
    const items = (proposed as Array<{ kind?: unknown; notes?: unknown }>).filter((t) => t?.kind !== 'decompose');
    if (items.length === canonical.length && items.every((t, i) => t?.kind === canonical[i])) {
      return { tasks: items.map((t) => ({ kind: t.kind as TaskKind, notes: typeof t.notes === 'string' && redact(t.notes).redaction_status === 'clean' ? t.notes.slice(0, 400) : '' })), source: 'chief' };
    }
  }
  return { tasks: canonical.map((kind) => ({ kind, notes: '' })), source: 'canonical' };
}

const plannerSystem = (appName: string, tier: GoalTier): string =>
  [
    `GOAL-PLANNER. You plan work for ${appName}, a household assistant. The member asked for a goal; you order the tasks that deliver it.`,
    `Tier: ${tier}. Use exactly these task kinds, in this order: ${CANONICAL_DAG[tier].join(', ')}.`,
    'Return {"title": string (the name of the deliverable, under 60 characters), "slug": string (lower-case letters, digits and hyphens), "summary": string, "tasks": [{"kind": string, "notes": string}]}.',
    'Use only the household facts given. Never invent facts about the household. Do not mention taxes.',
  ].join('\n');

async function planDag(deps: GoalDeps, params: GoalExecutionParams): Promise<{ ok: true } | { ok: false; code: string }> {
  const db = deps.env.DB;
  const goal = (await loadGoal(db, params.goal_id, params.household_id))!;
  const existing = await loadTasks(db, goal.goal_id);
  if (existing.length > 0) return { ok: true }; // a replayed step: the DAG is already stored
  const prompt = redact(goal.prompt_text);
  if (prompt.redaction_status === 'quarantined') {
    await failGoal(db, goal, 'PROMPT_QUARANTINED', iso(deps));
    return { ok: false, code: 'PROMPT_QUARANTINED' };
  }
  const ctx = await gatherContext(db, goal);
  try {
    const out = (await callModel(deps, goal, 'chief', {
      purpose: 'goal.plan',
      system: plannerSystem(deps.env.APP_NAME, goal.tier),
      user: `The member's goal:\n<<<\n${prompt.redacted_text}\n>>>\n\nWhat the household record says:\n${contextText(ctx)}`,
      schema: {
        type: 'object',
        additionalProperties: false,
        required: ['title', 'slug', 'summary', 'tasks'],
        properties: {
          title: { type: 'string' },
          slug: { type: 'string' },
          summary: { type: 'string' },
          tasks: { type: 'array', items: { type: 'object', additionalProperties: false, required: ['kind', 'notes'], properties: { kind: { type: 'string' }, notes: { type: 'string' } } } },
        },
      },
      maxOutput: 1500,
    })) as { title?: unknown; slug?: unknown; summary?: unknown; tasks?: unknown };
    const dag = acceptDag(goal.tier, out.tasks);
    // the planner's words go into later prompts and onto a page: anything the redactor would change or withhold is replaced, never passed on
    const clean = (v: unknown): string => {
      if (typeof v !== 'string') return '';
      const text = v.replace(/\s+/g, ' ').trim();
      return redact(text).redaction_status === 'clean' ? text : '';
    };
    const title = clean(out.title).slice(0, 80) || 'Household app';
    const taken = new Set((await db.prepare('SELECT slug FROM micro_apps WHERE household_id = ?').bind(goal.household_id).all<{ slug: string }>()).results.map((r) => r.slug));
    let slug = slugOf(typeof out.slug === 'string' ? out.slug : title);
    for (let n = 2; taken.has(slug); n++) slug = `${slugOf(typeof out.slug === 'string' ? out.slug : title).slice(0, 36)}-${n}`;
    const decompose = { title, slug, summary: clean(out.summary).slice(0, 600), dag_source: dag.source };
    const rows = [
      db.prepare(`INSERT OR IGNORE INTO goal_tasks (task_id, goal_id, seq_no, kind, spec_json, status, output_json) VALUES (?, ?, 0, 'decompose', ?, 'PASSED', ?)`).bind(`tsk_${goal.goal_id}_0`, goal.goal_id, JSON.stringify({ kind: 'decompose' }), JSON.stringify(decompose)),
      ...dag.tasks.map((t, i) =>
        db.prepare(`INSERT OR IGNORE INTO goal_tasks (task_id, goal_id, seq_no, kind, spec_json, status) VALUES (?, ?, ?, ?, ?, 'PENDING')`).bind(`tsk_${goal.goal_id}_${i + 1}`, goal.goal_id, i + 1, t.kind, JSON.stringify({ kind: t.kind, notes: t.notes })),
      ),
    ];
    await db.batch(rows);
    return { ok: true };
  } catch (err) {
    if (err instanceof GoalGateError) {
      await failGoal(db, goal, err.code, iso(deps));
      return { ok: false, code: err.code };
    }
    if (err instanceof ModelOutputError) {
      await failGoal(db, goal, 'PLAN_FAILED', iso(deps));
      return { ok: false, code: 'PLAN_FAILED' };
    }
    throw err;
  }
}

// ─── step 3: execute, verify, repair ────────────────────────────────────────

const BUNDLE_SCHEMA = { type: 'object', required: ['manifest', 'html'], properties: { manifest: { type: 'object' }, html: { type: 'string' } } };
const PLAN_SCHEMA = {
  type: 'object',
  required: ['title', 'summary', 'sections', 'next_steps'],
  properties: { title: { type: 'string' }, summary: { type: 'string' }, sections: { type: 'array', items: { type: 'object' } }, next_steps: { type: 'array', items: { type: 'string' } } },
};

const schemaSystem = (tier: GoalTier): string =>
  tier === 'micro_app'
    ? 'GOAL-SCHEMA. Design the data for a small household web app. Return {"collections": [{"name": string, "fields": string[]}], "ui_notes": string}. Keep it to the one collection the request needs.'
    : 'GOAL-SCHEMA. Outline a research plan. Return {"collections": [], "ui_notes": string} where ui_notes lists the section headings you will write.';

const codeSystem = (appName: string, goal: GoalRow, slug: string, title: string): string =>
  goal.tier === 'micro_app'
    ? [
        `GOAL-CODE. You write a small hosted web app for ${appName}. Return {"manifest": object, "html": string}.`,
        `The app is a recipe book: template "recipe_book", slug "${slug}", title "${title}". Use EXACTLY the structure of this verified reference, changing wording and style only:`,
        JSON.stringify(referenceRecipeBundle({ slug, title })),
        'Rules: no eval, no new Function, no process.env, no external scripts, images or links that load, fetch calls only with a literal relative path that starts with api/, no <base>, never mention a household id or an app id, no cookies, no navigation. Every input needs a label, every image an alt, every button text.',
      ].join('\n')
    : [
        `GOAL-CODE. You write a research plan for ${appName}. Return {"title": string, "summary": string, "sections": [{"heading": string, "body": string}], "next_steps": string[]}.`,
        'At least three sections. Use only the household facts given and the member\'s request. State uncertainty plainly; never invent figures. Never characterize anyone\'s tax treatment.',
      ].join('\n');

const repairSystem = (): string =>
  'GOAL-REPAIR. The deliverable below failed an automatic check. Fix EVERY problem listed and change nothing else. Return the complete corrected deliverable in the same JSON shape.';

interface Slot {
  seq: number;
  row: TaskRow;
}

function slotOf(tasks: readonly TaskRow[], kind: TaskKind): Slot | null {
  const row = tasks.find((t) => t.kind === kind);
  return row === undefined ? null : { seq: row.seq_no, row };
}

const outputOf = <T>(row: TaskRow | undefined): T | null => (row?.output_json == null ? null : (JSON.parse(row.output_json) as T));

async function blocklist(db: D1Database): Promise<string[]> {
  return (await db.prepare('SELECT domain FROM policy_blocklist').all<{ domain: string }>()).results.map((r) => r.domain);
}

type Executed = { ok: true } | { ok: false; code: string };

async function execute(deps: GoalDeps, params: GoalExecutionParams): Promise<Executed> {
  const db = deps.env.DB;
  const goal = (await loadGoal(db, params.goal_id, params.household_id))!;
  const at = (): string => iso(deps);
  await moveTo(db, goal, 'EXECUTING', ['PLANNING', 'EXECUTING', 'VERIFYING'], at()); // a retried step may start from VERIFYING
  const tasks = await loadTasks(db, goal.goal_id);
  const plan = outputOf<{ title: string; slug: string }>(tasks.find((t) => t.kind === 'decompose'));
  if (plan === null) throw new Error('the goal has no stored plan');
  const get = (kind: TaskKind): Slot => {
    const s = slotOf(tasks, kind);
    if (s === null) throw new Error(`the stored DAG has no ${kind} task`);
    return s;
  };
  const fresh = async (kind: TaskKind): Promise<TaskRow> => (await loadTasks(db, goal.goal_id)).find((t) => t.kind === kind)!;
  try {
    // context_gather: deterministic
    const ctxSlot = get('context_gather');
    let ctx = outputOf<GoalContext>(ctxSlot.row);
    if (ctxSlot.row.status !== 'PASSED' || ctx === null) {
      ctx = await gatherContext(db, goal);
      await setTask(db, goal.goal_id, ctxSlot.seq, 'PASSED', { output: ctx });
    }

    // schema_gen: worker
    const schemaSlot = get('schema_gen');
    let schema = outputOf<unknown>(schemaSlot.row);
    if (schemaSlot.row.status !== 'PASSED' || schema === null) {
      await setTask(db, goal.goal_id, schemaSlot.seq, 'RUNNING');
      schema = await callModel(deps, goal, 'worker', {
        purpose: 'goal.schema',
        system: schemaSystem(goal.tier),
        user: `Goal:\n<<<\n${redact(goal.prompt_text).redacted_text ?? ''}\n>>>\n${contextText(ctx)}`,
        schema: { type: 'object', required: ['collections', 'ui_notes'], properties: { collections: { type: 'array', items: { type: 'object' } }, ui_notes: { type: 'string' } } },
        maxOutput: 800,
      });
      await setTask(db, goal.goal_id, schemaSlot.seq, 'PASSED', { output: schema });
    }

    // code_gen: worker. The deliverable is a bundle (micro_app) or a plan (research_plan).
    const codeSlot = get('code_gen');
    const verifySlot = get('verify_bundle');
    const smokeSlot = goal.tier === 'micro_app' ? get('smoke_test') : null;
    const generate = async (system: string, user: string): Promise<unknown> => {
      const out = await callModel(deps, goal, 'worker', { purpose: user.startsWith('REPAIR') ? 'goal.repair' : 'goal.code', system, user, schema: goal.tier === 'micro_app' ? BUNDLE_SCHEMA : PLAN_SCHEMA, maxOutput: goal.tier === 'micro_app' ? 12_000 : 4_000 });
      return out;
    };
    const baseUser = `Goal:\n<<<\n${redact(goal.prompt_text).redacted_text ?? ''}\n>>>\n${contextText(ctx)}\nData design: ${JSON.stringify(schema).slice(0, 2000)}\nTask notes: ${codeSlot.row.spec_json.slice(0, 600)}`;
    let deliverable = outputOf<unknown>(codeSlot.row);
    let generated = codeSlot.row.status === 'PASSED' && deliverable !== null;
    if (!generated) {
      await setTask(db, goal.goal_id, codeSlot.seq, 'RUNNING');
      try {
        deliverable = await generate(codeSystem(deps.env.APP_NAME, goal, plan.slug, plan.title), baseUser);
      } catch (err) {
        if (!(err instanceof ModelOutputError)) throw err;
        deliverable = { error: err.message };
      }
      generated = true;
    }
    const block = await blocklist(db);
    for (;;) {
      const codeRow = await fresh('code_gen');
      // Gate G7 on every generation, repaired or not: it fails the goal outright, with no repair
      gateG7(JSON.stringify(deliverable));
      await setTask(db, goal.goal_id, codeSlot.seq, 'PASSED', { output: deliverable });

      // verify_bundle (and smoke_test for apps)
      await moveTo(db, goal, 'VERIFYING', ['EXECUTING', 'VERIFYING'], at());
      let failure: { slot: Slot; diagnostics: Diagnostic[] } | null = null;
      await setTask(db, goal.goal_id, verifySlot.seq, 'RUNNING');
      const verdict = goal.tier === 'micro_app' ? verifyBundle(deliverable, { household_id: goal.household_id, blocklist: block, slug: plan.slug }) : verifyPlan(deliverable);
      if (!verdict.ok) failure = { slot: verifySlot, diagnostics: verdict.diagnostics };
      else {
        await setTask(db, goal.goal_id, verifySlot.seq, 'PASSED', { output: { ok: true } });
        if (smokeSlot !== null) {
          await setTask(db, goal.goal_id, smokeSlot.seq, 'RUNNING');
          const smoke = await (deps.smoke ?? ((b: Bundle) => smokeTestBundle(b, { household_id: goal.household_id, appName: deps.env.APP_NAME })))(deliverable as Bundle);
          if (!smoke.ok) failure = { slot: smokeSlot, diagnostics: smoke.diagnostics };
          else await setTask(db, goal.goal_id, smokeSlot.seq, 'PASSED', { output: { ok: true, checks: smoke.checks } });
        }
      }
      if (failure === null) break;

      const attempts = codeRow.retries;
      const trace = formatDiagnostics(failure.diagnostics);
      await setTask(db, goal.goal_id, failure.slot.seq, 'FAILED', { error: trace.slice(0, 2000) });
      if (attempts >= MAX_REPAIRS) throw new GoalGateError('REPAIR_EXHAUSTED', `still failing after ${MAX_REPAIRS} repairs:\n${trace}`);
      await setTask(db, goal.goal_id, failure.slot.seq, 'FAILED', { error: trace.slice(0, 2000), retry: true });
      await setTask(db, goal.goal_id, codeSlot.seq, 'RUNNING', { retry: true });
      await moveTo(db, goal, 'EXECUTING', ['VERIFYING', 'EXECUTING'], at());
      try {
        deliverable = await generate(repairSystem(), `REPAIR\nGoal:\n<<<\n${redact(goal.prompt_text).redacted_text ?? ''}\n>>>\nThe failing deliverable:\n${JSON.stringify(deliverable).slice(0, 60_000)}\n\nThe exact problems found (${failure.slot.row.kind}):\n${trace}`);
      } catch (err) {
        if (!(err instanceof ModelOutputError)) throw err;
        deliverable = { error: err.message };
      }
    }

    // package_install (apps): deterministic, after the bundle is verified
    if (goal.tier === 'micro_app') {
      const pkg = get('package_install');
      await setTask(db, goal.goal_id, pkg.seq, 'RUNNING');
      const app_id = await appIdFor(goal.goal_id);
      const token = await signAppToken(deps.env.SESSION_SECRET, { household_id: goal.household_id, app_id, scope: 'install' }, INSTALL_TOKEN_TTL_SECONDS, deps.now());
      const artifact = buildInstallArtifact({ base_url: params.base_url, household_id: goal.household_id, slug: plan.slug, title: plan.title, install_token: token, target_domain: ctx.domain });
      await setTask(db, goal.goal_id, pkg.seq, 'PASSED', { output: artifact });
    }
    return { ok: true };
  } catch (err) {
    if (err instanceof GoalGateError) {
      await failGoal(db, goal, err.code, at());
      return { ok: false, code: err.code };
    }
    throw err;
  }
}

// ─── step 4: stage ──────────────────────────────────────────────────────────

interface Staged {
  approval_hash: string;
  preview_url: string | null;
}

async function stage(deps: GoalDeps, params: GoalExecutionParams): Promise<Staged> {
  const db = deps.env.DB;
  const goal = (await loadGoal(db, params.goal_id, params.household_id))!;
  const at = iso(deps);
  if (goal.status === 'AWAITING_APPROVAL' && goal.approval_hash !== null) return { approval_hash: goal.approval_hash, preview_url: goal.preview_url }; // replayed step
  const tasks = await loadTasks(db, goal.goal_id);
  const plan = outputOf<{ title: string; slug: string }>(tasks.find((t) => t.kind === 'decompose'))!;
  const deliverable = outputOf<unknown>(tasks.find((t) => t.kind === 'code_gen'));
  if (goal.tier === 'research_plan') {
    const summary = { ...(deliverable as ResearchPlan), title: (deliverable as ResearchPlan).title };
    const approval_hash = await sha256Hex(canonicalJson({ goal_id: goal.goal_id, tier: goal.tier, plan: summary }));
    await db.prepare(`UPDATE goals SET status = 'AWAITING_APPROVAL', approval_hash = ?, result_summary_json = ?, updated_at = ? WHERE goal_id = ? AND household_id = ?`).bind(approval_hash, JSON.stringify(summary), at, goal.goal_id, goal.household_id).run();
    await notifyHousehold({ db, fetch: deps.fetch, env: deps.env, now: deps.now }, goal.household_id, 'goal_ready');
    return { approval_hash, preview_url: null };
  }
  const bundle = deliverable as Bundle;
  // belt and braces: nothing reaches R2 that the verifier would not pass right now
  const verdict = verifyBundle(bundle, { household_id: goal.household_id, blocklist: await blocklist(db), slug: plan.slug });
  if (!verdict.ok) throw new GoalGateError('REGATE_FAILED', formatDiagnostics(verdict.diagnostics));
  const artifact = outputOf<InstallArtifact>(tasks.find((t) => t.kind === 'package_install'))!;
  const app_id = await appIdFor(goal.goal_id);
  const bytes = new TextEncoder().encode(JSON.stringify(bundle));
  const bundle_sha256 = await sha256Hex(bytes);
  const bundle_r2_key = `apps/${goal.household_id}/${app_id}/v1.bundle.enc`;
  await putEncrypted(deps.env, goal.household_id, bundle_r2_key, bytes);
  const cap = bundle.manifest.limits?.monthly_ai_calls_cap ?? 100;
  await db
    .prepare(
      `INSERT OR IGNORE INTO micro_apps (app_id, household_id, goal_id, slug, title, bundle_r2_key, manifest_json, status, monthly_ai_calls_used, monthly_ai_calls_cap, version, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, 'STAGED', 0, ?, 1, ?)`,
    )
    .bind(app_id, goal.household_id, goal.goal_id, plan.slug, plan.title, bundle_r2_key, JSON.stringify(bundle.manifest), cap, at)
    .run();
  const base = params.base_url.replace(/\/+$/, '');
  const preview_url = `${base}/apps/${goal.household_id}/${plan.slug}/preview`;
  const summary = { title: plan.title, slug: plan.slug, app_id, bundle_sha256, version: 1 };
  const approval_hash = await sha256Hex(canonicalJson({ goal_id: goal.goal_id, tier: goal.tier, bundle_sha256, slug: plan.slug, install: artifact }));
  await db
    .prepare(`UPDATE goals SET status = 'AWAITING_APPROVAL', approval_hash = ?, preview_url = ?, install_artifact_json = ?, result_summary_json = ?, updated_at = ? WHERE goal_id = ? AND household_id = ?`)
    .bind(approval_hash, preview_url, JSON.stringify(artifact), JSON.stringify(summary), at, goal.goal_id, goal.household_id)
    .run();
  await notifyHousehold({ db, fetch: deps.fetch, env: deps.env, now: deps.now }, goal.household_id, 'goal_ready');
  return { approval_hash, preview_url };
}

// ─── step 5: publish, or decline and refund ─────────────────────────────────

async function publish(deps: GoalDeps, params: GoalExecutionParams, staged: Staged): Promise<{ ok: true; prod_url: string | null } | { ok: false; code: string }> {
  const db = deps.env.DB;
  const goal = (await loadGoal(db, params.goal_id, params.household_id))!;
  const at = iso(deps);
  if (goal.status === 'COMPLETED') return { ok: true, prod_url: goal.prod_url };
  if (goal.status !== 'AWAITING_APPROVAL' || goal.approval_hash !== staged.approval_hash) throw new Error(`goal ${goal.goal_id} is not awaiting this approval`);
  const summary = JSON.parse(goal.result_summary_json ?? '{}') as { title?: string; slug?: string; app_id?: string; bundle_sha256?: string };
  let prod_url: string | null = null;
  if (goal.tier === 'micro_app') {
    const app = await db.prepare(`SELECT app_id, slug, title, bundle_r2_key FROM micro_apps WHERE goal_id = ? AND household_id = ?`).bind(goal.goal_id, goal.household_id).first<{ app_id: string; slug: string; title: string; bundle_r2_key: string }>();
    if (app === null) throw new Error('the staged app is missing');
    // regate: the stored bundle must be byte-for-byte what the member previewed, and must still pass the verifier
    const raw = await getDecrypted(deps.env, goal.household_id, app.bundle_r2_key);
    const intact = raw !== null && (await sha256Hex(raw)) === summary.bundle_sha256;
    const verdict = intact ? verifyBundle(JSON.parse(new TextDecoder().decode(raw!)) as unknown, { household_id: goal.household_id, blocklist: await blocklist(db), slug: app.slug }) : null;
    if (!intact || verdict === null || !verdict.ok) {
      await failGoal(db, goal, 'REGATE_FAILED', at);
      return { ok: false, code: 'REGATE_FAILED' };
    }
    prod_url = `${params.base_url.replace(/\/+$/, '')}/apps/${goal.household_id}/${app.slug}`;
    await db.batch([
      db.prepare(`UPDATE micro_apps SET status = 'LIVE' WHERE app_id = ? AND household_id = ? AND status = 'STAGED'`).bind(app.app_id, goal.household_id),
      db.prepare(`UPDATE goals SET status = 'COMPLETED', prod_url = ?, error_code = NULL, updated_at = ? WHERE goal_id = ? AND household_id = ? AND status = 'AWAITING_APPROVAL'`).bind(prod_url, at, goal.goal_id, goal.household_id),
    ]);
    await recordHostedApp(db, goal, app.title, prod_url, at);
  } else {
    await db.prepare(`UPDATE goals SET status = 'COMPLETED', error_code = NULL, updated_at = ? WHERE goal_id = ? AND household_id = ? AND status = 'AWAITING_APPROVAL'`).bind(at, goal.goal_id, goal.household_id).run();
  }
  return { ok: true, prod_url };
}

/** The household's record gains a fact, grounded in an episode that says it in the member's own request and the app's address. */
async function recordHostedApp(db: D1Database, goal: GoalRow, title: string, prod_url: string, at: string): Promise<void> {
  const quote = `Published: ${title} at ${prod_url}`;
  const red = redact(`Goal: ${goal.prompt_text.slice(0, 500)}\n${quote}`);
  if (red.redaction_status === 'quarantined' || !red.redacted_text.includes(quote)) return;
  const text = red.redacted_text;
  const episode_id = `ep_goal_${(await sha256Hex(`${goal.goal_id}:hosted`)).slice(0, 24)}`;
  await db
    .prepare(`INSERT OR IGNORE INTO episodes (episode_id, household_id, source, redacted_text, sha256, occurred_at, received_at, redaction_status) VALUES (?, ?, 'user_input', ?, ?, ?, ?, ?)`)
    .bind(episode_id, goal.household_id, text, await sha256Hex(`${goal.household_id}:${text}`), at, at, red.redaction_status)
    .run();
  const start = text.indexOf(quote);
  const res = await writeFact(db, { household_id: goal.household_id, source_episode_id: episode_id, subject: { type: 'document', name: title }, predicate: 'hosted_micro_app', value_text: prod_url, confidence: 1, quotes: [{ quote, char_start: start, char_end: start + quote.length }] });
  if (res.status !== 'written' && res.status !== 'reaffirmed') {
    await db.prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'goals', 'hosted_fact_rejected', ?, ?)`).bind(`aud_${crypto.randomUUID().replace(/-/g, '')}`, goal.household_id, JSON.stringify({ goal_id: goal.goal_id, reason: res.reason }), at).run();
  }
}

async function declineGoal(deps: GoalDeps, params: GoalExecutionParams, reason: string): Promise<void> {
  const db = deps.env.DB;
  const goal = (await loadGoal(db, params.goal_id, params.household_id))!;
  if (TERMINAL.includes(goal.status)) return;
  const at = iso(deps);
  await db.prepare(`UPDATE micro_apps SET status = 'ARCHIVED' WHERE goal_id = ? AND household_id = ? AND status IN ('STAGED', 'LIVE')`).bind(goal.goal_id, goal.household_id).run();
  await refundCredit(db, goal.household_id, goal.goal_id, at);
  await db.prepare(`UPDATE goals SET status = 'DECLINED', error_code = ?, updated_at = ? WHERE goal_id = ? AND household_id = ? AND status NOT IN ('COMPLETED', 'DECLINED', 'FAILED')`).bind(reason, at, goal.goal_id, goal.household_id).run();
}

// ─── the state machine ──────────────────────────────────────────────────────

export async function runGoal(step: StepLike, deps: GoalDeps, params: GoalExecutionParams): Promise<GoalOutcome> {
  const db = deps.env.DB;
  const started = await step.do('classify_and_debit', () => classifyAndDebit(deps, params));
  if (started.kind === 'halted') return { status: 'insufficient_credits', auto_recharge: started.auto_recharge };
  if (started.kind === 'ended') return started.status === 'COMPLETED' ? { status: 'completed', prod_url: started.prod_url } : started.status === 'DECLINED' ? { status: 'declined', reason: 'declined' } : { status: 'failed', code: started.code ?? 'FAILED' };

  const systemError = async (err: unknown): Promise<GoalOutcome> => {
    await step.do('system_error', async () => {
      const goal = await loadGoal(db, params.goal_id, params.household_id);
      if (goal !== null) await failGoal(db, goal, 'SYSTEM_ERROR', iso(deps));
      await db.prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'goals', 'goal_system_error', ?, ?)`).bind(`aud_${crypto.randomUUID().replace(/-/g, '')}`, params.household_id, JSON.stringify({ goal_id: params.goal_id, error: err instanceof Error ? err.message.slice(0, 200) : 'error' }), iso(deps)).run();
    });
    return { status: 'failed', code: 'SYSTEM_ERROR' };
  };

  let staged: Staged;
  try {
    const planned = await step.do('plan_dag', STEP_RETRY, () => planDag(deps, params));
    if (!planned.ok) return { status: 'failed', code: planned.code };
    const executed = await step.do('execute_with_self_repair', STEP_RETRY, () => execute(deps, params));
    if (!executed.ok) return { status: 'failed', code: executed.code };
    staged = await step.do('stage_r2_preview', () => stage(deps, params));
  } catch (err) {
    if (err instanceof GoalGateError) {
      await step.do('gate_failure', async () => {
        const goal = await loadGoal(db, params.goal_id, params.household_id);
        if (goal !== null) await failGoal(db, goal, err.code, iso(deps));
      });
      return { status: 'failed', code: err.code };
    }
    return systemError(err);
  }

  let approval: { payload: GoalApprovalEvent } | null = null;
  try {
    approval = await step.waitForEvent<GoalApprovalEvent>('await_approval', { type: `goal_approval:${params.goal_id}`, timeout: GOAL_APPROVAL_TIMEOUT_MS });
  } catch {
    approval = null;
  }
  if (approval === null) {
    await step.do('expire', () => declineGoal(deps, params, 'expired'));
    return { status: 'declined', reason: 'expired' };
  }
  const decision = approval.payload;
  if (decision.decision !== 'approve' || decision.approval_hash !== staged.approval_hash) {
    const reason = decision.decision !== 'approve' ? 'declined' : 'approval_hash_mismatch';
    await step.do('decline', () => declineGoal(deps, params, reason));
    return { status: 'declined', reason };
  }
  try {
    const live = await step.do('publish_r3_live', () => publish(deps, params, staged));
    return live.ok ? { status: 'completed', prod_url: live.prod_url } : { status: 'failed', code: live.code };
  } catch (err) {
    return systemError(err);
  }
}

export { stripeClient };
