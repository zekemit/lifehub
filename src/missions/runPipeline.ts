/**
 * src/missions/runPipeline.ts — the MissionRunWF state machine (CLAUDE.md §5.1), written against StepLike so the exact code that
 * runs inside a Cloudflare Workflow also runs in the tests.
 *
 *   compile -> debit_credit -> r0_read -> r0_fill -> gates -> r1_stage -> notify -> await_approval
 *           -> r3_regate -> r3_link -> deliver_link -> await_confirmation -> confirm
 *
 * Money and state rules (§5.3):
 *   - one credit is debited at compile (atomic, conditional on the balance) and given back on FAILED_GATE or a system error;
 *     DECLINED and EXPIRED keep the debit. A run that cannot be paid for is CANCELED and nothing is refunded (nothing was taken).
 *   - every transition goes through runState.transitionRun, so an illegal move throws and a repeated step is a no-op.
 *   - the member approves the exact card they were shown: the approval event must carry the staged payload hash, and R3 re-runs
 *     every gate on fresh data and refuses to issue a link if the card changed after it was approved.
 *   - R3 is a deep link. Nothing is booked, paid or fetched from a merchant.
 */

import { evaluateTrigger, loadTriggers, type ClaimRef } from './engine';
import { buildCopy } from './copy';
import { buildDeepLink, parseAffiliateTags, resolveDomain, type DeepLink } from './deeplinks';
import { runGates, type CardPayload, type FactState, type FreshWindow, type GateOutcome, type PlanStep, type ReadContext } from './gates';
import { debitCredit, refundCredit } from './ledger';
import { getRun, recordStep, STEP_NO, transitionRun } from './runState';
import { CalendarApiError, CalendarAuthError, computeFreeWindows, findCalendarConnector, isoMs, queryBusy } from '../connectors/googleCalendar';
import { sha256Hex } from '../crypto/vault';
import type { StepLike } from '../ingest/steps';
import { createGateway, estimateTokens, TokenCliffError, type Gateway } from '../llm/gateway';
import { createGuardedFetch, matchBlocklist, type BlocklistEntry } from '../net/egress';
import { notifyHousehold } from '../notify/webPush';
import type { Env, MissionRunParams, MissionTier } from '../types';

export type MissionEnv = Pick<
  Env,
  | 'DB' | 'KV' | 'DOCS_BUCKET' | 'KEK_B64' | 'GOOGLE_CLIENT_ID' | 'GOOGLE_CLIENT_SECRET'
  | 'OPENAI_API_KEY' | 'ANTHROPIC_API_KEY' | 'CHIEF_MODEL' | 'WORKER_MODEL'
  | 'AFFILIATE_TAGS' | 'VAPID_PUBLIC_KEY' | 'VAPID_PRIVATE_KEY' | 'VAPID_SUBJECT'
>;

export interface MissionDeps {
  env: MissionEnv;
  /** MUST be a guarded fetch (createGuardedFetch). */
  fetch: typeof fetch;
  gateway: Gateway;
  now: () => Date;
}

export const DEFAULT_APPROVAL_TIMEOUT_MS = 72 * 3_600_000;
export const MAX_APPROVAL_TIMEOUT_MS = 30 * 86_400_000;
export const MIN_APPROVAL_TIMEOUT_MS = 60_000;
export const CONFIRMATION_TIMEOUT = '72 hours';
/** What a dinner out is assumed to cost when Gate G3 asks whether the dining envelope can take it. */
export const DINNER_ESTIMATE_CENTS = 15_000;
export const DINNER_SLOT_MS = 2 * 3_600_000;
export const CALENDAR_HORIZON_DAYS = 21;
export const MAX_SCRIPT_CHARS = 2_000;

export interface ApprovalEvent {
  decision: 'approve' | 'decline';
  payload_hash: string;
  member_id?: string;
}

export type MissionOutcome =
  | { status: 'canceled'; reason: string }
  | { status: 'failed_gate'; stage: 'r1' | 'r3'; failed: string[] }
  | { status: 'expired' }
  | { status: 'declined'; reason: 'declined' | 'payload_hash_mismatch' }
  | { status: 'confirmed'; link: DeepLink | null }
  | { status: 'link_issued'; link: DeepLink };

/**
 * Production wiring. One guarded fetch serves the gateway, Google and Web Push, and records any blocked attempt against the run (Gate G5).
 * `network` replaces the real network underneath the guard (tests only; production leaves it out).
 */
export function missionDepsFromEnv(
  env: MissionEnv,
  run_id: string,
  household_id: string,
  opts: { now?: () => Date; network?: typeof fetch } = {},
): MissionDeps {
  const now = opts.now ?? (() => new Date());
  const guarded = createGuardedFetch({
    db: env.DB,
    ...(opts.network === undefined ? {} : { fetchImpl: opts.network, ttlMs: 0 }),
    onBlocked: async (blocked) => {
      await env.DB
        .prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'egress', 'egress_blocked', ?, ?)`)
        .bind(`aud_${crypto.randomUUID().replace(/-/g, '')}`, household_id, JSON.stringify({ run_id, host: blocked.host, domain: blocked.domain }), now().toISOString())
        .run();
    },
  });
  return {
    env,
    fetch: guarded,
    gateway: createGateway({ db: env.DB, apiKeys: { openai: env.OPENAI_API_KEY, anthropic: env.ANTHROPIC_API_KEY }, fetch: guarded, now, models: { chief: env.CHIEF_MODEL, worker: env.WORKER_MODEL } }),
    now,
  };
}

// ─── helpers ────────────────────────────────────────────────────────────────

/** JSON with object keys sorted at every level, so the same card always hashes the same. */
export function canonicalJson(value: unknown): string {
  const norm = (v: unknown): unknown => {
    if (Array.isArray(v)) return v.map(norm);
    if (v !== null && typeof v === 'object') {
      return Object.fromEntries(Object.entries(v as Record<string, unknown>).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([k, x]) => [k, norm(x)]));
    }
    return v;
  };
  return JSON.stringify(norm(value));
}

export const payloadHash = (payload: CardPayload): Promise<string> => sha256Hex(canonicalJson(payload));

const CARD_KIND: Readonly<Record<string, CardPayload['kind']>> = {
  expiring_credit: 'use_credit',
  annual_fee_roi: 'retention_script',
  anniv_dining: 'dinner_plan',
  subscription_creep: 'cancel_script',
  occasion_plan: 'plan',
};

interface Compiled {
  run_id: string;
  household_id: string;
  template_key: string;
  template_version: number;
  credits_cost: number;
  max_tier: MissionTier;
  plan: PlanStep[];
  trigger_key: string;
  dedupe_key: string;
  cooldown_days: number;
  timezone: string;
  expires_at: string | null;
  staged_claim_ids: string[];
}

interface Filled {
  script: string | null;
  fill_failure: string | null;
  input_tokens: number;
  output_tokens: number;
  cost_usd: number;
}

interface Staged {
  payload_hash: string;
  timeout_ms: number;
  gate_results: Array<{ gate: string; ok: boolean }>;
}

const PLAN_TIERS = ['R0', 'R1', 'R2', 'R3'];

function parsePlan(raw: string): PlanStep[] {
  const parsed = JSON.parse(raw) as unknown;
  if (!Array.isArray(parsed)) throw new Error('template plan_json is not an array');
  return parsed.map((s) => {
    const o = s as Record<string, unknown>;
    if (typeof o['id'] !== 'string' || typeof o['kind'] !== 'string' || typeof o['tier'] !== 'string' || !PLAN_TIERS.includes(o['tier'])) throw new Error('template plan_json has a malformed step');
    return {
      id: o['id'],
      kind: o['kind'],
      tier: o['tier'] as MissionTier,
      worker_prompt_id: typeof o['worker_prompt_id'] === 'string' ? o['worker_prompt_id'] : null,
      max_input_tokens: typeof o['max_input_tokens'] === 'number' ? o['max_input_tokens'] : 0,
    };
  });
}

const iso = (deps: MissionDeps): string => deps.now().toISOString();

// ─── r0_read ────────────────────────────────────────────────────────────────

interface ReadResult {
  read: ReadContext;
}

async function factStates(db: D1Database, household_id: string, claims: readonly ClaimRef[], now: Date): Promise<FactState[]> {
  const ids = [...new Set(claims.flatMap((c) => c.facts).filter((f): f is string => typeof f === 'string' && f !== ''))];
  const out: FactState[] = [];
  const at = now.toISOString();
  for (let i = 0; i < ids.length; i += 80) {
    const group = ids.slice(i, i + 80);
    const { results } = await db
      .prepare(
        `SELECT f.fact_id, f.predicate, r.value_type,
                (f.expired_at IS NULL AND f.valid_from <= ? AND (f.valid_to IS NULL OR f.valid_to > ?)) AS is_current,
                (SELECT MAX(e.received_at) FROM fact_quotes q JOIN episodes e ON e.episode_id = q.episode_id WHERE q.fact_id = f.fact_id) AS last_support
           FROM facts f JOIN predicate_registry r ON r.predicate = f.predicate
          WHERE f.household_id = ? AND f.fact_id IN (${group.map(() => '?').join(', ')})`,
      )
      .bind(at, at, household_id, ...group)
      .all<{ fact_id: string; predicate: string; value_type: string | null; is_current: number; last_support: string | null }>();
    for (const r of results) out.push({ fact_id: r.fact_id, predicate: r.predicate, value_type: r.value_type, current: r.is_current === 1, last_support: r.last_support });
  }
  return out;
}

async function freshWindows(deps: MissionDeps, c: Compiled): Promise<ReadContext['calendar']> {
  const now = deps.now();
  const base = { required: true, fetched_at: null, windows: [] as FreshWindow[], error: null as string | null };
  const connector = await findCalendarConnector(deps.env.DB, c.household_id);
  if (connector === null) return { ...base, error: 'Google Calendar is not connected' };
  try {
    const calDeps = { env: deps.env, fetch: deps.fetch, now: deps.now };
    const busy = await queryBusy(calDeps, connector, c.timezone, CALENDAR_HORIZON_DAYS);
    const windows = computeFreeWindows(busy, c.timezone, now.toISOString().slice(0, 10), CALENDAR_HORIZON_DAYS, now.getTime());
    return {
      required: true,
      fetched_at: now.toISOString(),
      error: null,
      windows: windows.map((w) => ({ date: w.date, start: isoMs(w.startMs), end: isoMs(w.endMs) })),
    };
  } catch (err) {
    // A revoked or unreadable calendar is a fact about the household, so it fails G2/G4. Anything else (a 5xx, a network error)
    // is transient: rethrow so the Workflow step retries.
    if (err instanceof CalendarAuthError || (err instanceof CalendarApiError && err.status >= 400 && err.status < 500)) {
      return { ...base, error: `Google Calendar could not be read: ${err.message.slice(0, 120)}` };
    }
    throw err;
  }
}

export async function readFacts(deps: MissionDeps, c: Compiled): Promise<ReadResult> {
  const db = deps.env.DB;
  const now = deps.now();
  const kind = CARD_KIND[c.template_key];
  const trigger = (await loadTriggers(db)).find((t) => t.trigger_key === c.trigger_key);
  const found = trigger === undefined ? undefined : (await evaluateTrigger(db, c.household_id, trigger, now)).candidates.find((x) => x.dedupe_key === c.dedupe_key);

  let envelope_key: string | null = null;
  let headroom_cents: number | null = null;
  if (kind === 'dinner_plan') {
    const env = await db
      .prepare(
        `SELECT envelope_key, headroom_cents FROM envelope_headroom
          WHERE household_id = ? AND (envelope_key LIKE '%dining%' OR envelope_key LIKE '%restaurant%')
          ORDER BY headroom_cents DESC, envelope_key LIMIT 1`,
      )
      .bind(c.household_id)
      .first<{ envelope_key: string; headroom_cents: number }>();
    envelope_key = env?.envelope_key ?? null;
    headroom_cents = env?.headroom_cents ?? null;
  }
  const spend = await db
    .prepare(`SELECT MAX(e.received_at) AS at FROM transactions t JOIN episodes e ON e.episode_id = t.episode_id WHERE t.household_id = ?`)
    .bind(c.household_id)
    .first<{ at: string | null }>();

  const claims = found?.claims ?? [];
  const calendar: ReadContext['calendar'] =
    kind === 'dinner_plan' ? await freshWindows(deps, c) : { required: false, fetched_at: null, windows: [], error: null };
  return {
    read: {
      candidate_found: found !== undefined,
      data: found?.data ?? {},
      claims,
      facts: await factStates(db, c.household_id, claims, now),
      headroom_cents,
      envelope_key,
      spend_data_at: spend?.at ?? null,
      calendar,
      fetched_at: now.toISOString(),
    },
  };
}

// ─── r0_fill ────────────────────────────────────────────────────────────────

const str = (v: unknown, fallback = ''): string => (typeof v === 'string' ? v : fallback);
const cents = (v: unknown): number => (typeof v === 'number' && Number.isFinite(v) ? Math.round(v) : 0);
const dollars = (v: unknown): string => `$${(cents(v) / 100).toFixed(2)}`;

interface WorkerPrompt {
  system: string;
  user: (data: Record<string, unknown>) => string;
}

const GUARDRAILS =
  'Write plain, polite text a person can read aloud. Use only the facts given. Do not say anything about taxes, deductions or write-offs. ' +
  'Do not include account numbers, links or anything you were not given.';

/** Template worker_prompt_id -> prompt. Prompts are code, not data: a model never chooses which prompt runs. */
export const WORKER_PROMPTS: Readonly<Record<string, WorkerPrompt>> = {
  retention_script: {
    system: `You write a short phone script for asking a credit card issuer for a retention offer before an annual fee posts. ${GUARDRAILS}`,
    user: (d) =>
      `Card: ${str(d['card_name'], 'the card')}\nAnnual fee: ${dollars(d['fee_cents'])}, posting ${str(d['next_post'])}\nCredits used this year: ${dollars(d['credits_used_cents'])}\n` +
      'Write a 5-8 line script: greet, state the fee, mention the credits actually used, ask what retention offers exist, and say you will decide after hearing them.',
  },
  cancel_script: {
    system: `You write a short script for cancelling or downgrading a subscription after a price change. ${GUARDRAILS}`,
    user: (d) =>
      `Subscription: ${str(d['subscription'])}\n` +
      (str(d['kind']) === 'duplicate'
        ? `Two charges: ${str(d['first'])} (${dollars(d['first_cents'])}) and ${str(d['second'])} (${dollars(d['second_cents'])})\n`
        : `Price went from ${dollars(d['old_cents'])} to ${dollars(d['new_cents'])} on ${str(d['since'])}\n`) +
      'Write a 4-6 line script for the support chat or call: say what changed, ask to cancel or move to a cheaper plan, and ask for written confirmation.',
  },
};

export async function workerFill(deps: MissionDeps, c: Compiled, data: Record<string, unknown>): Promise<Filled> {
  const stepDef = c.plan.find((s) => s.worker_prompt_id !== null);
  const empty: Filled = { script: null, fill_failure: null, input_tokens: 0, output_tokens: 0, cost_usd: 0 };
  if (stepDef === undefined || stepDef.worker_prompt_id === null) return empty;
  const prompt = WORKER_PROMPTS[stepDef.worker_prompt_id];
  if (prompt === undefined) throw new Error(`template ${c.template_key} names unknown worker prompt '${stepDef.worker_prompt_id}'`);
  const user = prompt.user(data);
  if (stepDef.max_input_tokens > 0 && estimateTokens(prompt.system) + estimateTokens(user) > stepDef.max_input_tokens) {
    return { ...empty, fill_failure: `worker input exceeds the template budget of ${stepDef.max_input_tokens} tokens` };
  }
  try {
    const res = await deps.gateway.worker({
      household_id: c.household_id,
      run_id: c.run_id,
      purpose: `mission.${stepDef.worker_prompt_id}`,
      system: prompt.system,
      user,
      json_schema: { name: 'script', schema: { type: 'object', properties: { script: { type: 'string' } }, required: ['script'], additionalProperties: false } },
      max_output_tokens: 700,
    });
    let script: unknown;
    try {
      script = (JSON.parse(res.text) as { script?: unknown }).script;
    } catch {
      throw new Error('worker did not return JSON');
    }
    if (typeof script !== 'string' || script.trim() === '' || script.length > MAX_SCRIPT_CHARS) throw new Error('worker returned no usable script');
    return { script: script.trim(), fill_failure: null, input_tokens: res.input_tokens, output_tokens: res.output_tokens, cost_usd: res.cost_usd };
  } catch (err) {
    if (err instanceof TokenCliffError) return { ...empty, fill_failure: `worker input needs ~${err.estimatedTokens} tokens, over the ${err.ceiling}-token ceiling` };
    throw err;
  }
}

// ─── the card ───────────────────────────────────────────────────────────────

async function linkTarget(db: D1Database, kind: CardPayload['kind'], data: Record<string, unknown>): Promise<CardPayload['action']> {
  if (kind !== 'use_credit' && kind !== 'dinner_plan') return { type: 'none' };
  const benefit =
    typeof data['benefit_key'] === 'string'
      ? await db.prepare('SELECT merchant_match, card_product FROM benefit_catalog WHERE benefit_key = ?').bind(data['benefit_key']).first<{ merchant_match: string; card_product: string }>()
      : await db.prepare('SELECT merchant_match, card_product FROM benefit_catalog WHERE label = ? LIMIT 1').bind(str(data['credit_label'])).first<{ merchant_match: string; card_product: string }>();
  const domain = benefit === null ? null : resolveDomain(benefit.merchant_match, benefit.card_product);
  if (domain === null) return { type: 'none' };
  const query: Record<string, string> = kind === 'dinner_plan' ? { date: str(data['window_date']), seats: '2' } : {};
  return { type: 'deeplink', merchant_domain: domain, path: '/', query };
}

export async function buildPayload(db: D1Database, c: Compiled, read: ReadContext, filled: Filled): Promise<CardPayload> {
  const kind = CARD_KIND[c.template_key] ?? 'plan';
  const data = read.data;
  const span = str(data['window_span']).split('/');
  const spanStart = Date.parse(span[0] ?? '');
  const proposed =
    kind === 'dinner_plan' && Number.isFinite(spanStart)
      ? { start: new Date(spanStart).toISOString(), end: new Date(spanStart + DINNER_SLOT_MS).toISOString() }
      : null;
  return {
    version: 1,
    run_id: c.run_id,
    template_key: c.template_key,
    trigger_key: c.trigger_key,
    dedupe_key: c.dedupe_key,
    kind,
    copy: buildCopy(c.trigger_key, data),
    script: filled.script,
    price_cents: kind === 'dinner_plan' ? DINNER_ESTIMATE_CENTS : 0,
    envelope_key: kind === 'dinner_plan' ? read.envelope_key : null,
    proposed_range: proposed,
    action: await linkTarget(db, kind, data),
    expires_on: typeof data['expires_on'] === 'string' ? data['expires_on'] : null,
  };
}

async function gateRun(deps: MissionDeps, c: Compiled, read: ReadContext, filled: Filled, payload: CardPayload, r3: boolean): Promise<GateOutcome> {
  return runGates({
    db: deps.env.DB,
    now: deps.now(),
    household_id: c.household_id,
    run_id: c.run_id,
    template_key: c.template_key,
    dedupe_key: c.dedupe_key,
    cooldown_days: c.cooldown_days,
    plan: c.plan,
    max_tier: c.max_tier,
    read,
    payload,
    r3,
    fill_failure: filled.fill_failure,
  });
}

async function syncRunCost(db: D1Database, run_id: string, at: string): Promise<void> {
  await db
    .prepare(`UPDATE mission_runs SET cost_usd = (SELECT COALESCE(SUM(cost_usd), 0) FROM llm_usage WHERE run_id = ?1), updated_at = ?2 WHERE run_id = ?1`)
    .bind(run_id, at)
    .run();
}

// ─── the state machine ──────────────────────────────────────────────────────

export async function runMission(step: StepLike, deps: MissionDeps, params: MissionRunParams): Promise<MissionOutcome> {
  const db = deps.env.DB;
  const { run_id, household_id } = params;

  // compile: stored plan, no model planning. Adopts a staged (R1_STAGED) card when the member taps it.
  const c = await step.do('compile', async (): Promise<Compiled> => {
    const run = await getRun(db, household_id, run_id);
    if (run === null) throw new Error(`mission run ${run_id} not found for household`);
    const tpl = await db
      .prepare('SELECT version, plan_json, max_tier, credits_cost FROM mission_templates WHERE template_key = ? AND version = ?')
      .bind(run.template_key, run.template_version)
      .first<{ version: number; plan_json: string; max_tier: MissionTier; credits_cost: number }>();
    if (tpl === null) throw new Error(`template ${run.template_key} v${run.template_version} not found`);
    const trigger_key = str(run.params['trigger_key']);
    const dedupe_key = str(run.params['dedupe_key']);
    const trigger = (await loadTriggers(db)).find((t) => t.trigger_key === trigger_key);
    const hh = await db.prepare('SELECT timezone FROM households WHERE household_id = ?').bind(household_id).first<{ timezone: string }>();
    const at = iso(deps);
    await transitionRun(db, household_id, run_id, 'COMPILED', at);
    await recordStep(db, run_id, STEP_NO.compile, { kind: 'read', tier: 'R0', status: 'ok', at });
    const claimIds = Array.isArray(run.params['claim_ids']) ? (run.params['claim_ids'] as unknown[]).filter((x): x is string => typeof x === 'string') : [];
    return {
      run_id,
      household_id,
      template_key: run.template_key,
      template_version: tpl.version,
      credits_cost: tpl.credits_cost,
      max_tier: tpl.max_tier,
      plan: parsePlan(tpl.plan_json),
      trigger_key,
      dedupe_key,
      cooldown_days: trigger?.cooldown_days ?? 14,
      timezone: hh?.timezone ?? 'America/New_York',
      expires_at: run.expires_at,
      staged_claim_ids: claimIds,
    };
  });

  if (trigger_missing(c) || !c.plan.some((s) => s.kind === 'approve')) {
    await step.do('cancel', async () => {
      await transitionRun(db, household_id, run_id, 'CANCELED', iso(deps), { failure: { stage: 'compile', reason: 'template_not_runnable' } });
    });
    return { status: 'canceled', reason: 'template_not_runnable' };
  }

  // debit_credit: fails (CANCELED, nothing refunded because nothing was taken) when the balance is below the cost.
  const debit = await step.do('debit_credit', async () => {
    const at = iso(deps);
    const res = await debitCredit(db, household_id, run_id, c.credits_cost, at);
    await recordStep(db, run_id, STEP_NO.debit_credit, { kind: 'read', tier: 'R0', status: res.ok ? 'ok' : 'failed', gate_name: 'credit_debit', gate_result: { cost: c.credits_cost, balance: res.balance }, at });
    if (!res.ok) await transitionRun(db, household_id, run_id, 'CANCELED', at, { failure: { stage: 'debit_credit', reason: 'insufficient_credits', balance: res.balance, cost: c.credits_cost } });
    return res;
  });
  if (!debit.ok) return { status: 'canceled', reason: 'insufficient_credits' };

  const systemError = async (stage: string, err: unknown): Promise<MissionOutcome> => {
    await step.do('system_error', async () => {
      const at = iso(deps);
      await refundCredit(db, household_id, run_id, at);
      await transitionRun(db, household_id, run_id, 'CANCELED', at, { failure: { stage, reason: 'system_error', error: err instanceof Error ? err.message.slice(0, 200) : 'error' } });
    });
    return { status: 'canceled', reason: 'system_error' };
  };

  const failGate = async (stage: 'r1' | 'r3', outcome: GateOutcome): Promise<MissionOutcome> => {
    await step.do('refund', async () => {
      const at = iso(deps);
      await refundCredit(db, household_id, run_id, at);
      await transitionRun(db, household_id, run_id, 'FAILED_GATE', at, { failure: { stage, gates: outcome.failed, results: outcome.results } });
    });
    return { status: 'failed_gate', stage, failed: outcome.failed };
  };

  let read: ReadContext;
  let filled: Filled;
  let gates: GateOutcome;
  let staged: Staged;
  try {
    // r0_read: facts_current, headroom and live free windows, re-derived now (nothing from the earlier staging is trusted).
    read = (
      await step.do('r0_read', { retries: { limit: 2, delay: '10 seconds', backoff: 'exponential' } }, async () => {
        const res = await readFacts(deps, c);
        const at = iso(deps);
        await transitionRun(db, household_id, run_id, 'R0_READ', at);
        await recordStep(db, run_id, STEP_NO.r0_read, { kind: 'read', tier: 'R0', status: 'ok', at });
        return res;
      })
    ).read;

    // r0_fill: the worker model writes assistive script text only (<90K tokens); card claims never come from a model.
    filled = await step.do('r0_fill', { retries: { limit: 2, delay: '10 seconds', backoff: 'exponential' } }, async () => {
      const res = await workerFill(deps, c, read.data);
      const at = iso(deps);
      await syncRunCost(db, run_id, at);
      await recordStep(db, run_id, STEP_NO.r0_fill, { kind: 'llm_worker', tier: 'R0', status: res.fill_failure === null ? 'ok' : 'failed', input_tokens: res.input_tokens, output_tokens: res.output_tokens, cost_usd: res.cost_usd, at });
      return res;
    });

    // gates: G1-G9, all evaluated. Pass moves R0_READ -> GATED; fail is handled below.
    gates = await step.do('gates', async () => {
      const payload = await buildPayload(db, c, read, filled);
      const out = await gateRun(deps, c, read, filled, payload, false);
      const at = iso(deps);
      await recordStep(db, run_id, STEP_NO.gates, { kind: 'gate', tier: 'R0', status: out.ok ? 'ok' : 'failed', gate_name: 'G1-G9', gate_result: out.results, at });
      if (out.ok) await transitionRun(db, household_id, run_id, 'GATED', at);
      return out;
    });
  } catch (err) {
    return systemError('r0', err);
  }
  if (!gates.ok) return failGate('r1', gates);

  try {
    // r1_stage: private. The card the member will see is frozen here and identified by its hash.
    staged = await step.do('r1_stage', async () => {
      const payload = await buildPayload(db, c, read, filled);
      const hash = await payloadHash(payload);
      const at = iso(deps);
      const expiresMs = c.expires_at === null ? NaN : Date.parse(c.expires_at) - deps.now().getTime();
      const timeout_ms = Number.isFinite(expiresMs)
        ? Math.min(MAX_APPROVAL_TIMEOUT_MS, Math.max(MIN_APPROVAL_TIMEOUT_MS, expiresMs))
        : DEFAULT_APPROVAL_TIMEOUT_MS;
      const gate_results = gates.results.map((g) => ({ gate: g.gate, ok: g.ok }));
      await transitionRun(db, household_id, run_id, 'R1_STAGED', at, {
        payload,
        payload_hash: hash,
        staged_at: at,
        gates: gate_results,
        link: null,
        failure: null,
      });
      await recordStep(db, run_id, STEP_NO.r1_stage, { kind: 'stage', tier: 'R1', status: 'ok', at });
      return { payload_hash: hash, timeout_ms, gate_results };
    });
  } catch (err) {
    return systemError('r1', err);
  }

  // notify: R2_AWAITING first (so a fast tap finds the run waiting), then a payload-less Web Push. A push failure never fails the run.
  await step.do('notify', async () => {
    const at = iso(deps);
    await transitionRun(db, household_id, run_id, 'R2_AWAITING', at);
    const push = await notifyHousehold({ db, fetch: deps.fetch, env: deps.env, now: deps.now }, household_id, 'card_ready');
    await recordStep(db, run_id, STEP_NO.notify, { kind: 'notify', tier: 'R1', status: 'ok', gate_result: push, at });
    return push;
  });

  let approval: { payload: ApprovalEvent } | null = null;
  try {
    approval = await step.waitForEvent<ApprovalEvent>('await_approval', { type: `approve:${run_id}`, timeout: staged.timeout_ms });
  } catch {
    approval = null;
  }
  if (approval === null) {
    await step.do('expire', async () => {
      await transitionRun(db, household_id, run_id, 'EXPIRED', iso(deps));
    });
    return { status: 'expired' };
  }
  const decision = approval.payload;
  if (decision.decision !== 'approve' || decision.payload_hash !== staged.payload_hash) {
    const reason = decision.decision !== 'approve' ? 'declined' : 'payload_hash_mismatch';
    await step.do('decline', async () => {
      const at = iso(deps);
      await transitionRun(db, household_id, run_id, 'DECLINED', at, { declined_reason: reason });
      await recordStep(db, run_id, STEP_NO.await_approval, { kind: 'approve', tier: 'R2', status: 'failed', gate_result: { reason }, at });
    });
    return { status: 'declined', reason };
  }

  // r3_regate: every gate again on fresh data, and the card must be exactly what was approved.
  const regate = await step.do('r3_regate', async (): Promise<GateOutcome> => {
    const at = iso(deps);
    await transitionRun(db, household_id, run_id, 'R2_APPROVED', at);
    await recordStep(db, run_id, STEP_NO.await_approval, { kind: 'approve', tier: 'R2', status: 'ok', at });
    const fresh = (await readFacts(deps, c)).read;
    const payload = await buildPayload(db, c, fresh, filled);
    const out = await gateRun(deps, c, fresh, filled, payload, true);
    if ((await payloadHash(payload)) !== staged.payload_hash) {
      out.ok = false;
      out.failed.push('G2');
      out.results.push({ gate: 'G2', name: 'freshness', ok: false, detail: 'the card changed after it was approved' });
    }
    await recordStep(db, run_id, STEP_NO.r3_regate, { kind: 'gate', tier: 'R3', status: out.ok ? 'ok' : 'failed', gate_name: 'G1-G9', gate_result: out.results, at });
    return out;
  });
  if (!regate.ok) return failGate('r3', regate);

  // A template whose top tier is R2 ends here: the member carries out the script themselves.
  if (c.max_tier !== 'R3') {
    await step.do('confirm', async () => {
      const at = iso(deps);
      await transitionRun(db, household_id, run_id, 'CONFIRMED', at);
      await recordStep(db, run_id, STEP_NO.confirm, { kind: 'wait', tier: 'R2', status: 'ok', at });
    });
    return { status: 'confirmed', link: null };
  }

  // r3_link: deep link only, affiliate-tagged where the domain has a tag and is not blocklisted. Never fetched.
  const link = await step.do('r3_link', async (): Promise<DeepLink> => {
    const payload = await buildPayload(db, c, (await readFacts(deps, c)).read, filled);
    if (payload.action.type !== 'deeplink') throw new Error('R3 reached without a deep link target');
    const { results } = await db.prepare('SELECT domain, reason FROM policy_blocklist').all<BlocklistEntry>();
    const built = buildDeepLink(
      { merchant_domain: payload.action.merchant_domain, path: payload.action.path, query: payload.action.query },
      parseAffiliateTags(deps.env.AFFILIATE_TAGS),
      (domain) => matchBlocklist(domain, results) !== null,
    );
    const at = iso(deps);
    await db
      .prepare('INSERT OR IGNORE INTO deeplinks (link_id, run_id, merchant_domain, url, affiliate_program, issued_at) VALUES (?, ?, ?, ?, ?, ?)')
      .bind(`lnk_${run_id}`, run_id, built.merchant_domain, built.url, built.affiliate_program, at)
      .run();
    await transitionRun(db, household_id, run_id, 'R3_LINK_ISSUED', at, { link: { ...built, link_id: `lnk_${run_id}`, issued_at: at } });
    await recordStep(db, run_id, STEP_NO.r3_link, { kind: 'deeplink', tier: 'R3', status: 'ok', at });
    return built;
  });

  await step.do('deliver_link', async () => {
    const push = await notifyHousehold({ db, fetch: deps.fetch, env: deps.env, now: deps.now }, household_id, 'link_ready');
    await recordStep(db, run_id, STEP_NO.deliver_link, { kind: 'notify', tier: 'R3', status: 'ok', gate_result: push, at: iso(deps) });
    return push;
  });

  let confirmed = true;
  try {
    await step.waitForEvent('await_confirmation', { type: `confirmed:${run_id}`, timeout: CONFIRMATION_TIMEOUT });
  } catch {
    confirmed = false;
  }
  if (!confirmed) {
    await step.do('unconfirmed', async () => {
      await transitionRun(db, household_id, run_id, 'R3_LINK_ISSUED', iso(deps), { unconfirmed_at: iso(deps) });
    });
    return { status: 'link_issued', link };
  }
  await step.do('confirm', async () => {
    const at = iso(deps);
    await transitionRun(db, household_id, run_id, 'CONFIRMED', at, { confirmed_at: at });
    await recordStep(db, run_id, STEP_NO.confirm, { kind: 'wait', tier: 'R3', status: 'ok', at });
  });
  return { status: 'confirmed', link };
}

function trigger_missing(c: Compiled): boolean {
  return c.trigger_key === '' || c.dedupe_key === '';
}
