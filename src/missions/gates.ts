/**
 * src/missions/gates.ts — the deterministic validation gates G1-G9 of CLAUDE.md §5.2. All of them must pass. They run
 * at R1 staging and again at R3 (before a link is issued), and ALL of them are evaluated every time (no short-circuit), so a
 * failed run records everything that was wrong, not just the first thing.
 *
 *   G1 grounding    every claim maps to >= 1 fact with a verified quote
 *   G2 freshness    the trigger still fires; every claim keeps a current fact; money facts and spend data <= 35 days old;
 *                   calendar data fetched within the last 15 minutes
 *   G3 headroom     price + 10% <= the envelope's headroom (when the card spends money)
 *   G4 calendar     the proposed time range lies inside a free window fetched in THIS run
 *   G5 policy       nothing this run fetches is on policy_blocklist, and no egress attempt was blocked
 *   G6 tier         every plan step respects max_tier, R3 steps are deep links, and an R3 run has a link to issue
 *   G7 tax-language no tax characterization anywhere in the card or its script
 *   G8 cost         run cost <= $0.25 per credit charged and no model call above 90,000 input tokens
 *   G9 idempotency  no other open run, or run inside the cooldown, for the same template and dedupe_key
 */

import { groundClaims, type ClaimRef } from './engine';
import { assertNoTaxLanguage, TaxLanguageError, type CardCopy } from './copy';
import { matchBlocklist } from '../net/egress';
import { WORKER_INPUT_TOKEN_CEILING } from '../llm/gateway';
import { isTierAllowed, type MissionTier } from '../types';

/** Gate G8: model spend a run may cost, per credit it charges (1 credit = $0.25, 3 = $0.75, 5 = $1.25). */
export const COST_CAP_USD_PER_CREDIT = 0.25;
export const maxRunCostUsd = (creditCost: number): number => COST_CAP_USD_PER_CREDIT * Math.max(1, creditCost);
export const MONEY_MAX_AGE_DAYS = 35;
export const CALENDAR_MAX_AGE_MS = 15 * 60_000;
/** G3: the price plus 10% must fit. Compared as price * 11 <= headroom * 10 so no float rounding decides it. */
export const HEADROOM_MARGIN_NUM = 11;
export const HEADROOM_MARGIN_DEN = 10;
/** Hosts a mission may contact: Google Calendar, the model vendors, and nothing else. */
export const MISSION_FETCH_HOSTS = ['www.googleapis.com', 'oauth2.googleapis.com', 'api.openai.com', 'api.anthropic.com'] as const;

export interface PlanStep {
  id: string;
  kind: string;
  tier: MissionTier;
  worker_prompt_id: string | null;
  max_input_tokens: number;
}

export interface FactState {
  fact_id: string;
  predicate: string;
  value_type: string | null;
  current: boolean;
  last_support: string | null;
}

export interface FreshWindow {
  date: string;
  start: string;
  end: string;
}

export interface ReadContext {
  /** Did the trigger still produce this card's dedupe_key when it was re-evaluated? */
  candidate_found: boolean;
  data: Record<string, unknown>;
  claims: ClaimRef[];
  facts: FactState[];
  headroom_cents: number | null;
  envelope_key: string | null;
  /** Newest episode received_at among the household's transactions (the freshness of the spend data). */
  spend_data_at: string | null;
  calendar: { required: boolean; fetched_at: string | null; windows: FreshWindow[]; error: string | null };
  fetched_at: string;
}

export interface CardPayload {
  version: 1;
  run_id: string;
  template_key: string;
  trigger_key: string;
  dedupe_key: string;
  kind: 'use_credit' | 'retention_script' | 'dinner_plan' | 'cancel_script' | 'plan' | 'drop_alarm' | 'flight_deal';
  copy: CardCopy;
  /** Model-written script text (retention / cancel calls). Assistive only, never a claim. */
  script: string | null;
  price_cents: number;
  envelope_key: string | null;
  proposed_range: { start: string; end: string } | null;
  action: { type: 'deeplink'; merchant_domain: string; path: string; query: Record<string, string> } | { type: 'none' };
  expires_on: string | null;
}

export interface GateResult {
  gate: 'G1' | 'G2' | 'G3' | 'G4' | 'G5' | 'G6' | 'G7' | 'G8' | 'G9';
  name: string;
  ok: boolean;
  detail: string;
}

export interface GateOutcome {
  ok: boolean;
  failed: string[];
  results: GateResult[];
}

export interface GateInput {
  db: D1Database;
  now: Date;
  household_id: string;
  run_id: string;
  template_key: string;
  dedupe_key: string;
  cooldown_days: number;
  plan: readonly PlanStep[];
  max_tier: MissionTier;
  read: ReadContext;
  payload: CardPayload;
  /** True for the re-gate that precedes issuing the deep link. */
  r3: boolean;
  /** Credits this run charges; Gate G8's cost cap scales with it. */
  credit_cost: number;
  /** Set when the worker step could not run inside its token budget (the call was refused, so llm_usage has no row for it). */
  fill_failure?: string | null;
}

const OPEN_STATES = ['COMPILED', 'R0_READ', 'GATED', 'R1_STAGED', 'R2_AWAITING', 'R2_APPROVED', 'R3_LINK_ISSUED'];

const result = (gate: GateResult['gate'], name: string, ok: boolean, detail: string): GateResult => ({ gate, name, ok, detail });

async function g1(i: GateInput): Promise<GateResult> {
  const grounded = await groundClaims(i.db, i.household_id, i.read.claims);
  return result('G1', 'grounding', grounded.ok, grounded.ok ? `${i.read.claims.length} claims, every supporting fact has a verified quote` : grounded.reason);
}

function g2(i: GateInput): GateResult {
  const problems: string[] = [];
  const nowMs = i.now.getTime();
  if (!i.read.candidate_found) problems.push('the conditions behind this card no longer hold');
  const byId = new Map(i.read.facts.map((f) => [f.fact_id, f]));
  for (const claim of i.read.claims) {
    const supporting = claim.facts.map((f) => (f === null ? undefined : byId.get(f)));
    if (!supporting.some((f) => f?.current === true)) problems.push(`claim '${claim.key}' has no current supporting fact`);
  }
  for (const f of i.read.facts) {
    if (f.value_type === 'money') {
      const at = f.last_support === null ? NaN : Date.parse(f.last_support);
      if (!(nowMs - at <= MONEY_MAX_AGE_DAYS * 86_400_000)) problems.push(`money fact ${f.predicate} was last confirmed more than ${MONEY_MAX_AGE_DAYS} days ago`);
    }
  }
  if (i.payload.envelope_key !== null) {
    const at = i.read.spend_data_at === null ? NaN : Date.parse(i.read.spend_data_at);
    if (!(nowMs - at <= MONEY_MAX_AGE_DAYS * 86_400_000)) problems.push(`spend data is older than ${MONEY_MAX_AGE_DAYS} days or missing`);
  }
  if (i.read.calendar.required) {
    const at = i.read.calendar.fetched_at === null ? NaN : Date.parse(i.read.calendar.fetched_at);
    if (!(nowMs - at <= CALENDAR_MAX_AGE_MS)) problems.push(i.read.calendar.error ?? 'calendar was not fetched in the last 15 minutes');
  }
  return result('G2', 'freshness', problems.length === 0, problems.length === 0 ? 'supporting data is current' : problems.join('; '));
}

/** Gate G3's rule, shared with the price watch's pre-check: price + 10% must fit in the headroom. */
export function fitsHeadroom(priceCents: number, headroomCents: number): boolean {
  return priceCents * HEADROOM_MARGIN_NUM <= headroomCents * HEADROOM_MARGIN_DEN;
}

function g3(i: GateInput): GateResult {
  const price = i.payload.price_cents;
  if (price <= 0 || i.payload.envelope_key === null) return result('G3', 'headroom', true, 'this card spends no money');
  const headroom = i.read.headroom_cents;
  if (headroom === null) return result('G3', 'headroom', false, `no headroom is known for envelope ${i.payload.envelope_key}`);
  const ok = fitsHeadroom(price, headroom);
  return result('G3', 'headroom', ok, `price ${price} + 10% ${ok ? 'fits' : 'does not fit'} in headroom ${headroom}`);
}

function g4(i: GateInput): GateResult {
  const range = i.payload.proposed_range;
  if (range === null) {
    return i.read.calendar.required
      ? result('G4', 'calendar', false, 'this card depends on a free window but proposes no time')
      : result('G4', 'calendar', true, 'this card proposes no time');
  }
  const start = Date.parse(range.start);
  const end = Date.parse(range.end);
  const inside = i.read.calendar.windows.some((w) => Date.parse(w.start) <= start && end <= Date.parse(w.end));
  return result('G4', 'calendar', inside && i.read.calendar.fetched_at !== null, inside ? 'the proposed time is inside a free window fetched this run' : 'the proposed time is not inside a free window fetched this run');
}

async function g5(i: GateInput): Promise<GateResult> {
  const { results } = await i.db.prepare('SELECT domain, reason FROM policy_blocklist').all<{ domain: string; reason: string }>();
  const hit = MISSION_FETCH_HOSTS.map((h) => ({ h, e: matchBlocklist(h, results) })).find((x) => x.e !== null);
  if (hit !== undefined) return result('G5', 'policy', false, `${hit.h} is on policy_blocklist`);
  const blocked = await i.db
    .prepare(`SELECT COUNT(*) AS n FROM audit_log WHERE household_id = ? AND action = 'egress_blocked' AND json_extract(detail_json, '$.run_id') = ?`)
    .bind(i.household_id, i.run_id)
    .first<{ n: number }>();
  if ((blocked?.n ?? 0) > 0) return result('G5', 'policy', false, `${blocked!.n} fetch attempt(s) to blocklisted domains were blocked during this run`);
  return result('G5', 'policy', true, 'no blocklisted domain was contacted');
}

function g6(i: GateInput): GateResult {
  const problems: string[] = [];
  for (const s of i.plan) {
    if (!isTierAllowed(s.tier, i.max_tier)) problems.push(`step '${s.id}' is ${s.tier}, above the template's max tier ${i.max_tier}`);
    if (s.tier === 'R3' && s.kind !== 'deeplink') problems.push(`step '${s.id}' is R3 but is a ${s.kind}, not a deep link`);
  }
  if (i.r3 && i.max_tier === 'R3' && i.payload.action.type !== 'deeplink') problems.push('R3 needs a deep link target and this card has none');
  return result('G6', 'tier', problems.length === 0, problems.length === 0 ? `plan stays within ${i.max_tier}; R3 is a deep link only` : problems.join('; '));
}

function g7(i: GateInput): GateResult {
  const p = i.payload;
  try {
    assertNoTaxLanguage(p.copy.title, p.copy.why, p.copy.r2_label, ...Object.values(p.copy.claims), ...(p.script === null ? [] : [p.script]));
    return result('G7', 'tax-language', true, 'no tax characterization');
  } catch (err) {
    return result('G7', 'tax-language', false, err instanceof TaxLanguageError ? err.message : 'tax-language check failed');
  }
}

async function g8(i: GateInput): Promise<GateResult> {
  const row = await i.db
    .prepare('SELECT COALESCE(SUM(cost_usd), 0) AS cost, COALESCE(MAX(input_tokens), 0) AS max_in FROM llm_usage WHERE run_id = ?')
    .bind(i.run_id)
    .first<{ cost: number; max_in: number }>();
  const cost = row?.cost ?? 0;
  const maxIn = row?.max_in ?? 0;
  const failure = i.fill_failure ?? null;
  const cap = maxRunCostUsd(i.credit_cost);
  const ok = cost <= cap && maxIn <= WORKER_INPUT_TOKEN_CEILING && failure === null;
  return result(
    'G8',
    'cost',
    ok,
    `cost $${cost.toFixed(4)} (cap $${cap.toFixed(2)} for ${i.credit_cost} credit${i.credit_cost === 1 ? '' : 's'}), largest call ${maxIn} input tokens (cap ${WORKER_INPUT_TOKEN_CEILING})${failure === null ? '' : `; ${failure}`}`,
  );
}

async function g9(i: GateInput): Promise<GateResult> {
  const cutoff = new Date(i.now.getTime() - i.cooldown_days * 86_400_000).toISOString();
  const dup = await i.db
    .prepare(
      `SELECT run_id FROM mission_runs
        WHERE household_id = ?1 AND template_key = ?2 AND run_id <> ?3 AND json_extract(params_json, '$.dedupe_key') = ?4
          AND (state IN (${OPEN_STATES.map((s) => `'${s}'`).join(', ')}) OR created_at > ?5)
        LIMIT 1`,
    )
    .bind(i.household_id, i.template_key, i.run_id, i.dedupe_key, cutoff)
    .first<{ run_id: string }>();
  return result('G9', 'idempotency', dup === null, dup === null ? 'no other run for this card is open or inside its cooldown' : `run ${dup.run_id} already covers this card`);
}

export async function runGates(i: GateInput): Promise<GateOutcome> {
  const results = [await g1(i), g2(i), g3(i), g4(i), await g5(i), g6(i), g7(i), await g8(i), await g9(i)];
  const failed = results.filter((r) => !r.ok).map((r) => r.gate);
  return { ok: failed.length === 0, failed, results };
}
