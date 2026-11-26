/**
 * src/missions/engine.ts — standing triggers -> grounded, staged Mission Cards (CLAUDE.md §5.2, §7).
 *
 * A trigger is a stored read-only SELECT (migrations/0002_m3.sql). This module:
 *   - refuses to run any sql_condition that is not a single read-only SELECT using ?1 (assertSafeTriggerSql);
 *   - applies Gate G9 (idempotency): a (template, dedupe_key) is not staged again while an earlier run is still active
 *     or inside the trigger's cooldown_days — whatever state that run ended in, including DECLINED;
 *   - applies Gate G1 (grounding): every claim on the card must rest on facts that have at least one quote which is a
 *     verbatim substring of its episode's redacted text (re-verified here, not trusted from the writer);
 *   - applies Gate G7 (tax language) through buildCopy();
 *   - stages at most HOME_CAPACITY active cards per household, choosing a diverse set first (credit, fee, then the
 *     flagship / subscription card) and filling the rest by score = recoverable_cents * confidence * urgency.
 * Staged runs are R1_STAGED: private, nothing is sent, nothing is debited. Approval (R2) and links (R3) belong to MissionRunWF.
 */

import { buildCopy, UNLOCK_SCAN_COPY, type CardCopy } from './copy';

export const HOME_CAPACITY = 3;
export const ACTIVE_STATES = ['R1_STAGED', 'R2_AWAITING', 'R2_APPROVED', 'R3_LINK_ISSUED'] as const;
const STAGED_STATES = ['R1_STAGED', 'R2_AWAITING'] as const;
/** Evaluated only by the cold-start step, never by the standing-trigger step. */
export const FALLBACK_TRIGGERS: readonly string[] = ['T_OCCASION_PLAN'];
const MAX_ROWS_PER_TRIGGER = 50;

export class UnsafeTriggerSqlError extends Error {
  override readonly name = 'UnsafeTriggerSqlError';
}

const FORBIDDEN_SQL =
  /\b(insert|update|delete|drop|alter|create|attach|detach|pragma|replace|vacuum|reindex|analyze|begin|commit|rollback|savepoint|load_extension)\b/i;

/** A stored trigger must be ONE read-only SELECT / WITH...SELECT that binds ?1 (the household). */
export function assertSafeTriggerSql(sql: string): void {
  const stripped = sql.replace(/'(?:[^']|'')*'/g, "''").trim();
  if (!/^(select|with)\b/i.test(stripped)) throw new UnsafeTriggerSqlError('trigger SQL must start with SELECT or WITH');
  if (stripped.includes(';')) throw new UnsafeTriggerSqlError('trigger SQL must be a single statement');
  if (FORBIDDEN_SQL.test(stripped)) throw new UnsafeTriggerSqlError('trigger SQL must be read-only');
  if (!stripped.includes('?1')) throw new UnsafeTriggerSqlError('trigger SQL must be scoped to the household with ?1');
}

export interface TriggerDef {
  trigger_key: string;
  template_key: string;
  sql_condition: string;
  cooldown_days: number;
}

export interface ClaimRef {
  key: string;
  facts: Array<string | null>;
}

export interface Candidate {
  trigger_key: string;
  template_key: string;
  cooldown_days: number;
  dedupe_key: string;
  recoverable_cents: number;
  urgency: number;
  confidence: number;
  /** recoverable_cents * confidence * urgency, rounded. */
  score: number;
  claims: ClaimRef[];
  data: Record<string, unknown>;
}

export async function loadTriggers(db: D1Database): Promise<TriggerDef[]> {
  const { results } = await db.prepare('SELECT trigger_key, template_key, sql_condition, cooldown_days FROM triggers ORDER BY trigger_key').all<TriggerDef>();
  return results;
}

function parseJson<T>(raw: unknown, fallback: T): T {
  if (typeof raw !== 'string') return fallback;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

/** Runs one trigger for one household. Malformed rows are dropped and counted, never staged. */
export async function evaluateTrigger(
  db: D1Database,
  household_id: string,
  trigger: TriggerDef,
  now: Date,
): Promise<{ candidates: Candidate[]; malformed: number }> {
  assertSafeTriggerSql(trigger.sql_condition);
  const { results } = await db
    .prepare(trigger.sql_condition)
    .bind(household_id, now.toISOString(), now.toISOString().slice(0, 10))
    .all<Record<string, unknown>>();
  const candidates: Candidate[] = [];
  let malformed = 0;
  for (const row of results.slice(0, MAX_ROWS_PER_TRIGGER)) {
    const dedupe = row['dedupe_key'];
    const recoverable = Number(row['recoverable_cents']);
    const urgency = Number(row['urgency']);
    const confidence = Number(row['confidence']);
    const claims = parseJson<ClaimRef[]>(row['claims_json'], []);
    const data = parseJson<Record<string, unknown>>(row['data_json'], {});
    if (
      typeof dedupe !== 'string' || dedupe === '' || !Number.isFinite(recoverable) || recoverable < 0 ||
      !Number.isFinite(urgency) || !Number.isFinite(confidence) || !Array.isArray(claims)
    ) {
      malformed += 1;
      continue;
    }
    const u = Math.min(1, Math.max(0, urgency));
    const c = Math.min(1, Math.max(0, confidence));
    candidates.push({
      trigger_key: trigger.trigger_key,
      template_key: trigger.template_key,
      cooldown_days: trigger.cooldown_days,
      dedupe_key: dedupe,
      recoverable_cents: Math.round(recoverable),
      urgency: u,
      confidence: c,
      score: Math.round(recoverable * c * u),
      claims,
      data,
    });
  }
  return { candidates, malformed };
}

/** Gate G9. True when this (template, dedupe_key) must not be staged again right now. */
export async function isBlockedByCooldown(db: D1Database, household_id: string, c: Candidate, now: Date): Promise<boolean> {
  const cutoff = new Date(now.getTime() - c.cooldown_days * 86_400_000).toISOString();
  const row = await db
    .prepare(
      `SELECT 1 AS hit FROM mission_runs
        WHERE household_id = ?1 AND template_key = ?2 AND json_extract(params_json, '$.dedupe_key') = ?3
          AND (state IN (${ACTIVE_STATES.map((s) => `'${s}'`).join(', ')}) OR created_at > ?4)
        LIMIT 1`,
    )
    .bind(household_id, c.template_key, c.dedupe_key, cutoff)
    .first<{ hit: number }>();
  return row !== null;
}

const FAMILY_ORDER: ReadonlyArray<readonly string[]> = [['T_EXPIRING_CREDIT'], ['T_ANNUAL_FEE_ROI'], ['T_ANNIV_DINING', 'T_SUBSCRIPTION_CREEP']];

/**
 * Staging order: one card per family in the order credit -> annual fee -> (flagship, else subscription), each the best
 * by score; then everything left by score. The flagship always beats the subscription card for the third slot.
 */
export function orderCandidates(candidates: readonly Candidate[]): Candidate[] {
  const bestScore = (list: Candidate[]): Candidate | undefined => [...list].sort((a, b) => b.score - a.score || a.dedupe_key.localeCompare(b.dedupe_key))[0];
  const ordered: Candidate[] = [];
  for (const family of FAMILY_ORDER) {
    for (const key of family) {
      const pick = bestScore(candidates.filter((c) => c.trigger_key === key && !ordered.includes(c)));
      if (pick !== undefined) {
        ordered.push(pick);
        break;
      }
    }
  }
  const rest = candidates.filter((c) => !ordered.includes(c)).sort((a, b) => b.score - a.score || a.dedupe_key.localeCompare(b.dedupe_key));
  return [...ordered, ...rest];
}

// ─── grounding (G1) ─────────────────────────────────────────────────────────

export interface Evidence {
  fact_id: string;
  predicate: string;
  quote: string;
  episode_id: string;
  source: string;
  char_start: number;
  char_end: number;
}

export type GroundResult =
  | { ok: true; claims: Array<{ key: string; facts: string[] }>; evidence: Map<string, Evidence[]> }
  | { ok: false; reason: string };

const IN_CHUNK = 90;

async function chunked<T>(ids: readonly string[], fn: (group: string[]) => Promise<T[]>): Promise<T[]> {
  const out: T[] = [];
  for (let i = 0; i < ids.length; i += IN_CHUNK) out.push(...(await fn(ids.slice(i, i + IN_CHUNK))));
  return out;
}

/** Loads the verified quotes of `factIds`. A quote counts only if redacted_text.slice(start, end) === quote right now. */
export async function loadVerifiedEvidence(db: D1Database, household_id: string, factIds: readonly string[]): Promise<Map<string, Evidence[]>> {
  const out = new Map<string, Evidence[]>();
  if (factIds.length === 0) return out;
  const facts = await chunked(factIds, async (group) =>
    (
      await db
        .prepare(`SELECT fact_id, predicate FROM facts WHERE household_id = ? AND expired_at IS NULL AND fact_id IN (${group.map(() => '?').join(', ')})`)
        .bind(household_id, ...group)
        .all<{ fact_id: string; predicate: string }>()
    ).results,
  );
  const predicateOf = new Map(facts.map((f) => [f.fact_id, f.predicate]));
  const quotes = await chunked([...predicateOf.keys()], async (group) =>
    (
      await db
        .prepare(
          `SELECT fact_id, episode_id, quote, char_start, char_end FROM fact_quotes
            WHERE fact_id IN (${group.map(() => '?').join(', ')}) ORDER BY fact_id, char_start`,
        )
        .bind(...group)
        .all<{ fact_id: string; episode_id: string; quote: string; char_start: number; char_end: number }>()
    ).results,
  );
  const episodes = new Map<string, { text: string | null; status: string; source: string }>();
  const episodeIds = [...new Set(quotes.map((q) => q.episode_id))];
  for (const e of await chunked(episodeIds, async (group) =>
    (
      await db
        .prepare(`SELECT episode_id, redacted_text, redaction_status, source FROM episodes WHERE household_id = ? AND episode_id IN (${group.map(() => '?').join(', ')})`)
        .bind(household_id, ...group)
        .all<{ episode_id: string; redacted_text: string | null; redaction_status: string; source: string }>()
    ).results,
  )) {
    episodes.set(e.episode_id, { text: e.redacted_text, status: e.redaction_status, source: e.source });
  }
  for (const q of quotes) {
    const ep = episodes.get(q.episode_id);
    if (ep === undefined || ep.text === null || (ep.status !== 'clean' && ep.status !== 'redacted')) continue;
    if (ep.text.slice(q.char_start, q.char_end) !== q.quote) continue;
    const list = out.get(q.fact_id) ?? [];
    list.push({ fact_id: q.fact_id, predicate: predicateOf.get(q.fact_id) ?? '', quote: q.quote, episode_id: q.episode_id, source: ep.source, char_start: q.char_start, char_end: q.char_end });
    out.set(q.fact_id, list);
  }
  return out;
}

/** Gate G1: every claim needs >= 1 fact and every one of its facts needs >= 1 verified quote. */
export async function groundClaims(db: D1Database, household_id: string, claims: readonly ClaimRef[]): Promise<GroundResult> {
  if (claims.length === 0) return { ok: false, reason: 'no claims' };
  const ids: string[] = [];
  for (const c of claims) {
    if (!Array.isArray(c.facts) || c.facts.length === 0) return { ok: false, reason: `claim '${c.key}' has no supporting facts` };
    for (const f of c.facts) {
      if (typeof f !== 'string' || f === '') return { ok: false, reason: `claim '${c.key}' references a missing fact` };
      ids.push(f);
    }
  }
  const evidence = await loadVerifiedEvidence(db, household_id, [...new Set(ids)]);
  for (const c of claims) {
    for (const f of c.facts as string[]) {
      if ((evidence.get(f) ?? []).length === 0) return { ok: false, reason: `claim '${c.key}': fact ${f} has no verified quote` };
    }
  }
  return { ok: true, claims: claims.map((c) => ({ key: c.key, facts: c.facts as string[] })), evidence };
}

// ─── staging ────────────────────────────────────────────────────────────────

export async function expireStaleRuns(db: D1Database, household_id: string, now: Date): Promise<number> {
  const iso = now.toISOString();
  const res = await db
    .prepare(
      `UPDATE mission_runs SET state = 'EXPIRED', updated_at = ?1
        WHERE household_id = ?2 AND state IN (${STAGED_STATES.map((s) => `'${s}'`).join(', ')}) AND expires_at IS NOT NULL AND expires_at < ?1`,
    )
    .bind(iso, household_id)
    .run();
  return res.meta.changes ?? 0;
}

/** Cards that hold one of the household's HOME_CAPACITY slots: staged, in flight in MissionRunWF, or approved and awaiting their link. */
export const CAPACITY_STATES = ['COMPILED', 'R0_READ', 'GATED', 'R1_STAGED', 'R2_AWAITING', 'R2_APPROVED'] as const;

export async function activeStagedCount(db: D1Database, household_id: string): Promise<number> {
  const row = await db
    .prepare(`SELECT COUNT(*) AS n FROM mission_runs WHERE household_id = ? AND state IN (${CAPACITY_STATES.map((s) => `'${s}'`).join(', ')})`)
    .bind(household_id)
    .first<{ n: number }>();
  return row?.n ?? 0;
}

const newRunId = (): string => `run_${crypto.randomUUID().replace(/-/g, '')}`;
const newId = (prefix: string): string => `${prefix}_${crypto.randomUUID().replace(/-/g, '')}`;

function expiresAtOf(data: Record<string, unknown>): string | null {
  const on = data['expires_on'];
  return typeof on === 'string' && /^\d{4}-\d{2}-\d{2}/.test(on) ? `${on.slice(0, 10)}T23:59:59.999Z` : null;
}

/** Inserts the run, its claims, their support and the gate steps in one batch. */
async function insertRun(
  db: D1Database,
  household_id: string,
  o: {
    template_key: string;
    trigger_key: string;
    dedupe_key: string;
    copy: CardCopy;
    recoverable_cents: number;
    urgency: number;
    confidence: number;
    score: number;
    data: Record<string, unknown>;
    claims: Array<{ key: string; facts: string[] }>;
    now: Date;
  },
): Promise<string> {
  const run_id = newRunId();
  const iso = o.now.toISOString();
  const claimIds = o.claims.map(() => newId('clm'));
  const params = {
    trigger_key: o.trigger_key,
    dedupe_key: o.dedupe_key,
    title: o.copy.title,
    why: o.copy.why,
    r2_label: o.copy.r2_label,
    recoverable_cents: o.recoverable_cents,
    urgency: o.urgency,
    confidence: o.confidence,
    score: o.score,
    unconfirmed: o.copy.unconfirmed,
    data: o.data,
    claim_ids: claimIds,
  };
  const statements: D1PreparedStatement[] = [
    db
      .prepare(
        `INSERT INTO mission_runs (run_id, household_id, template_key, template_version, origin, params_json, state, expires_at, cost_usd, created_at, updated_at)
         SELECT ?, ?, template_key, version, 'trigger', ?, 'R1_STAGED', ?, 0, ?, ? FROM mission_templates WHERE template_key = ? ORDER BY version DESC LIMIT 1`,
      )
      .bind(run_id, household_id, JSON.stringify(params), expiresAtOf(o.data), iso, iso, o.template_key),
  ];
  for (const [i, claim] of o.claims.entries()) {
    const claim_id = claimIds[i]!;
    statements.push(
      db
        .prepare(`INSERT INTO claims (claim_id, household_id, run_id, text, status, created_at) VALUES (?, ?, ?, ?, 'grounded', ?)`)
        .bind(claim_id, household_id, run_id, o.copy.claims[claim.key] ?? claim.key, iso),
    );
    for (const fact_id of new Set(claim.facts)) {
      statements.push(db.prepare('INSERT INTO claim_support (claim_id, fact_id) VALUES (?, ?)').bind(claim_id, fact_id));
    }
  }
  const step = (no: number, kind: string, tier: string, gate: string | null, result: unknown): D1PreparedStatement =>
    db
      .prepare(
        `INSERT INTO steps (run_id, step_no, kind, tier, status, gate_name, gate_result_json, started_at, finished_at)
         VALUES (?, ?, ?, ?, 'ok', ?, ?, ?, ?)`,
      )
      .bind(run_id, no, kind, tier, gate, result === null ? null : JSON.stringify(result), iso, iso);
  statements.push(
    step(1, 'read', 'R0', null, null),
    step(2, 'gate', 'R0', 'G1_grounding', { claims: o.claims.length }),
    step(3, 'gate', 'R0', 'G7_tax_language', { ok: true }),
    step(4, 'stage', 'R1', null, null),
  );
  await db.batch(statements);
  return run_id;
}

export type StageOutcome = { staged: true; run_id: string } | { staged: false; reason: string };

/** Gates G9 -> G1 -> G7, then stage. Writes an audit row when a candidate is rejected for grounding. */
export async function stageCandidate(db: D1Database, household_id: string, c: Candidate, now: Date): Promise<StageOutcome> {
  if (await isBlockedByCooldown(db, household_id, c, now)) return { staged: false, reason: 'cooldown' };
  const grounded = await groundClaims(db, household_id, c.claims);
  if (!grounded.ok) {
    await db
      .prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'mission-engine', 'mission_ungrounded', ?, ?)`)
      .bind(newId('aud'), household_id, JSON.stringify({ trigger_key: c.trigger_key, dedupe_key: c.dedupe_key, reason: grounded.reason }), now.toISOString())
      .run();
    return { staged: false, reason: 'ungrounded' };
  }
  const copy = buildCopy(c.trigger_key, c.data);
  const run_id = await insertRun(db, household_id, { ...c, copy, claims: grounded.claims, now });
  return { staged: true, run_id };
}

/** The Section 7 placeholder card: no claims, no evidence, no money. */
export async function stageUnlockScan(db: D1Database, household_id: string, now: Date): Promise<string> {
  return insertRun(db, household_id, {
    template_key: 'unlock_scan',
    trigger_key: 'COLD_START_UNLOCK_SCAN',
    dedupe_key: 'unlock_scan',
    copy: UNLOCK_SCAN_COPY,
    recoverable_cents: 0,
    urgency: 0,
    confidence: 1,
    score: 0,
    data: {},
    claims: [],
    now,
  });
}
