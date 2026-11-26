/**
 * src/maintenance/sweepPipeline.ts — NightlySweepWF's logic (CLAUDE.md §5): decay -> recompute -> triggers -> cold start.
 *
 *   decay       close facts past their predicate's decay_days; log 'decay_invalidate'
 *   recompute   credit_periods for the current periods (a card with no history becomes 'unconfirmed') and envelope targets,
 *               so envelope_headroom is current
 *   triggers    run the standing SQL triggers; respect cooldown_days (G9); stage up to 3 active cards, diversity first
 *   cold_start  Section 7 guarantee: fill the remaining slots (occasion plan, then the "forward receipts" card) so a new
 *               household with only Calendar + Cards (or a CSV) is never left with an empty Home
 *
 * Step names are fixed: 'decay', 'recompute', 'triggers', 'cold_start'.
 */

import { recomputeCreditPeriods, recomputeEnvelopes } from '../ingest/spend';
import { DEFAULT_STEP_CONFIG, type StepLike } from '../ingest/steps';
import type { NightlySweepParams } from '../types';
import { decayFacts } from './decay';
import {
  FALLBACK_TRIGGERS,
  HOME_CAPACITY,
  UnsafeTriggerSqlError,
  activeStagedCount,
  evaluateTrigger,
  expireStaleRuns,
  isBlockedByCooldown,
  loadTriggers,
  orderCandidates,
  stageCandidate,
  stageUnlockScan,
  type Candidate,
} from '../missions/engine';

export interface SweepDeps {
  db: D1Database;
  /** The sweep's "now". In a Workflow this is the instance timestamp, so replays see the same value. */
  now: () => Date;
}

export interface StagedCard {
  trigger_key: string;
  run_id: string;
  score: number;
}

export interface SweepResult {
  decay: { closed: number; examined: number };
  recompute: { credit_periods: number; envelopes: number };
  triggers: { evaluated: number; suppressed: number; ungrounded: number; staged: StagedCard[]; expired: number };
  cold_start: { staged: StagedCard[]; unlock_scan: boolean };
}

const newAudit = (): string => `aud_${crypto.randomUUID().replace(/-/g, '')}`;

/** Evaluates the given triggers and returns the candidates that are not blocked by cooldown or an active duplicate. */
async function eligibleCandidates(
  db: D1Database,
  household_id: string,
  keys: (key: string) => boolean,
  now: Date,
): Promise<{ eligible: Candidate[]; evaluated: number; suppressed: number }> {
  const eligible: Candidate[] = [];
  let evaluated = 0;
  let suppressed = 0;
  for (const trigger of (await loadTriggers(db)).filter((t) => keys(t.trigger_key))) {
    let found: Candidate[];
    try {
      found = (await evaluateTrigger(db, household_id, trigger, now)).candidates;
    } catch (err) {
      if (!(err instanceof UnsafeTriggerSqlError)) throw err;
      await db
        .prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'mission-engine', 'trigger_rejected', ?, ?)`)
        .bind(newAudit(), household_id, JSON.stringify({ trigger_key: trigger.trigger_key, reason: err.message }), now.toISOString())
        .run();
      continue;
    }
    evaluated += found.length;
    for (const c of found) {
      if (await isBlockedByCooldown(db, household_id, c, now)) suppressed += 1;
      else eligible.push(c);
    }
  }
  return { eligible, evaluated, suppressed };
}

async function stageInOrder(
  db: D1Database,
  household_id: string,
  ordered: readonly Candidate[],
  capacity: number,
  now: Date,
): Promise<{ staged: StagedCard[]; ungrounded: number }> {
  const staged: StagedCard[] = [];
  let ungrounded = 0;
  for (const c of ordered) {
    if (staged.length >= capacity) break;
    const outcome = await stageCandidate(db, household_id, c, now);
    if (outcome.staged) staged.push({ trigger_key: c.trigger_key, run_id: outcome.run_id, score: c.score });
    else if (outcome.reason === 'ungrounded') ungrounded += 1;
  }
  return { staged, ungrounded };
}

export async function runNightlySweep(deps: SweepDeps, params: NightlySweepParams, step: StepLike): Promise<SweepResult> {
  const { db, now } = deps;
  const household_id = params.household_id;

  const decay = await step.do('decay', DEFAULT_STEP_CONFIG, () => decayFacts(db, household_id, now()));

  const recompute = await step.do('recompute', DEFAULT_STEP_CONFIG, async () => {
    const today = now().toISOString().slice(0, 10);
    const credit = await recomputeCreditPeriods(db, household_id, { from: today, to: today }, now());
    const envelopes = await recomputeEnvelopes(db, household_id, now());
    return { credit_periods: credit.updated, envelopes: envelopes.updated };
  });

  const triggers = await step.do('triggers', DEFAULT_STEP_CONFIG, async () => {
    const at = now();
    const expired = await expireStaleRuns(db, household_id, at);
    const capacity = HOME_CAPACITY - (await activeStagedCount(db, household_id));
    const { eligible, evaluated, suppressed } = await eligibleCandidates(db, household_id, (k) => !FALLBACK_TRIGGERS.includes(k), at);
    const { staged, ungrounded } = capacity > 0 ? await stageInOrder(db, household_id, orderCandidates(eligible), capacity, at) : { staged: [], ungrounded: 0 };
    return { evaluated, suppressed, ungrounded, staged, expired };
  });

  const cold_start = await step.do('cold_start', DEFAULT_STEP_CONFIG, async () => {
    const at = now();
    let capacity = HOME_CAPACITY - (await activeStagedCount(db, household_id));
    const out = { staged: [] as StagedCard[], unlock_scan: false };
    if (capacity <= 0) return out;

    const { eligible } = await eligibleCandidates(db, household_id, (k) => FALLBACK_TRIGGERS.includes(k), at);
    // Do not plan the same occasion twice: skip anything an active card already covers.
    const fresh: Candidate[] = [];
    for (const c of eligible) {
      const dup = await db
        .prepare(
          `SELECT 1 AS hit FROM mission_runs WHERE household_id = ? AND state IN ('R1_STAGED', 'R2_AWAITING') AND json_extract(params_json, '$.dedupe_key') = ? LIMIT 1`,
        )
        .bind(household_id, c.dedupe_key)
        .first<{ hit: number }>();
      if (dup === null) fresh.push(c);
    }
    const { staged } = await stageInOrder(db, household_id, orderCandidates(fresh), capacity, at);
    out.staged = staged;
    capacity -= staged.length;

    if (capacity > 0) {
      const hasSpend = await db.prepare('SELECT 1 AS hit FROM transactions WHERE household_id = ? LIMIT 1').bind(household_id).first<{ hit: number }>();
      const recent = await db
        .prepare(`SELECT 1 AS hit FROM mission_runs WHERE household_id = ? AND template_key = 'unlock_scan' AND created_at > ? LIMIT 1`)
        .bind(household_id, new Date(at.getTime() - 30 * 86_400_000).toISOString())
        .first<{ hit: number }>();
      if (hasSpend === null && recent === null) {
        await stageUnlockScan(db, household_id, at);
        out.unlock_scan = true;
      }
    }
    return out;
  });

  return { decay, recompute, triggers, cold_start };
}
