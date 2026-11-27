/**
 * src/missions/triggers.ts — evaluating standing triggers and staging their cards, shared by NightlySweepWF and the post-ingest step of IngestEmailWF,
 * plus the three Milestone 6 life-admin triggers.
 *
 *   T_CLOSING_RETURN_OR_WARRANTY   return_deadline (1 credit)   a return window closes within 5 days on an item of $40 or more
 *                                  warranty_claim  (3 credits)  a product warranty expires within 30 days, or the member asks for the packet
 *   T_PRICE_DROP_OR_DELAY_CLAIM    price_drop_claim (3 credits) a price drop of $15+ inside the return/adjustment window of a purchase
 *                                  delay_claim      (3 credits) a flight delay or cancellation of 180+ minutes with the booking on record
 *   T_FAMILY_EVENT_AUTOPILOT       family_event     (1 credit)  an event whose registration opens today or later, with its checklist
 *
 * The conditions are stored SELECTs (migrations/0004_milestone6.sql) run by evaluateTrigger(); each card is grounded (Gate G1) in verbatim quotes,
 * deduplicated (Gate G9), and staged R1 — private, nothing sent or debited. Credits are debited by MissionRunWF when the member taps, weighted by the
 * template's credits_cost (src/billing/credits.ts), and Gate G8 caps the run's model spend at $0.25 per credit.
 */

import { HOME_CAPACITY, UnsafeTriggerSqlError, activeStagedCount, evaluateTrigger, isBlockedByCooldown, loadTriggers, orderCandidates, stageCandidate, type Candidate } from './engine';

export const M6_TRIGGER_KEYS = ['T_CLOSING_RETURN_OR_WARRANTY', 'T_PRICE_DROP_OR_DELAY_CLAIM', 'T_FAMILY_EVENT_AUTOPILOT'] as const;

/** Credits each M6 template charges. migrations/0004 stores the same numbers; the test suite asserts they agree. */
export const M6_TEMPLATE_CREDITS: Readonly<Record<string, number>> = {
  return_deadline: 1,
  warranty_claim: 3,
  price_drop_claim: 3,
  delay_claim: 3,
  family_event: 1,
};

export interface StagedCard {
  trigger_key: string;
  run_id: string;
  score: number;
}

const newAudit = (): string => `aud_${crypto.randomUUID().replace(/-/g, '')}`;

/** Evaluates the selected triggers and returns the candidates that are not blocked by cooldown or an active duplicate. */
export async function eligibleCandidates(
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

/** Stages candidates in order until `capacity` cards are staged. Counts those rejected for lacking grounding. */
export async function stageInOrder(
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

export interface PostIngestResult {
  evaluated: number;
  suppressed: number;
  ungrounded: number;
  staged: StagedCard[];
}

/**
 * Right after an email's facts are written: evaluate only the M6 triggers and stage what is new, within the household's free Home slots. The
 * nightly sweep evaluates the same triggers again, so a card this misses (Home was full) is picked up at 03:00 or when a slot frees.
 */
export async function stageAfterIngest(db: D1Database, household_id: string, now: Date): Promise<PostIngestResult> {
  const empty: PostIngestResult = { evaluated: 0, suppressed: 0, ungrounded: 0, staged: [] };
  const capacity = HOME_CAPACITY - (await activeStagedCount(db, household_id));
  if (capacity <= 0) return empty;
  const { eligible, evaluated, suppressed } = await eligibleCandidates(db, household_id, (k) => (M6_TRIGGER_KEYS as readonly string[]).includes(k), now);
  const { staged, ungrounded } = await stageInOrder(db, household_id, orderCandidates(eligible), capacity, now);
  return { evaluated, suppressed, ungrounded, staged };
}

export type WarrantyClaimOutcome = { ok: true; run_id: string } | { ok: false; reason: 'no_warranty_on_record' | 'already_requested' | 'ungrounded' };

/**
 * The warranty claim-packet candidate for one product: its newest unexpired warranty fact, whether or not the 30-day trigger would surface it today.
 * Staging uses it for a member's request; MissionRunWF uses it to re-read such a card at run time (Gate G2's "the conditions still hold"), since the
 * trigger itself would not return a warranty that ends in six months.
 */
export async function warrantyCandidate(db: D1Database, household_id: string, product_entity_id: string, now: Date): Promise<Candidate | null> {
  const today = now.toISOString().slice(0, 10);
  const row = await db
    .prepare(
      `SELECT w.fact_id, w.subject_id, w.value_text AS expires_on, e.canonical_name AS product,
              CAST(julianday(w.value_text) - julianday(?3) AS INTEGER) AS days_left,
              (SELECT s.value_text FROM facts_current s WHERE s.household_id = w.household_id AND s.subject_id = w.subject_id AND s.predicate = 'serial_number' LIMIT 1) AS serial
         FROM facts_current w JOIN entities e ON e.entity_id = w.subject_id
        WHERE w.household_id = ?1 AND w.subject_id = ?2 AND w.predicate = 'warranty_expires_on' AND w.value_text >= ?3
        ORDER BY w.value_text DESC, w.fact_id LIMIT 1`,
    )
    .bind(household_id, product_entity_id, today)
    .first<{ fact_id: string; subject_id: string; expires_on: string; product: string; days_left: number; serial: string | null }>();
  if (row === null) return null;
  return {
    trigger_key: 'T_CLOSING_RETURN_OR_WARRANTY',
    template_key: 'warranty_claim',
    cooldown_days: 30,
    dedupe_key: `warranty|${row.subject_id}|${row.expires_on}`,
    recoverable_cents: 0,
    urgency: 1,
    confidence: 1,
    score: 0,
    claims: [{ key: 'warranty', facts: [row.fact_id] }],
    data: { kind: 'warranty', product: row.product, expires_on: row.expires_on, days_left: row.days_left, serial: row.serial },
  };
}

/**
 * A member asks for a warranty claim packet for a product before its warranty ends. It stages the same 3-credit card the trigger would, under the
 * same dedupe_key (so the two can never both exist), with origin 'user'. Nothing is debited until the member taps the staged card.
 */
export async function requestWarrantyClaim(db: D1Database, household_id: string, product_entity_id: string, now: Date): Promise<WarrantyClaimOutcome> {
  const candidate = await warrantyCandidate(db, household_id, product_entity_id, now);
  if (candidate === null) return { ok: false, reason: 'no_warranty_on_record' };
  const outcome = await stageCandidate(db, household_id, candidate, now, { origin: 'user' });
  if (outcome.staged) return { ok: true, run_id: outcome.run_id };
  return { ok: false, reason: outcome.reason === 'ungrounded' ? 'ungrounded' : 'already_requested' };
}
