/**
 * src/receipt/assemble.ts — the weekly Life Receipt, assembled from the household's own record. No model writes any of it: every line is
 * built from stored facts, runs and counts, and every line that asserts something about the household is a CLAIM that names the facts
 * it rests on, so Gate G1 can check it against verbatim quotes (src/receipt/pipeline.ts).
 *
 *   1 Found             money on the table right now: expiring card credits, creeping subscriptions, closing return windows
 *   2 Done              this week: documents filed, facts updated, habits that decayed, missions completed
 *   3 Waiting on one tap  up to 3 staged cards
 *   4 Next week         drop alarms about to fire, annual fees about to post, trips about to start
 *   5 Proof             built later from the claims: each claim, its facts, and their quotes
 */

import { buildCopy, shortDate, usd } from '../missions/copy';
import { evaluateTrigger, loadTriggers } from '../missions/engine';
import { getHome } from '../missions/home';
import { localDate } from '../workers/scheduled';

export interface DraftClaim {
  claim_id: string;
  text: string;
  /** Facts the claim rests on. A null (a trigger that could not find its fact) can never be grounded. */
  facts: Array<string | null>;
}

export interface DraftLine {
  text: string;
  /** Money this line contributes to the Found total, in cents. Null for lines that carry none. */
  cents: number | null;
  claim_id: string | null;
}

export interface WaitingCard {
  run_id: string;
  title: string;
  why: string;
  state: string;
  claim_id: string | null;
}

export interface ReceiptDraft {
  week_start: string;
  as_of: string;
  found: DraftLine[];
  done: DraftLine[];
  waiting: WaitingCard[];
  next_week: DraftLine[];
  claims: DraftClaim[];
  counts: { documents_filed: number; facts_updated: number; habits_decayed: number; missions_completed: number };
}

const DAY = 86_400_000;
const DOCUMENT_SOURCES = ['email', 'pdf', 'csv', 'upload'];
/** One claim cites at most this many facts: enough to show the receipt is real without a thousand-row proof. */
const MAX_SUPPORT = 25;
const MAX_FOUND_LINES = 8;

const newClaimId = (): string => `rclm_${crypto.randomUUID().replace(/-/g, '')}`;
const dayOf = (iso: string): string => iso.slice(0, 10);
const addDays = (date: string, n: number): string => new Date(Date.parse(`${date}T00:00:00Z`) + n * DAY).toISOString().slice(0, 10);

/** The Monday (local date) of the week that `at` falls in. */
export function weekStartOf(at: Date, timeZone: string): string {
  const today = localDate(at, timeZone);
  const dow = new Date(`${today}T00:00:00Z`).getUTCDay(); // 0 = Sunday
  return addDays(today, -((dow + 6) % 7));
}

export async function assembleReceipt(db: D1Database, household_id: string, timeZone: string, now: Date): Promise<ReceiptDraft> {
  const claims: DraftClaim[] = [];
  const claim = (text: string, facts: Array<string | null>): string => {
    const claim_id = newClaimId();
    claims.push({ claim_id, text, facts });
    return claim_id;
  };
  const iso = now.toISOString();
  const since = new Date(now.getTime() - 7 * DAY).toISOString();
  const today = localDate(now, timeZone);
  const found: DraftLine[] = [];
  const done: DraftLine[] = [];
  const next_week: DraftLine[] = [];

  // 1 ── Found: the standing triggers' own candidates (already grounded by construction; G1 re-verifies them)
  const triggers = await loadTriggers(db);
  for (const key of ['T_EXPIRING_CREDIT', 'T_SUBSCRIPTION_CREEP']) {
    const trigger = triggers.find((t) => t.trigger_key === key);
    if (trigger === undefined) continue;
    const { candidates } = await evaluateTrigger(db, household_id, trigger, now);
    for (const c of candidates) {
      const copy = buildCopy(c.trigger_key, c.data);
      const cents = Math.round(c.recoverable_cents * c.confidence);
      found.push({ text: copy.title, cents, claim_id: claim(Object.values(copy.claims).join('; '), c.claims.flatMap((x) => x.facts)) });
    }
  }
  const returns = await db
    .prepare(
      `SELECT w.fact_id AS window_fact, w.value_text AS ends_on, e.canonical_name AS subject, a.fact_id AS amount_fact, a.value_cents AS cents
         FROM facts_current w
         JOIN entities e ON e.entity_id = w.subject_id
         JOIN facts_current a ON a.household_id = w.household_id AND a.subject_id = w.subject_id AND a.predicate = 'return_eligible_cents'
        WHERE w.household_id = ?1 AND w.predicate = 'return_window_ends_on' AND w.value_text >= ?2 AND w.value_text <= date(?2, '+14 days')
        ORDER BY w.value_text, w.fact_id LIMIT 5`,
    )
    .bind(household_id, today)
    .all<{ window_fact: string; ends_on: string; subject: string; amount_fact: string; cents: number }>();
  for (const r of returns.results) {
    const text = `The return window for ${r.subject} closes ${shortDate(r.ends_on)} — ${usd(r.cents)} is still returnable`;
    found.push({ text, cents: r.cents, claim_id: claim(text, [r.window_fact, r.amount_fact]) });
  }
  found.sort((a, b) => (b.cents ?? 0) - (a.cents ?? 0));
  found.splice(MAX_FOUND_LINES);

  // 2 ── Done
  const docs = await db
    .prepare(
      `SELECT COUNT(*) AS n FROM episodes e
        WHERE e.household_id = ?1 AND e.source IN (${DOCUMENT_SOURCES.map((_, i) => `?${i + 3}`).join(', ')}) AND e.received_at >= ?2
          AND e.redaction_status IN ('clean', 'redacted') AND EXISTS (SELECT 1 FROM facts f WHERE f.source_episode_id = e.episode_id)`,
    )
    .bind(household_id, since, ...DOCUMENT_SOURCES)
    .first<{ n: number }>();
  const documents_filed = docs?.n ?? 0;
  if (documents_filed > 0) {
    const sample = await db
      .prepare(
        `SELECT fact_id FROM (
           SELECT (SELECT f.fact_id FROM facts f WHERE f.source_episode_id = e.episode_id ORDER BY f.fact_id LIMIT 1) AS fact_id, e.received_at AS at
             FROM episodes e
            WHERE e.household_id = ?1 AND e.source IN (${DOCUMENT_SOURCES.map((_, i) => `?${i + 3}`).join(', ')}) AND e.received_at >= ?2 AND e.redaction_status IN ('clean', 'redacted'))
          WHERE fact_id IS NOT NULL ORDER BY at DESC LIMIT ${MAX_SUPPORT}`,
      )
      .bind(household_id, since, ...DOCUMENT_SOURCES)
      .all<{ fact_id: string }>();
    const text = `Filed ${documents_filed} ${documents_filed === 1 ? 'document' : 'documents'} into your household record`;
    done.push({ text, cents: null, claim_id: claim(text, sample.results.map((r) => r.fact_id)) });
  }
  const learned = await db.prepare('SELECT COUNT(*) AS n FROM facts WHERE household_id = ? AND learned_at >= ?').bind(household_id, since).first<{ n: number }>();
  const facts_updated = learned?.n ?? 0;
  if (facts_updated > 0) {
    const recent = await db.prepare('SELECT fact_id FROM facts WHERE household_id = ? AND learned_at >= ? ORDER BY learned_at DESC, fact_id LIMIT ?').bind(household_id, since, MAX_SUPPORT).all<{ fact_id: string }>();
    const text = `Updated ${facts_updated} ${facts_updated === 1 ? 'fact' : 'facts'} about your household`;
    done.push({ text, cents: null, claim_id: claim(text, recent.results.map((r) => r.fact_id)) });
  }
  const decayAll = await db.prepare(`SELECT COUNT(*) AS n FROM audit_log WHERE household_id = ? AND action = 'decay_invalidate' AND created_at >= ?`).bind(household_id, since).first<{ n: number }>();
  const habits = await db
    .prepare(
      `SELECT f.fact_id, ent.canonical_name AS cuisine FROM audit_log a
         JOIN facts f ON f.fact_id = json_extract(a.detail_json, '$.fact_id') AND f.household_id = a.household_id
         JOIN entities ent ON ent.entity_id = f.object_id
        WHERE a.household_id = ? AND a.action = 'decay_invalidate' AND a.created_at >= ? AND f.predicate = 'prefers_cuisine'
        ORDER BY a.created_at, f.fact_id LIMIT 5`,
    )
    .bind(household_id, since)
    .all<{ fact_id: string; cuisine: string }>();
  for (const h of habits.results) {
    const text = `Stopped assuming you prefer ${h.cuisine}`;
    done.push({ text, cents: null, claim_id: claim(text, [h.fact_id]) });
  }
  const completed = await db
    .prepare(`SELECT run_id, json_extract(params_json, '$.title') AS title FROM mission_runs WHERE household_id = ? AND state = 'CONFIRMED' AND updated_at >= ? ORDER BY updated_at DESC LIMIT 5`)
    .bind(household_id, since)
    .all<{ run_id: string; title: string | null }>();
  const missions_completed = completed.results.length;
  for (const m of completed.results) {
    if (m.title === null) continue;
    const support = await db.prepare('SELECT DISTINCT s.fact_id FROM claim_support s JOIN claims c ON c.claim_id = s.claim_id WHERE c.run_id = ? AND c.household_id = ? LIMIT ?').bind(m.run_id, household_id, MAX_SUPPORT).all<{ fact_id: string }>();
    done.push({ text: `Completed: ${m.title}`, cents: null, claim_id: claim(`Completed: ${m.title}`, support.results.map((r) => r.fact_id)) });
  }

  // 3 ── Waiting on one tap: the Home cards still waiting for the member
  const waiting: WaitingCard[] = [];
  const home = await getHome(db, household_id, now);
  for (const card of home.cards.filter((c) => c.state === 'R1_STAGED' || c.state === 'R2_AWAITING').slice(0, 3)) {
    const support = await db.prepare('SELECT DISTINCT s.fact_id FROM claim_support s JOIN claims c ON c.claim_id = s.claim_id WHERE c.run_id = ? AND c.household_id = ? LIMIT ?').bind(card.run_id, household_id, MAX_SUPPORT).all<{ fact_id: string }>();
    // the "forward receipts" card makes no claim about the household, so it has nothing to ground
    const claim_id = card.trigger_key === 'COLD_START_UNLOCK_SCAN' ? null : claim(card.title, support.results.map((r) => r.fact_id));
    waiting.push({ run_id: card.run_id, title: card.title, why: card.why, state: card.state, claim_id });
  }

  // 4 ── Next week
  const horizon = addDays(today, 7);
  const alarms = await db
    .prepare(
      `SELECT a.reservation_date, a.release_at, a.grounding_fact_id, e.canonical_name AS venue FROM drop_alarms a JOIN entities e ON e.entity_id = a.venue_entity_id
        WHERE a.household_id = ? AND a.status IN ('scheduled', 'staged') AND a.release_at > ? AND a.release_at <= ? ORDER BY a.release_at LIMIT 5`,
    )
    .bind(household_id, iso, new Date(now.getTime() + 7 * DAY).toISOString())
    .all<{ reservation_date: string; release_at: string; grounding_fact_id: string; venue: string }>();
  for (const a of alarms.results) {
    const text = `Table alert: ${a.venue} releases tables for ${shortDate(a.reservation_date)} on ${shortDate(dayOf(a.release_at))}`;
    next_week.push({ text, cents: null, claim_id: claim(text, [a.grounding_fact_id]) });
  }
  const fees = await db
    .prepare(
      `SELECT p.fact_id AS posts_fact, p.value_text AS posts_on, e.canonical_name AS card, a.fact_id AS fee_fact, a.value_cents AS fee
         FROM facts_current p JOIN entities e ON e.entity_id = p.subject_id
         JOIN facts_current a ON a.household_id = p.household_id AND a.subject_id = p.subject_id AND a.predicate = 'annual_fee'
        WHERE p.household_id = ? AND p.predicate = 'annual_fee_posts_on' ORDER BY e.canonical_name, p.fact_id LIMIT 20`,
    )
    .bind(household_id)
    .all<{ posts_fact: string; posts_on: string; card: string; fee_fact: string; fee: number }>();
  for (const f of fees.results) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(f.posts_on)) continue;
    const year = Number(today.slice(0, 4));
    const candidates = [`${year}${f.posts_on.slice(4)}`, `${year + 1}${f.posts_on.slice(4)}`];
    const next = candidates.find((d) => d >= today);
    if (next === undefined || next > horizon) continue;
    const text = `${f.card} ${usd(f.fee)} annual fee posts ${shortDate(next)}`;
    next_week.push({ text, cents: null, claim_id: claim(text, [f.posts_fact, f.fee_fact]) });
  }
  // a trip is a `trip_window` fact ("<start>/<end>") on a trip entity, as the calendar sync writes it
  const trips = await db
    .prepare(
      `SELECT te.canonical_name AS name, substr(t.value_text, 1, 10) AS starts, t.fact_id AS trip_fact
         FROM facts_current t JOIN entities te ON te.entity_id = t.subject_id AND te.type = 'trip'
        WHERE t.household_id = ?1 AND t.predicate = 'trip_window' AND substr(t.value_text, 1, 10) >= ?2 AND substr(t.value_text, 1, 10) <= ?3
        ORDER BY starts, t.fact_id LIMIT 5`,
    )
    .bind(household_id, today, horizon)
    .all<{ name: string; starts: string; trip_fact: string }>();
  for (const t of trips.results) {
    const text = `${t.name} starts ${shortDate(t.starts)}`;
    next_week.push({ text, cents: null, claim_id: claim(text, [t.trip_fact]) });
  }

  return {
    week_start: weekStartOf(now, timeZone),
    as_of: iso,
    found,
    done,
    waiting,
    next_week,
    claims,
    counts: { documents_filed, facts_updated, habits_decayed: decayAll?.n ?? 0, missions_completed },
  };
}
