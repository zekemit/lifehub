/**
 * src/concierge/priceWatchPipeline.ts — PriceWatchWF (Engine B): a loop of one check a day, for as long as the watch is active.
 *
 *   check_<n>    one Duffel search (never more than one a day per watch). A fare at or under the ceiling that also fits the travel
 *                envelope (Gate G3's rule) and leaves the calendar free for the trip (Gate G4's rule) is a deal.
 *   trigger_<n>  stage the deal as a card and start MissionRunWF for it. The run re-checks everything with gates G1-G9, asks the
 *                member to approve the exact card, and only then issues the booking deep link (affiliate-tagged where a tag exists).
 *   sleep_<n>    step.sleep('1 day'), then the next check.
 *
 * The workflow ends after a trigger (the watch is switched off so one deal is one card), when the member stops the watch, or after the
 * trip's departure date. Nothing is booked, and Duffel is only ever searched.
 */

import { sha256Hex } from '../crypto/vault';
import type { StepLike } from '../ingest/steps';
import { buildCopy } from '../missions/copy';
import { insertRun } from '../missions/engine';
import { MONEY_MAX_AGE_DAYS, fitsHeadroom } from '../missions/gates';
import type { MissionDeps } from '../missions/runPipeline';
import type { PriceWatchParams } from '../types';
import { localDate } from '../workers/scheduled';
import { MIN_CHECK_INTERVAL_MS, parseWatchParams } from './flights';
import { evaluateFlightWatch, flightDealData } from './readers';

export const MAX_WATCH_DAYS = 366;

export interface PriceWatchDeps extends MissionDeps {
  /** Starts MissionRunWF for the staged deal. Production: env.MISSION_RUN_WF.create. */
  startMission(o: { id: string; params: { run_id: string; household_id: string } }): Promise<unknown>;
}

export type PriceWatchOutcome =
  | { status: 'triggered'; run_id: string; day: number }
  | { status: 'stopped'; reason: string; day: number }
  | { status: 'expired' };

export const watchRunId = async (watch_id: string): Promise<string> => `run_${(await sha256Hex(`watch:${watch_id}`)).slice(0, 32)}`;

export type Check =
  | { action: 'stop'; reason: string }
  | { action: 'sleep'; reason: string }
  | { action: 'trigger'; offer: { offer_id: string; total_cents: number; carrier: string | null } };

interface WatchRecord {
  watch_id: string;
  params_json: string;
  ceiling_cents: number;
  checks_per_day: number;
  active: number;
  last_checked_at: string | null;
}

export async function dailyCheck(deps: PriceWatchDeps, params: PriceWatchParams): Promise<Check> {
  const db = deps.env.DB;
  const now = deps.now();
  const watch = await db
    .prepare('SELECT watch_id, params_json, ceiling_cents, checks_per_day, active, last_checked_at FROM price_watches WHERE watch_id = ? AND household_id = ?')
    .bind(params.watch_id, params.household_id)
    .first<WatchRecord>();
  if (watch === null || watch.active !== 1) return { action: 'stop', reason: 'inactive' };
  const p = parseWatchParams(watch.params_json);
  if (p === null) return { action: 'stop', reason: 'unusable watch' };
  const hh = await db.prepare('SELECT timezone FROM households WHERE household_id = ? AND deleted_at IS NULL').bind(params.household_id).first<{ timezone: string }>();
  if (hh === null) return { action: 'stop', reason: 'household gone' };
  if (p.depart_date <= localDate(now, hh.timezone)) {
    await db.prepare('UPDATE price_watches SET active = 0 WHERE watch_id = ?').bind(watch.watch_id).run();
    return { action: 'stop', reason: 'departed' };
  }
  if (watch.checks_per_day < 1) return { action: 'sleep', reason: 'paused' };
  if (!deps.env.DUFFEL_API_KEY) return { action: 'sleep', reason: 'no_api_key' };
  if (watch.last_checked_at !== null && now.getTime() - Date.parse(watch.last_checked_at) < MIN_CHECK_INTERVAL_MS) return { action: 'sleep', reason: 'checked_recently' };

  // Claim today's check BEFORE searching, compare-and-set on the last check, so two instances (or a replay) cannot both search.
  const iso = now.toISOString();
  const claim = await db
    .prepare('UPDATE price_watches SET last_checked_at = ? WHERE watch_id = ? AND last_checked_at IS ?')
    .bind(iso, watch.watch_id, watch.last_checked_at)
    .run();
  if ((claim.meta.changes ?? 0) === 0) return { action: 'sleep', reason: 'checked_recently' };

  let ev;
  try {
    ev = await evaluateFlightWatch(deps, params.household_id, hh.timezone, p, { search: true });
  } catch (err) {
    // a failed search must not use up the day: give the claim back, then let the step retry
    await db.prepare('UPDATE price_watches SET last_checked_at = ? WHERE watch_id = ? AND last_checked_at = ?').bind(watch.last_checked_at, watch.watch_id, iso).run();
    throw err;
  }
  await db.prepare('UPDATE price_watches SET last_fare_cents = ? WHERE watch_id = ?').bind(ev.offer?.total_cents ?? null, watch.watch_id).run();
  if (ev.offer === null) return { action: 'sleep', reason: 'no_offers' };
  if (ev.offer.total_cents > watch.ceiling_cents) return { action: 'sleep', reason: 'above_ceiling' };
  if (ev.headroom_cents === null || !fitsHeadroom(ev.offer.total_cents, ev.headroom_cents)) return { action: 'sleep', reason: 'no_headroom' }; // Gate G3
  // Gate G2 will not trust an envelope whose spend data is more than 35 days old, so a card built on one would only be refused
  if (ev.spend_data_at === null || now.getTime() - Date.parse(ev.spend_data_at) > MONEY_MAX_AGE_DAYS * 86_400_000) return { action: 'sleep', reason: 'stale_spend_data' };
  if (ev.calendar.windows.length === 0) return { action: 'sleep', reason: 'calendar_busy' }; // Gate G4
  return { action: 'trigger', offer: ev.offer };
}

async function stageDeal(deps: PriceWatchDeps, params: PriceWatchParams, offer: Extract<Check, { action: 'trigger' }>['offer']): Promise<string> {
  const db = deps.env.DB;
  const run_id = await watchRunId(params.watch_id);
  const watch = await db.prepare('SELECT watch_id, params_json, ceiling_cents, run_id FROM price_watches WHERE watch_id = ? AND household_id = ?').bind(params.watch_id, params.household_id).first<{ watch_id: string; params_json: string; ceiling_cents: number; run_id: string | null }>();
  const p = watch === null ? null : parseWatchParams(watch.params_json);
  if (watch === null || p === null) throw new Error('the watch disappeared before its deal could be staged');
  const existing = await db.prepare('SELECT 1 AS ok FROM mission_runs WHERE run_id = ? AND household_id = ?').bind(run_id, params.household_id).first<{ ok: number }>();
  if (existing === null) {
    const data = flightDealData(watch, p, offer);
    await insertRun(db, params.household_id, {
      run_id,
      origin: 'user',
      template_key: 'flight_watch',
      trigger_key: 'T_PRICE_WATCH',
      dedupe_key: watch.watch_id,
      copy: buildCopy('T_PRICE_WATCH', data),
      recoverable_cents: 0,
      urgency: 1,
      confidence: 1,
      score: 1,
      data,
      claims: [{ key: 'watch', facts: [p.fact_id] }],
      now: deps.now(),
    });
  }
  // from here only this run may act on the watch, and the watch is switched off: one deal, one card
  await db.prepare('UPDATE price_watches SET run_id = ?, active = 0 WHERE watch_id = ?').bind(run_id, watch.watch_id).run();
  try {
    await deps.startMission({ id: run_id, params: { run_id, household_id: params.household_id } });
  } catch (err) {
    if (!/already|exist/i.test(err instanceof Error ? err.message : String(err))) throw err;
  }
  return run_id;
}

export async function runPriceWatch(step: StepLike, deps: PriceWatchDeps, params: PriceWatchParams): Promise<PriceWatchOutcome> {
  for (let day = 0; day < MAX_WATCH_DAYS; day++) {
    let check: Check;
    try {
      check = await step.do(`check_${day}`, { retries: { limit: 2, delay: '1 minute', backoff: 'exponential' } }, () => dailyCheck(deps, params));
    } catch {
      check = { action: 'sleep', reason: 'search_failed' }; // a bad day for Duffel must not end a watch that runs for months
    }
    if (check.action === 'stop') return { status: 'stopped', reason: check.reason, day };
    if (check.action === 'trigger') {
      const offer = check.offer;
      const run_id = await step.do(`trigger_${day}`, () => stageDeal(deps, params, offer));
      return { status: 'triggered', run_id, day };
    }
    await step.sleep(`sleep_${day}`, '1 day');
  }
  return { status: 'expired' };
}
