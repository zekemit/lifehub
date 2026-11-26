/**
 * src/concierge/readers.ts — what MissionRunWF's r0_read and card builder do for the two Engine B templates.
 * Standing-trigger templates read facts through a stored SQL trigger; these read the alarm or watch row, re-check the world, and
 * answer in the same shape (ReadContext), so the same gates G1-G9 judge them.
 *
 *   drop_alarm    the stored release rule still gives the release time the alarm was created with (a changed rule fails G2)
 *   flight_watch  a FRESH Duffel search must still be at or under the ceiling, the fare must fit the travel envelope (G3), and the
 *                 member's calendar must be free for the trip (G4, from a freeBusy fetched in this run)
 */

import { CalendarApiError, CalendarAuthError, findCalendarConnector, isoMs, localToUtcMs, queryBusyRange, type BusyBlock } from '../connectors/googleCalendar';
import { factStates, type Compiled, type MissionDeps, type ReadResult } from '../missions/runPipeline';
import type { CardPayload, FreshWindow, ReadContext } from '../missions/gates';
import { DuffelError, searchFlights, type FlightOffer } from '../travel/duffel';
import { BOOKING_TARGETS, parseWatchParams, type WatchParams } from './flights';
import { loadRule, releaseAtMs, releaseClock, venueLinkTarget } from './venues';

/** What the readers need; MissionDeps satisfies it, and so do the concierge workflows' own deps. */
export type ReadDeps = Pick<MissionDeps, 'env' | 'fetch' | 'now'>;

const HOUR = 3_600_000;
/** A drop-alarm card is worth acting on for this long after the release. */
export const ALARM_CARD_WINDOW_MS = 2 * HOUR;

type Obj = Record<string, unknown>;

export async function readDropAlarm(deps: ReadDeps, household_id: string, alarm_id: string): Promise<ReadContext> {
  const db = deps.env.DB;
  const now = deps.now();
  const alarm = await db
    .prepare('SELECT alarm_id, venue_entity_id, reservation_date, party_size, release_at, grounding_fact_id, status FROM drop_alarms WHERE alarm_id = ? AND household_id = ?')
    .bind(alarm_id, household_id)
    .first<{ alarm_id: string; venue_entity_id: string; reservation_date: string; party_size: number; release_at: string; grounding_fact_id: string; status: string }>();
  const rule = alarm === null ? null : await loadRule(db, household_id, alarm.venue_entity_id);
  const releaseMs = alarm === null || rule === null ? null : releaseAtMs(rule, alarm.reservation_date);
  const target = alarm === null || rule === null ? null : venueLinkTarget(rule, alarm.reservation_date, alarm.party_size);
  const found =
    alarm !== null && rule !== null && releaseMs !== null && target !== null &&
    (alarm.status === 'scheduled' || alarm.status === 'staged') &&
    new Date(releaseMs).toISOString() === alarm.release_at &&
    now.getTime() < releaseMs + ALARM_CARD_WINDOW_MS;
  const claims = found && alarm !== null ? [{ key: 'alarm', facts: [alarm.grounding_fact_id] }] : [];
  const data: Obj =
    found && alarm !== null && rule !== null && releaseMs !== null
      ? {
          alarm_id: alarm.alarm_id,
          venue: rule.venue_name,
          platform: rule.platform,
          party_size: alarm.party_size,
          reservation_date: alarm.reservation_date,
          release_at: alarm.release_at,
          release_clock: releaseClock(rule, releaseMs),
          link_target: target,
          expires_at: new Date(releaseMs + ALARM_CARD_WINDOW_MS).toISOString(),
        }
      : {};
  return {
    candidate_found: found,
    data,
    claims,
    facts: await factStates(db, household_id, claims, now),
    headroom_cents: null,
    envelope_key: null,
    spend_data_at: null,
    calendar: { required: false, fetched_at: null, windows: [], error: null },
    fetched_at: now.toISOString(),
  };
}

// ─── flight watches ─────────────────────────────────────────────────────────

function overlaps(busy: readonly BusyBlock[], startMs: number, endMs: number): boolean {
  return busy.some((b) => Date.parse(b.start) < endMs && Date.parse(b.end) > startMs);
}

/** The trip's span in the household's time zone: the start of the departure day to the end of the return (or departure) day. */
export function tripSpan(p: Pick<WatchParams, 'depart_date' | 'return_date'>, timeZone: string): { start: string; end: string } {
  return { start: isoMs(localToUtcMs(p.depart_date, '00:00', timeZone)), end: isoMs(localToUtcMs(p.return_date ?? p.depart_date, '23:59', timeZone)) };
}

async function tripCalendar(deps: ReadDeps, household_id: string, timeZone: string, p: WatchParams): Promise<ReadContext['calendar']> {
  const now = deps.now();
  const base = { required: true, fetched_at: null, windows: [] as FreshWindow[], error: null as string | null };
  const connector = await findCalendarConnector(deps.env.DB, household_id);
  if (connector === null) return { ...base, error: 'Google Calendar is not connected' };
  const span = tripSpan(p, timeZone);
  try {
    const busy = await queryBusyRange({ env: deps.env, fetch: deps.fetch, now: deps.now }, connector, timeZone, span.start, span.end);
    // "free" for a trip means nothing on the calendar overlaps it; the free window is then the trip itself
    const free = !overlaps(busy, Date.parse(span.start), Date.parse(span.end));
    return { required: true, fetched_at: now.toISOString(), error: null, windows: free ? [{ date: p.depart_date, start: span.start, end: span.end }] : [] };
  } catch (err) {
    if (err instanceof CalendarAuthError || (err instanceof CalendarApiError && err.status >= 400 && err.status < 500)) {
      return { ...base, error: `Google Calendar could not be read: ${err.message.slice(0, 120)}` };
    }
    throw err;
  }
}

export interface WatchRow {
  watch_id: string;
  params_json: string;
  ceiling_cents: number;
  run_id: string | null;
}

export interface FlightEvaluation {
  offer: FlightOffer | null;
  headroom_cents: number | null;
  envelope_key: string | null;
  spend_data_at: string | null;
  calendar: ReadContext['calendar'];
}

/**
 * Everything the gates need to know about a watched trip right now: a fresh cheapest fare, the travel envelope's headroom, and whether
 * the calendar is free. Shared by the daily check (to decide whether to stage a card) and by r0_read (to judge it).
 */
export async function evaluateFlightWatch(deps: ReadDeps, household_id: string, timeZone: string, p: WatchParams, o: { search: boolean }): Promise<FlightEvaluation> {
  const db = deps.env.DB;
  let offer: FlightOffer | null = null;
  if (o.search && deps.env.DUFFEL_API_KEY) {
    try {
      offer = await searchFlights(
        { fetch: deps.fetch, apiKey: deps.env.DUFFEL_API_KEY },
        { origin: p.origin, destination: p.destination, depart_date: p.depart_date, return_date: p.return_date, adults: p.adults },
      );
    } catch (err) {
      if (!(err instanceof DuffelError && err.status >= 400 && err.status < 500)) throw err; // a 5xx or a network error is transient: retry
    }
  }
  const envelope = await db
    .prepare(
      `SELECT envelope_key, headroom_cents FROM envelope_headroom
        WHERE household_id = ? AND (envelope_key LIKE '%travel%' OR envelope_key LIKE '%flight%')
        ORDER BY headroom_cents DESC, envelope_key LIMIT 1`,
    )
    .bind(household_id)
    .first<{ envelope_key: string; headroom_cents: number }>();
  const spend = await db.prepare(`SELECT MAX(e.received_at) AS at FROM transactions t JOIN episodes e ON e.episode_id = t.episode_id WHERE t.household_id = ?`).bind(household_id).first<{ at: string | null }>();
  return {
    offer,
    headroom_cents: envelope?.headroom_cents ?? null,
    envelope_key: envelope?.envelope_key ?? null,
    spend_data_at: spend?.at ?? null,
    calendar: await tripCalendar(deps, household_id, timeZone, p),
  };
}

/** The values a flight card is built from (and that the run's params keep). */
export function flightDealData(watch: Pick<WatchRow, 'watch_id' | 'ceiling_cents'>, p: WatchParams, offer: FlightOffer): Obj {
  return {
    watch_id: watch.watch_id,
    origin: p.origin,
    destination: p.destination,
    depart_date: p.depart_date,
    return_date: p.return_date,
    fare_cents: offer.total_cents,
    ceiling_cents: watch.ceiling_cents,
    offer_id: offer.offer_id,
    carrier: offer.carrier,
    booking: (BOOKING_TARGETS[p.booking_domain] ?? BOOKING_TARGETS['google.com']!)(p),
    expires_on: p.depart_date,
  };
}

async function readFlightDeal(deps: ReadDeps, c: Pick<Compiled, 'household_id' | 'dedupe_key' | 'run_id' | 'timezone'>): Promise<ReadContext> {
  const db = deps.env.DB;
  const now = deps.now();
  const watch = await db
    .prepare('SELECT watch_id, params_json, ceiling_cents, run_id FROM price_watches WHERE watch_id = ? AND household_id = ?')
    .bind(c.dedupe_key, c.household_id)
    .first<WatchRow>();
  const params = watch === null ? null : parseWatchParams(watch.params_json);
  // only the run PriceWatchWF created for this watch may act on it
  const mine = watch !== null && params !== null && watch.run_id === c.run_id;
  if (!mine || watch === null || params === null) {
    return {
      candidate_found: false, data: {}, claims: [], facts: [], headroom_cents: null, envelope_key: null, spend_data_at: null,
      calendar: { required: true, fetched_at: null, windows: [], error: 'the watch is not available to this run' }, fetched_at: now.toISOString(),
    };
  }
  const ev = await evaluateFlightWatch(deps, c.household_id, c.timezone, params, { search: true });
  const found = ev.offer !== null && ev.offer.total_cents <= watch.ceiling_cents;
  const claims = found ? [{ key: 'watch', facts: [params.fact_id] }] : [];
  return {
    candidate_found: found,
    data: found && ev.offer !== null ? flightDealData(watch, params, ev.offer) : {},
    claims,
    facts: await factStates(db, c.household_id, claims, now),
    headroom_cents: ev.headroom_cents,
    envelope_key: ev.envelope_key,
    spend_data_at: ev.spend_data_at,
    calendar: ev.calendar,
    fetched_at: now.toISOString(),
  };
}

/** The Engine B reader for this template, or null when the template reads through a stored trigger. */
export async function readConcierge(deps: MissionDeps, c: Compiled): Promise<ReadResult | null> {
  if (c.template_key === 'drop_alarm') return { read: await readDropAlarm(deps, c.household_id, c.dedupe_key) };
  if (c.template_key === 'flight_watch') return { read: await readFlightDeal(deps, c) };
  return null;
}

const asAction = (v: unknown): CardPayload['action'] => {
  const t = v as { merchant_domain?: unknown; path?: unknown; query?: unknown } | null | undefined;
  if (t && typeof t.merchant_domain === 'string' && typeof t.path === 'string' && t.query !== null && typeof t.query === 'object') {
    return { type: 'deeplink', merchant_domain: t.merchant_domain, path: t.path, query: t.query as Record<string, string> };
  }
  return { type: 'none' };
};

/** The money, time and link of an Engine B card. Null for every other kind. */
export function conciergePayloadParts(
  kind: CardPayload['kind'],
  timeZone: string,
  read: ReadContext,
): Pick<CardPayload, 'price_cents' | 'envelope_key' | 'proposed_range' | 'action'> | null {
  if (kind === 'drop_alarm') return { price_cents: 0, envelope_key: null, proposed_range: null, action: asAction(read.data['link_target']) };
  if (kind === 'flight_deal') {
    const d = read.data;
    const fare = typeof d['fare_cents'] === 'number' ? d['fare_cents'] : 0;
    const depart = typeof d['depart_date'] === 'string' ? d['depart_date'] : null;
    const back = typeof d['return_date'] === 'string' ? d['return_date'] : null;
    return {
      price_cents: fare,
      envelope_key: read.envelope_key,
      proposed_range: depart === null ? null : tripSpan({ depart_date: depart, return_date: back }, timeZone),
      action: asAction(d['booking']),
    };
  }
  return null;
}
