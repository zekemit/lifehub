/**
 * src/concierge/flights.ts — flight price watches (Engine B). A member asks to be told when a trip drops under a ceiling;
 * PriceWatchWF checks Duffel once a day and, when the fare fits, hands MissionRunWF a card whose approval yields a deep link.
 * At most two watches per household are active at once (a trigger in migration 0003 enforces it too), and never more than one
 * check a day per watch.
 */

import { writeFact } from '../graph/writeFact';
import { sha256Hex } from '../crypto/vault';
import { IATA } from '../travel/duffel';
import { usd } from '../missions/copy';
import type { LinkTarget } from '../missions/deeplinks';
import { addDays } from './venues';

export const MAX_ACTIVE_WATCHES = 2;
export const CHECKS_PER_DAY = 1;
export const MAX_WATCH_DAYS_AHEAD = 330;
export const MIN_CEILING_CENTS = 5_000;
export const MAX_CEILING_CENTS = 1_000_000;
/** A second instance of a workflow must not search again within this long of the last check. */
export const MIN_CHECK_INTERVAL_MS = 20 * 3_600_000;

export interface WatchParams {
  origin: string;
  destination: string;
  depart_date: string;
  return_date: string | null;
  adults: number;
  booking_domain: string;
  /** The fact that records what the member asked for; the staged card is grounded on it. */
  fact_id: string;
}

/** Where an approved deal sends the member. The member books there themselves; nothing is booked from here. */
export const BOOKING_TARGETS: Readonly<Record<string, (p: WatchParams) => LinkTarget>> = {
  'google.com': (p) => ({
    merchant_domain: 'google.com',
    path: '/travel/flights',
    query: { q: `Flights from ${p.origin} to ${p.destination} on ${p.depart_date}${p.return_date === null ? '' : ` through ${p.return_date}`}` },
  }),
  'capitalonetravel.com': () => ({ merchant_domain: 'capitalonetravel.com', path: '/', query: {} }),
  'amextravel.com': () => ({ merchant_domain: 'amextravel.com', path: '/', query: {} }),
};

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const isRealDate = (d: unknown): d is string => typeof d === 'string' && DATE.test(d) && new Date(`${d}T00:00:00Z`).toISOString().slice(0, 10) === d;

export function parseWatchParams(raw: string): WatchParams | null {
  try {
    const p = JSON.parse(raw) as Partial<WatchParams>;
    if (typeof p.origin !== 'string' || typeof p.destination !== 'string' || !isRealDate(p.depart_date) || typeof p.booking_domain !== 'string' || typeof p.fact_id !== 'string') return null;
    if (p.return_date !== null && p.return_date !== undefined && !isRealDate(p.return_date)) return null;
    return { origin: p.origin, destination: p.destination, depart_date: p.depart_date, return_date: p.return_date ?? null, adults: Number.isInteger(p.adults) ? (p.adults as number) : 1, booking_domain: p.booking_domain, fact_id: p.fact_id };
  } catch {
    return null;
  }
}

export type CreateWatchResult =
  | { ok: true; watch_id: string }
  | { ok: false; code: 'bad_input' | 'watch_limit'; message: string };

export async function createPriceWatch(
  db: D1Database,
  household_id: string,
  input: { origin: unknown; destination: unknown; depart_date: unknown; return_date?: unknown; adults?: unknown; ceiling_cents: unknown; booking_domain?: unknown },
  now: Date,
): Promise<CreateWatchResult> {
  const origin = typeof input.origin === 'string' ? input.origin.toUpperCase() : '';
  const destination = typeof input.destination === 'string' ? input.destination.toUpperCase() : '';
  const return_date = input.return_date === undefined || input.return_date === null ? null : input.return_date;
  const adults = input.adults === undefined ? 1 : input.adults;
  const booking_domain = input.booking_domain === undefined ? 'google.com' : input.booking_domain;
  const today = now.toISOString().slice(0, 10);
  const bad = (message: string): CreateWatchResult => ({ ok: false, code: 'bad_input', message });
  if (!IATA.test(origin) || !IATA.test(destination) || origin === destination) return bad('origin and destination must be different 3-letter airport codes.');
  if (!isRealDate(input.depart_date) || input.depart_date <= today || input.depart_date > addDays(today, MAX_WATCH_DAYS_AHEAD)) return bad('depart_date must be a future date within the next 330 days.');
  if (return_date !== null && (!isRealDate(return_date) || return_date < input.depart_date || return_date > addDays(input.depart_date, 60))) return bad('return_date must be on or after depart_date, within 60 days.');
  if (!Number.isInteger(adults) || (adults as number) < 1 || (adults as number) > 4) return bad('adults must be 1-4.');
  if (!Number.isInteger(input.ceiling_cents) || (input.ceiling_cents as number) < MIN_CEILING_CENTS || (input.ceiling_cents as number) > MAX_CEILING_CENTS) return bad('ceiling_cents must be between $50 and $10,000.');
  if (typeof booking_domain !== 'string' || !Object.prototype.hasOwnProperty.call(BOOKING_TARGETS, booking_domain)) return bad(`booking_domain must be one of ${Object.keys(BOOKING_TARGETS).join(', ')}.`);
  const ceiling = input.ceiling_cents as number;

  const active = await db.prepare('SELECT COUNT(*) AS n FROM price_watches WHERE household_id = ? AND active = 1').bind(household_id).first<{ n: number }>();
  if ((active?.n ?? 0) >= MAX_ACTIVE_WATCHES) return { ok: false, code: 'watch_limit', message: `You can have at most ${MAX_ACTIVE_WATCHES} active flight watches.` };

  const watch_id = `pwt_${crypto.randomUUID().replace(/-/g, '')}`;
  const text = `Watch flights from ${origin} to ${destination} departing ${input.depart_date}${return_date === null ? '' : ` returning ${return_date}`} under ${usd(ceiling)}.`;
  const episode_id = `ep_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;
  await db
    .prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, occurred_at, redaction_status) VALUES (?, ?, 'user_input', ?, ?, NULL, 'clean')`)
    .bind(episode_id, household_id, text, await sha256Hex(`${watch_id}:${text}`))
    .run();
  const fact = await writeFact(db, {
    household_id,
    source_episode_id: episode_id,
    subject: { type: 'trip', name: `${origin} to ${destination} ${input.depart_date}` },
    predicate: 'price_watch_target',
    value_text: `${origin}-${destination}|${input.depart_date}|${return_date ?? ''}|${ceiling}`,
    confidence: 1,
    quotes: [{ quote: text, char_start: 0, char_end: text.length }],
  });
  if (fact.status !== 'written' && fact.status !== 'reaffirmed') return bad('The watch could not be recorded.');
  const params: WatchParams = { origin, destination, depart_date: input.depart_date, return_date, adults: adults as number, booking_domain, fact_id: fact.fact_id };
  try {
    await db
      .prepare(`INSERT INTO price_watches (watch_id, household_id, kind, params_json, ceiling_cents, checks_per_day, active, created_at) VALUES (?, ?, 'flight', ?, ?, ?, 1, ?)`)
      .bind(watch_id, household_id, JSON.stringify(params), ceiling, CHECKS_PER_DAY, now.toISOString())
      .run();
  } catch (err) {
    if (/at most 2 active/i.test(err instanceof Error ? err.message : String(err))) return { ok: false, code: 'watch_limit', message: `You can have at most ${MAX_ACTIVE_WATCHES} active flight watches.` };
    throw err;
  }
  return { ok: true, watch_id };
}
