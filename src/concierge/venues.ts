/**
 * src/concierge/venues.ts — drop alarms (Engine B): "tell me the moment this venue releases tables for my date".
 *
 * Everything is driven by a STORED release rule (venue_release_rules): how many days ahead tables open, at what local time, in which
 * time zone. Nothing is ever fetched from a venue or a booking platform. venue_url is only ever the target of a deep link the member
 * taps themselves; the domains involved (Resy, OpenTable, Tock, SevenRooms) are on policy_blocklist and the egress guard would refuse
 * to contact them anyway.
 */

import { localToUtcMs, localClock } from '../connectors/googleCalendar';
import { writeFact } from '../graph/writeFact';
import { sha256Hex } from '../crypto/vault';
import type { LinkTarget } from '../missions/deeplinks';

/** The workflow wakes this long before tables are released, so the card is on the member's phone first. */
export const RELEASE_LEAD_MS = 120_000;
/** Workflow sleeps are capped at 365 days. */
export const MAX_SLEEP_MS = 365 * 86_400_000;

export interface ReleaseRule {
  venue_entity_id: string;
  venue_name: string;
  platform: 'resy' | 'opentable' | 'tock' | 'sevenrooms' | 'direct';
  venue_url: string;
  days_ahead: number;
  release_local_time: string;
  release_tz: string;
  verified_on: string;
}

/** The host a venue_url must belong to for each platform; 'direct' (the venue's own site) may be any public https host. */
const PLATFORM_HOSTS: Readonly<Record<ReleaseRule['platform'], readonly string[] | null>> = {
  resy: ['resy.com'],
  opentable: ['opentable.com'],
  tock: ['exploretock.com'],
  sevenrooms: ['sevenrooms.com'],
  direct: null,
};

/** The query parameters each platform understands for a pre-filled date and party size. */
const PLATFORM_PARAMS: Readonly<Record<ReleaseRule['platform'], { date: string; party: string }>> = {
  resy: { date: 'date', party: 'seats' },
  opentable: { date: 'dateTime', party: 'covers' },
  tock: { date: 'date', party: 'size' },
  sevenrooms: { date: 'date', party: 'party_size' },
  direct: { date: 'date', party: 'party_size' },
};

export function addDays(date: string, days: number): string {
  return new Date(Date.parse(`${date}T00:00:00Z`) + days * 86_400_000).toISOString().slice(0, 10);
}

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const isRealDate = (d: string): boolean => DATE.test(d) && new Date(`${d}T00:00:00Z`).toISOString().slice(0, 10) === d;

export async function loadRule(db: D1Database, household_id: string, venue_entity_id: string): Promise<ReleaseRule | null> {
  return db
    .prepare(
      `SELECT r.venue_entity_id, e.canonical_name AS venue_name, r.platform, r.venue_url, r.days_ahead, r.release_local_time, r.release_tz, r.verified_on
         FROM venue_release_rules r JOIN entities e ON e.entity_id = r.venue_entity_id
        WHERE r.venue_entity_id = ? AND e.household_id = ? AND e.type = 'venue' AND e.merged_into IS NULL`,
    )
    .bind(venue_entity_id, household_id)
    .first<ReleaseRule>();
}

/** The instant (epoch ms) tables for `reservation_date` open, per the rule. Null when the rule cannot be evaluated (bad time or zone). */
export function releaseAtMs(rule: ReleaseRule, reservation_date: string): number | null {
  if (!isRealDate(reservation_date) || !Number.isInteger(rule.days_ahead) || rule.days_ahead < 0 || rule.days_ahead > 365) return null;
  if (!/^([01]\d|2[0-3]):[0-5]\d$/.test(rule.release_local_time)) return null;
  try {
    const ms = localToUtcMs(addDays(reservation_date, -rule.days_ahead), rule.release_local_time, rule.release_tz);
    return Number.isFinite(ms) ? ms : null;
  } catch {
    return null; // unknown time zone
  }
}

/** A venue_url that may be offered as a link: https, no credentials, a host that matches the platform. */
export function safeVenueUrl(rule: ReleaseRule): URL | null {
  let u: URL;
  try {
    u = new URL(rule.venue_url);
  } catch {
    return null;
  }
  if (u.protocol !== 'https:' || u.username !== '' || u.password !== '' || u.port !== '') return null;
  const host = u.hostname.toLowerCase();
  const allowed = PLATFORM_HOSTS[rule.platform];
  if (allowed === null) return host.includes('.') && !/^[\d.]+$/.test(host) && host !== 'localhost' && !host.endsWith('.local') ? u : null;
  return allowed.some((h) => host === h || host.endsWith(`.${h}`)) ? u : null;
}

/** The deep link the card offers: the venue's own page with the date and party size filled in. Null when the stored URL is not safe. */
export function venueLinkTarget(rule: ReleaseRule, reservation_date: string, party_size: number): LinkTarget | null {
  const u = safeVenueUrl(rule);
  if (u === null) return null;
  const params = PLATFORM_PARAMS[rule.platform];
  const query: Record<string, string> = {};
  for (const [k, v] of u.searchParams) query[k] = v;
  query[params.party] = String(party_size);
  query[params.date] = rule.platform === 'opentable' ? `${reservation_date}T19:00` : reservation_date;
  return { merchant_domain: u.hostname.toLowerCase(), path: u.pathname === '' ? '/' : u.pathname, query };
}

/** The time of day the rule releases tables, as the venue's local clock reads it (for the card). */
export function releaseClock(rule: ReleaseRule, releaseMs: number): string {
  return `${localClock(releaseMs, rule.release_tz)} ${rule.release_tz}`;
}

export type CreateAlarmResult =
  | { ok: true; alarm_id: string; release_at: string; sleep_ms: number }
  | { ok: false; code: 'bad_input' | 'unknown_venue' | 'no_release_rule' | 'unsafe_venue_url' | 'release_passed' | 'too_far' | 'duplicate'; message: string };

export async function createDropAlarm(
  db: D1Database,
  household_id: string,
  input: { venue_entity_id: unknown; reservation_date: unknown; party_size: unknown },
  now: Date,
): Promise<CreateAlarmResult> {
  const { venue_entity_id, reservation_date, party_size } = input;
  if (typeof venue_entity_id !== 'string' || typeof reservation_date !== 'string' || !isRealDate(reservation_date) || !Number.isInteger(party_size) || (party_size as number) < 1 || (party_size as number) > 12) {
    return { ok: false, code: 'bad_input', message: 'Send venue_entity_id, reservation_date (YYYY-MM-DD) and party_size (1-12).' };
  }
  const party = party_size as number;
  const entity = await db.prepare(`SELECT 1 AS ok FROM entities WHERE entity_id = ? AND household_id = ? AND type = 'venue'`).bind(venue_entity_id, household_id).first<{ ok: number }>();
  if (entity === null) return { ok: false, code: 'unknown_venue', message: 'That venue is not one of yours.' };
  const rule = await loadRule(db, household_id, venue_entity_id);
  if (rule === null) return { ok: false, code: 'no_release_rule', message: 'We do not know when this venue releases tables.' };
  if (venueLinkTarget(rule, reservation_date, party) === null) return { ok: false, code: 'unsafe_venue_url', message: 'The stored booking link for this venue is not usable.' };
  const release = releaseAtMs(rule, reservation_date);
  if (release === null) return { ok: false, code: 'bad_input', message: 'The venue release rule could not be evaluated.' };
  if (release <= now.getTime()) return { ok: false, code: 'release_passed', message: 'Tables for that date have already been released.' };
  const sleep_ms = Math.max(0, release - RELEASE_LEAD_MS - now.getTime());
  if (sleep_ms > MAX_SLEEP_MS) return { ok: false, code: 'too_far', message: 'That release is more than a year away.' };

  const alarm_id = `alm_${crypto.randomUUID().replace(/-/g, '')}`;
  // The member's own request is the evidence the card rests on: an episode holding it, and a fact quoting it. Code writes both.
  const text = `Alert me when ${rule.venue_name} releases tables for ${reservation_date}, party of ${party}.`;
  const episode_id = `ep_${crypto.randomUUID().replace(/-/g, '').slice(0, 24)}`;
  await db
    .prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, occurred_at, redaction_status) VALUES (?, ?, 'user_input', ?, ?, NULL, 'clean')`)
    .bind(episode_id, household_id, text, await sha256Hex(`${alarm_id}:${text}`))
    .run();
  const fact = await writeFact(db, {
    household_id,
    source_episode_id: episode_id,
    subject: { entity_id: venue_entity_id },
    predicate: 'drop_alarm_target',
    value_text: `${reservation_date}|${party}`,
    confidence: 1,
    quotes: [{ quote: text, char_start: 0, char_end: text.length }],
  });
  if (fact.status !== 'written' && fact.status !== 'reaffirmed') return { ok: false, code: 'bad_input', message: 'The alarm could not be recorded.' };
  const release_at = new Date(release).toISOString();
  try {
    await db
      .prepare(
        `INSERT INTO drop_alarms (alarm_id, household_id, venue_entity_id, reservation_date, party_size, release_at, grounding_fact_id, status)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'scheduled')`,
      )
      .bind(alarm_id, household_id, venue_entity_id, reservation_date, party, release_at, fact.fact_id)
      .run();
  } catch (err) {
    if (/UNIQUE/i.test(err instanceof Error ? err.message : String(err))) return { ok: false, code: 'duplicate', message: 'You already have an alarm for that venue, date and party size.' };
    throw err;
  }
  return { ok: true, alarm_id, release_at, sleep_ms };
}
