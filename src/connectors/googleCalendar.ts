/**
 * src/connectors/googleCalendar.ts — read-only Google Calendar access (CLAUDE.md §1: scopes
 * `openid email calendar.events.readonly calendar.freebusy`, nothing else, never a Gmail scope).
 *
 *   OAuth      buildAuthUrl() asks for exactly GOOGLE_SCOPES. exchangeCode() REFUSES (and stores nothing) if Google
 *              answers with any scope outside that list. Tokens are stored encrypted in KV (AES-GCM under the
 *              household DEK, AAD = kv:<household>:<key>); connectors.token_kv_key holds only the KV key.
 *   Sync       the last 365 days and next 180 days, then incremental via syncToken (cursor kept in connectors.sync_cursor).
 *              A 410 from Google, or a forward window that has shrunk below 120 days, triggers a full re-list.
 *   Extract    deterministic, no model: birthdays/anniversaries (yearly occasions), trips, and the household's free
 *              Fri/Sat 18:00-22:00 windows from freeBusy. Ordinary meetings are never stored.
 *
 * All HTTP goes through the guarded fetch passed in `deps.fetch` (src/net/egress.ts).
 */

import { decryptForHousehold, encryptForHousehold, fromBase64Bytes, toBase64 } from '../crypto/vault';
import type { Env } from '../types';

export const GOOGLE_SCOPES = [
  'openid',
  'email',
  'https://www.googleapis.com/auth/calendar.events.readonly',
  'https://www.googleapis.com/auth/calendar.freebusy',
] as const;

const AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth';
const TOKEN_URL = 'https://oauth2.googleapis.com/token';
const EVENTS_URL = 'https://www.googleapis.com/calendar/v3/calendars/primary/events';
const FREEBUSY_URL = 'https://www.googleapis.com/calendar/v3/freeBusy';

export const SYNC_PAST_DAYS = 365;
export const SYNC_FUTURE_DAYS = 180;
/** A cursor whose forward window ends sooner than this is replaced by a full sync. */
export const MIN_FORWARD_DAYS = 120;
const MAX_PAGES = 20;
const DAY_MS = 86_400_000;

export class ScopeViolationError extends Error {
  override readonly name = 'ScopeViolationError';
  constructor(readonly granted: string) {
    super(`Google returned scopes outside the allowed set: ${granted}`);
  }
}

export class CalendarAuthError extends Error {
  override readonly name = 'CalendarAuthError';
}

export class CalendarApiError extends Error {
  override readonly name = 'CalendarApiError';
  constructor(readonly status: number, message: string) {
    super(message);
  }
}

export interface CalendarDeps {
  env: Pick<Env, 'DB' | 'KV' | 'DOCS_BUCKET' | 'KEK_B64' | 'GOOGLE_CLIENT_ID' | 'GOOGLE_CLIENT_SECRET'>;
  /** Guarded fetch. */
  fetch: typeof fetch;
  now: () => Date;
}

// ─── OAuth ──────────────────────────────────────────────────────────────────

export function buildAuthUrl(o: { clientId: string; redirectUri: string; state: string; codeChallenge?: string }): string {
  const u = new URL(AUTH_URL);
  u.searchParams.set('client_id', o.clientId);
  u.searchParams.set('redirect_uri', o.redirectUri);
  u.searchParams.set('response_type', 'code');
  u.searchParams.set('scope', GOOGLE_SCOPES.join(' '));
  u.searchParams.set('access_type', 'offline');
  u.searchParams.set('prompt', 'consent');
  u.searchParams.set('include_granted_scopes', 'false');
  u.searchParams.set('state', o.state);
  if (o.codeChallenge !== undefined) {
    u.searchParams.set('code_challenge', o.codeChallenge);
    u.searchParams.set('code_challenge_method', 'S256');
  }
  return u.href;
}

interface StoredTokens {
  access_token: string;
  refresh_token: string;
  /** epoch ms */
  expires_at: number;
  scope: string;
}

interface TokenResponse {
  access_token?: string;
  refresh_token?: string;
  expires_in?: number;
  scope?: string;
  id_token?: string;
  error?: string;
  error_description?: string;
}

/** Google reports the `email` scope back in its long form; it is the same permission, not an extra one. */
const SCOPE_ALIASES = ['https://www.googleapis.com/auth/userinfo.email'] as const;

function assertAllowedScopes(granted: string): void {
  const allowed = new Set<string>([...GOOGLE_SCOPES, ...SCOPE_ALIASES]);
  const extra = granted.split(/\s+/).filter((s) => s !== '' && !allowed.has(s));
  if (extra.length > 0) throw new ScopeViolationError(extra.join(' '));
}

function kvKeyFor(connector_id: string): string {
  return `gcal:${connector_id}`;
}

const aadFor = (household_id: string, key: string): string => `kv:${household_id}:${key}`;

async function saveTokens(deps: CalendarDeps, household_id: string, key: string, tokens: StoredTokens): Promise<void> {
  const blob = await encryptForHousehold(deps.env, household_id, aadFor(household_id, key), new TextEncoder().encode(JSON.stringify(tokens)));
  await deps.env.KV.put(key, toBase64(blob));
}

async function loadTokens(deps: CalendarDeps, household_id: string, key: string): Promise<StoredTokens> {
  const stored = await deps.env.KV.get(key);
  if (stored === null) throw new CalendarAuthError('calendar tokens are missing; the household must reconnect');
  const plain = await decryptForHousehold(deps.env, household_id, aadFor(household_id, key), fromBase64Bytes(stored));
  return JSON.parse(new TextDecoder().decode(plain)) as StoredTokens;
}

async function postToken(deps: CalendarDeps, form: Record<string, string>): Promise<{ status: number; body: TokenResponse }> {
  const res = await deps.fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams(form).toString(),
  });
  let body: TokenResponse = {};
  try {
    body = (await res.json()) as TokenResponse;
  } catch {
    body = {};
  }
  return { status: res.status, body };
}

export interface ConnectorRecord {
  connector_id: string;
  household_id: string;
  status: string;
  token_kv_key: string | null;
  sync_cursor: string | null;
}

function emailFromIdToken(idToken: string | undefined): string | null {
  if (idToken === undefined) return null;
  try {
    const payload = idToken.split('.')[1] ?? '';
    const json = JSON.parse(atob(payload.replace(/-/g, '+').replace(/_/g, '/'))) as { email?: unknown };
    return typeof json.email === 'string' ? json.email : null;
  } catch {
    return null;
  }
}

/** Exchanges an authorization code and activates the household's calendar connector. */
export async function exchangeCode(
  deps: CalendarDeps,
  household_id: string,
  code: string,
  redirectUri: string,
): Promise<{ connector_id: string; email: string | null }> {
  const { status, body } = await postToken(deps, {
    code,
    client_id: deps.env.GOOGLE_CLIENT_ID,
    client_secret: deps.env.GOOGLE_CLIENT_SECRET,
    redirect_uri: redirectUri,
    grant_type: 'authorization_code',
  });
  if (status !== 200 || !body.access_token) {
    throw new CalendarAuthError(`token exchange failed: ${body.error ?? status}`);
  }
  assertAllowedScopes(body.scope ?? '');
  const existing = await deps.env.DB.prepare(`SELECT connector_id, token_kv_key FROM connectors WHERE household_id = ? AND kind = 'google_calendar'`)
    .bind(household_id)
    .first<{ connector_id: string; token_kv_key: string | null }>();
  const connector_id = existing?.connector_id ?? `con_${crypto.randomUUID().replace(/-/g, '')}`;
  const key = kvKeyFor(connector_id);
  // A refresh token is only issued on first consent; keep the old one if Google does not send a new one.
  let refresh = body.refresh_token;
  if (refresh === undefined && existing?.token_kv_key) refresh = (await loadTokens(deps, household_id, existing.token_kv_key).catch(() => null))?.refresh_token;
  if (refresh === undefined) throw new CalendarAuthError('Google did not return a refresh token; reconnect with consent');
  await saveTokens(deps, household_id, key, {
    access_token: body.access_token,
    refresh_token: refresh,
    expires_at: deps.now().getTime() + (body.expires_in ?? 3600) * 1000,
    scope: body.scope ?? '',
  });
  await deps.env.DB.prepare(
    `INSERT INTO connectors (connector_id, household_id, kind, status, scopes, token_kv_key, sync_cursor, last_error)
     VALUES (?, ?, 'google_calendar', 'active', ?, ?, NULL, NULL)
     ON CONFLICT (connector_id) DO UPDATE SET status = 'active', scopes = excluded.scopes, token_kv_key = excluded.token_kv_key,
                                              sync_cursor = NULL, last_error = NULL`,
  )
    .bind(connector_id, household_id, body.scope ?? '', key)
    .run();
  return { connector_id, email: emailFromIdToken(body.id_token) };
}

export async function findCalendarConnector(db: D1Database, household_id: string): Promise<ConnectorRecord | null> {
  return db
    .prepare(`SELECT connector_id, household_id, status, token_kv_key, sync_cursor FROM connectors WHERE household_id = ? AND kind = 'google_calendar' AND status = 'active'`)
    .bind(household_id)
    .first<ConnectorRecord>();
}

async function markConnector(db: D1Database, connector_id: string, status: 'active' | 'error' | 'revoked', error: string | null): Promise<void> {
  await db.prepare('UPDATE connectors SET status = ?, last_error = ? WHERE connector_id = ?').bind(status, error, connector_id).run();
}

/** A valid access token, refreshed (and re-stored encrypted) when it is within 60 s of expiry. */
export async function getAccessToken(deps: CalendarDeps, connector: ConnectorRecord, forceRefresh = false): Promise<string> {
  if (connector.token_kv_key === null) throw new CalendarAuthError('connector has no token');
  const tokens = await loadTokens(deps, connector.household_id, connector.token_kv_key);
  if (!forceRefresh && tokens.expires_at - deps.now().getTime() > 60_000) return tokens.access_token;
  const { status, body } = await postToken(deps, {
    client_id: deps.env.GOOGLE_CLIENT_ID,
    client_secret: deps.env.GOOGLE_CLIENT_SECRET,
    refresh_token: tokens.refresh_token,
    grant_type: 'refresh_token',
  });
  if (status !== 200 || !body.access_token) {
    const revoked = body.error === 'invalid_grant';
    await markConnector(deps.env.DB, connector.connector_id, revoked ? 'revoked' : 'error', `refresh failed: ${body.error ?? status}`);
    throw new CalendarAuthError(`token refresh failed: ${body.error ?? status}`);
  }
  if (body.scope !== undefined) assertAllowedScopes(body.scope);
  await saveTokens(deps, connector.household_id, connector.token_kv_key, {
    access_token: body.access_token,
    refresh_token: body.refresh_token ?? tokens.refresh_token,
    expires_at: deps.now().getTime() + (body.expires_in ?? 3600) * 1000,
    scope: body.scope ?? tokens.scope,
  });
  return body.access_token;
}

async function googleGet(deps: CalendarDeps, connector: ConnectorRecord, url: URL, init?: { method: 'POST'; body: string }): Promise<unknown> {
  for (let attempt = 0; attempt < 2; attempt++) {
    const token = await getAccessToken(deps, connector, attempt > 0);
    const res = await deps.fetch(url.href, {
      method: init?.method ?? 'GET',
      headers: { authorization: `Bearer ${token}`, ...(init ? { 'content-type': 'application/json' } : {}) },
      ...(init ? { body: init.body } : {}),
    });
    if (res.status === 401 && attempt === 0) continue;
    if (!res.ok) throw new CalendarApiError(res.status, `Google Calendar ${res.status}: ${(await res.text()).slice(0, 200)}`);
    return res.json();
  }
  throw new CalendarAuthError('Google rejected a freshly refreshed token');
}

// ─── events ─────────────────────────────────────────────────────────────────

export interface GoogleEvent {
  id: string;
  status?: string;
  summary?: string;
  start?: { date?: string; dateTime?: string };
  end?: { date?: string; dateTime?: string };
  recurringEventId?: string;
  transparency?: string;
  attendees?: Array<{ self?: boolean; responseStatus?: string }>;
}

export interface SyncCursor {
  syncToken: string | null;
  window_start: string;
  window_end: string;
  /** Classified events currently reflected in the graph: event id -> what they became. */
  tracked: Record<string, { kind: 'occasion' | 'trip'; name: string }>;
}

export function parseCursor(raw: string | null): SyncCursor | null {
  if (raw === null) return null;
  try {
    const c = JSON.parse(raw) as SyncCursor;
    return typeof c.window_end === 'string' && typeof c.tracked === 'object' ? c : null;
  } catch {
    return null;
  }
}

export interface EventPage {
  events: GoogleEvent[];
  nextSyncToken: string | null;
  mode: 'full' | 'incremental';
  window_start: string;
  window_end: string;
}

/** Lists events: incremental when the cursor is usable, otherwise the full 365-back / 180-forward window. */
export async function listEvents(deps: CalendarDeps, connector: ConnectorRecord, cursor: SyncCursor | null): Promise<EventPage> {
  const now = deps.now();
  const today = now.toISOString().slice(0, 10);
  const forwardDays = cursor === null ? 0 : (Date.parse(`${cursor.window_end}T00:00:00Z`) - Date.parse(`${today}T00:00:00Z`)) / DAY_MS;
  const incremental = cursor !== null && cursor.syncToken !== null && forwardDays >= MIN_FORWARD_DAYS;

  const run = async (useToken: boolean): Promise<EventPage> => {
    const start = new Date(now.getTime() - SYNC_PAST_DAYS * DAY_MS).toISOString().slice(0, 10);
    const end = new Date(now.getTime() + SYNC_FUTURE_DAYS * DAY_MS).toISOString().slice(0, 10);
    const events: GoogleEvent[] = [];
    let pageToken: string | null = null;
    let nextSyncToken: string | null = null;
    for (let page = 0; page < MAX_PAGES; page++) {
      const url = new URL(EVENTS_URL);
      url.searchParams.set('maxResults', '2500');
      url.searchParams.set('showDeleted', 'true');
      if (useToken && cursor?.syncToken) {
        url.searchParams.set('syncToken', cursor.syncToken);
      } else {
        url.searchParams.set('singleEvents', 'true');
        url.searchParams.set('timeMin', `${start}T00:00:00Z`);
        url.searchParams.set('timeMax', `${end}T00:00:00Z`);
      }
      if (pageToken !== null) url.searchParams.set('pageToken', pageToken);
      const body = (await googleGet(deps, connector, url)) as { items?: GoogleEvent[]; nextPageToken?: string; nextSyncToken?: string };
      events.push(...(body.items ?? []));
      if (body.nextPageToken) pageToken = body.nextPageToken;
      else {
        nextSyncToken = body.nextSyncToken ?? null;
        break;
      }
    }
    return {
      events,
      nextSyncToken,
      mode: useToken ? 'incremental' : 'full',
      window_start: useToken && cursor ? cursor.window_start : start,
      window_end: useToken && cursor ? cursor.window_end : end,
    };
  };

  if (!incremental) return run(false);
  try {
    return await run(true);
  } catch (err) {
    if (err instanceof CalendarApiError && err.status === 410) return run(false); // sync token expired
    throw err;
  }
}

// ─── classification (deterministic) ─────────────────────────────────────────

export type EventKind = 'birthday' | 'anniversary' | 'trip';

export interface ClassifiedEvent {
  id: string;
  kind: EventKind;
  /** Entity name: the event title (occasions) or title (trips). */
  name: string;
  /** Occasions: the instance date. Trips: first day. */
  start: string;
  /** Trips: last day, inclusive. Occasions: same as start. */
  end: string;
  allDay: boolean;
}

const OCCASION = /\b(birthday|b-?day|anniversary)\b/i;
const TRIP = /\b(trip|vacation|holiday|flight|travel|getaway|honeymoon)\b/i;

function dateOf(t: { date?: string; dateTime?: string } | undefined): string | null {
  const raw = t?.date ?? t?.dateTime;
  return raw !== undefined && /^\d{4}-\d{2}-\d{2}/.test(raw) ? raw.slice(0, 10) : null;
}

function addDays(date: string, days: number): string {
  return new Date(Date.parse(`${date}T00:00:00Z`) + days * DAY_MS).toISOString().slice(0, 10);
}

/** Cancelled and declined events are ignored; only occasions and trips that have not finished are returned. */
export function classifyEvent(e: GoogleEvent, today: string): ClassifiedEvent | null {
  if (e.status === 'cancelled') return null;
  if (e.attendees?.some((a) => a.self === true && a.responseStatus === 'declined')) return null;
  const summary = (e.summary ?? '').replace(/\s+/g, ' ').trim().slice(0, 80);
  const start = dateOf(e.start);
  if (summary === '' || start === null) return null;
  const allDay = e.start?.date !== undefined;
  const endRaw = dateOf(e.end) ?? start;
  // Google's all-day end date is exclusive.
  const endDate = allDay && endRaw > start ? addDays(endRaw, -1) : endRaw;
  if (OCCASION.test(summary)) {
    if (start < today) return null;
    return { id: e.id, kind: /anniversary/i.test(summary) ? 'anniversary' : 'birthday', name: summary, start, end: start, allDay };
  }
  const spanDays = Math.round((Date.parse(`${endDate}T00:00:00Z`) - Date.parse(`${start}T00:00:00Z`)) / DAY_MS) + 1;
  if (TRIP.test(summary) || (allDay && spanDays >= 3)) {
    if (endDate < today) return null;
    return { id: e.id, kind: 'trip', name: summary, start, end: endDate < start ? start : endDate, allDay };
  }
  return null;
}

// ─── free windows ───────────────────────────────────────────────────────────

export const EVENING_START = '18:00';
export const EVENING_END = '22:00';
export const MIN_FREE_WINDOW_MINUTES = 120;

function tzParts(ms: number, timeZone: string): { y: number; mo: number; d: number; h: number; mi: number; s: number } {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    hourCycle: 'h23',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).formatToParts(new Date(ms));
  const get = (t: string): number => Number(parts.find((p) => p.type === t)?.value ?? 0);
  return { y: get('year'), mo: get('month'), d: get('day'), h: get('hour'), mi: get('minute'), s: get('second') };
}

/** The UTC instant (ms) at which the wall clock in `timeZone` reads `date` + `time`. DST-safe for normal times. */
export function localToUtcMs(date: string, time: string, timeZone: string): number {
  const [y, mo, d] = date.split('-').map(Number) as [number, number, number];
  const [h, mi] = time.split(':').map(Number) as [number, number];
  const wall = Date.UTC(y, mo - 1, d, h, mi);
  let guess = wall;
  for (let i = 0; i < 3; i++) {
    const p = tzParts(guess, timeZone);
    const shown = Date.UTC(p.y, p.mo - 1, p.d, p.h, p.mi, p.s);
    guess += wall - shown;
  }
  return guess;
}

export function localClock(ms: number, timeZone: string): string {
  const p = tzParts(ms, timeZone);
  return `${String(p.h).padStart(2, '0')}:${String(p.mi).padStart(2, '0')}`;
}

export interface FreeWindow {
  /** Local calendar date of the evening. */
  date: string;
  weekday: 'Fri' | 'Sat';
  startMs: number;
  endMs: number;
}

export interface BusyBlock {
  start: string;
  end: string;
}

/**
 * For every Friday and Saturday from `today` for `horizonDays`: the longest free stretch inside 18:00-22:00 local,
 * if it is at least MIN_FREE_WINDOW_MINUTES long. `nowMs` clips tonight's window so it never starts in the past.
 */
export function computeFreeWindows(busy: readonly BusyBlock[], timeZone: string, today: string, horizonDays: number, nowMs: number): FreeWindow[] {
  const blocks = busy
    .map((b) => ({ s: Date.parse(b.start), e: Date.parse(b.end) }))
    .filter((b) => Number.isFinite(b.s) && Number.isFinite(b.e) && b.e > b.s)
    .sort((a, b) => a.s - b.s);
  const out: FreeWindow[] = [];
  for (let i = 0; i < horizonDays; i++) {
    const date = addDays(today, i);
    const dow = new Date(`${date}T00:00:00Z`).getUTCDay();
    if (dow !== 5 && dow !== 6) continue;
    let cursor = Math.max(localToUtcMs(date, EVENING_START, timeZone), nowMs);
    const end = localToUtcMs(date, EVENING_END, timeZone);
    let best: { s: number; e: number } | null = null;
    const consider = (s: number, e: number): void => {
      if (e - s >= MIN_FREE_WINDOW_MINUTES * 60_000 && (best === null || e - s > best.e - best.s)) best = { s, e };
    };
    for (const b of blocks) {
      if (b.e <= cursor || b.s >= end) continue;
      if (b.s > cursor) consider(cursor, Math.min(b.s, end));
      cursor = Math.max(cursor, b.e);
      if (cursor >= end) break;
    }
    if (cursor < end) consider(cursor, end);
    if (best !== null) {
      const w = best as { s: number; e: number };
      out.push({ date, weekday: dow === 5 ? 'Fri' : 'Sat', startMs: w.s, endMs: w.e });
    }
  }
  return out;
}

/** Busy blocks of the primary calendar for the next `horizonDays` (freeBusy.query, scope calendar.freebusy). */
export async function queryBusy(deps: CalendarDeps, connector: ConnectorRecord, timeZone: string, horizonDays: number): Promise<BusyBlock[]> {
  const now = deps.now();
  const body = (await googleGet(deps, connector, new URL(FREEBUSY_URL), {
    method: 'POST',
    body: JSON.stringify({
      timeMin: now.toISOString(),
      timeMax: new Date(now.getTime() + horizonDays * DAY_MS).toISOString(),
      timeZone,
      items: [{ id: 'primary' }],
    }),
  })) as { calendars?: { primary?: { busy?: BusyBlock[]; errors?: unknown[] } } };
  if (body.calendars?.primary?.errors !== undefined && body.calendars.primary.errors.length > 0) {
    throw new CalendarApiError(502, 'freeBusy returned errors for the primary calendar');
  }
  return body.calendars?.primary?.busy ?? [];
}

export function windowLine(w: FreeWindow, timeZone: string): string {
  return `Free window ${w.weekday} ${w.date} ${localClock(w.startMs, timeZone)}-${localClock(w.endMs, timeZone)} ${timeZone}`;
}

export function windowEntityName(date: string): string {
  return `evening:${date}`;
}

export function isoMs(ms: number): string {
  return new Date(ms).toISOString();
}
