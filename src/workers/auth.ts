/**
 * src/workers/auth.ts — Google sign-in, the session cookie, and the middleware that protects every other /api route.
 *
 *   GET  /api/auth/google/login      302 to Google (scopes: openid email calendar.events.readonly calendar.freebusy; PKCE S256; state).
 *                                    The state and PKCE verifier travel in a sealed, 10-minute __Host-oauth cookie.
 *   GET  /api/auth/google/callback   checks state, exchanges the code (+ verifier), validates the id_token (issuer, audience, expiry,
 *                                    verified e-mail), finds or creates the household + owner member, stores the calendar tokens
 *                                    encrypted (KV, per-household key), and answers 302 / with a sealed, HttpOnly, Secure __Host-session cookie.
 *   GET  /api/auth/me, POST /api/auth/logout
 *   authenticate()                   used by handleApi for every protected route: sealed session cookie OR Bearer token, then the member
 *                                    must still belong to a live household. The household id comes only from the sealed cookie / token.
 *
 * Cookie-authenticated unsafe requests must also carry an Origin header equal to the request's own origin (CSRF); Bearer requests are
 * not ambient credentials and are exempt.
 */

import {
  CalendarAuthError, IdentityError, ScopeViolationError, buildAuthUrl, parseIdToken, requestTokens, storeConnector,
  type CalendarDeps,
} from '../connectors/googleCalendar';
import { GmailConsentError, buildGmailAuthUrl, requestGmailTokens } from '../connectors/gmailConsent';
import { GmailScopeError, storeGmailConnector } from '../connectors/gmailSync';
import type { Env } from '../types';
import { trialGrantStatement } from '../billing/credits';
import { error, json, newId } from './http';
import {
  OAUTH_COOKIE, OAUTH_TTL_SECONDS, SESSION_COOKIE, SESSION_TTL_SECONDS, b64url, clearCookieHeader, openCookie, readCookie, sealCookie,
  setCookieHeader, timingSafeEqual, verifySessionToken,
} from './session';

export interface Auth {
  household_id: string;
  member_id: string;
  via: 'cookie' | 'bearer';
}

export interface AuthDeps {
  now: () => Date;
  /** Guarded fetch used for Google's token endpoint. */
  fetch: typeof fetch;
}

const CALLBACK_PATH = '/api/auth/google/callback';
export const GMAIL_CONNECT_PATH = '/api/auth/google/gmail-connect';
export const GMAIL_CALLBACK_PATH = '/api/auth/google/gmail-callback';
const BASE32 = 'abcdefghijklmnopqrstuvwxyz234567';

const randomB64url = (bytes: number): string => b64url(crypto.getRandomValues(new Uint8Array(bytes)));

function randomAlias(): string {
  let s = 'h_';
  for (const b of crypto.getRandomValues(new Uint8Array(10))) s += BASE32[b % 32];
  return s;
}

async function audit(db: D1Database, household_id: string | null, action: string, detail: Record<string, unknown>, at: Date): Promise<void> {
  await db
    .prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'auth', ?, ?, ?)`)
    .bind(newId('aud'), household_id, action, JSON.stringify(detail), at.toISOString())
    .run();
}

// ─── middleware ─────────────────────────────────────────────────────────────

/** Resolves the caller, or null. A malformed Authorization header never falls back to the cookie. */
export async function authenticate(request: Request, env: Env, now: Date): Promise<Auth | null> {
  let claims: { household_id: string; member_id: string } | null = null;
  let via: Auth['via'] = 'cookie';
  const header = request.headers.get('authorization');
  if (header !== null) {
    const match = /^Bearer\s+(\S+)$/i.exec(header);
    if (match === null) return null;
    claims = await verifySessionToken(env.SESSION_SECRET, match[1]!, now);
    via = 'bearer';
  } else {
    const raw = readCookie(request, SESSION_COOKIE);
    if (raw === null) return null;
    const sealed = await openCookie(env.SESSION_SECRET, 'session', raw, now);
    if (sealed !== null && typeof sealed['household_id'] === 'string' && typeof sealed['member_id'] === 'string') {
      claims = { household_id: sealed['household_id'], member_id: sealed['member_id'] };
    }
  }
  if (claims === null) return null;
  const row = await env.DB.prepare(
    `SELECT 1 AS ok FROM members m JOIN households h ON h.household_id = m.household_id
      WHERE m.member_id = ? AND m.household_id = ? AND h.deleted_at IS NULL`,
  )
    .bind(claims.member_id, claims.household_id)
    .first<{ ok: number }>();
  return row === null ? null : { ...claims, via };
}

/** CSRF: an unsafe, cookie-authenticated request must come from this very origin. */
export function passesCsrf(request: Request, auth: Auth): boolean {
  if (auth.via === 'bearer' || ['GET', 'HEAD', 'OPTIONS'].includes(request.method)) return true;
  return request.headers.get('origin') === new URL(request.url).origin;
}

// ─── login ──────────────────────────────────────────────────────────────────

export async function handleGoogleLogin(request: Request, env: Env, deps: AuthDeps): Promise<Response> {
  if (!env.GOOGLE_CLIENT_ID || !env.GOOGLE_CLIENT_SECRET) return error(503, 'not_configured', 'Google sign-in is not configured.');
  const url = new URL(request.url);
  const state = randomB64url(24);
  const verifier = randomB64url(32);
  const challenge = b64url(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier))));
  const sealed = await sealCookie(env.SESSION_SECRET, 'oauth', { state, verifier, attest18: url.searchParams.get('attest_18') === '1' }, OAUTH_TTL_SECONDS, deps.now());
  const location = buildAuthUrl({ clientId: env.GOOGLE_CLIENT_ID, redirectUri: `${url.origin}${CALLBACK_PATH}`, state, codeChallenge: challenge });
  return new Response(null, { status: 302, headers: { location, 'set-cookie': setCookieHeader(OAUTH_COOKIE, sealed, OAUTH_TTL_SECONDS), 'cache-control': 'no-store' } });
}

// ─── callback ───────────────────────────────────────────────────────────────

interface MemberRow {
  member_id: string;
  household_id: string;
  display_name: string | null;
}

async function findMember(db: D1Database, email: string): Promise<MemberRow | null> {
  return db
    .prepare(
      `SELECT m.member_id, m.household_id, m.display_name FROM members m JOIN households h ON h.household_id = m.household_id
        WHERE lower(m.email) = ? AND h.deleted_at IS NULL ORDER BY m.created_at, m.member_id LIMIT 1`,
    )
    .bind(email)
    .first<MemberRow>();
}

/**
 * Creates a household with its owner. Both inserts are conditional on no live member owning the e-mail yet, so two callbacks racing
 * for a brand-new address cannot make two households: the loser's statements become no-ops and it re-reads the winner's rows.
 */
async function createHousehold(db: D1Database, email: string, name: string | null): Promise<{ member: MemberRow; created: boolean }> {
  const household_id = newId('hh');
  const member_id = newId('mem');
  const free = `NOT EXISTS (SELECT 1 FROM members m JOIN households h ON h.household_id = m.household_id WHERE lower(m.email) = ?3 AND h.deleted_at IS NULL AND h.household_id <> ?1)`;
  for (let attempt = 0; ; attempt++) {
    try {
      await db.batch([
        db
          .prepare(`INSERT INTO households (household_id, shard, alias_local, plan) SELECT ?1, 0, ?2, 'trial' WHERE ${free}`)
          .bind(household_id, randomAlias(), email),
        db
          .prepare(
            `INSERT INTO members (member_id, household_id, email, display_name, role, age_attested_18)
             SELECT ?4, ?1, ?3, ?5, 'owner', 1 WHERE EXISTS (SELECT 1 FROM households WHERE household_id = ?1) AND ${free}`,
          )
          .bind(household_id, '', email, member_id, name),
        // Day-1 grant: enough for the three cold-start cards and one multi-credit goal. Idempotent, and only for the creation that won.
        trialGrantStatement(db, household_id, new Date().toISOString(), member_id),
      ]);
      break;
    } catch (err) {
      // alias_local is UNIQUE; a 1-in-10^15 collision is retried with a fresh alias.
      if (attempt < 2 && /alias_local/i.test(err instanceof Error ? err.message : String(err))) continue;
      throw err;
    }
  }
  const member = await findMember(db, email);
  if (member === null) throw new Error('household creation did not produce a member');
  return { member, created: member.household_id === household_id };
}

export async function handleGoogleCallback(request: Request, env: Env, deps: AuthDeps): Promise<Response> {
  const now = deps.now();
  const clearOauth = { 'set-cookie': clearCookieHeader(OAUTH_COOKIE) };
  const fail = (status: number, code: string, message: string): Response => error(status, code, message, clearOauth);
  if (!env.GOOGLE_CLIENT_ID || !env.GOOGLE_CLIENT_SECRET) return fail(503, 'not_configured', 'Google sign-in is not configured.');

  const url = new URL(request.url);
  if (url.searchParams.get('error') !== null) return fail(400, 'oauth_denied', 'Google sign-in was cancelled or denied.');
  const code = url.searchParams.get('code');
  const state = url.searchParams.get('state');
  if (code === null || state === null || code === '' || state === '') return fail(400, 'bad_request', 'Missing code or state.');

  const rawCookie = readCookie(request, OAUTH_COOKIE);
  const sealed = rawCookie === null ? null : await openCookie(env.SESSION_SECRET, 'oauth', rawCookie, now);
  if (sealed === null || typeof sealed['state'] !== 'string' || typeof sealed['verifier'] !== 'string' || !timingSafeEqual(sealed['state'], state)) {
    return fail(400, 'bad_state', 'The sign-in attempt expired or did not start here. Try again.');
  }

  const calDeps: CalendarDeps = { env, fetch: deps.fetch, now: deps.now };
  let grant;
  try {
    grant = await requestTokens(calDeps, code, `${url.origin}${CALLBACK_PATH}`, sealed['verifier']);
  } catch (err) {
    if (err instanceof ScopeViolationError) return fail(403, 'scope_violation', 'Google granted more access than this app asks for. Sign-in was refused.');
    if (err instanceof CalendarAuthError) return fail(502, 'token_exchange_failed', 'Google did not accept the sign-in code.');
    throw err;
  }
  let identity;
  try {
    identity = parseIdToken(grant.id_token, { clientId: env.GOOGLE_CLIENT_ID, nowMs: now.getTime() });
  } catch (err) {
    if (err instanceof IdentityError) return fail(401, 'identity_rejected', err.message);
    throw err;
  }

  let member = await findMember(env.DB, identity.email);
  let created = false;
  if (member === null) {
    if (sealed['attest18'] !== true) return fail(403, 'age_attestation_required', 'Confirm you are 18 or older to create an account.');
    ({ member, created } = await createHousehold(env.DB, identity.email, identity.name));
    if (created) await audit(env.DB, member.household_id, 'household_created', { member_id: member.member_id, google_sub: identity.sub }, now);
  }
  if (member.display_name === null && identity.name !== null) {
    await env.DB.prepare('UPDATE members SET display_name = ? WHERE member_id = ? AND display_name IS NULL').bind(identity.name, member.member_id).run();
  }

  // Calendar access is part of the sign-in grant. Without a refresh token (Google issues one only on first consent) the member is
  // still signed in and told to reconnect.
  let calendar: 'connected' | 'reconnect' = 'connected';
  try {
    await storeConnector(calDeps, member.household_id, grant);
  } catch (err) {
    if (!(err instanceof CalendarAuthError)) throw err;
    calendar = 'reconnect';
  }
  await audit(env.DB, member.household_id, 'signin', { member_id: member.member_id, google_sub: identity.sub, calendar, created }, now);

  const session = await sealCookie(env.SESSION_SECRET, 'session', { household_id: member.household_id, member_id: member.member_id }, SESSION_TTL_SECONDS, now);
  const headers = new Headers({ location: calendar === 'connected' ? '/?signed_in=1' : '/?signed_in=1&calendar=reconnect', 'cache-control': 'no-store' });
  headers.append('set-cookie', setCookieHeader(SESSION_COOKIE, session, SESSION_TTL_SECONDS));
  headers.append('set-cookie', clearCookieHeader(OAUTH_COOKIE));
  return new Response(null, { status: 302, headers });
}

// ─── me / logout ────────────────────────────────────────────────────────────

export async function handleMe(env: Env, auth: Auth): Promise<Response> {
  const row = await env.DB.prepare(
    `SELECT m.member_id, m.household_id, m.email, m.display_name, m.role, h.plan, h.timezone, h.alias_local
       FROM members m JOIN households h ON h.household_id = m.household_id WHERE m.member_id = ? AND m.household_id = ?`,
  )
    .bind(auth.member_id, auth.household_id)
    .first<Record<string, unknown>>();
  if (row === null) return error(401, 'unauthorized', 'Sign in again.');
  const connector = await env.DB.prepare(`SELECT 1 AS ok FROM connectors WHERE household_id = ? AND kind = 'google_calendar' AND status = 'active'`).bind(auth.household_id).first<{ ok: number }>();
  return json(200, { ...row, calendar_connected: connector !== null });
}

export function handleLogout(): Response {
  return json(200, { status: 'signed_out' }, { 'set-cookie': clearCookieHeader(SESSION_COOKIE) });
}

// ─── Gmail consent (feature-flagged; restricted scope, Claude.md §0 and §9) ─────────────────────────────────────

/** 403 CASA_PENDING unless ENABLE_GMAIL_OAUTH is exactly "true". Called before anything else, so no Google URL is ever built while it is off. */
export function gmailOauthDisabled(env: Pick<Env, 'ENABLE_GMAIL_OAUTH'>): Response | null {
  return env.ENABLE_GMAIL_OAUTH === 'true' ? null : json(403, { error: 'CASA_PENDING', message: 'Connecting Gmail is not available yet.' });
}

/** GET /api/auth/google/gmail-connect (signed in): 302 to Google for the Gmail read-only scope, PKCE S256. Which member and household is sealed in the cookie. */
export async function handleGmailConnect(request: Request, env: Env, auth: Auth, deps: AuthDeps): Promise<Response> {
  if (!env.GOOGLE_CLIENT_ID || !env.GOOGLE_CLIENT_SECRET) return error(503, 'not_configured', 'Google sign-in is not configured.');
  const url = new URL(request.url);
  const state = randomB64url(24);
  const verifier = randomB64url(32);
  const challenge = b64url(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier))));
  const sealed = await sealCookie(env.SESSION_SECRET, 'gmail_oauth', { state, verifier, household_id: auth.household_id, member_id: auth.member_id }, OAUTH_TTL_SECONDS, deps.now());
  const location = buildGmailAuthUrl({ clientId: env.GOOGLE_CLIENT_ID, redirectUri: `${url.origin}${GMAIL_CALLBACK_PATH}`, state, codeChallenge: challenge });
  return new Response(null, { status: 302, headers: { location, 'set-cookie': setCookieHeader(OAUTH_COOKIE, sealed, OAUTH_TTL_SECONDS), 'cache-control': 'no-store' } });
}

/**
 * GET /api/auth/google/gmail-callback: public (Google redirects the browser here), authenticated by the sealed cookie the connect step set. It checks
 * state, exchanges the code with the verifier, and has storeGmailConnector() refuse any grant but the Gmail read-only scope and keep the tokens
 * AES-GCM-encrypted in KV. inbox_connections gets its row, so the setup endpoint and the first sync have somewhere to put the cursor.
 */
export async function handleGmailCallback(request: Request, env: Env, deps: AuthDeps): Promise<Response> {
  const now = deps.now();
  const clearOauth = { 'set-cookie': clearCookieHeader(OAUTH_COOKIE) };
  const fail = (status: number, code: string, message: string): Response => error(status, code, message, clearOauth);
  if (!env.GOOGLE_CLIENT_ID || !env.GOOGLE_CLIENT_SECRET) return fail(503, 'not_configured', 'Google sign-in is not configured.');
  const url = new URL(request.url);
  if (url.searchParams.get('error') !== null) return fail(400, 'oauth_denied', 'Connecting Gmail was cancelled or denied.');
  const code = url.searchParams.get('code');
  const state = url.searchParams.get('state');
  if (code === null || state === null || code === '' || state === '') return fail(400, 'bad_request', 'Missing code or state.');
  const rawCookie = readCookie(request, OAUTH_COOKIE);
  const sealed = rawCookie === null ? null : await openCookie(env.SESSION_SECRET, 'gmail_oauth', rawCookie, now);
  if (
    sealed === null || typeof sealed['state'] !== 'string' || typeof sealed['verifier'] !== 'string' || typeof sealed['household_id'] !== 'string' ||
    typeof sealed['member_id'] !== 'string' || !timingSafeEqual(sealed['state'], state)
  ) {
    return fail(400, 'bad_state', 'The attempt expired or did not start here. Try again.');
  }
  const { household_id, member_id } = sealed as { household_id: string; member_id: string };
  const member = await env.DB.prepare(
    `SELECT 1 AS ok FROM members m JOIN households h ON h.household_id = m.household_id WHERE m.member_id = ? AND m.household_id = ? AND h.deleted_at IS NULL`,
  )
    .bind(member_id, household_id)
    .first<{ ok: number }>();
  if (member === null) return fail(401, 'unauthorized', 'Sign in again.');

  try {
    const grant = await requestGmailTokens({ env, fetch: deps.fetch }, code, `${url.origin}${GMAIL_CALLBACK_PATH}`, sealed['verifier'] as string);
    await storeGmailConnector({ env, fetch: deps.fetch, now: deps.now }, household_id, grant);
  } catch (err) {
    if (err instanceof GmailScopeError) return fail(403, 'scope_violation', 'Google granted a different access than this app asks for. Nothing was stored.');
    if (err instanceof GmailConsentError) return fail(502, 'token_exchange_failed', 'Google did not accept the code.');
    throw err;
  }
  await env.DB.prepare('INSERT OR IGNORE INTO inbox_connections (household_id) VALUES (?)').bind(household_id).run();
  await audit(env.DB, household_id, 'gmail_connected', { member_id }, now);
  const headers = new Headers({ location: '/?gmail=connected', 'cache-control': 'no-store' });
  headers.append('set-cookie', clearCookieHeader(OAUTH_COOKIE));
  return new Response(null, { status: 302, headers });
}
