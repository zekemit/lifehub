/**
 * Milestone 3 fixtures: a fake Google (OAuth token endpoint, Calendar events with sync tokens, freeBusy) that is reached
 * through the real egress guard, plus date helpers. Only the network is fake; D1, R2 and KV are the real local ones.
 */

import type { BusyBlock, GoogleEvent } from '../../src/connectors/googleCalendar';

const NOW = (): Date => new Date();

/** 'YYYY-MM-DD' for today + `days` (UTC). */
export function day(days: number, from: Date = NOW()): string {
  return new Date(Date.UTC(from.getUTCFullYear(), from.getUTCMonth(), from.getUTCDate() + days)).toISOString().slice(0, 10);
}

export interface GoogleLogEntry {
  method: string;
  path: string;
  query: Record<string, string>;
  body: string;
  auth: string | null;
}

export interface FakeGoogleState {
  /** Returned by a full listing (no syncToken). */
  events: GoogleEvent[];
  /** Returned by an incremental listing (syncToken present). */
  delta: GoogleEvent[];
  busy: BusyBlock[];
  /** Scope string Google reports on code exchange and refresh. */
  scope: string;
  /** Next listing with a syncToken answers 410. */
  expireSyncToken: boolean;
  /** Events per page (to exercise pagination). */
  pageSize: number;
  refreshCount: number;
  /** Make the next events call answer 401 once (expired access token). */
  rejectNextAccessToken: boolean;
  syncCounter: number;
  /** Refresh-token grants answer invalid_grant (the member revoked access). */
  revokeRefresh: boolean;
  /** Who Google says signed in (Milestone 4 sign-in). */
  identity: { email: string; sub: string; name: string | null; email_verified: boolean };
  /** Merged over the id_token claims (iss, aud, exp ...). */
  idClaims: Record<string, unknown>;
  /** The first code exchange returns no refresh_token (the member had already consented before). */
  omitRefreshToken: boolean;
  log: GoogleLogEntry[];
}

export const GOOD_SCOPE =
  'openid https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/calendar.events.readonly https://www.googleapis.com/auth/calendar.freebusy';

export function newGoogle(): FakeGoogleState {
  return { events: [], delta: [], busy: [], scope: 'openid email https://www.googleapis.com/auth/calendar.events.readonly https://www.googleapis.com/auth/calendar.freebusy', expireSyncToken: false, pageSize: 2500, refreshCount: 0, rejectNextAccessToken: false, syncCounter: 0, revokeRefresh: false, identity: { email: 'member@example.org', sub: 'google-sub-1', name: 'Test Member', email_verified: true }, idClaims: {}, omitRefreshToken: false, log: [] };
}

const jsonRes = (status: number, body: unknown): Response => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

function idToken(state: FakeGoogleState, clientId: string): string {
  const b64 = (o: unknown): string => btoa(String.fromCharCode(...new TextEncoder().encode(JSON.stringify(o)))).replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
  const claims = {
    iss: 'https://accounts.google.com',
    aud: clientId,
    sub: state.identity.sub,
    exp: Math.floor(Date.now() / 1000) + 3600,
    email: state.identity.email,
    email_verified: state.identity.email_verified,
    ...(state.identity.name === null ? {} : { name: state.identity.name }),
    ...state.idClaims,
  };
  return `${b64({ alg: 'none' })}.${b64(claims)}.sig`;
}

export function fakeGoogle(state: FakeGoogleState): typeof fetch {
  const impl = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url);
    const headers = new Headers(init?.headers);
    const body = typeof init?.body === 'string' ? init.body : '';
    const query = Object.fromEntries(url.searchParams.entries());
    state.log.push({ method: init?.method ?? 'GET', path: url.pathname, query, body, auth: headers.get('authorization') });

    if (url.hostname === 'oauth2.googleapis.com') {
      const form = new URLSearchParams(body);
      if (form.get('grant_type') === 'authorization_code') {
        if (form.get('code') !== 'good-code') return jsonRes(400, { error: 'invalid_grant' });
        return jsonRes(200, { access_token: 'at-initial', ...(state.omitRefreshToken ? {} : { refresh_token: 'rt-secret' }), expires_in: 3600, scope: state.scope, id_token: idToken(state, form.get('client_id') ?? '') });
      }
      if (form.get('grant_type') === 'refresh_token') {
        if (state.revokeRefresh || form.get('refresh_token') !== 'rt-secret') return jsonRes(400, { error: 'invalid_grant' });
        state.refreshCount += 1;
        return jsonRes(200, { access_token: `at-refreshed-${state.refreshCount}`, expires_in: 3600, scope: state.scope });
      }
      return jsonRes(400, { error: 'unsupported_grant_type' });
    }

    if (url.hostname === 'www.googleapis.com' && url.pathname.endsWith('/events')) {
      if (state.rejectNextAccessToken) {
        state.rejectNextAccessToken = false;
        return jsonRes(401, { error: { message: 'Invalid Credentials' } });
      }
      let items: GoogleEvent[];
      if (query['syncToken'] !== undefined) {
        if (state.expireSyncToken) {
          state.expireSyncToken = false;
          return jsonRes(410, { error: { message: 'Sync token is no longer valid' } });
        }
        items = state.delta;
      } else items = state.events;
      const offset = query['pageToken'] === undefined ? 0 : Number(query['pageToken']);
      const page = items.slice(offset, offset + state.pageSize);
      const more = offset + state.pageSize < items.length;
      if (!more) state.syncCounter += 1;
      return jsonRes(200, { items: page, ...(more ? { nextPageToken: String(offset + state.pageSize) } : { nextSyncToken: `sync-${state.syncCounter}` }) });
    }

    if (url.hostname === 'www.googleapis.com' && url.pathname.endsWith('/freeBusy')) {
      return jsonRes(200, { calendars: { primary: { busy: state.busy } } });
    }
    return jsonRes(404, { error: 'unexpected request' });
  };
  return impl as typeof fetch;
}
