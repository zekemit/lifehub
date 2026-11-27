/**
 * src/connectors/gmailConsent.ts — the member's one-time consent to read their Gmail (the Gmail read-only scope, GMAIL_SCOPE), behind the
 * ENABLE_GMAIL_OAUTH flag. It only builds the authorization request and exchanges the code; storing the tokens (AES-GCM, in KV) and the
 * scope check belong to storeGmailConnector() in gmailSync.ts, the one file that names the scope. Sign-in (googleCalendar.ts) never asks for it.
 *
 * Restricted-scope policy (Claude.md §0, §9): the flag ships "false", and src/workers/auth.ts answers 403 CASA_PENDING before this module is
 * reached, so no Google URL is built while the flag is off.
 */

import type { Env } from '../types';
import { GMAIL_SCOPE, type GmailDeps } from './gmailSync';

const AUTH_URL = 'https://accounts.google.com/o/oauth2/v2/auth';
const TOKEN_URL = 'https://oauth2.googleapis.com/token';

export function buildGmailAuthUrl(o: { clientId: string; redirectUri: string; state: string; codeChallenge: string; loginHint?: string }): string {
  const u = new URL(AUTH_URL);
  u.searchParams.set('client_id', o.clientId);
  u.searchParams.set('redirect_uri', o.redirectUri);
  u.searchParams.set('response_type', 'code');
  u.searchParams.set('scope', GMAIL_SCOPE);
  u.searchParams.set('access_type', 'offline');
  u.searchParams.set('prompt', 'consent');
  u.searchParams.set('include_granted_scopes', 'false');
  u.searchParams.set('state', o.state);
  u.searchParams.set('code_challenge', o.codeChallenge);
  u.searchParams.set('code_challenge_method', 'S256');
  if (o.loginHint !== undefined) u.searchParams.set('login_hint', o.loginHint);
  return u.href;
}

export class GmailConsentError extends Error {
  override readonly name = 'GmailConsentError';
}

export interface GmailGrant {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  scope: string;
}

/** Exchanges the authorization code (and PKCE verifier). Stores nothing; the caller hands the grant to storeGmailConnector(). */
export async function requestGmailTokens(deps: Pick<GmailDeps, 'fetch'> & { env: Pick<Env, 'GOOGLE_CLIENT_ID' | 'GOOGLE_CLIENT_SECRET'> }, code: string, redirectUri: string, codeVerifier: string): Promise<GmailGrant> {
  const res = await deps.fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      code,
      client_id: deps.env.GOOGLE_CLIENT_ID,
      client_secret: deps.env.GOOGLE_CLIENT_SECRET,
      redirect_uri: redirectUri,
      grant_type: 'authorization_code',
      code_verifier: codeVerifier,
    }).toString(),
  });
  let body: { access_token?: string; refresh_token?: string; expires_in?: number; scope?: string; error?: string } = {};
  try {
    body = (await res.json()) as typeof body;
  } catch {
    body = {};
  }
  if (res.status !== 200 || !body.access_token) throw new GmailConsentError(`token exchange failed: ${body.error ?? res.status}`);
  if (!body.refresh_token) throw new GmailConsentError('Google did not return a refresh token; reconnect with consent');
  return { access_token: body.access_token, refresh_token: body.refresh_token, expires_in: body.expires_in ?? 3600, scope: body.scope ?? '' };
}
