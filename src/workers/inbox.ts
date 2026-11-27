/**
 * src/workers/inbox.ts — the inbox setup surface.
 *
 *   GET  /api/inbox/setup    the drop address, a ready-to-paste Gmail filter, the auto-forward confirmation code and link captured by Layer 2
 *                            (when the member has asked Gmail / Outlook / iCloud to forward), whether forwarding is verified, whether the Gmail
 *                            connector is on, and how many bulk messages the funnel has dropped for this household.
 *   POST /api/inbox/sync     an immediate incremental Gmail sync for the signed-in household (409 when Gmail is not connected).
 *   POST /api/claims/warranty  {"product_entity_id": "..."}  stages the 3-credit warranty claim-packet card for a product with a live warranty.
 *
 * The household id always comes from the session, never from the request.
 */

import { GmailApiError, GmailAuthError, GmailNotConnectedError, findGmailConnector, syncGmail } from '../connectors/gmailSync';
import { gmailFilterQuery } from '../ingest/inboxTriage';
import { requestWarrantyClaim } from '../missions/triggers';
import type { Env } from '../types';
import type { Auth } from './auth';
import { error, json } from './http';

export async function handleInboxSetup(env: Pick<Env, 'DB' | 'INBOUND_DOMAIN'>, auth: Auth): Promise<Response> {
  const household = await env.DB.prepare('SELECT alias_local FROM households WHERE household_id = ? AND deleted_at IS NULL').bind(auth.household_id).first<{ alias_local: string }>();
  if (household === null) return error(401, 'unauthorized', 'Sign in again.');
  const state = await env.DB
    .prepare('SELECT forwarding_confirm_code, forwarding_confirm_url, forwarding_verified_at, triage_dropped_bulk_count FROM inbox_connections WHERE household_id = ?')
    .bind(auth.household_id)
    .first<{ forwarding_confirm_code: string | null; forwarding_confirm_url: string | null; forwarding_verified_at: string | null; triage_dropped_bulk_count: number }>();
  return json(200, {
    forwarding_address: `${household.alias_local}@${env.INBOUND_DOMAIN}`,
    gmail_filter_query: gmailFilterQuery(),
    forwarding_confirm_code: state?.forwarding_confirm_code ?? null,
    forwarding_confirm_url: state?.forwarding_confirm_url ?? null,
    forwarding_verified_at: state?.forwarding_verified_at ?? null,
    gmail_connected: (await findGmailConnector(env.DB, auth.household_id)) !== null,
    triage_dropped_bulk_count: state?.triage_dropped_bulk_count ?? 0,
  });
}

export async function handleInboxSync(env: Env, auth: Auth, deps: { fetch: typeof fetch; now: () => Date }): Promise<Response> {
  try {
    return json(200, await syncGmail({ env, fetch: deps.fetch, now: deps.now }, auth.household_id));
  } catch (err) {
    if (err instanceof GmailNotConnectedError) return error(409, 'gmail_not_connected', 'Connect Gmail first, or forward mail to your drop address.');
    if (err instanceof GmailAuthError) return error(409, 'gmail_reconnect_required', 'Google no longer allows this connection. Reconnect Gmail.');
    if (err instanceof GmailApiError) return error(502, 'gmail_unavailable', 'Gmail could not be read right now. Try again shortly.');
    throw err;
  }
}

export async function handleWarrantyClaim(request: Request, env: Pick<Env, 'DB'>, auth: Auth, now: Date): Promise<Response> {
  let body: unknown = null;
  try {
    body = await request.json();
  } catch {
    body = null;
  }
  const id = body !== null && typeof body === 'object' ? (body as Record<string, unknown>)['product_entity_id'] : undefined;
  if (typeof id !== 'string' || id === '') return error(400, 'bad_request', 'Send {"product_entity_id": "..."}.');
  const out = await requestWarrantyClaim(env.DB, auth.household_id, id, now);
  if (out.ok) return json(202, { status: 'staged', run_id: out.run_id });
  if (out.reason === 'no_warranty_on_record') return error(404, 'no_warranty_on_record', 'There is no live warranty on record for that product.');
  if (out.reason === 'already_requested') return error(409, 'already_requested', 'A claim packet for this warranty is already on your Home screen or was recently handled.');
  return error(422, 'ungrounded', 'The warranty could not be backed by a verified quote.');
}
