/**
 * src/workers/api.ts — the minimal authenticated HTTP surface for Day-1 onboarding and the Home screen.
 *
 *   POST /api/ingest/file     multipart upload (field `file`; optional `kind`, `default_card_entity_id`, `sign_convention`,
 *                             `statement_year`). CSV, or statement TEXT already extracted from a PDF (kind=pdf_text);
 *                             a binary PDF is refused with 415 because text extraction happens in the app before upload.
 *                             Stored encrypted in R2, then IngestFileWF is started.
 *   POST /api/calendar/sync   starts CalendarSyncWF (which ends by starting NightlySweepWF). 409 if no calendar is connected.
 *   GET  /api/home            header metrics + the household's open Mission Cards with verbatim evidence quotes.
 *   GET  /api/auth/google/login, /api/auth/google/callback   public: Google sign-in (src/workers/auth.ts).
 *   GET  /api/auth/me, POST /api/auth/logout
 *   POST /api/missions/<run_id>/(start|approve|decline|confirm), POST /api/push/subscribe   (src/workers/missions.ts)
 *
 * Authentication (every route except the two sign-in routes): the sealed __Host-session cookie set by the Google callback, or
 * `Authorization: Bearer <session token>` (src/workers/session.ts); the member must still belong to a live household. Cookie-authenticated
 * unsafe requests need a same-origin Origin header. The household id is NEVER taken from the request body, query or path.
 */

import { objectExists, putEncrypted, sha256Hex } from '../crypto/vault';
import { findCalendarConnector } from '../connectors/googleCalendar';
import { getHome } from '../missions/home';
import { createGuardedFetch } from '../net/egress';
import type { Env, IngestFileParams, SignConvention } from '../types';
import { handleStripeWebhook } from '../billing/stripeWebhook';
import { authenticate, handleGoogleCallback, handleGoogleLogin, handleLogout, handleMe, passesCsrf, type Auth } from './auth';
import { error, isAlreadyExists, json } from './http';
import { handleAutoRecharge, handleBalance, handleCheckout } from './billing';
import { handleConciergeAction, handleCreateAlarm, handleCreateWatch, parseConciergePath } from './concierge';
import { handleMission, handlePushSubscribe, parseMissionPath } from './missions';
import { handleReceiptGenerate, handleReceiptLatest, handleShareCard } from './receipt';

export const MAX_UPLOAD_BYTES = 25 * 1024 * 1024;

const SIGN_CONVENTIONS: readonly SignConvention[] = ['auto', 'negative_is_outflow', 'positive_is_outflow'];

async function ingestFile(request: Request, env: Env, auth: Auth): Promise<Response> {
  const declared = Number(request.headers.get('content-length') ?? 0);
  if (declared > MAX_UPLOAD_BYTES + 64 * 1024) return error(413, 'too_large', 'Files may be at most 25 MiB.');
  if (!(request.headers.get('content-type') ?? '').toLowerCase().startsWith('multipart/form-data')) {
    return error(415, 'multipart_required', 'Send the file as multipart/form-data in the field "file".');
  }
  const form = await request.formData();
  const file = form.get('file');
  if (file === null || typeof file === 'string') return error(400, 'file_required', 'The "file" field must be a file.');
  if (file.size > MAX_UPLOAD_BYTES) return error(413, 'too_large', 'Files may be at most 25 MiB.');
  const bytes = new Uint8Array(await file.arrayBuffer());
  if (bytes.length === 0) return error(400, 'empty_file', 'The file is empty.');
  if (String.fromCharCode(...bytes.subarray(0, 5)) === '%PDF-') {
    return error(415, 'pdf_text_required', 'Upload the statement text extracted from the PDF (kind=pdf_text); binary PDFs are not parsed here.');
  }

  const requestedKind = form.get('kind');
  const name = file.name.toLowerCase();
  const kind: IngestFileParams['kind'] | null =
    requestedKind === 'csv' || requestedKind === 'pdf_text' ? requestedKind : requestedKind === null ? (name.endsWith('.csv') ? 'csv' : name.endsWith('.txt') ? 'pdf_text' : null) : null;
  if (kind === null) return error(400, 'kind_required', 'Set kind to "csv" or "pdf_text".');
  try {
    new TextDecoder('utf-8', { fatal: true, ignoreBOM: false }).decode(bytes);
  } catch {
    return error(415, 'text_required', 'The file must be UTF-8 text.');
  }

  const signRaw = form.get('sign_convention');
  if (signRaw !== null && !SIGN_CONVENTIONS.includes(signRaw as SignConvention)) return error(400, 'bad_sign_convention', 'sign_convention is not valid.');
  const yearRaw = form.get('statement_year');
  const year = yearRaw === null || yearRaw === '' ? null : Number(yearRaw);
  if (year !== null && (!Number.isInteger(year) || year < 2000 || year > 2100)) return error(400, 'bad_statement_year', 'statement_year is not valid.');

  const cardRaw = form.get('default_card_entity_id');
  let card: string | null = null;
  if (typeof cardRaw === 'string' && cardRaw !== '') {
    const owned = await env.DB.prepare(`SELECT 1 AS ok FROM entities WHERE entity_id = ? AND household_id = ? AND type = 'card_account'`)
      .bind(cardRaw, auth.household_id)
      .first<{ ok: number }>();
    if (owned === null) return error(400, 'unknown_card', 'default_card_entity_id is not a card of this household.');
    card = cardRaw;
  }

  const sha256 = await sha256Hex(bytes);
  const r2_key = `upload/${auth.household_id}/${sha256}.${kind === 'csv' ? 'csv' : 'txt'}.enc`;
  if (!(await objectExists(env, r2_key))) await putEncrypted(env, auth.household_id, r2_key, bytes);

  const instance_id = `file_${(await sha256Hex(`${auth.household_id}:${sha256}`)).slice(0, 32)}`;
  const params: IngestFileParams = {
    household_id: auth.household_id,
    r2_key,
    file_name: file.name.slice(0, 200),
    kind,
    sha256,
    default_card_entity_id: card,
    sign_convention: (signRaw as SignConvention | null) ?? 'auto',
    ...(year === null ? {} : { statement_year: year }),
  };
  try {
    await env.INGEST_FILE_WF.create({ id: instance_id, params });
  } catch (err) {
    if (isAlreadyExists(err)) return json(200, { status: 'duplicate', instance_id, sha256 });
    throw err;
  }
  return json(202, { status: 'accepted', instance_id, sha256, kind });
}

async function calendarSync(env: Env, auth: Auth, now: Date): Promise<Response> {
  const connector = await findCalendarConnector(env.DB, auth.household_id);
  if (connector === null) return error(409, 'calendar_not_connected', 'Connect Google Calendar first.');
  const stamp = now.toISOString().slice(0, 16).replace(/\D/g, '');
  const instance_id = `calsync_${auth.household_id}_${stamp}`;
  try {
    await env.CALENDAR_SYNC_WF.create({ id: instance_id, params: { household_id: auth.household_id, then_sweep: true } });
  } catch (err) {
    if (isAlreadyExists(err)) return json(200, { status: 'already_running', instance_id });
    throw err;
  }
  return json(202, { status: 'accepted', instance_id });
}

const ROUTES: Readonly<Record<string, string>> = {
  '/api/ingest/file': 'POST',
  '/api/calendar/sync': 'POST',
  '/api/home': 'GET',
  '/api/auth/me': 'GET',
  '/api/auth/logout': 'POST',
  '/api/push/subscribe': 'POST',
  '/api/credits/balance': 'GET',
  '/api/credits/checkout': 'POST',
  '/api/credits/auto-recharge': 'POST',
  '/api/receipt/latest': 'GET',
  '/api/receipt/generate': 'POST',
  '/api/receipt/share-card.svg': 'GET',
  '/api/drop-alarms': 'POST',
  '/api/price-watches': 'POST',
};
/** No session: Google's sign-in redirects, and Stripe's webhook (authenticated by its signature, not a cookie, so no CSRF check either). */
const PUBLIC_ROUTES: Readonly<Record<string, string>> = { '/api/auth/google/login': 'GET', '/api/auth/google/callback': 'GET', '/api/webhooks/stripe': 'POST' };

/** `deps.fetch` is the network used for Google's token endpoint and for Stripe; production gets the policy-guarded fetch. */
export async function handleApi(request: Request, env: Env, deps: { now?: () => Date; fetch?: typeof fetch } = {}): Promise<Response> {
  const nowFn = deps.now ?? (() => new Date());
  const now = nowFn();
  const path = new URL(request.url).pathname;

  const publicMethod = PUBLIC_ROUTES[path];
  if (publicMethod !== undefined) {
    if (request.method !== publicMethod) return json(405, { error: 'method_not_allowed', message: `Use ${publicMethod}.` }, { allow: publicMethod });
    if (path === '/api/webhooks/stripe') return handleStripeWebhook(request, env, { now: nowFn });
    const authDeps = { now: nowFn, fetch: deps.fetch ?? createGuardedFetch({ db: env.DB }) };
    return path === '/api/auth/google/login' ? handleGoogleLogin(request, env, authDeps) : handleGoogleCallback(request, env, authDeps);
  }

  const mission = parseMissionPath(path);
  const concierge = parseConciergePath(path);
  const method = mission !== null || concierge !== null ? 'POST' : ROUTES[path];
  if (method === undefined) return error(404, 'not_found', 'No such endpoint.');
  if (request.method !== method) return json(405, { error: 'method_not_allowed', message: `Use ${method}.` }, { allow: method });

  const auth = await authenticate(request, env, now);
  if (auth === null) return error(401, 'unauthorized', 'Sign in again.');
  if (!passesCsrf(request, auth)) return error(403, 'csrf', 'Cross-site request refused.');

  if (mission !== null) return handleMission(request, env, auth, mission, now);
  if (concierge !== null) return handleConciergeAction(env, auth, concierge);
  const network = deps.fetch ?? createGuardedFetch({ db: env.DB });
  switch (path) {
    case '/api/ingest/file':
      return ingestFile(request, env, auth);
    case '/api/calendar/sync':
      return calendarSync(env, auth, now);
    case '/api/auth/me':
      return handleMe(env, auth);
    case '/api/auth/logout':
      return handleLogout();
    case '/api/push/subscribe':
      return handlePushSubscribe(request, env, auth);
    case '/api/credits/balance':
      return handleBalance(env, auth, now);
    case '/api/credits/checkout':
      return handleCheckout(request, env, auth, { fetch: network });
    case '/api/credits/auto-recharge':
      return handleAutoRecharge(request, env, auth, now);
    case '/api/receipt/latest':
      return handleReceiptLatest(env, auth, { fetch: network, now: nowFn });
    case '/api/receipt/generate':
      return handleReceiptGenerate(env, auth, { fetch: network, now: nowFn });
    case '/api/receipt/share-card.svg':
      return handleShareCard(env, auth);
    case '/api/drop-alarms':
      return handleCreateAlarm(request, env, auth, now);
    case '/api/price-watches':
      return handleCreateWatch(request, env, auth, now);
    default:
      return json(200, await getHome(env.DB, auth.household_id, now));
  }
}
