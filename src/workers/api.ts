/**
 * src/workers/api.ts — the minimal authenticated HTTP surface for Day-1 onboarding and the Home screen.
 *
 *   POST /api/ingest/file     multipart upload (field `file`; optional `kind`, `default_card_entity_id`, `sign_convention`,
 *                             `statement_year`). CSV, or statement TEXT already extracted from a PDF (kind=pdf_text);
 *                             a binary PDF is refused with 415 because text extraction happens in the app before upload.
 *                             Stored encrypted in R2, then IngestFileWF is started.
 *   POST /api/calendar/sync   starts CalendarSyncWF (which ends by starting NightlySweepWF). 409 if no calendar is connected.
 *   GET  /api/home            header metrics + top 3 staged Mission Cards with verbatim evidence quotes.
 *
 * Authentication: `Authorization: Bearer <session token>` (src/workers/session.ts), and the member must still belong to the
 * household in the token. The household id is NEVER taken from the request body, query or path.
 */

import { objectExists, putEncrypted, sha256Hex } from '../crypto/vault';
import { findCalendarConnector } from '../connectors/googleCalendar';
import { getHome } from '../missions/home';
import type { Env, IngestFileParams, SignConvention } from '../types';
import { verifySessionToken } from './session';

export const MAX_UPLOAD_BYTES = 25 * 1024 * 1024;

const JSON_HEADERS = { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' };

function json(status: number, body: unknown, extra: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), { status, headers: { ...JSON_HEADERS, ...extra } });
}

const error = (status: number, code: string, message: string): Response => json(status, { error: code, message });

function isAlreadyExists(err: unknown): boolean {
  return /already|exist/i.test(err instanceof Error ? err.message : String(err));
}

interface Auth {
  household_id: string;
  member_id: string;
}

async function authenticate(request: Request, env: Env, now: Date): Promise<Auth | null> {
  const header = request.headers.get('authorization') ?? '';
  const match = /^Bearer\s+(\S+)$/i.exec(header);
  if (match === null) return null;
  const claims = await verifySessionToken(env.SESSION_SECRET, match[1]!, now);
  if (claims === null) return null;
  const row = await env.DB.prepare(
    `SELECT 1 AS ok FROM members m JOIN households h ON h.household_id = m.household_id
      WHERE m.member_id = ? AND m.household_id = ? AND h.deleted_at IS NULL`,
  )
    .bind(claims.member_id, claims.household_id)
    .first<{ ok: number }>();
  return row === null ? null : claims;
}

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

export async function handleApi(request: Request, env: Env, deps: { now?: () => Date } = {}): Promise<Response> {
  const now = (deps.now ?? (() => new Date()))();
  const path = new URL(request.url).pathname;
  const routes: Record<string, string> = { '/api/ingest/file': 'POST', '/api/calendar/sync': 'POST', '/api/home': 'GET' };
  const method = routes[path];
  if (method === undefined) return error(404, 'not_found', 'No such endpoint.');
  if (request.method !== method) return json(405, { error: 'method_not_allowed', message: `Use ${method}.` }, { allow: method });

  const auth = await authenticate(request, env, now);
  if (auth === null) return error(401, 'unauthorized', 'Sign in again.');

  switch (path) {
    case '/api/ingest/file':
      return ingestFile(request, env, auth);
    case '/api/calendar/sync':
      return calendarSync(env, auth, now);
    default:
      return json(200, await getHome(env.DB, auth.household_id, now));
  }
}
