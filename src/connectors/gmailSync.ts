/**
 * src/connectors/gmailSync.ts — the second way mail reaches the household: an incremental read of the member's own Gmail (scope
 * gmail.readonly), feeding the SAME funnel, vault and pipeline as forwarded mail (src/ingest/inboxTriage.ts -> handoff.ts -> IngestEmailWF).
 *
 *   - tokens are AES-GCM under the household DEK in KV (AAD = kv:<household>:<key>); connectors.token_kv_key holds only the KV key. The calendar
 *     connector's sign-in grant is a separate connector and a separate token: its scope check still refuses anything outside the calendar scopes.
 *   - every Google call goes through the guarded fetch passed in (src/net/egress.ts); nothing here calls fetch() itself.
 *   - incremental: users.history.list from inbox_connections.gmail_history_id, then users.messages.get (format=raw) for each added message. The
 *     cursor advances only past messages that were fully handled, so a failure re-reads them next time; Layer 0 makes re-reading harmless.
 *   - no cursor, or Gmail answering 404/410 for it (history expired), starts a bounded full read: the newest MAX_MESSAGES_PER_SYNC messages of the
 *     last 90 days outside promotions, social and forums, with the cursor taken from the mailbox profile BEFORE listing so nothing falls in a gap.
 *
 * gmail.readonly is a RESTRICTED Google scope (CLAUDE.md §0, §9: Gmail waits for CASA AL1). This module is the sync engine only. Nothing in this
 * repository requests that scope: sign-in still asks for the calendar scopes alone, so no household can have a Gmail connector until a consent flow is
 * added deliberately. storeGmailConnector() is where that flow will hand over its grant.
 */

import { decryptForHousehold, encryptForHousehold, fromBase64Bytes, sha256Hex, toBase64 } from '../crypto/vault';
import { handOff } from '../ingest/handoff';
import { admitInbound, parseInbound } from '../ingest/inboxTriage';
import type { Env } from '../types';

export const GMAIL_SCOPE = 'https://www.googleapis.com/auth/gmail.readonly';
const SCOPE_ALIASES = ['openid', 'email', 'https://www.googleapis.com/auth/userinfo.email'] as const;
const API = 'https://gmail.googleapis.com/gmail/v1/users/me';
const TOKEN_URL = 'https://oauth2.googleapis.com/token';

/** Messages fetched per sync call: each costs a Gmail read, an R2 write and a Workflow start, and a request has 1,000 queries to spend. */
export const MAX_MESSAGES_PER_SYNC = 50;
export const INITIAL_SYNC_DAYS = 90;
export const INITIAL_SYNC_QUERY = `newer_than:${INITIAL_SYNC_DAYS}d -category:promotions -category:social -category:forums`;
export const MAX_MESSAGE_BYTES = 25 * 1024 * 1024;
const HISTORY_PAGE = 100;
/** Messages in these states are the member's own outgoing or discarded mail, not something sent to them. */
const SKIP_LABELS = ['DRAFT', 'SPAM', 'TRASH', 'SENT'] as const;

export class GmailAuthError extends Error {
  override readonly name = 'GmailAuthError';
}
export class GmailScopeError extends Error {
  override readonly name = 'GmailScopeError';
  constructor(readonly granted: string) {
    super(`the grant must be exactly gmail.readonly; Google returned: ${granted}`);
  }
}
export class GmailNotConnectedError extends Error {
  override readonly name = 'GmailNotConnectedError';
}
export class GmailApiError extends Error {
  override readonly name = 'GmailApiError';
  constructor(readonly status: number, message: string) {
    super(message);
  }
}

export interface GmailDeps {
  env: Pick<Env, 'DB' | 'KV' | 'DOCS_BUCKET' | 'KEK_B64' | 'GOOGLE_CLIENT_ID' | 'GOOGLE_CLIENT_SECRET' | 'INGEST_EMAIL_WF'>;
  /** MUST be a guarded fetch (createGuardedFetch). */
  fetch: typeof fetch;
  now: () => Date;
  /** Largest raw message read; default MAX_MESSAGE_BYTES (25 MiB, the same cap as forwarded mail). Tests lower it. */
  maxMessageBytes?: number;
}

// ─── tokens and the connector row ───────────────────────────────────────────

interface StoredTokens {
  access_token: string;
  refresh_token: string;
  expires_at: number;
  scope: string;
}

export interface GmailConnector {
  connector_id: string;
  household_id: string;
  token_kv_key: string | null;
}

/** Throws unless gmail.readonly was granted and nothing broader (gmail.modify, mail.google.com, ...). openid and email are identity, not mail access. */
export function assertGmailScope(granted: string): void {
  const scopes = granted.split(/\s+/).filter((s) => s !== '');
  const extra = scopes.filter((s) => s !== GMAIL_SCOPE && !(SCOPE_ALIASES as readonly string[]).includes(s));
  if (!scopes.includes(GMAIL_SCOPE) || extra.length > 0) throw new GmailScopeError(granted);
}

const kvKeyFor = (connector_id: string): string => `gmail:${connector_id}`;
const aadFor = (household_id: string, key: string): string => `kv:${household_id}:${key}`;

async function saveTokens(deps: GmailDeps, household_id: string, key: string, tokens: StoredTokens): Promise<void> {
  const blob = await encryptForHousehold(deps.env, household_id, aadFor(household_id, key), new TextEncoder().encode(JSON.stringify(tokens)));
  await deps.env.KV.put(key, toBase64(blob));
}

async function loadTokens(deps: GmailDeps, household_id: string, key: string): Promise<StoredTokens> {
  const stored = await deps.env.KV.get(key);
  if (stored === null) throw new GmailAuthError('gmail tokens are missing; the member must reconnect Gmail');
  const plain = await decryptForHousehold(deps.env, household_id, aadFor(household_id, key), fromBase64Bytes(stored));
  return JSON.parse(new TextDecoder().decode(plain)) as StoredTokens;
}

/** Encrypts a Gmail grant into KV and activates the household's gmail connector. Rejects any scope other than gmail.readonly. Stores nothing on rejection. */
export async function storeGmailConnector(
  deps: GmailDeps,
  household_id: string,
  grant: { access_token: string; refresh_token: string; expires_in: number; scope: string },
): Promise<{ connector_id: string }> {
  assertGmailScope(grant.scope);
  const connector_id = `gmail_${household_id}`;
  const key = kvKeyFor(connector_id);
  await saveTokens(deps, household_id, key, {
    access_token: grant.access_token,
    refresh_token: grant.refresh_token,
    expires_at: deps.now().getTime() + grant.expires_in * 1000,
    scope: grant.scope,
  });
  await deps.env.DB.prepare(
    `INSERT INTO connectors (connector_id, household_id, kind, status, scopes, token_kv_key, last_error)
     VALUES (?, ?, 'gmail', 'active', ?, ?, NULL)
     ON CONFLICT (connector_id) DO UPDATE SET status = 'active', scopes = excluded.scopes, token_kv_key = excluded.token_kv_key, last_error = NULL`,
  )
    .bind(connector_id, household_id, grant.scope, key)
    .run();
  return { connector_id };
}

export async function findGmailConnector(db: D1Database, household_id: string): Promise<GmailConnector | null> {
  return db
    .prepare(`SELECT connector_id, household_id, token_kv_key FROM connectors WHERE household_id = ? AND kind = 'gmail' AND status = 'active'`)
    .bind(household_id)
    .first<GmailConnector>();
}

async function markConnector(db: D1Database, connector_id: string, status: 'error' | 'revoked', error: string): Promise<void> {
  await db.prepare('UPDATE connectors SET status = ?, last_error = ? WHERE connector_id = ?').bind(status, error, connector_id).run();
}

async function accessToken(deps: GmailDeps, connector: GmailConnector, forceRefresh = false): Promise<string> {
  if (connector.token_kv_key === null) throw new GmailAuthError('connector has no token');
  const tokens = await loadTokens(deps, connector.household_id, connector.token_kv_key);
  if (!forceRefresh && tokens.expires_at - deps.now().getTime() > 60_000) return tokens.access_token;
  const res = await deps.fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: deps.env.GOOGLE_CLIENT_ID,
      client_secret: deps.env.GOOGLE_CLIENT_SECRET,
      refresh_token: tokens.refresh_token,
      grant_type: 'refresh_token',
    }).toString(),
  });
  let body: { access_token?: string; refresh_token?: string; expires_in?: number; scope?: string; error?: string } = {};
  try {
    body = (await res.json()) as typeof body;
  } catch {
    body = {};
  }
  if (res.status !== 200 || !body.access_token) {
    await markConnector(deps.env.DB, connector.connector_id, body.error === 'invalid_grant' ? 'revoked' : 'error', `refresh failed: ${body.error ?? res.status}`);
    throw new GmailAuthError(`token refresh failed: ${body.error ?? res.status}`);
  }
  if (body.scope !== undefined) assertGmailScope(body.scope);
  await saveTokens(deps, connector.household_id, connector.token_kv_key, {
    access_token: body.access_token,
    refresh_token: body.refresh_token ?? tokens.refresh_token,
    expires_at: deps.now().getTime() + (body.expires_in ?? 3600) * 1000,
    scope: body.scope ?? tokens.scope,
  });
  return body.access_token;
}

async function gmailGet<T>(deps: GmailDeps, connector: GmailConnector, path: string, query: Record<string, string> = {}): Promise<T> {
  const url = new URL(`${API}${path}`);
  for (const [k, v] of Object.entries(query)) url.searchParams.set(k, v);
  for (let attempt = 0; attempt < 2; attempt++) {
    const res = await deps.fetch(url.href, { method: 'GET', headers: { authorization: `Bearer ${await accessToken(deps, connector, attempt > 0)}` } });
    if (res.status === 401 && attempt === 0) continue;
    if (!res.ok) throw new GmailApiError(res.status, `Gmail ${res.status}: ${(await res.text()).slice(0, 200)}`);
    return (await res.json()) as T;
  }
  throw new GmailAuthError('Google rejected a freshly refreshed token');
}

// ─── the sync ───────────────────────────────────────────────────────────────

export interface SyncSummary {
  /** incremental: from the stored cursor. initial: no cursor yet. resync: Gmail no longer had the cursor (404/410), so a full bounded read. */
  mode: 'incremental' | 'initial' | 'resync';
  /** Messages offered to the funnel. */
  seen: number;
  ingested: number;
  duplicates: number;
  dropped_bulk: number;
  /** Drafts, spam, trash, sent mail, oversize or unreadable messages, and messages deleted before they could be read. */
  skipped: number;
  /** More added messages remain than one call reads: call again. Only for mode 'incremental'. */
  has_more: boolean;
  /** An initial read or resync stopped at the newest MAX_MESSAGES_PER_SYNC; older mail was not read. */
  truncated: boolean;
  history_id: string | null;
}

interface HistoryPage {
  history?: Array<{ id: string; messagesAdded?: Array<{ message: { id: string } }> }>;
  nextPageToken?: string;
  historyId?: string;
}

interface RawMessage {
  id: string;
  labelIds?: string[];
  raw?: string;
}

function decodeBase64Url(s: string): Uint8Array {
  const b64 = s.replace(/-/g, '+').replace(/_/g, '/');
  return fromBase64Bytes(b64 + '='.repeat((4 - (b64.length % 4)) % 4));
}

type Handled = 'ingested' | 'duplicate' | 'dropped_bulk' | 'skipped';

async function ingestMessage(deps: GmailDeps, connector: GmailConnector, id: string): Promise<Handled> {
  let message: RawMessage;
  try {
    message = await gmailGet<RawMessage>(deps, connector, `/messages/${encodeURIComponent(id)}`, { format: 'raw' });
  } catch (err) {
    if (err instanceof GmailApiError && err.status === 404) return 'skipped'; // deleted since it was listed
    throw err;
  }
  if ((message.labelIds ?? []).some((l) => (SKIP_LABELS as readonly string[]).includes(l)) || typeof message.raw !== 'string') return 'skipped';
  const raw = decodeBase64Url(message.raw);
  if (raw.length === 0 || raw.length > (deps.maxMessageBytes ?? MAX_MESSAGE_BYTES)) return 'skipped';

  const household_id = connector.household_id;
  const sha256 = await sha256Hex(raw);
  const episode_id = `ep_${(await sha256Hex(`${household_id}:${sha256}`)).slice(0, 32)}`;
  let mail;
  try {
    mail = await parseInbound(raw);
  } catch {
    return 'skipped'; // unreadable MIME will not become readable on a retry
  }
  const admission = await admitInbound(deps.env.DB, household_id, 'gmail', mail, { now: deps.now(), episode_id });
  if (admission.action === 'duplicate') return 'duplicate';
  if (admission.action === 'dropped_bulk') return 'dropped_bulk';
  if (admission.action !== 'process') return 'skipped';
  const handed = await handOff(deps.env, { household_id, raw, sha256, episode_id, envelope_from: mail.from_address, claimed: admission.message_id_norm, channel: 'gmail' });
  return handed.status === 'accepted' ? 'ingested' : 'duplicate';
}

async function ingestAll(deps: GmailDeps, connector: GmailConnector, ids: readonly string[], into: SyncSummary): Promise<void> {
  for (const id of ids) {
    const handled = await ingestMessage(deps, connector, id);
    into.seen += 1;
    if (handled === 'ingested') into.ingested += 1;
    else if (handled === 'duplicate') into.duplicates += 1;
    else if (handled === 'dropped_bulk') into.dropped_bulk += 1;
    else into.skipped += 1;
  }
}

/** A bounded read of the newest mail, for a household with no cursor or whose cursor Gmail has expired. */
async function bootstrap(deps: GmailDeps, connector: GmailConnector, into: SyncSummary): Promise<string> {
  const profile = await gmailGet<{ historyId?: string }>(deps, connector, '/profile');
  if (typeof profile.historyId !== 'string') throw new GmailApiError(502, 'Gmail did not return a history id');
  const list = await gmailGet<{ messages?: Array<{ id: string }>; nextPageToken?: string }>(deps, connector, '/messages', {
    q: INITIAL_SYNC_QUERY,
    maxResults: String(MAX_MESSAGES_PER_SYNC),
  });
  await ingestAll(deps, connector, (list.messages ?? []).map((m) => m.id), into);
  into.truncated = list.nextPageToken !== undefined;
  return profile.historyId;
}

/** Added message ids after `startHistoryId`, up to the per-call cap, and the cursor that is safe to store afterwards. */
async function readHistory(deps: GmailDeps, connector: GmailConnector, startHistoryId: string): Promise<{ ids: string[]; cursor: string; has_more: boolean }> {
  const ids: string[] = [];
  const seen = new Set<string>();
  let cursor = startHistoryId;
  let pageToken: string | undefined;
  for (;;) {
    const page = await gmailGet<HistoryPage>(deps, connector, '/history', {
      startHistoryId,
      historyTypes: 'messageAdded',
      maxResults: String(HISTORY_PAGE),
      ...(pageToken === undefined ? {} : { pageToken }),
    });
    for (const record of page.history ?? []) {
      for (const added of record.messagesAdded ?? []) {
        if (!seen.has(added.message.id)) {
          seen.add(added.message.id);
          ids.push(added.message.id);
        }
      }
      cursor = record.id;
      // stop at a record boundary once the cap is reached; the cursor is that record's id, so the next call continues right after it
      if (ids.length >= MAX_MESSAGES_PER_SYNC) return { ids, cursor, has_more: true };
    }
    if (page.nextPageToken === undefined) return { ids, cursor: typeof page.historyId === 'string' ? page.historyId : cursor, has_more: false };
    pageToken = page.nextPageToken;
  }
}

/**
 * One sync call for one household. Throws GmailNotConnectedError when there is no active gmail connector and GmailAuthError when Google no longer
 * honours the grant (the connector is then marked revoked/error). Any other failure leaves the cursor where it was.
 */
export async function syncGmail(deps: GmailDeps, household_id: string): Promise<SyncSummary> {
  const connector = await findGmailConnector(deps.env.DB, household_id);
  if (connector === null) throw new GmailNotConnectedError('no active Gmail connector');
  const state = await deps.env.DB.prepare('SELECT gmail_history_id FROM inbox_connections WHERE household_id = ?').bind(household_id).first<{ gmail_history_id: string | null }>();
  const summary: SyncSummary = { mode: 'incremental', seen: 0, ingested: 0, duplicates: 0, dropped_bulk: 0, skipped: 0, has_more: false, truncated: false, history_id: null };

  let cursor: string;
  if (state?.gmail_history_id === null || state === null) {
    summary.mode = 'initial';
    cursor = await bootstrap(deps, connector, summary);
  } else {
    try {
      const read = await readHistory(deps, connector, state.gmail_history_id);
      await ingestAll(deps, connector, read.ids, summary);
      summary.has_more = read.has_more;
      cursor = read.cursor;
    } catch (err) {
      if (!(err instanceof GmailApiError) || (err.status !== 404 && err.status !== 410)) throw err;
      // Gmail has dropped the history this cursor points into: read the newest mail again (Layer 0 skips whatever is already known)
      summary.mode = 'resync';
      Object.assign(summary, { seen: 0, ingested: 0, duplicates: 0, dropped_bulk: 0, skipped: 0 });
      cursor = await bootstrap(deps, connector, summary);
    }
  }

  const at = deps.now().toISOString();
  await deps.env.DB.batch([
    deps.env.DB.prepare('INSERT OR IGNORE INTO inbox_connections (household_id) VALUES (?)').bind(household_id),
    deps.env.DB.prepare('UPDATE inbox_connections SET gmail_history_id = ?, last_sync_at = ? WHERE household_id = ?').bind(cursor, at, household_id),
    deps.env.DB.prepare('UPDATE connectors SET sync_cursor = ?, last_success_at = ?, last_error = NULL WHERE connector_id = ?').bind(cursor, at, connector.connector_id),
  ]);
  summary.history_id = cursor;
  return summary;
}
