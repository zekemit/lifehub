/**
 * Milestone 6 fixtures: raw mail with controllable headers, a fake Gmail (profile, message list, history, raw messages, token refresh) reached
 * through the REAL egress guard, and a router that sends everything else to the Milestone 4 network. D1, R2 and KV are the real local ones.
 */

import { randomUUID } from 'node:crypto';
import { newNetwork, type Network } from './m4';
import { openaiReply, type ProviderCall } from './m2';

// ── raw mail ──

export interface MailOptions {
  to: string;
  from?: string;
  subject?: string;
  body?: string;
  /** Message-ID without brackets. Omitted => a random one; null => no Message-ID header at all. */
  messageId?: string | null;
  date?: string;
  /** Extra headers, `Name: value`. */
  headers?: string[];
  /** Authentication-Results value; null => none. */
  auth?: string | null;
  /** An attachment file name (a small text part). */
  attachment?: string;
  html?: boolean;
}

export const PASSING = 'mx.example.net; dkim=pass header.d=example.com; spf=pass smtp.mailfrom=example.com';
export const authAs = (domain: string): string => `mx.example.net; dkim=pass header.d=${domain}; spf=pass smtp.mailfrom=${domain}`;

export function mail(o: MailOptions): Uint8Array {
  const id = o.messageId === undefined ? randomUUID() : o.messageId;
  const headers = [
    ...(o.auth === null ? [] : [`Authentication-Results: ${o.auth ?? PASSING}`]),
    ...(o.headers ?? []),
    `From: Sender <${o.from ?? 'sender@example.com'}>`,
    `To: ${o.to}`,
    `Subject: ${o.subject ?? 'Hello'}`,
    `Date: ${o.date ?? 'Tue, 03 Nov 2026 10:00:00 +0000'}`,
    ...(id === null ? [] : [`Message-ID: <${id}>`]),
    'MIME-Version: 1.0',
  ];
  const body = o.body ?? 'Hello';
  const text = o.html === true ? `<html><body><p>${body}</p></body></html>` : body;
  const type = o.html === true ? 'text/html' : 'text/plain';
  if (o.attachment === undefined) return new TextEncoder().encode([...headers, `Content-Type: ${type}; charset=utf-8`, '', text].join('\r\n'));
  const b = `b${randomUUID().replace(/-/g, '')}`;
  return new TextEncoder().encode(
    [
      ...headers,
      `Content-Type: multipart/mixed; boundary="${b}"`,
      '',
      `--${b}`,
      `Content-Type: ${type}; charset=utf-8`,
      '',
      text,
      `--${b}`,
      `Content-Type: application/octet-stream; name="${o.attachment}"`,
      `Content-Disposition: attachment; filename="${o.attachment}"`,
      'Content-Transfer-Encoding: base64',
      '',
      Buffer.from('attachment bytes').toString('base64'),
      `--${b}--`,
      '',
    ].join('\r\n'),
  );
}

export const BULK = ['List-Unsubscribe: <mailto:unsub@example.com>'];

// ── fake Gmail ──

export const GMAIL_RT = 'gmail-rt-secret';
export const GMAIL_SCOPE_URL = 'https://www.googleapis.com/auth/gmail.readonly';

export interface FakeMessage {
  raw: Uint8Array;
  labels: string[];
}

export interface HistoryRecord {
  id: string;
  added: string[];
}

export interface FakeGmailState {
  messages: Map<string, FakeMessage>;
  /** Newest first, as users.messages.list returns them. */
  listing: string[];
  history: HistoryRecord[];
  /** What the profile (and the tail of a history listing) reports as the mailbox's current historyId. */
  historyId: string;
  /** Answers history.list with this status (404/410: the cursor is too old). One-shot. */
  historyFailure: number | null;
  /** Answers messages.get for these ids with this status. */
  messageFailure: Map<string, number>;
  /** The next API call (not the token endpoint) answers 401 once. */
  rejectNextAccessToken: boolean;
  /** Refresh-token grants answer invalid_grant. */
  revokeRefresh: boolean;
  refreshCount: number;
  /** Scope Google reports when refreshing. */
  scope: string;
  pageSize: number;
  log: Array<{ path: string; query: Record<string, string>; auth: string | null }>;
}

export function newGmail(): FakeGmailState {
  return { messages: new Map(), listing: [], history: [], historyId: '1000', historyFailure: null, messageFailure: new Map(), rejectNextAccessToken: false, revokeRefresh: false, refreshCount: 0, scope: GMAIL_SCOPE_URL, pageSize: 100, log: [] };
}

/** Adds a message to the mailbox. `history` => it also appears as a messageAdded record with that id. */
export function gmailAdd(state: FakeGmailState, raw: Uint8Array, o: { id?: string; labels?: string[]; history?: string } = {}): string {
  const id = o.id ?? `m${state.messages.size + 1}`;
  state.messages.set(id, { raw, labels: o.labels ?? ['INBOX'] });
  state.listing.unshift(id);
  if (o.history !== undefined) {
    const record = state.history.find((r) => r.id === o.history);
    if (record === undefined) state.history.push({ id: o.history, added: [id] });
    else record.added.push(id);
    if (Number(o.history) > Number(state.historyId)) state.historyId = o.history;
  }
  return id;
}

const jsonRes = (status: number, body: unknown): Response => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

export function fakeGmail(state: FakeGmailState): typeof fetch {
  return (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url);
    const headers = new Headers(init?.headers);
    const query = Object.fromEntries(url.searchParams.entries());
    if (url.hostname === 'oauth2.googleapis.com') {
      const form = new URLSearchParams(typeof init?.body === 'string' ? init.body : '');
      if (form.get('grant_type') !== 'refresh_token' || state.revokeRefresh || form.get('refresh_token') !== GMAIL_RT) return jsonRes(400, { error: 'invalid_grant' });
      state.refreshCount += 1;
      return jsonRes(200, { access_token: `gmail-at-${state.refreshCount}`, expires_in: 3600, scope: state.scope });
    }
    const path = url.pathname.replace('/gmail/v1/users/me', '');
    state.log.push({ path, query, auth: headers.get('authorization') });
    if (state.rejectNextAccessToken) {
      state.rejectNextAccessToken = false;
      return jsonRes(401, { error: { message: 'Invalid Credentials' } });
    }
    if (path === '/profile') return jsonRes(200, { emailAddress: 'member@example.org', historyId: state.historyId });
    if (path === '/messages') {
      const cap = Number(query['maxResults'] ?? 100);
      const ids = state.listing.slice(0, cap);
      return jsonRes(200, { messages: ids.map((id) => ({ id })), ...(state.listing.length > cap ? { nextPageToken: 'more' } : {}) });
    }
    if (path.startsWith('/messages/')) {
      const id = decodeURIComponent(path.slice('/messages/'.length));
      const failure = state.messageFailure.get(id);
      if (failure !== undefined) return jsonRes(failure, { error: { message: 'failure' } });
      const m = state.messages.get(id);
      if (m === undefined) return jsonRes(404, { error: { message: 'Requested entity was not found.' } });
      return jsonRes(200, { id, labelIds: m.labels, raw: Buffer.from(m.raw).toString('base64url') });
    }
    if (path === '/history') {
      if (state.historyFailure !== null) {
        const status = state.historyFailure;
        state.historyFailure = null;
        return jsonRes(status, { error: { message: 'Requested entity was not found.' } });
      }
      const start = Number(query['startHistoryId']);
      const records = state.history.filter((r) => Number(r.id) > start);
      const offset = query['pageToken'] === undefined ? 0 : Number(query['pageToken']);
      const page = records.slice(offset, offset + state.pageSize);
      const more = offset + state.pageSize < records.length;
      return jsonRes(200, {
        ...(page.length === 0 ? {} : { history: page.map((r) => ({ id: r.id, messagesAdded: r.added.map((id) => ({ message: { id } })) })) }),
        historyId: state.historyId,
        ...(more ? { nextPageToken: String(offset + state.pageSize) } : {}),
      });
    }
    return jsonRes(404, { error: { message: 'unexpected Gmail path' } });
  }) as typeof fetch;
}

/** The Milestone 4 network with Gmail added: gmail.googleapis.com and Google's token endpoint (for the Gmail refresh token) go to the fake Gmail. */
export function gmailNetwork(): { net: Network; gmail: FakeGmailState; fetch: typeof fetch; hosts: string[] } {
  const net = newNetwork();
  const gmail = newGmail();
  const gmailFetch = fakeGmail(gmail);
  const hosts: string[] = [];
  const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url);
    hosts.push(url.hostname);
    if (url.hostname === 'gmail.googleapis.com') return gmailFetch(input, init);
    if (url.hostname === 'oauth2.googleapis.com' && new URLSearchParams(typeof init?.body === 'string' ? init.body : '').get('refresh_token') === GMAIL_RT) return gmailFetch(input, init);
    return net.fetch(input, init);
  }) as typeof fetch;
  return { net, gmail, fetch: fetchImpl, hosts };
}

// ── model replies ──

/** Triage and extraction are different calls to the same worker model; this answers each from its own script. */
export interface ModelScript {
  /** What the micro-classifier answers. A string is a bucket name; a function can read the call; `'garbage'` makes it malformed. */
  bucket: string | ((call: ProviderCall) => string);
  facts: (call: ProviderCall) => unknown[];
}

export const isTriage = (call: ProviderCall): boolean => call.sentText.includes('You sort one email');

export function scriptedModel(script: ModelScript): (call: ProviderCall) => Response {
  return (call) => {
    if (isTriage(call)) {
      const bucket = typeof script.bucket === 'function' ? script.bucket(call) : script.bucket;
      return bucket === 'garbage' ? openaiReply('not json at all') : openaiReply({ bucket });
    }
    return openaiReply({ facts: script.facts(call) });
  };
}

export const factJson = (o: Record<string, unknown>): Record<string, unknown> => ({
  subject_type: 'product', subject_name: 'Dyson V15', predicate: 'purchase_price_cents', object_type: null, object_name: null,
  value_text: null, value_num: null, value_cents: null, currency: null, valid_from: null, valid_to: null, confidence: 0.95, quote: '', ...o,
});
