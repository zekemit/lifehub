/**
 * src/ingest/inboxTriage.ts — the funnel in front of IngestEmailWF, shared by both channels a message can arrive by (auto-forward to
 * the drop address, and the Gmail connector). A 2,000-message-a-month inbox must not burn model tokens or pollute facts_current.
 *
 *   Layer 0  cross-channel dedupe   The same message (Message-ID, or a hash of From + Subject + Date) is claimed ONCE per household in
 *                                   ingested_messages. The second arrival, by either channel, stops here: no R2 write, no model call.
 *   Layer 1  deterministic bulk     A bulk header (List-Unsubscribe, Precedence: bulk, Auto-Submitted, X-Campaign) AND none of the
 *                                   high-signal tokens (order, receipt, flight, warranty, ...) => dropped and counted. No R2, no model.
 *   Layer 2  forwarding confirm     Gmail / Outlook / iCloud "confirm auto-forwarding" mail is read BEFORE redact() would quarantine its
 *                                   confirmation code. Only an authenticated message from the provider's own domain counts, and the link
 *                                   is kept only if it points at the provider. Stored for the member to act on; never fetched.
 *   Layer 3  micro-classifier       After redact() (a quarantined episode never gets here), `from`, `subject` and the first 600 characters
 *                                   of the redacted text — at most 2,000 input tokens — go to the worker model, which picks one of eight
 *                                   buckets. IGNORE stops the message before any fact extraction.
 *
 * Layers 0-2 run before anything is written to R2 (admitInbound). Layer 3 runs inside IngestEmailWF, after the episode is stored.
 * Everything that is not a model call is deterministic and has no side effects other than the rows named above.
 */

import PostalMime from 'postal-mime';
import { sha256Hex } from '../crypto/vault';
import { estimateTokens, type Gateway } from '../llm/gateway';
import { redact } from '../redact';
import { htmlToText } from './mime';

// ─── buckets ────────────────────────────────────────────────────────────────

export const TRIAGE_BUCKETS = [
  'RECEIPT_ORDER',
  'BILL_SUBSCRIPTION',
  'TRAVEL_ITINERARY',
  'WARRANTY_PRODUCT',
  'TAX_LEGAL_DOC',
  'FAMILY_SCHOOL_EVENT',
  'PERSONAL_FOLLOWUP_NEEDED',
  'IGNORE',
] as const;
export type TriageBucket = (typeof TRIAGE_BUCKETS)[number];

/** What ingested_messages.triage_bucket can hold besides a model bucket. */
export type RecordedBucket = TriageBucket | 'DROPPED_BULK' | 'FORWARDING_CONFIRM' | 'QUARANTINED';

export type Channel = 'email_forward' | 'gmail';

// ─── parsing ────────────────────────────────────────────────────────────────

export interface InboundMail {
  /** The Message-ID header as written, or null. */
  message_id: string | null;
  /** Header values by lower-cased name, in the order they appear (the first is the topmost, i.e. the last hop's). */
  headers: Record<string, string[]>;
  from: string;
  from_address: string;
  subject: string;
  /** ISO timestamp of the Date header, or null. */
  date_iso: string | null;
  /** Plain text of the body (HTML converted when there is no text part). Headers are not included. */
  text: string;
  attachment_names: string[];
}

/** How much of a body the funnel reads. Receipts put what matters near the top; a blast's footer must not decide anything. */
const SCAN_BODY_CHARS = 20_000;

export async function parseInbound(raw: Uint8Array): Promise<InboundMail> {
  const mail = await PostalMime.parse(raw);
  const headers: Record<string, string[]> = {};
  for (const h of mail.headers) {
    const key = h.key.toLowerCase();
    (headers[key] ??= []).push(h.value);
  }
  const when = mail.date ? new Date(mail.date) : null;
  const body = mail.text && mail.text.trim().length > 0 ? mail.text : mail.html ? htmlToText(mail.html) : '';
  return {
    message_id: mail.messageId ?? null,
    headers,
    from: mail.from?.name ? `${mail.from.name} <${mail.from.address ?? ''}>` : (mail.from?.address ?? ''),
    from_address: (mail.from?.address ?? '').trim().toLowerCase(),
    subject: (mail.subject ?? '').trim(),
    date_iso: when !== null && Number.isFinite(when.getTime()) ? new Date(Math.floor(when.getTime() / 1000) * 1000).toISOString() : null,
    text: body.slice(0, 400_000),
    attachment_names: mail.attachments.map((a) => a.filename ?? '').filter((n) => n !== ''),
  };
}

// ─── Layer 0: message identity ──────────────────────────────────────────────

/** `<ABC@mail.example.com>` -> `abc@mail.example.com`. Null when nothing usable is left. */
export function normalizeMessageId(raw: string): string | null {
  const bracketed = /<([^<>\s]+)>/.exec(raw);
  const id = (bracketed?.[1] ?? raw).trim().toLowerCase().replace(/\s+/g, '');
  return id.length >= 3 && id.length <= 998 ? id : null;
}

/**
 * The household-scoped identity of a message: the hash of its normalized Message-ID, else of From + Subject + Date. Hashed, so the stored key is a
 * fixed size and carries none of the message's own text.
 */
export async function messageKey(mail: Pick<InboundMail, 'message_id' | 'from_address' | 'subject' | 'date_iso'>): Promise<string> {
  const id = mail.message_id === null ? null : normalizeMessageId(mail.message_id);
  if (id !== null) return `mid:${await sha256Hex(id)}`;
  const basis = [mail.from_address.trim().toLowerCase(), mail.subject.replace(/\s+/g, ' ').trim().toLowerCase(), mail.date_iso ?? ''].join('\n');
  return `fsd:${await sha256Hex(basis)}`;
}

// ─── Layer 1: the deterministic bulk filter ─────────────────────────────────

/** A message that carries a bulk header survives only if its subject, body or an attachment name holds one of these. */
export const HIGH_SIGNAL_TOKENS = [
  'order', 'receipt', 'invoice', 'total', 'confirmation', 'flight', 'itinerary', 'airline', 'delay', 'warranty', 'serial', 'policy', 'claim',
  'return', 'refund', 'statement', 'due', 'school', 'camp', 'registration', 'permission', 'roster', 'appointment', 'tax', '1099', 'w-2', '.pdf', '.ics',
] as const;

/** Word tokens match their usual inflections (orders, returned, delays, policies) and nothing longer (no "campus", no "taxi"). */
function tokenPattern(token: string): RegExp {
  switch (token) {
    case '.pdf':
    case '.ics':
      return new RegExp(`\\${token}\\b`, 'i');
    case 'warranty':
      return /\bwarrant(?:y|ies)\b/i;
    case 'policy':
      return /\bpolic(?:y|ies)\b/i;
    case 'itinerary':
      return /\bitinerar(?:y|ies)\b/i;
    case 'w-2':
      return /\bw-?2\b/i;
    case '1099':
      return /\b1099\b/;
    default:
      return new RegExp(`\\b${token}(?:s|es|ed|d|ing)?\\b`, 'i');
  }
}
const TOKEN_PATTERNS: ReadonlyArray<readonly [string, RegExp]> = HIGH_SIGNAL_TOKENS.map((t) => [t, tokenPattern(t)] as const);

/** The high-signal tokens present in the subject, the first 20,000 characters of the body, and the attachment file names. */
export function highSignalTokens(mail: Pick<InboundMail, 'subject' | 'text' | 'attachment_names'>): string[] {
  const haystack = `${mail.subject}\n${mail.text.slice(0, SCAN_BODY_CHARS)}\n${mail.attachment_names.join('\n')}`;
  return TOKEN_PATTERNS.filter(([, re]) => re.test(haystack)).map(([t]) => t);
}

/**
 * A ready-to-paste Gmail filter for "forward matching mail to your drop address": subject words from the same high-signal list Layer 1 uses, plus PDF and
 * calendar attachments, minus Gmail's promotional, social and forum categories. Layer 1 and the classifier still run on whatever it forwards.
 */
export function gmailFilterQuery(): string {
  const words = HIGH_SIGNAL_TOKENS.filter((t) => !t.startsWith('.')).map((t) => (/^[a-z]+$/.test(t) ? t : `"${t}"`));
  return `{subject:(${words.join(' OR ')}) filename:pdf filename:ics} -category:promotions -category:social -category:forums`;
}

/** The bulk markers a message carries, by header name. */
export function bulkSignals(headers: Record<string, string[]>): string[] {
  const signals: string[] = [];
  const first = (name: string): string | undefined => headers[name]?.[0];
  if (headers['list-unsubscribe'] !== undefined) signals.push('List-Unsubscribe');
  if (/\b(?:bulk|list|junk)\b/i.test(first('precedence') ?? '')) signals.push('Precedence');
  const auto = first('auto-submitted')?.trim().toLowerCase();
  if (auto !== undefined && auto !== '' && auto !== 'no') signals.push('Auto-Submitted');
  if (Object.keys(headers).some((k) => k.startsWith('x-campaign'))) signals.push('X-Campaign');
  return signals;
}

export interface BulkVerdict {
  drop: boolean;
  signals: string[];
  tokens: string[];
}

export function bulkVerdict(mail: InboundMail): BulkVerdict {
  const signals = bulkSignals(mail.headers);
  const tokens = signals.length === 0 ? [] : highSignalTokens(mail);
  return { drop: signals.length > 0 && tokens.length === 0, signals, tokens };
}

// ─── Layer 2: forwarding confirmation ───────────────────────────────────────

export type ForwardingProvider = 'gmail' | 'outlook' | 'icloud';

interface ProviderRule {
  provider: ForwardingProvider;
  /** Exact sender addresses that only ever send forwarding confirmations. */
  senders: readonly string[];
  /** The provider's own domains (exact or subdomain). The message must be authenticated AS one of these. */
  domains: readonly string[];
  /** Hosts a confirmation link may point at (exact or subdomain). Anything else is not kept. */
  link_hosts: readonly string[];
}

const PROVIDERS: readonly ProviderRule[] = [
  { provider: 'gmail', senders: ['forwarding-noreply@google.com'], domains: ['google.com'], link_hosts: ['mail-settings.google.com'] },
  { provider: 'outlook', senders: [], domains: ['microsoft.com', 'outlook.com', 'live.com', 'office.com'], link_hosts: ['account.live.com', 'outlook.live.com', 'outlook.office.com', 'account.microsoft.com', 'login.live.com'] },
  { provider: 'icloud', senders: [], domains: ['apple.com', 'icloud.com', 'me.com'], link_hosts: ['www.icloud.com', 'icloud.com', 'appleid.apple.com'] },
];

const FORWARDING_MARKER = /\bforward(?:ing)?\b[\s\S]{0,240}\b(?:verif\w*|confirm\w*)\b|\b(?:verif\w*|confirm\w*)\b[\s\S]{0,240}\bforward(?:ing)?\b/i;
const MAX_LINK_CHARS = 2_048;

const inDomain = (host: string, domain: string): boolean => host === domain || host.endsWith(`.${domain}`);

/**
 * The domains the message's topmost Authentication-Results header says it was authenticated as: the d= of a passing DKIM signature and the
 * domain of a passing SPF MAIL FROM. Only the topmost header counts (it is the last hop's; the rest are sender-controlled).
 */
export function authenticatedDomains(headers: Record<string, string[]>): Set<string> {
  const line = headers['authentication-results']?.[0] ?? '';
  const out = new Set<string>();
  for (const part of line.split(';')) {
    const seg = part.trim().toLowerCase();
    if (/^dkim=pass\b/.test(seg)) {
      const d = /header\.d=([a-z0-9.-]+)/.exec(seg)?.[1];
      if (d !== undefined) out.add(d.replace(/\.+$/, ''));
    } else if (/^spf=pass\b/.test(seg)) {
      const from = /smtp\.mailfrom=(?:[^@\s;]*@)?([a-z0-9.-]+)/.exec(seg)?.[1];
      if (from !== undefined) out.add(from.replace(/\.+$/, ''));
    }
  }
  return out;
}

export interface ForwardingConfirmation {
  provider: ForwardingProvider;
  code: string | null;
  url: string | null;
}

function extractCode(mail: InboundMail): string | null {
  const fromSubject = /\(#(\d{4,12})\)/.exec(mail.subject)?.[1];
  if (fromSubject !== undefined) return fromSubject;
  return /\b(?:confirmation|verification)\s+code\s*[:\-]?\s*(\d{4,12})\b/i.exec(mail.text)?.[1] ?? null;
}

function extractLink(mail: InboundMail, hosts: readonly string[]): string | null {
  for (const m of mail.text.matchAll(/https:\/\/[^\s<>"')\]]+/gi)) {
    const candidate = m[0].replace(/[.,;:!?]+$/, '');
    if (candidate.length > MAX_LINK_CHARS) continue;
    let url: URL;
    try {
      url = new URL(candidate);
    } catch {
      continue;
    }
    if (url.protocol !== 'https:' || url.username !== '' || url.password !== '') continue;
    const host = url.hostname.toLowerCase();
    if (hosts.some((h) => inDomain(host, h))) return url.href;
  }
  return null;
}

/**
 * Recognizes a "confirm auto-forwarding" message from Gmail, Outlook or iCloud. It must (a) come from the provider's own sender or
 * domain, (b) be authenticated AS that domain, and (c) for the domain-wide providers, say it is about forwarding. A look-alike sender, an
 * unauthenticated message, or a one-time code from anyone else is NOT recognized here, and goes on to redact(), which quarantines it.
 * Returns null unless a code or a provider link could be read.
 */
export function detectForwardingConfirmation(mail: InboundMail): ForwardingConfirmation | null {
  const authenticated = authenticatedDomains(mail.headers);
  const sender = mail.from_address;
  const senderHost = sender.slice(sender.lastIndexOf('@') + 1);
  for (const rule of PROVIDERS) {
    const exact = rule.senders.includes(sender);
    const domainMatch = rule.domains.some((d) => inDomain(senderHost, d));
    if (!exact && !domainMatch) continue;
    if (![...authenticated].some((d) => rule.domains.some((rd) => inDomain(d, rd)))) continue;
    if (!exact && !FORWARDING_MARKER.test(`${mail.subject}\n${mail.text}`)) continue;
    const code = extractCode(mail);
    const url = extractLink(mail, rule.link_hosts);
    if (code === null && url === null) continue;
    return { provider: rule.provider, code, url };
  }
  return null;
}

// ─── ingested_messages: the claim, the funnel's decisions ───────────────────

export type Admission =
  | { action: 'duplicate'; message_id_norm: string; episode_id: string | null }
  | { action: 'dropped_bulk'; message_id_norm: string; signals: string[] }
  | { action: 'forwarding_confirmation'; message_id_norm: string; provider: ForwardingProvider }
  | { action: 'process'; message_id_norm: string };

/** A claim that never reached an episode or a bucket within this long is taken to belong to a crashed delivery and may be claimed again. */
export const STALE_CLAIM_MS = 60 * 60_000;

const ensureConnection = (db: D1Database, household_id: string): D1PreparedStatement =>
  db.prepare('INSERT OR IGNORE INTO inbox_connections (household_id) VALUES (?)').bind(household_id);

/** Atomically claims the message. True when this call owns it; false when it was already claimed (a duplicate). */
async function claimMessage(db: D1Database, household_id: string, key: string, channel: Channel, episode_id: string, now: Date): Promise<{ owned: boolean; episode_id: string | null }> {
  const at = now.toISOString();
  const inserted = await db
    .prepare('INSERT OR IGNORE INTO ingested_messages (household_id, message_id_norm, source, received_at, episode_id) VALUES (?, ?, ?, ?, ?)')
    .bind(household_id, key, channel, at, episode_id)
    .run();
  if ((inserted.meta.changes ?? 0) === 1) return { owned: true, episode_id };
  const existing = await db
    .prepare('SELECT received_at, triage_bucket, episode_id FROM ingested_messages WHERE household_id = ? AND message_id_norm = ?')
    .bind(household_id, key)
    .first<{ received_at: string; triage_bucket: string | null; episode_id: string | null }>();
  if (existing === null) return { owned: false, episode_id: null };
  const stale = now.getTime() - Date.parse(existing.received_at) > STALE_CLAIM_MS;
  if (existing.triage_bucket === null && stale) {
    const hasEpisode =
      existing.episode_id === null
        ? null
        : await db.prepare('SELECT 1 AS ok FROM episodes WHERE household_id = ? AND episode_id = ?').bind(household_id, existing.episode_id).first<{ ok: number }>();
    if (hasEpisode === null) {
      // compare-and-set on the old claim time, so two redeliveries cannot both reclaim it
      const taken = await db
        .prepare('UPDATE ingested_messages SET received_at = ?, source = ?, episode_id = ? WHERE household_id = ? AND message_id_norm = ? AND received_at = ? AND triage_bucket IS NULL')
        .bind(at, channel, episode_id, household_id, key, existing.received_at)
        .run();
      if ((taken.meta.changes ?? 0) === 1) return { owned: true, episode_id };
    }
  }
  return { owned: false, episode_id: existing.episode_id };
}

/** Gives the claim back (the delivery failed after the claim); only a message that has not been classified can be released. */
export async function releaseClaim(db: D1Database, household_id: string, key: string): Promise<void> {
  await db.prepare('DELETE FROM ingested_messages WHERE household_id = ? AND message_id_norm = ? AND triage_bucket IS NULL').bind(household_id, key).run();
}

/** Records what the funnel decided about a claimed message (and, once known, its episode). */
export async function recordBucket(db: D1Database, household_id: string, key: string, bucket: RecordedBucket | null, episode_id: string | null): Promise<void> {
  await db
    .prepare('UPDATE ingested_messages SET triage_bucket = COALESCE(?, triage_bucket), episode_id = COALESCE(?, episode_id) WHERE household_id = ? AND message_id_norm = ?')
    .bind(bucket, episode_id, household_id, key)
    .run();
}

/**
 * The first ordinary message after a forwarding confirmation proves the member's auto-forward works: stamp it, and drop the one-time code and
 * link, which are of no further use.
 */
function markForwardingVerified(db: D1Database, household_id: string, at: string): D1PreparedStatement {
  return db
    .prepare(
      `UPDATE inbox_connections SET forwarding_verified_at = ?, forwarding_confirm_code = NULL, forwarding_confirm_url = NULL
        WHERE household_id = ? AND forwarding_verified_at IS NULL AND (forwarding_confirm_code IS NOT NULL OR forwarding_confirm_url IS NOT NULL)`,
    )
    .bind(at, household_id);
}

/**
 * Layers 0, 1 and 2. Claims the message and decides its fate WITHOUT writing to R2 or calling a model.
 *   `episode_id` the episode this message will become if it is processed (recorded on the claim).
 * Layer 2 reads authentication from the message's own topmost Authentication-Results header (authenticatedDomains), so it holds whichever
 * caller supplies the mail; it is skipped for the Gmail channel, whose messages are the member's own mailbox, not mail sent to the drop address.
 */
export async function admitInbound(
  db: D1Database,
  household_id: string,
  channel: Channel,
  mail: InboundMail,
  o: { now: Date; episode_id: string },
): Promise<Admission> {
  const key = await messageKey(mail);
  const claim = await claimMessage(db, household_id, key, channel, o.episode_id, o.now);
  if (!claim.owned) return { action: 'duplicate', message_id_norm: key, episode_id: claim.episode_id };
  const at = o.now.toISOString();

  if (channel === 'email_forward') {
    const confirmation = detectForwardingConfirmation(mail);
    if (confirmation !== null) {
      await db.batch([
        ensureConnection(db, household_id),
        db
          .prepare(
            `UPDATE inbox_connections SET forwarding_confirm_code = COALESCE(?, forwarding_confirm_code), forwarding_confirm_url = COALESCE(?, forwarding_confirm_url),
                                          forwarding_verified_at = NULL WHERE household_id = ?`,
          )
          .bind(confirmation.code, confirmation.url, household_id),
        db.prepare(`UPDATE ingested_messages SET triage_bucket = 'FORWARDING_CONFIRM', episode_id = NULL WHERE household_id = ? AND message_id_norm = ?`).bind(household_id, key),
      ]);
      return { action: 'forwarding_confirmation', message_id_norm: key, provider: confirmation.provider };
    }
  }

  const verdict = bulkVerdict(mail);
  if (verdict.drop) {
    await db.batch([
      ensureConnection(db, household_id),
      db.prepare(`UPDATE ingested_messages SET triage_bucket = 'DROPPED_BULK', episode_id = NULL WHERE household_id = ? AND message_id_norm = ?`).bind(household_id, key),
      db.prepare('UPDATE inbox_connections SET triage_dropped_bulk_count = triage_dropped_bulk_count + 1 WHERE household_id = ?').bind(household_id),
      ...(channel === 'email_forward' ? [markForwardingVerified(db, household_id, at)] : []),
    ]);
    return { action: 'dropped_bulk', message_id_norm: key, signals: verdict.signals };
  }

  if (channel === 'email_forward') await db.batch([ensureConnection(db, household_id), markForwardingVerified(db, household_id, at)]);
  return { action: 'process', message_id_norm: key };
}

// ─── Layer 3: the redacted micro-classifier ─────────────────────────────────

/** The classifier is a first filter, not a reader: its whole input stays at or below this many (estimated) tokens. */
export const TRIAGE_INPUT_TOKEN_CAP = 2_000;
export const TRIAGE_SNIPPET_CHARS = 600;
const TRIAGE_FROM_TOKENS = 100;
const TRIAGE_SUBJECT_TOKENS = 200;
/** The gateway adds a small fixed overhead to every prompt; counted here so the cap is on what is really sent. */
const PROMPT_OVERHEAD_TOKENS = 64;

export class TriageBudgetError extends Error {
  override readonly name = 'TriageBudgetError';
  constructor(readonly tokens: number) {
    super(`triage input is ~${tokens} tokens; the cap is ${TRIAGE_INPUT_TOKEN_CAP}`);
  }
}

/** The longest code-point prefix of `text` that fits `maxTokens` by estimateTokens(). */
export function clampToTokens(text: string, maxTokens: number): string {
  const chars = Array.from(text.slice(0, maxTokens * 3));
  if (estimateTokens(chars.join('')) <= maxTokens) return chars.join('');
  let lo = 0;
  let hi = chars.length;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (estimateTokens(chars.slice(0, mid).join('')) <= maxTokens) lo = mid;
    else hi = mid - 1;
  }
  return chars.slice(0, lo).join('');
}

/** A header field passes through redact() like any other text; one that would be quarantined is withheld, never sent. */
function safeField(value: string): string {
  const r = redact(value.replace(/\s+/g, ' ').trim());
  return r.redaction_status === 'quarantined' ? '[withheld]' : r.redacted_text;
}

function triageSystem(appName: string): string {
  return [
    `You sort one email for ${appName}, a household life-admin assistant. Choose exactly ONE bucket:`,
    'RECEIPT_ORDER: a purchase receipt, order or shipping confirmation, or a price-adjustment notice about something already bought.',
    'BILL_SUBSCRIPTION: a bill, a statement, an amount due, or a subscription renewal or price change.',
    'TRAVEL_ITINERARY: a flight, hotel or trip booking, or a delay, cancellation or schedule change.',
    'WARRANTY_PRODUCT: a warranty, a product registration, a serial number, or a recall.',
    'TAX_LEGAL_DOC: a tax form, an insurance policy, or a legal or government document.',
    'FAMILY_SCHOOL_EVENT: school, camp, sports or activity news, a registration date, a permission slip, a roster.',
    'PERSONAL_FOLLOWUP_NEEDED: a real person is waiting on the household for an answer or an action.',
    'IGNORE: marketing, newsletters, social notifications, alerts with nothing to act on, anything else.',
    'Answer with the bucket name only.',
  ].join('\n');
}

export interface TriageRequest {
  system: string;
  user: string;
  tokens: number;
}

/**
 * Builds the classifier prompt. The snippet is at most 600 characters AND is clamped so the whole prompt stays within TRIAGE_INPUT_TOKEN_CAP even
 * for text that is expensive per character (CJK, emoji). TriageBudgetError is the backstop: it cannot fire unless the fixed parts alone outgrow the cap.
 */
export function buildTriageRequest(appName: string, input: { from: string; subject: string; redacted_text: string }): TriageRequest {
  const from = clampToTokens(safeField(input.from), TRIAGE_FROM_TOKENS);
  const subject = clampToTokens(safeField(input.subject), TRIAGE_SUBJECT_TOKENS);
  const system = triageSystem(appName);
  const frame = (snippet: string): string => `From: ${from}\nSubject: ${subject}\nStart of the message:\n<<<\n${snippet}\n>>>`;
  const room = TRIAGE_INPUT_TOKEN_CAP - PROMPT_OVERHEAD_TOKENS - estimateTokens(system) - estimateTokens(frame(''));
  const firstChars = Array.from(input.redacted_text.slice(0, TRIAGE_SNIPPET_CHARS * 2)).slice(0, TRIAGE_SNIPPET_CHARS).join('');
  const snippet = clampToTokens(firstChars, Math.max(0, room));
  const user = frame(snippet);
  const tokens = estimateTokens(system) + estimateTokens(user) + PROMPT_OVERHEAD_TOKENS;
  if (tokens > TRIAGE_INPUT_TOKEN_CAP) throw new TriageBudgetError(tokens);
  return { system, user, tokens };
}

const BUCKET_SCHEMA = {
  name: 'inbox_bucket',
  schema: { type: 'object', additionalProperties: false, required: ['bucket'], properties: { bucket: { type: 'string', enum: [...TRIAGE_BUCKETS] } } },
};

/**
 * Classifies one redacted episode. Returns null when the model's answer is not one of the eight buckets: the caller then extracts with
 * every predicate rather than losing the message.
 */
export async function classifyEmail(
  deps: { gateway: Gateway; appName: string },
  input: { household_id: string; from: string; subject: string; redacted_text: string },
): Promise<TriageBucket | null> {
  const req = buildTriageRequest(deps.appName, input);
  const res = await deps.gateway.worker({
    household_id: input.household_id,
    purpose: 'inbox.triage',
    system: req.system,
    user: req.user,
    json_schema: BUCKET_SCHEMA,
    max_output_tokens: 32,
  });
  try {
    const bucket = (JSON.parse(res.text) as { bucket?: unknown }).bucket;
    return typeof bucket === 'string' && (TRIAGE_BUCKETS as readonly string[]).includes(bucket) ? (bucket as TriageBucket) : null;
  } catch {
    return null;
  }
}

/** The `From:` and `Subject:` lines parseEmail() writes at the top of an episode (already redacted along with the rest). */
export function headerLinesOf(redacted_text: string): { from: string; subject: string } {
  const from = /^From: (.*)$/m.exec(redacted_text)?.[1] ?? '';
  const subject = /^Subject: (.*)$/m.exec(redacted_text)?.[1] ?? '';
  return { from, subject };
}
