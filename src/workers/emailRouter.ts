/**
 * src/workers/emailRouter.ts — the catch-all Cloudflare Email Routing handler (CLAUDE.md §6).
 *
 * One catch-all on the inbound domain delivers every message here. For each message:
 *   1. recipient must be  h_<10 base32>@<env.INBOUND_DOMAIN>  and match a live household, else setReject();
 *   2. size cap 25 MiB (larger => reject with a "please upload" message);
 *   3. accept only if the TOPMOST Authentication-Results header shows spf=pass or dkim=pass, OR the SMTP sender is one
 *      of the household's member emails; otherwise setReject();
 *   4. the inbox funnel's first three layers (src/ingest/inboxTriage.ts), before anything is written to R2 or any model is called:
 *        L0 a message already claimed by either channel (auto-forward OR Gmail sync) is a duplicate and stops here;
 *        L1 bulk mail with no transactional tokens is dropped and counted;
 *        L2 a Gmail / Outlook / iCloud forwarding confirmation is captured for the member and stops here;
 *   5. raw MIME is encrypted into R2 (per-household AES-GCM DEK wrapped by the KEK) — plaintext is never stored;
 *   6. an IngestEmailWF instance (id = episode id) does the rest: parse, redact, classify (L3), extract, write facts.
 * Duplicate deliveries (same Message-ID, or the same sha256) are accepted silently and processed once.
 *
 * Authentication-Results trust: the topmost header is the one prepended by the last hop before this Worker
 * (Cloudflare's MX). Headers further down are sender-controlled and are ignored.
 */

import { sha256Hex } from '../crypto/vault';
import { handOff } from '../ingest/handoff';
import { admitInbound, parseInbound, type ForwardingProvider, type InboundMail } from '../ingest/inboxTriage';
import type { Env } from '../types';

export const MAX_EMAIL_BYTES = 25 * 1024 * 1024;
const ALIAS_LOCAL = /^h_[a-z2-7]{10}$/;

/** The part of ForwardableEmailMessage this handler uses. */
export interface InboundMessage {
  readonly from: string;
  readonly to: string;
  readonly raw: ReadableStream<Uint8Array>;
  readonly rawSize: number;
  setReject(reason: string): void;
}

export type EmailOutcome =
  | { status: 'rejected'; reason: string }
  | { status: 'duplicate'; episode_id: string }
  /** Layer 1: bulk mail with no transactional content. Nothing was stored. */
  | { status: 'dropped_bulk'; signals: string[] }
  /** Layer 2: an auto-forward confirmation, captured for the member. Nothing was stored. */
  | { status: 'forwarding_confirmation'; provider: ForwardingProvider }
  | { status: 'accepted'; episode_id: string; instance_id: string };

/** Returns the alias local part ('h_xxxxxxxxxx') if `to` is a drop address on `inboundDomain`, else null. */
export function parseRecipient(to: string, inboundDomain: string): string | null {
  const address = to.trim().toLowerCase();
  const at = address.lastIndexOf('@');
  if (at < 1) return null;
  if (address.slice(at + 1) !== inboundDomain.trim().toLowerCase()) return null;
  const local = address.slice(0, at);
  return ALIAS_LOCAL.test(local) ? local : null;
}

export interface AuthVerdict {
  spf: string | null;
  dkim: string | null;
  pass: boolean;
}

/** Reads the topmost Authentication-Results header from the header block of a raw message. */
export function authenticationVerdict(raw: Uint8Array): AuthVerdict {
  const head = new TextDecoder('latin1').decode(raw.subarray(0, Math.min(raw.length, 256 * 1024)));
  const blockEnd = head.search(/\r?\n\r?\n/);
  const block = blockEnd < 0 ? head : head.slice(0, blockEnd);
  const unfolded = block.replace(/\r?\n[ \t]+/g, ' ');
  const line = unfolded.split(/\r?\n/).find((l) => /^authentication-results:/i.test(l));
  if (line === undefined) return { spf: null, dkim: null, pass: false };
  const results = (method: string): string[] =>
    [...line.matchAll(new RegExp(`\\b${method}=([a-z]+)`, 'gi'))].map((m) => (m[1] ?? '').toLowerCase());
  const spf = results('spf');
  const dkim = results('dkim');
  return {
    spf: spf[0] ?? null,
    dkim: dkim.includes('pass') ? 'pass' : (dkim[0] ?? null),
    pass: spf.includes('pass') || dkim.includes('pass'),
  };
}

async function readAll(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

export async function handleEmail(
  message: InboundMessage,
  env: Env,
  deps: { now?: () => Date } = {},
): Promise<EmailOutcome> {
  const reject = (reason: string): EmailOutcome => {
    message.setReject(reason);
    return { status: 'rejected', reason };
  };

  // 1. Recipient.
  const alias = parseRecipient(message.to, env.INBOUND_DOMAIN);
  const household =
    alias === null
      ? null
      : await env.DB.prepare('SELECT household_id FROM households WHERE alias_local = ? AND deleted_at IS NULL')
          .bind(alias)
          .first<{ household_id: string }>();
  if (household === null) return reject('Unknown recipient');
  const household_id = household.household_id;

  // 2. Size cap (declared size first, so oversized mail is never read).
  const tooBig = `Message is larger than 25 MiB. Please upload it in ${env.APP_NAME} instead.`;
  if (message.rawSize > MAX_EMAIL_BYTES) return reject(tooBig);
  const raw = await readAll(message.raw);
  if (raw.length > MAX_EMAIL_BYTES) return reject(tooBig);

  // 3. Authenticity: SPF/DKIM pass, or a known member's address.
  const envelopeFrom = message.from.trim().toLowerCase();
  if (!authenticationVerdict(raw).pass) {
    const member = await env.DB.prepare('SELECT 1 AS ok FROM members WHERE household_id = ? AND lower(email) = ?')
      .bind(household_id, envelopeFrom)
      .first<{ ok: number }>();
    if (member === null) return reject('Sender could not be verified for this address');
  }

  // 4. Identify, then the funnel's Layers 0-2 (no R2 write and no model call happens before this point is passed).
  const sha256 = await sha256Hex(raw);
  const episode_id = `ep_${(await sha256Hex(`${household_id}:${sha256}`)).slice(0, 32)}`;
  let claimed: string | null = null;
  let mail: InboundMail | null = null;
  try {
    mail = await parseInbound(raw);
  } catch {
    mail = null; // an unparseable message skips the funnel; the pipeline will deal with it as before
  }
  if (mail !== null) {
    const admission = await admitInbound(env.DB, household_id, 'email_forward', mail, { now: (deps.now ?? (() => new Date()))(), episode_id });
    if (admission.action === 'duplicate') return { status: 'duplicate', episode_id: admission.episode_id ?? episode_id };
    if (admission.action === 'dropped_bulk') return { status: 'dropped_bulk', signals: admission.signals };
    if (admission.action === 'forwarding_confirmation') return { status: 'forwarding_confirmation', provider: admission.provider };
    claimed = admission.message_id_norm;
  }

  // 5. Dedupe on the raw bytes, encrypt at rest, hand off (a failure after the claim gives the claim back).
  const handed = await handOff(env, { household_id, raw, sha256, episode_id, envelope_from: envelopeFrom, claimed, channel: 'email_forward' });
  if (handed.status === 'duplicate') return { status: 'duplicate', episode_id: handed.episode_id };
  return { status: 'accepted', episode_id, instance_id: episode_id };
}
