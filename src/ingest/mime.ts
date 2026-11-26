/**
 * src/ingest/mime.ts — raw MIME -> the plain text an episode is built from.
 */

import PostalMime from 'postal-mime';

/** Body characters kept per email. Keeps episodes.redacted_text well under D1's 2 MB row limit even at 4 bytes/char. */
export const MAX_EMAIL_TEXT_CHARS = 400_000;

export interface ParsedEmail {
  /** Subject, sender, date and body as one text block (this is what gets redacted and stored). */
  text: string;
  subject: string;
  from: string;
  /** ISO timestamp of the Date header, or null if absent/invalid. */
  occurred_at: string | null;
}

const ENTITIES: Record<string, string> = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ' };

export function htmlToText(html: string): string {
  return html
    .replace(/<(script|style)[^>]*>[\s\S]*?<\/\1>/gi, ' ')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/(p|div|tr|li|h[1-6]|table)>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&#(\d+);/g, (_m, n: string) => String.fromCodePoint(Number(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_m, n: string) => String.fromCodePoint(parseInt(n, 16)))
    .replace(/&([a-z]+);/gi, (m, name: string) => ENTITIES[name.toLowerCase()] ?? m)
    .replace(/[ \t ]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

export async function parseEmail(raw: Uint8Array): Promise<ParsedEmail> {
  const mail = await PostalMime.parse(raw);
  const subject = (mail.subject ?? '').trim();
  const fromAddress = mail.from?.address ?? '';
  const from = mail.from?.name ? `${mail.from.name} <${fromAddress}>` : fromAddress;
  const when = mail.date ? new Date(mail.date) : null;
  const occurred_at = when !== null && Number.isFinite(when.getTime()) ? when.toISOString() : null;
  const body = (mail.text && mail.text.trim().length > 0 ? mail.text : mail.html ? htmlToText(mail.html) : '').trim();
  const header = [`From: ${from}`, `Subject: ${subject}`];
  if (occurred_at !== null) header.push(`Date: ${occurred_at}`);
  const text = `${header.join('\n')}\n\n${body}`.slice(0, MAX_EMAIL_TEXT_CHARS);
  return { text, subject, from, occurred_at };
}
