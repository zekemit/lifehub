/**
 * src/receipt/render.ts — the Life Receipt as an e-mail (text and HTML). Everything interpolated is escaped for HTML.
 */

import { usd } from '../missions/copy';
import type { ReceiptPayload } from './pipeline';

export const escapeHtml = (s: string): string =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

export function renderReceiptText(p: ReceiptPayload, appName: string): string {
  const bullet = (items: string[]): string => (items.length === 0 ? '  (nothing this week)' : items.map((t) => `  - ${t}`).join('\n'));
  return [
    `${appName} — your week (${p.week_start})`,
    '',
    `FOUND: ${usd(p.found.total_cents)}`,
    bullet(p.found.lines.map((l) => l.text)),
    '',
    'DONE',
    bullet(p.done.lines.map((l) => l.text)),
    '',
    'WAITING ON ONE TAP',
    bullet(p.waiting.map((w) => w.title)),
    '',
    'NEXT WEEK',
    bullet(p.next_week.map((l) => l.text)),
    '',
    'PROOF',
    ...p.proof.flatMap((e) => [`  ${e.text}`, ...e.facts.flatMap((f) => f.quotes.slice(0, 1).map((q) => `    "${q.quote}"`))]),
    '',
    'Nothing is booked or paid without your tap.',
  ].join('\n');
}

export function renderReceiptHtml(p: ReceiptPayload, appName: string): string {
  const list = (items: string[]): string => (items.length === 0 ? '<p>Nothing this week.</p>' : `<ul>${items.map((t) => `<li>${escapeHtml(t)}</li>`).join('')}</ul>`);
  const proof = p.proof
    .map((e) => `<li>${escapeHtml(e.text)}${e.facts.flatMap((f) => f.quotes.slice(0, 1).map((q) => `<blockquote>${escapeHtml(q.quote)}</blockquote>`)).join('')}</li>`)
    .join('');
  return [
    `<h1>${escapeHtml(appName)} — your week</h1>`,
    `<h2>Found: ${escapeHtml(usd(p.found.total_cents))}</h2>${list(p.found.lines.map((l) => l.text))}`,
    `<h2>Done</h2>${list(p.done.lines.map((l) => l.text))}`,
    `<h2>Waiting on one tap</h2>${list(p.waiting.map((w) => w.title))}`,
    `<h2>Next week</h2>${list(p.next_week.map((l) => l.text))}`,
    `<h2>Proof</h2><ul>${proof}</ul>`,
    '<p>Nothing is booked or paid without your tap.</p>',
  ].join('\n');
}
