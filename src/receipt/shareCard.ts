/**
 * src/receipt/shareCard.ts — the shareable achievement card, GET /api/receipt/share-card.svg.
 *
 * Privacy is the whole design: the card is built from TWO aggregate numbers and the product name, and from nothing else.
 *   - money recovered, rounded DOWN to a $100 step and shown as "$400+"
 *   - documents filed (a count)
 *   - "0 prompts typed": the product has no prompt box, so this is always 0
 * No merchant, card, venue, date, place, person or tax-vault value can reach it, because none is ever read here: the queries below
 * return counts and one sum, and the SVG is a fixed template with only those values (and the escaped product name) filled in.
 */

import { escapeHtml } from './render';

export const SHARE_BUCKET_CENTS = 10_000;

export interface ShareStats {
  recovered_cents: number;
  documents_filed: number;
}

/** Credits actually used through missions: confirmed credit-recovery runs only, weighted by confidence like the Home header. */
export async function shareStats(db: D1Database, household_id: string): Promise<ShareStats> {
  const recovered = await db
    .prepare(
      `SELECT COALESCE(SUM(CAST(json_extract(params_json, '$.recoverable_cents') * COALESCE(json_extract(params_json, '$.confidence'), 1.0) AS INTEGER)), 0) AS cents
         FROM mission_runs WHERE household_id = ? AND state = 'CONFIRMED' AND template_key IN ('expiring_credit', 'anniv_dining')`,
    )
    .bind(household_id)
    .first<{ cents: number }>();
  const docs = await db
    .prepare(
      `SELECT COUNT(*) AS n FROM episodes e
        WHERE e.household_id = ? AND e.source IN ('email', 'pdf', 'csv', 'upload') AND e.redaction_status IN ('clean', 'redacted')
          AND EXISTS (SELECT 1 FROM facts f WHERE f.source_episode_id = e.episode_id)`,
    )
    .bind(household_id)
    .first<{ n: number }>();
  return { recovered_cents: recovered?.cents ?? 0, documents_filed: docs?.n ?? 0 };
}

/** $100 steps, rounded down. 39_999 cents -> 0; 40_000 -> 400. */
export const bucketDollars = (cents: number): number => Math.floor(Math.max(0, cents) / SHARE_BUCKET_CENTS) * 100;

export function renderShareCard(stats: ShareStats, appName: string): string {
  const dollars = bucketDollars(stats.recovered_cents);
  const headline = dollars > 0 ? `${appName} recovered $${dollars}+ in card credits` : `${appName} is finding card credits for you`;
  const sub = `${stats.documents_filed} ${stats.documents_filed === 1 ? 'receipt' : 'receipts'} filed · 0 prompts typed`;
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630" role="img" aria-label="${escapeHtml(`${headline} · ${sub}`)}">`,
    '<rect width="1200" height="630" fill="#101418"/>',
    `<text x="80" y="290" font-family="Helvetica, Arial, sans-serif" font-size="64" font-weight="700" fill="#ffffff">${escapeHtml(headline)}</text>`,
    `<text x="80" y="380" font-family="Helvetica, Arial, sans-serif" font-size="40" fill="#9fb3c8">${escapeHtml(sub)}</text>`,
    '</svg>',
    '',
  ].join('\n');
}
