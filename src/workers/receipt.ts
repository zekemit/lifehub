/**
 * src/workers/receipt.ts — the Life Receipt for the signed-in member.
 *
 *   GET  /api/receipt/latest          the newest stored receipt; if there is none yet, one is generated now
 *   POST /api/receipt/generate        regenerates this week's receipt now (replacing it) and returns it
 *   GET  /api/receipt/share-card.svg  the shareable achievement card: two aggregate numbers and the product name, nothing else
 *
 * An on-demand receipt runs the same pipeline as the Sunday workflow, in this request, without the e-mail and push.
 */

import { inlineStep } from '../ingest/steps';
import { runLifeReceipt, type ReceiptDeps, type ReceiptPayload } from '../receipt/pipeline';
import { renderShareCard, shareStats } from '../receipt/shareCard';
import type { Env } from '../types';
import type { Auth } from './auth';
import { error, json } from './http';

type ReceiptEnv = Pick<Env, 'DB' | 'APP_NAME' | 'INBOUND_DOMAIN' | 'VAPID_PUBLIC_KEY' | 'VAPID_PRIVATE_KEY' | 'VAPID_SUBJECT' | 'RECEIPT_EMAIL'>;

async function generate(env: ReceiptEnv, auth: Auth, deps: { fetch: typeof fetch; now: () => Date }): Promise<void> {
  const receiptDeps: ReceiptDeps = { db: env.DB, fetch: deps.fetch, env, now: deps.now };
  await runLifeReceipt(inlineStep, receiptDeps, { household_id: auth.household_id, notify: false });
}

async function latest(db: D1Database, household_id: string): Promise<ReceiptPayload | null> {
  const row = await db.prepare('SELECT payload_json FROM life_receipts WHERE household_id = ? ORDER BY week_start DESC LIMIT 1').bind(household_id).first<{ payload_json: string }>();
  return row === null ? null : (JSON.parse(row.payload_json) as ReceiptPayload);
}

export async function handleReceiptLatest(env: ReceiptEnv, auth: Auth, deps: { fetch: typeof fetch; now: () => Date }): Promise<Response> {
  let receipt = await latest(env.DB, auth.household_id);
  if (receipt === null) {
    await generate(env, auth, deps);
    receipt = await latest(env.DB, auth.household_id);
  }
  return receipt === null ? error(404, 'no_receipt', 'No receipt yet.') : json(200, { receipt });
}

export async function handleReceiptGenerate(env: ReceiptEnv, auth: Auth, deps: { fetch: typeof fetch; now: () => Date }): Promise<Response> {
  await generate(env, auth, deps);
  const receipt = await latest(env.DB, auth.household_id);
  return receipt === null ? error(500, 'receipt_failed', 'The receipt could not be generated.') : json(200, { receipt });
}

export async function handleShareCard(env: Pick<Env, 'DB' | 'APP_NAME'>, auth: Auth): Promise<Response> {
  const svg = renderShareCard(await shareStats(env.DB, auth.household_id), env.APP_NAME);
  return new Response(svg, {
    status: 200,
    headers: {
      'content-type': 'image/svg+xml; charset=utf-8',
      'cache-control': 'private, no-store',
      'x-content-type-options': 'nosniff',
      // an SVG opened directly can run script; this one never has any, and this policy forbids it anyway
      'content-security-policy': "default-src 'none'; style-src 'unsafe-inline'",
      'content-disposition': 'inline; filename="share-card.svg"',
    },
  });
}
