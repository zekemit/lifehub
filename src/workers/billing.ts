/**
 * src/workers/billing.ts — credits for the signed-in member.
 *
 *   GET  /api/credits/balance        total, subscription and purchased credits; the pack catalogue; the auto-recharge settings
 *   POST /api/credits/checkout       {pack_sku} -> a Stripe Checkout Session for that pack: {checkout_url, session_id}
 *   POST /api/credits/auto-recharge  {enabled, pack_sku?, monthly_cap_cents?} -> opt in or out of off-session top-ups (owner only)
 *
 * The card is entered on Stripe's page. This app stores only Stripe's ids for the customer and the payment method.
 */

import { autoRechargeSettings } from '../billing/autoRecharge';
import { creditBalances } from '../billing/credits';
import { MAX_AUTO_RECHARGE_CAP_CENTS, PACKS, PACK_SKUS, isPackSku } from '../billing/packs';
import { StripeError, stripeClient } from '../billing/stripe';
import type { Env } from '../types';
import type { Auth } from './auth';
import { error, json } from './http';

async function readJson(request: Request): Promise<Record<string, unknown> | null> {
  try {
    const body = (await request.json()) as unknown;
    return body !== null && typeof body === 'object' && !Array.isArray(body) ? (body as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

const packCatalogue = () => PACK_SKUS.map((sku) => ({ sku, credits: PACKS[sku].credits, price_cents: PACKS[sku].price_cents }));

async function settingsView(db: D1Database, household_id: string, now: Date) {
  const s = await autoRechargeSettings(db, household_id, now);
  if (s === null) return null;
  return {
    enabled: s.enabled,
    pack_sku: s.pack_sku,
    monthly_cap_cents: s.monthly_cap_cents,
    spent_this_month_cents: s.spent_this_month_cents,
    has_payment_method: s.payment_method_id !== null && s.customer_id !== null,
  };
}

export async function handleBalance(env: Pick<Env, 'DB'>, auth: Auth, now: Date): Promise<Response> {
  const [balances, hh, auto] = await Promise.all([
    creditBalances(env.DB, auth.household_id),
    env.DB.prepare('SELECT plan FROM households WHERE household_id = ?').bind(auth.household_id).first<{ plan: string }>(),
    settingsView(env.DB, auth.household_id, now),
  ]);
  return json(200, {
    balance: balances.total,
    subscription_credits: balances.subscription,
    purchased_credits: balances.purchased,
    plan: hh?.plan ?? null,
    packs: packCatalogue(),
    auto_recharge: auto,
  });
}

export async function handleCheckout(request: Request, env: Pick<Env, 'DB' | 'APP_NAME' | 'STRIPE_SECRET_KEY'>, auth: Auth, deps: { fetch: typeof fetch }): Promise<Response> {
  const body = await readJson(request);
  const sku = body?.['pack_sku'];
  if (!isPackSku(sku)) return error(400, 'bad_pack_sku', `pack_sku must be one of ${PACK_SKUS.join(', ')}.`);
  if (!env.STRIPE_SECRET_KEY) return error(503, 'billing_not_configured', 'Top-ups are not available right now.');
  const hh = await env.DB.prepare('SELECT stripe_customer_id FROM households WHERE household_id = ? AND deleted_at IS NULL').bind(auth.household_id).first<{ stripe_customer_id: string | null }>();
  if (hh === null) return error(401, 'unauthorized', 'Sign in again.');
  const origin = new URL(request.url).origin;
  try {
    const session = await stripeClient({ apiKey: env.STRIPE_SECRET_KEY, fetch: deps.fetch }).createCheckoutSession({
      household_id: auth.household_id,
      sku,
      productName: `${env.APP_NAME} credits (${PACKS[sku].credits})`,
      customer: hh.stripe_customer_id,
      successUrl: `${origin}/?checkout=success&session_id={CHECKOUT_SESSION_ID}`,
      cancelUrl: `${origin}/?checkout=canceled`,
    });
    return json(200, { checkout_url: session.url, session_id: session.id });
  } catch (err) {
    if (err instanceof StripeError) return error(502, 'stripe_error', 'Stripe could not start the checkout. Try again.');
    throw err;
  }
}

export async function handleAutoRecharge(request: Request, env: Pick<Env, 'DB'>, auth: Auth, now: Date): Promise<Response> {
  const role = await env.DB.prepare('SELECT role FROM members WHERE member_id = ? AND household_id = ?').bind(auth.member_id, auth.household_id).first<{ role: string }>();
  if (role?.role !== 'owner') return error(403, 'owner_only', 'Only the household owner can change auto-recharge.');
  const body = await readJson(request);
  if (body === null || typeof body['enabled'] !== 'boolean') return error(400, 'bad_request', 'Send {"enabled": true|false}.');
  const current = await autoRechargeSettings(env.DB, auth.household_id, now);
  if (current === null) return error(401, 'unauthorized', 'Sign in again.');
  const sku = body['pack_sku'] === undefined ? current.pack_sku : body['pack_sku'];
  if (!isPackSku(sku)) return error(400, 'bad_pack_sku', `pack_sku must be one of ${PACK_SKUS.join(', ')}.`);
  const cap = body['monthly_cap_cents'] === undefined ? current.monthly_cap_cents : body['monthly_cap_cents'];
  if (!Number.isInteger(cap) || (cap as number) < PACKS[sku].price_cents || (cap as number) > MAX_AUTO_RECHARGE_CAP_CENTS) {
    return error(400, 'bad_cap', `monthly_cap_cents must be a whole number of cents between the pack price (${PACKS[sku].price_cents}) and ${MAX_AUTO_RECHARGE_CAP_CENTS}.`);
  }
  if (body['enabled'] && (current.payment_method_id === null || current.customer_id === null)) {
    // opt-in needs a card Stripe already holds: it is saved when the member buys a pack through Checkout
    return json(409, { error: 'no_payment_method', message: 'Buy a top-up pack first so a card is saved, then turn auto-recharge on.', checkout: '/api/credits/checkout' });
  }
  await env.DB
    .prepare('UPDATE households SET auto_recharge_enabled = ?, auto_recharge_pack_sku = ?, auto_recharge_monthly_cap_cents = ? WHERE household_id = ?')
    .bind(body['enabled'] ? 1 : 0, sku, cap, auth.household_id)
    .run();
  return json(200, { auto_recharge: await settingsView(env.DB, auth.household_id, now) });
}
