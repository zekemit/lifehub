/**
 * Milestone 5 fixtures: Stripe webhook signing and event builders, a venue with a stored release rule, and the one shared app origin.
 * Real D1/R2/KV underneath; only the network (Stripe, Duffel, Google, models, push) and the Workflows engine are faked.
 */

import { createHmac, randomUUID } from 'node:crypto';
import { handleApi } from '../../src/workers/api';
import type { Env } from '../../src/types';
import type { Household } from './m2';

export const APP_ORIGIN = 'https://app.example';
export const WEBHOOK_SECRET = 'whsec_test_0123456789abcdef';

/** A `Stripe-Signature` header value: t=<unix>,v1=<hex HMAC-SHA256 of "<t>.<raw body>">. */
export function stripeSignature(body: string, o: { secret?: string; t?: number } = {}): string {
  const t = o.t ?? Math.floor(Date.now() / 1000);
  return `t=${t},v1=${createHmac('sha256', o.secret ?? WEBHOOK_SECRET).update(`${t}.${body}`).digest('hex')}`;
}

export interface StripeEvent {
  id: string;
  type: string;
  data: { object: Record<string, unknown> };
}

export const stripeEvent = (type: string, object: Record<string, unknown>, id = `evt_${randomUUID().replace(/-/g, '')}`): StripeEvent => ({ id, type, data: { object } });

/** POSTs a (by default correctly signed) webhook to the real handler, with no session and no Origin, like Stripe. */
export function postWebhook(env: Env, event: StripeEvent | string, o: { signature?: string | null; secret?: string; t?: number; now?: Date } = {}): Promise<Response> {
  const raw = typeof event === 'string' ? event : JSON.stringify(event);
  const signature = o.signature === undefined ? stripeSignature(raw, { ...(o.secret === undefined ? {} : { secret: o.secret }), ...(o.t === undefined ? {} : { t: o.t }) }) : o.signature;
  return handleApi(
    new Request(`${APP_ORIGIN}/api/webhooks/stripe`, { method: 'POST', headers: { 'content-type': 'application/json', ...(signature === null ? {} : { 'stripe-signature': signature }) }, body: raw }),
    env,
    { now: () => o.now ?? new Date() },
  );
}

export const invoicePaid = (o: { customer: string; interval: 'month' | 'year'; household_id?: string; amount_paid?: number; periodEnd?: number; id?: string }): StripeEvent =>
  stripeEvent('invoice.paid', {
    id: o.id ?? `in_${randomUUID().replace(/-/g, '').slice(0, 14)}`,
    customer: o.customer,
    amount_paid: o.amount_paid ?? (o.interval === 'year' ? 17999 : 1999),
    ...(o.household_id === undefined ? {} : { subscription_details: { metadata: { household_id: o.household_id } } }),
    lines: { data: [{ price: { recurring: { interval: o.interval } }, period: { end: o.periodEnd ?? Math.floor(Date.now() / 1000) + 30 * 86_400 } }] },
  });

export const packSession = (o: { household_id: string; sku: string; pi: string; amount: number; customer?: string; paid?: boolean; credits?: number }): StripeEvent =>
  stripeEvent('checkout.session.completed', {
    id: `cs_${randomUUID().replace(/-/g, '').slice(0, 14)}`,
    mode: 'payment',
    payment_status: o.paid === false ? 'unpaid' : 'paid',
    payment_intent: o.pi,
    customer: o.customer ?? null,
    amount_total: o.amount,
    client_reference_id: o.household_id,
    metadata: { household_id: o.household_id, pack_sku: o.sku, ...(o.credits === undefined ? {} : { credits: o.credits }) },
  });

export const packIntent = (o: { household_id: string; sku: string; pi: string; amount: number; customer?: string; paymentMethod?: string; saveCard?: boolean; kind?: string; recharge_id?: string }): StripeEvent =>
  stripeEvent('payment_intent.succeeded', {
    id: o.pi,
    amount_received: o.amount,
    customer: o.customer ?? null,
    payment_method: o.paymentMethod ?? null,
    ...(o.saveCard === true ? { setup_future_usage: 'off_session' } : {}),
    metadata: { household_id: o.household_id, pack_sku: o.sku, ...(o.kind === undefined ? {} : { kind: o.kind }), ...(o.recharge_id === undefined ? {} : { recharge_id: o.recharge_id }) },
  });

/** A venue (an entity of the household) with a stored release rule. Returns the entity id. */
export async function makeVenue(
  db: D1Database,
  h: Household,
  o: { name?: string; platform?: 'resy' | 'opentable' | 'tock' | 'sevenrooms' | 'direct'; url?: string; days_ahead?: number; time?: string; tz?: string } = {},
): Promise<{ venue_entity_id: string; name: string }> {
  const venue_entity_id = `ent_${randomUUID().replace(/-/g, '').slice(0, 20)}`;
  const name = o.name ?? `Venue ${venue_entity_id.slice(-6)}`;
  await db.batch([
    db.prepare(`INSERT INTO entities (entity_id, household_id, type, canonical_name) VALUES (?, ?, 'venue', ?)`).bind(venue_entity_id, h.household_id, name),
    db
      .prepare(`INSERT INTO venue_release_rules (venue_entity_id, platform, venue_url, days_ahead, release_local_time, release_tz, verified_on) VALUES (?, ?, ?, ?, ?, ?, '2026-11-26')`)
      .bind(venue_entity_id, o.platform ?? 'resy', o.url ?? 'https://resy.com/cities/ny/some-venue', o.days_ahead ?? 30, o.time ?? '09:00', o.tz ?? 'America/New_York'),
  ]);
  return { venue_entity_id, name };
}
