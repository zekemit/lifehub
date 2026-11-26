/**
 * src/billing/stripeWebhook.ts — POST /api/webhooks/stripe.
 *
 * Not behind the session middleware or the cookie CSRF check: Stripe has neither. What authenticates the request is the
 * Stripe-Signature header, an HMAC-SHA256 over "<timestamp>.<raw body>" with STRIPE_WEBHOOK_SECRET, checked against the RAW body
 * (request.text(), never a re-serialised JSON) with a 300-second tolerance.
 *
 * Every grant is idempotent through credit_ledger.stripe_event_id (UNIQUE), so Stripe's retries and replays cannot double-grant:
 *   invoice.paid                         +15 (monthly) or +12 (annual) subscription credits, keyed by the event id. Unused subscription
 *                                        credits above the 15-credit rollover cap expire first; purchased credits are never touched.
 *   checkout.session.completed           a paid top-up pack, keyed 'pack:<payment intent>' ...
 *   payment_intent.succeeded             ... the same key, so whichever of the two arrives first grants and the other is a no-op
 *   customer.subscription.deleted        plan = 'canceled'; purchased credits stay
 * A 2xx is returned for every verified event, including ones about households we do not know: a retry could not fix those.
 */

import type { Env } from '../types';
import { error, json } from '../workers/http';
import { grantMonthly, grantPack } from './credits';
import { ANNUAL_MONTHLY_GRANT_CREDITS, MONTHLY_GRANT_CREDITS, PACKS, isPackSku, type PackSku } from './packs';
import { verifyStripeSignature } from './stripe';

type Obj = Record<string, unknown>;
const obj = (v: unknown): Obj => (v !== null && typeof v === 'object' && !Array.isArray(v) ? (v as Obj) : {});
const str = (v: unknown): string | null => (typeof v === 'string' && v !== '' ? v : null);

interface Outcome {
  household_id: string | null;
  outcome: string;
}

async function householdFor(db: D1Database, o: { household_id?: string | null; customer?: string | null }): Promise<string | null> {
  if (o.household_id) {
    const row = await db.prepare('SELECT household_id FROM households WHERE household_id = ? AND deleted_at IS NULL').bind(o.household_id).first<{ household_id: string }>();
    if (row !== null) return row.household_id;
  }
  if (o.customer) {
    const row = await db.prepare('SELECT household_id FROM households WHERE stripe_customer_id = ? AND deleted_at IS NULL').bind(o.customer).first<{ household_id: string }>();
    if (row !== null) return row.household_id;
  }
  return null;
}

async function audit(db: D1Database, household_id: string | null, action: string, detail: Obj, at: string): Promise<void> {
  await db
    .prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'stripe', ?, ?, ?)`)
    .bind(`aud_${crypto.randomUUID().replace(/-/g, '')}`, household_id, action, JSON.stringify(detail), at)
    .run();
}

async function saveCustomer(db: D1Database, household_id: string, customer: string | null): Promise<void> {
  if (customer === null) return;
  await db.prepare('UPDATE households SET stripe_customer_id = COALESCE(stripe_customer_id, ?) WHERE household_id = ?').bind(customer, household_id).run();
}

async function invoicePaid(db: D1Database, eventId: string, invoice: Obj, at: string): Promise<Outcome> {
  const lines = obj(invoice['lines'])['data'];
  const line = obj(Array.isArray(lines) ? lines[0] : undefined);
  const interval = str(obj(obj(line['price'])['recurring'])['interval']) ?? str(obj(line['plan'])['interval']);
  const meta = { ...obj(invoice['metadata']), ...obj(line['metadata']), ...obj(obj(invoice['subscription_details'])['metadata']) };
  const customer = str(invoice['customer']);
  const household_id = await householdFor(db, { household_id: str(meta['household_id']), customer });
  if (household_id === null) return { household_id: null, outcome: 'unknown_household' };
  if (interval !== 'month' && interval !== 'year') return { household_id, outcome: 'ignored_interval' };
  const plan = interval === 'year' ? 'annual' : 'monthly';
  const end = typeof obj(line['period'])['end'] === 'number' ? new Date((obj(line['period'])['end'] as number) * 1000).toISOString() : null;
  await db
    .prepare(`UPDATE households SET plan = ?, stripe_customer_id = COALESCE(stripe_customer_id, ?), plan_period_end = ? WHERE household_id = ?`)
    .bind(plan, customer, end, household_id)
    .run();
  // A $0 invoice (a trial, or a 100% coupon) buys no credits: otherwise starting and cancelling a trial would print them.
  if (typeof invoice['amount_paid'] === 'number' && invoice['amount_paid'] <= 0) return { household_id, outcome: 'zero_amount_no_grant' };
  const credits = plan === 'annual' ? ANNUAL_MONTHLY_GRANT_CREDITS : MONTHLY_GRANT_CREDITS;
  const res = await grantMonthly(db, household_id, { key: eventId, credits }, at);
  return { household_id, outcome: res.granted ? `granted_${credits}_expired_${res.expired}` : 'already_granted' };
}

/** A paid top-up pack, from either event. The ledger key is the payment intent, so both events (and any replay) grant exactly once. */
async function packPaid(db: D1Database, o: { household_id: string | null; customer: string | null; sku: unknown; paymentIntent: string | null; fallbackKey: string; amount: unknown; paid: boolean }, at: string): Promise<Outcome> {
  const household_id = await householdFor(db, { household_id: o.household_id, customer: o.customer });
  if (household_id === null) return { household_id: null, outcome: 'unknown_household' };
  if (!isPackSku(o.sku)) return { household_id, outcome: 'ignored_unknown_sku' };
  const sku: PackSku = o.sku;
  if (!o.paid) return { household_id, outcome: 'not_paid_yet' };
  // The SKU table decides what was bought. An amount that is not that pack's price is refused, never trusted.
  if (typeof o.amount === 'number' && o.amount !== PACKS[sku].price_cents) {
    await audit(db, household_id, 'pack_amount_mismatch', { sku, expected: PACKS[sku].price_cents, got: o.amount }, at);
    return { household_id, outcome: 'amount_mismatch_no_grant' };
  }
  await saveCustomer(db, household_id, o.customer);
  const granted = await grantPack(db, household_id, { key: `pack:${o.paymentIntent ?? o.fallbackKey}`, sku }, at);
  return { household_id, outcome: granted ? `granted_${PACKS[sku].credits}` : 'already_granted' };
}

async function handleEvent(db: D1Database, event: Obj, at: string): Promise<Outcome> {
  const type = str(event['type']) ?? '';
  const data = obj(obj(event['data'])['object']);
  const meta = obj(data['metadata']);
  switch (type) {
    case 'invoice.paid':
      return invoicePaid(db, str(event['id']) ?? '', data, at);
    case 'checkout.session.completed': {
      const household_id = str(meta['household_id']) ?? str(data['client_reference_id']);
      if (data['mode'] === 'subscription') {
        const hh = await householdFor(db, { household_id, customer: str(data['customer']) });
        if (hh !== null) await saveCustomer(db, hh, str(data['customer']));
        return { household_id: hh, outcome: hh === null ? 'unknown_household' : 'customer_linked' };
      }
      if (data['mode'] !== 'payment') return { household_id: null, outcome: 'ignored_mode' };
      return packPaid(db, { household_id, customer: str(data['customer']), sku: meta['pack_sku'], paymentIntent: str(data['payment_intent']), fallbackKey: `cs:${str(data['id']) ?? ''}`, amount: data['amount_total'], paid: data['payment_status'] === 'paid' }, at);
    }
    case 'payment_intent.succeeded': {
      if (!isPackSku(meta['pack_sku'])) return { household_id: null, outcome: 'ignored_not_a_pack' };
      const out = await packPaid(db, { household_id: str(meta['household_id']), customer: str(data['customer']), sku: meta['pack_sku'], paymentIntent: str(data['id']), fallbackKey: `pi:${str(data['id']) ?? ''}`, amount: data['amount_received'], paid: true }, at);
      const pm = str(data['payment_method']);
      // Keep the card for auto-recharge only when the member agreed to that at checkout (setup_future_usage) or it is already an auto-recharge.
      if (out.household_id !== null && pm !== null && (data['setup_future_usage'] === 'off_session' || meta['kind'] === 'auto_recharge')) {
        await db.prepare('UPDATE households SET stripe_default_payment_method_id = ? WHERE household_id = ?').bind(pm, out.household_id).run();
      }
      if (meta['kind'] === 'auto_recharge' && str(meta['recharge_id']) !== null) {
        await db.prepare(`UPDATE auto_recharges SET status = 'succeeded', payment_intent_id = COALESCE(payment_intent_id, ?), updated_at = ? WHERE recharge_id = ? AND status = 'pending'`).bind(str(data['id']), at, str(meta['recharge_id'])).run();
      }
      return out;
    }
    case 'customer.subscription.deleted': {
      const household_id = await householdFor(db, { household_id: str(meta['household_id']), customer: str(data['customer']) });
      if (household_id === null) return { household_id: null, outcome: 'unknown_household' };
      await db.prepare(`UPDATE households SET plan = 'canceled', plan_period_end = NULL WHERE household_id = ?`).bind(household_id).run();
      return { household_id, outcome: 'plan_canceled' };
    }
    default:
      return { household_id: null, outcome: 'ignored_type' };
  }
}

export async function handleStripeWebhook(request: Request, env: Pick<Env, 'DB' | 'STRIPE_WEBHOOK_SECRET'>, deps: { now: () => Date }): Promise<Response> {
  if (!env.STRIPE_WEBHOOK_SECRET) return error(503, 'not_configured', 'Stripe webhooks are not configured.');
  const raw = await request.text();
  const verdict = await verifyStripeSignature(raw, request.headers.get('stripe-signature'), env.STRIPE_WEBHOOK_SECRET, deps.now().getTime());
  if (!verdict.ok) return error(400, verdict.reason, 'The Stripe signature did not verify.');
  let event: Obj;
  try {
    event = obj(JSON.parse(raw));
  } catch {
    return error(400, 'bad_json', 'The body is not JSON.');
  }
  const eventId = str(event['id']);
  const type = str(event['type']);
  if (eventId === null || type === null) return error(400, 'bad_event', 'The event has no id or type.');

  const at = deps.now().toISOString();
  const seen = await env.DB.prepare('SELECT outcome FROM stripe_events WHERE event_id = ?').bind(eventId).first<{ outcome: string }>();
  if (seen !== null) return json(200, { received: true, duplicate: true, outcome: seen.outcome });

  const result = await handleEvent(env.DB, event, at);
  await env.DB
    .prepare('INSERT OR IGNORE INTO stripe_events (event_id, type, household_id, outcome, received_at) VALUES (?, ?, ?, ?, ?)')
    .bind(eventId, type, result.household_id, result.outcome, at)
    .run();
  return json(200, { received: true, outcome: result.outcome });
}
