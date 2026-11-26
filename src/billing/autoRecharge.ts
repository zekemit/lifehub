/**
 * src/billing/autoRecharge.ts — optional, opt-in top-ups on the member's behalf.
 *
 * When a mission cannot be paid for and the household has switched auto-recharge on, saved a payment method, and this month's
 * auto-recharge spend plus the pack price still fits the monthly cap, the app charges that saved method off-session for the household's
 * chosen pack, grants the pack, and the mission carries on. Otherwise the mission halts as INSUFFICIENT_CREDITS and nothing is charged.
 *
 * The cap is enforced by ONE conditional INSERT into auto_recharges, so two missions racing for the last dollar of the cap cannot both
 * charge. The row id is per run and attempt, and the Stripe idempotency key follows it, so a retried step never charges twice.
 */

import { grantPack } from './credits';
import { PACKS, isPackSku, type PackSku } from './packs';
import type { StripeClient } from './stripe';

export const monthStartIso = (at: Date): string => new Date(Date.UTC(at.getUTCFullYear(), at.getUTCMonth(), 1)).toISOString();

export interface AutoRechargeSettings {
  enabled: boolean;
  pack_sku: PackSku;
  monthly_cap_cents: number;
  payment_method_id: string | null;
  customer_id: string | null;
  spent_this_month_cents: number;
}

export async function autoRechargeSettings(db: D1Database, household_id: string, at: Date): Promise<AutoRechargeSettings | null> {
  const h = await db
    .prepare(
      `SELECT auto_recharge_enabled AS enabled, auto_recharge_pack_sku AS sku, auto_recharge_monthly_cap_cents AS cap,
              stripe_default_payment_method_id AS pm, stripe_customer_id AS customer
         FROM households WHERE household_id = ? AND deleted_at IS NULL`,
    )
    .bind(household_id)
    .first<{ enabled: number; sku: string; cap: number; pm: string | null; customer: string | null }>();
  if (h === null) return null;
  const spent = await db
    .prepare(`SELECT COALESCE(SUM(amount_cents), 0) AS n FROM auto_recharges WHERE household_id = ? AND status IN ('pending', 'succeeded') AND created_at >= ?`)
    .bind(household_id, monthStartIso(at))
    .first<{ n: number }>();
  return {
    enabled: h.enabled === 1,
    pack_sku: isPackSku(h.sku) ? h.sku : 'pack_10',
    monthly_cap_cents: h.cap,
    payment_method_id: h.pm,
    customer_id: h.customer,
    spent_this_month_cents: spent?.n ?? 0,
  };
}

export type DeclineReason = 'disabled' | 'no_payment_method' | 'monthly_cap';

/** Would an auto-recharge be allowed right now? Pure: no charge, no write. */
export function autoRechargeAllowed(s: AutoRechargeSettings): { ok: true } | { ok: false; reason: DeclineReason } {
  if (!s.enabled) return { ok: false, reason: 'disabled' };
  if (s.payment_method_id === null || s.customer_id === null) return { ok: false, reason: 'no_payment_method' };
  if (s.spent_this_month_cents + PACKS[s.pack_sku].price_cents > s.monthly_cap_cents) return { ok: false, reason: 'monthly_cap' };
  return { ok: true };
}

export type AutoRechargeResult =
  | { status: 'recharged'; sku: PackSku; credits: number; payment_intent_id: string }
  | { status: 'declined'; reason: DeclineReason | 'stripe_not_configured' | 'payment_failed' };

export async function attemptAutoRecharge(
  deps: { db: D1Database; stripe: StripeClient | null; now: () => Date },
  household_id: string,
  run_id: string,
  attempt: number,
): Promise<AutoRechargeResult> {
  const { db } = deps;
  const at = deps.now();
  const iso = at.toISOString();
  const recharge_id = `rch_${run_id}_${attempt}`;

  const prior = await db.prepare('SELECT status, payment_intent_id, pack_sku FROM auto_recharges WHERE recharge_id = ? AND household_id = ?').bind(recharge_id, household_id).first<{ status: string; payment_intent_id: string | null; pack_sku: string }>();
  if (prior?.status === 'succeeded' && prior.payment_intent_id !== null && isPackSku(prior.pack_sku)) {
    await grantPack(db, household_id, { key: `pack:${prior.payment_intent_id}`, sku: prior.pack_sku }, iso);
    return { status: 'recharged', sku: prior.pack_sku, credits: PACKS[prior.pack_sku].credits, payment_intent_id: prior.payment_intent_id };
  }
  if (prior?.status === 'failed') return { status: 'declined', reason: 'payment_failed' };

  const settings = await autoRechargeSettings(db, household_id, at);
  if (settings === null) return { status: 'declined', reason: 'disabled' };
  if (prior === null) {
    const allowed = autoRechargeAllowed({ ...settings, spent_this_month_cents: 0 });
    if (!allowed.ok) return { status: 'declined', reason: allowed.reason };
    const pack = PACKS[settings.pack_sku];
    const reserved = await db
      .prepare(
        `INSERT OR IGNORE INTO auto_recharges (recharge_id, household_id, pack_sku, credits, amount_cents, run_id, status, created_at, updated_at)
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7, ?7
          WHERE (SELECT COALESCE(SUM(amount_cents), 0) FROM auto_recharges WHERE household_id = ?2 AND status IN ('pending', 'succeeded') AND created_at >= ?8) + ?5
                <= (SELECT auto_recharge_monthly_cap_cents FROM households WHERE household_id = ?2 AND auto_recharge_enabled = 1)`,
      )
      .bind(recharge_id, household_id, settings.pack_sku, pack.credits, pack.price_cents, run_id, iso, monthStartIso(at))
      .run();
    if ((reserved.meta.changes ?? 0) === 0) {
      const after = await autoRechargeSettings(db, household_id, at);
      return { status: 'declined', reason: after !== null && !after.enabled ? 'disabled' : 'monthly_cap' };
    }
  }
  const sku = prior !== null && isPackSku(prior.pack_sku) ? prior.pack_sku : settings.pack_sku;
  const fail = async (error: string, reason: 'stripe_not_configured' | 'payment_failed'): Promise<AutoRechargeResult> => {
    await db.prepare(`UPDATE auto_recharges SET status = 'failed', error = ?, updated_at = ? WHERE recharge_id = ?`).bind(error.slice(0, 200), deps.now().toISOString(), recharge_id).run();
    return { status: 'declined', reason };
  };
  if (deps.stripe === null) return fail('Stripe is not configured', 'stripe_not_configured');
  if (settings.customer_id === null || settings.payment_method_id === null) return fail('no saved payment method', 'payment_failed');
  try {
    const pi = await deps.stripe.createOffSessionPayment({
      household_id,
      sku,
      customer: settings.customer_id,
      paymentMethod: settings.payment_method_id,
      idempotencyKey: recharge_id,
      description: `Auto-recharge ${PACKS[sku].credits} credits`,
    });
    if (pi.status !== 'succeeded') return fail(`payment ${pi.status || 'did not succeed'}`, 'payment_failed');
    await db.prepare(`UPDATE auto_recharges SET status = 'succeeded', payment_intent_id = ?, updated_at = ? WHERE recharge_id = ?`).bind(pi.id, deps.now().toISOString(), recharge_id).run();
    await grantPack(db, household_id, { key: `pack:${pi.id}`, sku }, deps.now().toISOString());
    return { status: 'recharged', sku, credits: PACKS[sku].credits, payment_intent_id: pi.id };
  } catch (err) {
    return fail(err instanceof Error ? err.message : 'payment failed', 'payment_failed');
  }
}
