/**
 * src/billing/stripe.ts — the two Stripe calls the app makes, and webhook signature verification.
 *
 * The app never sees a card number: Checkout collects the card on Stripe's page, and an auto-recharge charges a payment method that
 * Stripe already holds, off-session. Calls go through the guarded fetch (src/net/egress.ts) like every other outbound request.
 */

import type { PackSku } from './packs';
import { PACKS } from './packs';

export const STRIPE_API = 'https://api.stripe.com';
/** Pinned so Stripe's response shapes cannot change under the code. */
export const STRIPE_VERSION = '2024-06-20';

export class StripeError extends Error {
  override readonly name = 'StripeError';
  constructor(readonly status: number, readonly code: string | null, message: string) {
    super(message);
  }
}

/** Stripe's form encoding: nested keys as a[b][c], arrays as a[0]. */
export function formEncode(value: Record<string, unknown>): string {
  const pairs: string[] = [];
  const walk = (prefix: string, v: unknown): void => {
    if (v === undefined || v === null) return;
    if (Array.isArray(v)) v.forEach((item, i) => walk(`${prefix}[${i}]`, item));
    else if (typeof v === 'object') for (const [k, x] of Object.entries(v as Record<string, unknown>)) walk(`${prefix}[${k}]`, x);
    else pairs.push(`${encodeURIComponent(prefix)}=${encodeURIComponent(String(v))}`);
  };
  for (const [k, v] of Object.entries(value)) walk(k, v);
  return pairs.join('&');
}

export interface CheckoutSession {
  id: string;
  url: string;
}

export interface PaymentIntentResult {
  id: string;
  status: string;
}

export interface StripeClient {
  createCheckoutSession(o: {
    household_id: string;
    sku: PackSku;
    productName: string;
    customer: string | null;
    successUrl: string;
    cancelUrl: string;
  }): Promise<CheckoutSession>;
  /** Charges the saved payment method without the member present. `idempotencyKey` makes a retry safe. */
  createOffSessionPayment(o: {
    household_id: string;
    sku: PackSku;
    customer: string;
    paymentMethod: string;
    idempotencyKey: string;
    description: string;
  }): Promise<PaymentIntentResult>;
}

export function stripeClient(o: { apiKey: string; fetch: typeof fetch }): StripeClient {
  async function post(path: string, body: Record<string, unknown>, idempotencyKey?: string): Promise<Record<string, unknown>> {
    const res = await o.fetch(`${STRIPE_API}${path}`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${o.apiKey}`,
        'content-type': 'application/x-www-form-urlencoded',
        'stripe-version': STRIPE_VERSION,
        ...(idempotencyKey === undefined ? {} : { 'idempotency-key': idempotencyKey }),
      },
      body: formEncode(body),
    });
    let json: Record<string, unknown> = {};
    try {
      json = (await res.json()) as Record<string, unknown>;
    } catch {
      json = {};
    }
    if (!res.ok) {
      const err = (json['error'] ?? {}) as { code?: unknown; message?: unknown };
      throw new StripeError(res.status, typeof err.code === 'string' ? err.code : null, typeof err.message === 'string' ? err.message.slice(0, 200) : `Stripe answered ${res.status}`);
    }
    return json;
  }

  return {
    async createCheckoutSession(c) {
      const pack = PACKS[c.sku];
      const meta = { household_id: c.household_id, pack_sku: c.sku, credits: pack.credits };
      const session = await post('/v1/checkout/sessions', {
        mode: 'payment',
        client_reference_id: c.household_id,
        success_url: c.successUrl,
        cancel_url: c.cancelUrl,
        line_items: [{ quantity: 1, price_data: { currency: 'usd', unit_amount: pack.price_cents, product_data: { name: c.productName } } }],
        metadata: meta,
        // the PaymentIntent carries the same metadata, and the card is kept for off-session auto-recharge if the member opts in later
        payment_intent_data: { metadata: meta, setup_future_usage: 'off_session' },
        ...(c.customer === null ? { customer_creation: 'always' } : { customer: c.customer }),
      });
      if (typeof session['id'] !== 'string' || typeof session['url'] !== 'string') throw new StripeError(502, null, 'Stripe returned no checkout URL');
      return { id: session['id'], url: session['url'] };
    },
    async createOffSessionPayment(c) {
      const pack = PACKS[c.sku];
      const pi = await post(
        '/v1/payment_intents',
        {
          amount: pack.price_cents,
          currency: 'usd',
          customer: c.customer,
          payment_method: c.paymentMethod,
          off_session: true,
          confirm: true,
          description: c.description,
          metadata: { household_id: c.household_id, pack_sku: c.sku, credits: pack.credits, kind: 'auto_recharge', recharge_id: c.idempotencyKey },
        },
        c.idempotencyKey,
      );
      return { id: String(pi['id'] ?? ''), status: String(pi['status'] ?? '') };
    },
  };
}

// ─── webhook signature ──────────────────────────────────────────────────────

export const WEBHOOK_TOLERANCE_SECONDS = 300;

export type SignatureResult = { ok: true } | { ok: false; reason: 'missing_signature' | 'malformed_signature' | 'stale_timestamp' | 'bad_signature' };

const enc = new TextEncoder();

function hexToBytes(hex: string): Uint8Array | null {
  if (hex.length === 0 || hex.length % 2 !== 0 || !/^[0-9a-f]+$/i.test(hex)) return null;
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

/**
 * Verifies a `Stripe-Signature` header (t=<unix>,v1=<hex HMAC-SHA256 of "<t>.<raw body>">, ...) against the RAW request body.
 * Any v1 may match (Stripe sends two while a signing secret is being rolled). The comparison is constant-time (WebCrypto verify).
 */
export async function verifyStripeSignature(rawBody: string, header: string | null, secret: string, nowMs: number, toleranceSeconds = WEBHOOK_TOLERANCE_SECONDS): Promise<SignatureResult> {
  if (header === null || header.trim() === '') return { ok: false, reason: 'missing_signature' };
  let timestamp: number | null = null;
  const v1: string[] = [];
  for (const part of header.split(',')) {
    const eq = part.indexOf('=');
    if (eq < 0) continue;
    const k = part.slice(0, eq).trim();
    const v = part.slice(eq + 1).trim();
    if (k === 't' && /^\d{1,12}$/.test(v)) timestamp = Number(v);
    else if (k === 'v1' && v !== '') v1.push(v);
  }
  if (timestamp === null || v1.length === 0) return { ok: false, reason: 'malformed_signature' };
  if (Math.abs(Math.floor(nowMs / 1000) - timestamp) > toleranceSeconds) return { ok: false, reason: 'stale_timestamp' };
  const key = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']);
  const signed = enc.encode(`${timestamp}.${rawBody}`);
  for (const candidate of v1) {
    const bytes = hexToBytes(candidate);
    if (bytes !== null && (await crypto.subtle.verify('HMAC', key, bytes, signed))) return { ok: true };
  }
  return { ok: false, reason: 'bad_signature' };
}
