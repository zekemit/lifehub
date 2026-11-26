/**
 * src/notify/webPush.ts — Web Push (RFC 8030) with VAPID (RFC 8292), sent through the guarded fetch.
 *
 * The push carries NO payload: it is a "something changed" tickle and the service worker then calls GET /api/home. So no card
 * text, name or amount ever leaves our Worker for a push service, and no per-subscription encryption keys are needed.
 *
 * Subscription endpoints are accepted only for the browsers' own push services (an allowlist of host suffixes), which also
 * closes the SSRF hole an arbitrary stored URL would open. A 404/410 means the subscription is gone and it is cleared.
 */

import type { Env } from '../types';

/** Host suffixes of the push services of Chrome/Edge/Android (FCM), Safari, Firefox and Windows. */
export const PUSH_HOST_SUFFIXES = ['fcm.googleapis.com', 'web.push.apple.com', 'push.services.mozilla.com', 'notify.windows.com'] as const;

export function isAllowedPushEndpoint(endpoint: string): boolean {
  try {
    const u = new URL(endpoint);
    if (u.protocol !== 'https:' || u.username !== '' || u.password !== '') return false;
    const host = u.hostname.toLowerCase();
    return PUSH_HOST_SUFFIXES.some((s) => host === s || host.endsWith(`.${s}`));
  } catch {
    return false;
  }
}

/** Parses members.push_endpoint, which stores the JSON of a PushSubscription ({endpoint, keys}). */
export function parseSubscription(raw: string | null): { endpoint: string } | null {
  if (raw === null) return null;
  try {
    const sub = JSON.parse(raw) as { endpoint?: unknown };
    return typeof sub.endpoint === 'string' && isAllowedPushEndpoint(sub.endpoint) ? { endpoint: sub.endpoint } : null;
  } catch {
    return null;
  }
}

const b64url = (bytes: Uint8Array): string => {
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
};

const unb64url = (s: string): Uint8Array => {
  const bin = atob(s.replace(/-/g, '+').replace(/_/g, '/'));
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
};

export interface VapidKeys {
  /** base64url, 65-byte uncompressed P-256 point */
  publicKey: string;
  /** base64url, 32-byte private scalar */
  privateKey: string;
  /** mailto: or https: URL identifying the sender */
  subject: string;
}

/** The `Authorization: vapid t=<jwt>, k=<public key>` header value for a push to `endpoint`. */
export async function vapidAuthorization(endpoint: string, keys: VapidKeys, now: Date): Promise<string> {
  const pub = unb64url(keys.publicKey);
  if (pub.length !== 65 || pub[0] !== 4) throw new Error('VAPID_PUBLIC_KEY must be a 65-byte uncompressed P-256 point');
  const key = await crypto.subtle.importKey(
    'jwk',
    { kty: 'EC', crv: 'P-256', d: keys.privateKey, x: b64url(pub.slice(1, 33)), y: b64url(pub.slice(33, 65)), ext: true },
    { name: 'ECDSA', namedCurve: 'P-256' },
    false,
    ['sign'],
  );
  const enc = new TextEncoder();
  const header = b64url(enc.encode(JSON.stringify({ typ: 'JWT', alg: 'ES256' })));
  const claims = b64url(enc.encode(JSON.stringify({ aud: new URL(endpoint).origin, exp: Math.floor(now.getTime() / 1000) + 12 * 3600, sub: keys.subject })));
  const signature = new Uint8Array(await crypto.subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, key, enc.encode(`${header}.${claims}`)));
  return `vapid t=${header}.${claims}.${b64url(signature)}, k=${keys.publicKey}`;
}

export interface PushResult {
  sent: number;
  expired: number;
  failed: number;
  skipped: 'no_vapid' | 'no_subscribers' | null;
}

export async function notifyHousehold(
  deps: { db: D1Database; fetch: typeof fetch; env: Pick<Env, 'VAPID_PUBLIC_KEY' | 'VAPID_PRIVATE_KEY' | 'VAPID_SUBJECT'>; now: () => Date },
  household_id: string,
  reason: string,
): Promise<PushResult> {
  const { db, env } = deps;
  const out: PushResult = { sent: 0, expired: 0, failed: 0, skipped: null };
  const audit = (action: string, detail: Record<string, unknown>) =>
    db
      .prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'web-push', ?, ?, ?)`)
      .bind(`aud_${crypto.randomUUID().replace(/-/g, '')}`, household_id, action, JSON.stringify(detail), deps.now().toISOString())
      .run();
  if (!env.VAPID_PUBLIC_KEY || !env.VAPID_PRIVATE_KEY || !env.VAPID_SUBJECT) {
    out.skipped = 'no_vapid';
    await audit('push_skipped', { reason: 'VAPID keys are not configured', why: reason });
    return out;
  }
  const keys: VapidKeys = { publicKey: env.VAPID_PUBLIC_KEY, privateKey: env.VAPID_PRIVATE_KEY, subject: env.VAPID_SUBJECT };
  const { results } = await db
    .prepare('SELECT member_id, push_endpoint FROM members WHERE household_id = ? AND push_endpoint IS NOT NULL')
    .bind(household_id)
    .all<{ member_id: string; push_endpoint: string }>();
  if (results.length === 0) {
    out.skipped = 'no_subscribers';
    return out;
  }
  for (const m of results) {
    const sub = parseSubscription(m.push_endpoint);
    if (sub === null) {
      out.failed += 1;
      await audit('push_failed', { member_id: m.member_id, error: 'stored subscription is not an allowed push endpoint' });
      continue;
    }
    try {
      const res = await deps.fetch(sub.endpoint, {
        method: 'POST',
        headers: { authorization: await vapidAuthorization(sub.endpoint, keys, deps.now()), ttl: '3600', urgency: 'normal' },
      });
      if (res.status === 404 || res.status === 410) {
        await db.prepare('UPDATE members SET push_endpoint = NULL WHERE member_id = ? AND household_id = ?').bind(m.member_id, household_id).run();
        await audit('push_endpoint_expired', { member_id: m.member_id, status: res.status });
        out.expired += 1;
      } else if (res.status >= 200 && res.status < 300) out.sent += 1;
      else {
        await audit('push_failed', { member_id: m.member_id, status: res.status });
        out.failed += 1;
      }
    } catch (err) {
      await audit('push_failed', { member_id: m.member_id, error: err instanceof Error ? err.message.slice(0, 200) : 'error' });
      out.failed += 1;
    }
  }
  return out;
}
