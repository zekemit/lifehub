/**
 * src/workers/session.ts — stateless API session tokens: base64url(payload) + '.' + base64url(HMAC-SHA256(secret, payload)).
 * Issued for API clients and verified here; browsers use the sealed cookie below (see src/workers/auth.ts). Verification is constant-time
 * (WebCrypto verify), rejects expired tokens, and never trusts a household id that the members table does not confirm.
 */

export interface SessionClaims {
  household_id: string;
  member_id: string;
}

interface Payload extends SessionClaims {
  /** expiry, epoch seconds */
  exp: number;
}

const enc = new TextEncoder();

export function b64url(bytes: Uint8Array): string {
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

export function unb64url(s: string): Uint8Array {
  const bin = atob(s.replace(/-/g, '+').replace(/_/g, '/'));
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

async function hmacKey(secret: string, usage: 'sign' | 'verify'): Promise<CryptoKey> {
  if (secret.length < 16) throw new Error('SESSION_SECRET must be at least 16 characters');
  return crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, [usage]);
}

export async function issueSessionToken(secret: string, claims: SessionClaims, ttlSeconds: number, now: Date): Promise<string> {
  const payload: Payload = { ...claims, exp: Math.floor(now.getTime() / 1000) + ttlSeconds };
  const body = b64url(enc.encode(JSON.stringify(payload)));
  const sig = new Uint8Array(await crypto.subtle.sign('HMAC', await hmacKey(secret, 'sign'), enc.encode(body)));
  return `${body}.${b64url(sig)}`;
}

/** Returns the claims of a valid, unexpired token, or null. */
export async function verifySessionToken(secret: string, token: string, now: Date): Promise<SessionClaims | null> {
  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const [body, sig] = parts as [string, string];
  try {
    const ok = await crypto.subtle.verify('HMAC', await hmacKey(secret, 'verify'), unb64url(sig), enc.encode(body));
    if (!ok) return null;
    const payload = JSON.parse(new TextDecoder().decode(unb64url(body))) as Partial<Payload>;
    if (typeof payload.household_id !== 'string' || typeof payload.member_id !== 'string' || typeof payload.exp !== 'number') return null;
    if (payload.exp * 1000 <= now.getTime()) return null;
    return { household_id: payload.household_id, member_id: payload.member_id };
  } catch {
    return null;
  }
}

// ─── sealed cookies ─────────────────────────────────────────────────────────
//
// Browser cookies are sealed, not merely signed: AES-256-GCM under a key derived (HKDF-SHA-256) from SESSION_SECRET, with the cookie's
// purpose as both HKDF info and AEAD associated data. Its contents are unreadable to the browser, any change fails authentication,
// and a cookie sealed for one purpose (the OAuth state) can never be replayed as another (the login session).

export type CookiePurpose = 'session' | 'oauth';

const COOKIE_VERSION = 'v1';

async function sealKey(secret: string, purpose: CookiePurpose, usage: 'encrypt' | 'decrypt'): Promise<CryptoKey> {
  if (secret.length < 16) throw new Error('SESSION_SECRET must be at least 16 characters');
  const base = await crypto.subtle.importKey('raw', enc.encode(secret), 'HKDF', false, ['deriveKey']);
  return crypto.subtle.deriveKey(
    { name: 'HKDF', hash: 'SHA-256', salt: enc.encode('lifehub.cookie.v1'), info: enc.encode(`cookie:${purpose}`) },
    base,
    { name: 'AES-GCM', length: 256 },
    false,
    [usage],
  );
}

/** `v1.<iv>.<ciphertext>` (base64url). `exp` (epoch seconds) is added to the sealed JSON. */
export async function sealCookie(secret: string, purpose: CookiePurpose, payload: Record<string, unknown>, ttlSeconds: number, now: Date): Promise<string> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const body = enc.encode(JSON.stringify({ ...payload, exp: Math.floor(now.getTime() / 1000) + ttlSeconds }));
  const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: enc.encode(purpose) }, await sealKey(secret, purpose, 'encrypt'), body));
  return `${COOKIE_VERSION}.${b64url(iv)}.${b64url(ct)}`;
}

/** The sealed JSON of an authentic, unexpired cookie of this purpose, else null. */
export async function openCookie(secret: string, purpose: CookiePurpose, value: string, now: Date): Promise<Record<string, unknown> | null> {
  const parts = value.split('.');
  if (parts.length !== 3 || parts[0] !== COOKIE_VERSION) return null;
  try {
    const iv = unb64url(parts[1]!);
    if (iv.length !== 12) return null;
    const plain = await crypto.subtle.decrypt({ name: 'AES-GCM', iv, additionalData: enc.encode(purpose) }, await sealKey(secret, purpose, 'decrypt'), unb64url(parts[2]!));
    const claims = JSON.parse(new TextDecoder().decode(plain)) as Record<string, unknown>;
    if (typeof claims['exp'] !== 'number' || claims['exp'] * 1000 <= now.getTime()) return null;
    return claims;
  } catch {
    return null;
  }
}

export const SESSION_COOKIE = '__Host-session';
export const OAUTH_COOKIE = '__Host-oauth';
export const SESSION_TTL_SECONDS = 14 * 86_400;
export const OAUTH_TTL_SECONDS = 600;

/** __Host- cookies must be Secure, Path=/ and carry no Domain. HttpOnly keeps them away from page scripts. */
export function setCookieHeader(name: string, value: string, maxAgeSeconds: number): string {
  return `${name}=${value}; Max-Age=${maxAgeSeconds}; Path=/; Secure; HttpOnly; SameSite=Lax`;
}

export const clearCookieHeader = (name: string): string => setCookieHeader(name, '', 0);

export function readCookie(request: Request, name: string): string | null {
  const header = request.headers.get('cookie');
  if (header === null) return null;
  for (const part of header.split(';')) {
    const eq = part.indexOf('=');
    if (eq > 0 && part.slice(0, eq).trim() === name) return part.slice(eq + 1).trim();
  }
  return null;
}

/** Constant-time string comparison (equal-length inputs; length differences leak only the length). */
export function timingSafeEqual(a: string, b: string): boolean {
  const x = enc.encode(a);
  const y = enc.encode(b);
  if (x.length !== y.length) return false;
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= x[i]! ^ y[i]!;
  return diff === 0;
}
