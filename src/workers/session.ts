/**
 * src/workers/session.ts — stateless API session tokens: base64url(payload) + '.' + base64url(HMAC-SHA256(secret, payload)).
 * Issued by the sign-in callback (a later milestone) and verified here. Verification is constant-time (WebCrypto verify),
 * rejects expired tokens, and never trusts a household id that the members table does not confirm (see api.ts).
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

function b64url(bytes: Uint8Array): string {
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function unb64url(s: string): Uint8Array {
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
