/**
 * src/crypto/vault.ts — encrypted document storage in R2 (CLAUDE.md §6: "Raw MIME to R2 (AES-GCM, per-household DEK
 * wrapped by KEK)").
 *
 *   KEK  : 32-byte secret in env.KEK_B64 (`wrangler secret put KEK_B64`). Never stored anywhere else.
 *   DEK  : random 32-byte AES-256-GCM key per household. Stored in R2 at `keys/<household_id>.dek`, wrapped
 *          (AES-GCM under the KEK, AAD = "dek:<household_id>"). Created race-safely with a conditional put.
 *   Blob : `raw/...` objects are  iv(12) || AES-GCM(DEK, plaintext, AAD = "<household_id>:<r2_key>").
 *          The AAD binds a blob to its household and key, so a copied or renamed object fails to decrypt.
 *
 * The plaintext of a document exists only in Worker memory; R2 only ever holds ciphertext.
 */

const IV_BYTES = 12;
const KEY_BYTES = 32;

type KekEnv = { KEK_B64: string; DOCS_BUCKET: R2Bucket };

function fromBase64(b64: string): Uint8Array {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

const utf8 = (s: string): Uint8Array => new TextEncoder().encode(s);

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

async function importAesKey(raw: Uint8Array, usages: Array<'encrypt' | 'decrypt'>): Promise<CryptoKey> {
  return crypto.subtle.importKey('raw', raw, { name: 'AES-GCM' }, false, usages);
}

async function sealWith(key: CryptoKey, aad: string, plaintext: Uint8Array): Promise<Uint8Array> {
  const iv = crypto.getRandomValues(new Uint8Array(IV_BYTES));
  const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: utf8(aad) }, key, plaintext));
  return concat(iv, ct);
}

async function openWith(key: CryptoKey, aad: string, blob: Uint8Array): Promise<Uint8Array> {
  if (blob.length <= IV_BYTES) throw new Error('ciphertext too short');
  const iv = blob.slice(0, IV_BYTES);
  const ct = blob.slice(IV_BYTES);
  return new Uint8Array(await crypto.subtle.decrypt({ name: 'AES-GCM', iv, additionalData: utf8(aad) }, key, ct));
}

async function kek(env: KekEnv): Promise<CryptoKey> {
  if (!env.KEK_B64) throw new Error('KEK_B64 secret is not configured');
  const raw = fromBase64(env.KEK_B64);
  if (raw.length !== KEY_BYTES) throw new Error('KEK_B64 must decode to exactly 32 bytes');
  return importAesKey(raw, ['encrypt', 'decrypt']);
}

function dekObjectKey(household_id: string): string {
  return `keys/${household_id}.dek`;
}

/** Loads (creating on first use) the household's data-encryption key. */
async function householdDek(env: KekEnv, household_id: string, create: boolean): Promise<CryptoKey> {
  const kekKey = await kek(env);
  const aad = `dek:${household_id}`;
  const objectKey = dekObjectKey(household_id);

  let stored = await env.DOCS_BUCKET.get(objectKey);
  if (stored === null) {
    if (!create) throw new Error(`no data key exists for household ${household_id}`);
    const dek = crypto.getRandomValues(new Uint8Array(KEY_BYTES));
    const wrapped = await sealWith(kekKey, aad, dek);
    // Only the first writer wins; a concurrent creator's key is read back below.
    await env.DOCS_BUCKET.put(objectKey, wrapped, { onlyIf: { etagDoesNotMatch: '*' } });
    stored = await env.DOCS_BUCKET.get(objectKey);
    if (stored === null) throw new Error('data key could not be stored');
  }
  const dek = await openWith(kekKey, aad, new Uint8Array(await stored.arrayBuffer()));
  return importAesKey(dek, ['encrypt', 'decrypt']);
}

/**
 * Encrypts small secrets (OAuth tokens in KV) under the household DEK. `aad` must name the household and the place the
 * ciphertext is stored, e.g. `kv:<household_id>:<key>`; decryption fails if it does not match.
 */
export async function encryptForHousehold(
  env: KekEnv,
  household_id: string,
  aad: string,
  plaintext: Uint8Array,
): Promise<Uint8Array> {
  return sealWith(await householdDek(env, household_id, true), aad, plaintext);
}

export async function decryptForHousehold(
  env: KekEnv,
  household_id: string,
  aad: string,
  blob: Uint8Array,
): Promise<Uint8Array> {
  return openWith(await householdDek(env, household_id, false), aad, blob);
}

export function toBase64(bytes: Uint8Array): string {
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}

export function fromBase64Bytes(b64: string): Uint8Array {
  return fromBase64(b64);
}

/** Encrypts `plaintext` and stores it at `r2_key`. Returns the stored size in bytes. */
export async function putEncrypted(
  env: KekEnv,
  household_id: string,
  r2_key: string,
  plaintext: Uint8Array,
): Promise<number> {
  const dek = await householdDek(env, household_id, true);
  const blob = await sealWith(dek, `${household_id}:${r2_key}`, plaintext);
  await env.DOCS_BUCKET.put(r2_key, blob, { customMetadata: { enc: 'aes-256-gcm', v: '1' } });
  return blob.length;
}

/** Fetches and decrypts `r2_key`. Returns null when the object does not exist. Throws if it was tampered with. */
export async function getDecrypted(env: KekEnv, household_id: string, r2_key: string): Promise<Uint8Array | null> {
  const object = await env.DOCS_BUCKET.get(r2_key);
  if (object === null) return null;
  const dek = await householdDek(env, household_id, false);
  return openWith(dek, `${household_id}:${r2_key}`, new Uint8Array(await object.arrayBuffer()));
}

export async function objectExists(env: Pick<KekEnv, 'DOCS_BUCKET'>, r2_key: string): Promise<boolean> {
  return (await env.DOCS_BUCKET.head(r2_key)) !== null;
}

export async function sha256Hex(bytes: Uint8Array | string): Promise<string> {
  const data = typeof bytes === 'string' ? utf8(bytes) : bytes;
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', data));
  return [...digest].map((b) => b.toString(16).padStart(2, '0')).join('');
}
