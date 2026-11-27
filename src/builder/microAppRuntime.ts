/**
 * src/builder/microAppRuntime.ts — hosts a verified micro-app at /apps/:household_id/:slug/*. The bundle's html is the app's page; the routes behind it
 * are the runtime's own, so a generated page can only reach its own household's data through them.
 *
 *   Who may call. A signed-in member of THAT household, or a token signed for exactly (household_id, app_id): scope 'preview' (a member's preview
 *   link, may write) or 'install' (what the embed on the member's own site carries: read only, GET page / search / suggest). Anything else is 401
 *   without looking the app up; a member of another household, or a token for another app, sees 404 (an app of someone else does not exist).
 *   Status. ARCHIVED is 404 for everyone. STAGED is served only to members and preview tokens, and only the preview page and its api/ calls; the
 *   public page is 404 until the member approves and the app is LIVE.
 *
 *   POST api/parse-recipe  redact() the caption and text first (a credential or an account number quarantines it: 422, no model, no quota), count one
 *                          AI call against monthly_ai_calls_cap (the 101st is 429 QUOTA_EXCEEDED), have the worker model parse the REDACTED text under a
 *                          4,000-token cap, store a photo encrypted in R2 under apps/<household>/<app>/, and write the row to micro_app_records.
 *                          No pixel ever goes to a model: the redaction rule is about text, so parsing runs on the typed or on-device-OCR text.
 *   GET  api/search?q=&tag=   rows of this (app_id, household_id) whose words all start with the query words.
 *   GET  api/suggest          three meals from the saved recipes and the household's live prefers_cuisine / dietary_restriction facts.
 */

import { fromBase64Bytes, getDecrypted, putEncrypted } from '../crypto/vault';
import { createGateway, estimateTokens, type Gateway } from '../llm/gateway';
import { createGuardedFetch } from '../net/egress';
import { redact } from '../redact';
import type { Env } from '../types';
import { authenticate, passesCsrf } from '../workers/auth';
import { b64url, unb64url } from '../workers/session';
import {
  likeEscape, normalizeParsed, queryWords, searchTokens, sourceQuote, suggestMeals, tagToken, type ParsedRecipe, type RecipeData,
} from './recipeLogic';
import type { AppManifest, Bundle } from './sandboxVerifier';

export const PARSE_TOKEN_CAP = 4_000;
export const MAX_PARSE_TEXT_CHARS = 8_000;
export const MAX_IMAGE_BYTES = 4 * 1024 * 1024;
export const MAX_BODY_BYTES = 6 * 1024 * 1024;
const SEARCH_LIMIT = 50;
const SUGGEST_POOL = 200;
const IMAGE_TYPES: Readonly<Record<string, (b: Uint8Array) => boolean>> = {
  'image/png': (b) => b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47,
  'image/jpeg': (b) => b[0] === 0xff && b[1] === 0xd8,
  'image/webp': (b) => b[0] === 0x52 && b[1] === 0x49 && b[2] === 0x46 && b[3] === 0x46,
};

export interface AppRow {
  app_id: string;
  household_id: string;
  goal_id: string;
  slug: string;
  title: string;
  bundle_r2_key: string;
  manifest_json: string;
  status: 'STAGED' | 'LIVE' | 'ARCHIVED';
  monthly_ai_calls_used: number;
  monthly_ai_calls_cap: number;
  version: number;
  quota_month: string | null;
}

export interface AppRecord {
  record_id: string;
  title: string;
  data: RecipeData;
  created_at: string;
}

/** Where an app's rows live. The D1 one below is production; the smoke test uses an in-memory one. */
export interface AppStore {
  insert(r: AppRecord & { search_tokens: string }): Promise<void>;
  search(q: string, tag: string): Promise<AppRecord[]>;
  recent(limit: number): Promise<AppRecord[]>;
  /** One AI call, atomically: false when the month's cap is spent. */
  takeQuota(): Promise<boolean>;
  returnQuota(): Promise<void>;
}

export interface RecipeParser {
  parse(redactedText: string): Promise<unknown>;
}

export interface Facts {
  cuisines: string[];
  restrictions: string[];
}

export type Access = 'member' | 'preview' | 'install';

export interface ServeContext {
  app: Pick<AppRow, 'app_id' | 'household_id' | 'slug' | 'status'>;
  bundle: Bundle;
  access: Access;
  store: AppStore;
  parser: RecipeParser;
  facts(): Promise<Facts>;
  /** Stores a photo and returns its key; absent in the smoke test (an image is then ignored). */
  saveImage?(bytes: Uint8Array, type: string, record_id: string): Promise<string>;
  now(): Date;
}

const jsonRes = (status: number, body: unknown): Response =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' } });
const errRes = (status: number, error: string, message: string): Response => jsonRes(status, { error, message });

// ─── tokens ─────────────────────────────────────────────────────────────────

export type TokenScope = 'preview' | 'install';

export interface AppTokenClaims {
  household_id: string;
  app_id: string;
  scope: TokenScope;
}

async function tokenKey(secret: string): Promise<CryptoKey> {
  const raw = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(`lifehub.app-token.v1:${secret}`));
  return crypto.subtle.importKey('raw', raw, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify']);
}

/** `<claims>.<signature>`: HMAC-SHA256 over the claims, with a key derived for this purpose only (a session token cannot be replayed as one). */
export async function signAppToken(secret: string, claims: AppTokenClaims, ttlSeconds: number, now: Date): Promise<string> {
  const body = b64url(new TextEncoder().encode(JSON.stringify({ ...claims, exp: Math.floor(now.getTime() / 1000) + ttlSeconds })));
  const sig = new Uint8Array(await crypto.subtle.sign('HMAC', await tokenKey(secret), new TextEncoder().encode(body)));
  return `${body}.${b64url(sig)}`;
}

export async function verifyAppToken(secret: string, token: string, now: Date): Promise<AppTokenClaims | null> {
  const parts = token.split('.');
  if (parts.length !== 2) return null;
  try {
    if (!(await crypto.subtle.verify('HMAC', await tokenKey(secret), unb64url(parts[1]!), new TextEncoder().encode(parts[0]!)))) return null;
    const c = JSON.parse(new TextDecoder().decode(unb64url(parts[0]!))) as Partial<AppTokenClaims> & { exp?: number };
    if (typeof c.exp !== 'number' || c.exp * 1000 <= now.getTime()) return null;
    if (typeof c.household_id !== 'string' || typeof c.app_id !== 'string' || (c.scope !== 'preview' && c.scope !== 'install')) return null;
    return { household_id: c.household_id, app_id: c.app_id, scope: c.scope };
  } catch {
    return null;
  }
}

export const PREVIEW_TOKEN_TTL_SECONDS = 86_400;
export const INSTALL_TOKEN_TTL_SECONDS = 365 * 86_400;

// ─── the core: one request against one app ──────────────────────────────────

const parseSystem = (appName: string): string =>
  [
    `APP-PARSE-RECIPE. You turn the text of one recipe, for the ${appName} recipe book, into JSON.`,
    'Use only what the text says. Never invent ingredients or steps.',
    'Output {"title": string, "ingredients": string[], "steps": string[], "tags": string[], "prep_minutes": number}.',
    'tags are short lower-case words: the cuisine, the main ingredient, the meal. prep_minutes is 0 when the text gives no time.',
    'Masked numbers such as [ACCT ****1234] must be left alone.',
  ].join('\n');

async function readJsonBody(request: Request): Promise<Record<string, unknown> | 'too_large' | null> {
  if (Number(request.headers.get('content-length') ?? 0) > MAX_BODY_BYTES) return 'too_large';
  const raw = await request.text();
  if (raw.length > MAX_BODY_BYTES) return 'too_large';
  try {
    const v = JSON.parse(raw) as unknown;
    return v !== null && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

async function parseRecipe(request: Request, ctx: ServeContext, appName: string): Promise<Response> {
  if (ctx.access === 'install') return errRes(403, 'READ_ONLY', 'This link can read the recipe book but not change it.');
  const body = await readJsonBody(request);
  if (body === 'too_large') return errRes(413, 'TOO_LARGE', 'That upload is too large.');
  if (body === null) return errRes(400, 'bad_request', 'Send JSON: { "text": "...", "image_b64"?: "...", "image_type"?: "image/jpeg" }.');
  const caption = typeof body['caption'] === 'string' ? body['caption'] : '';
  const text = typeof body['text'] === 'string' ? body['text'] : '';
  const combined = `${caption}\n${text}`.trim();
  if (combined === '') return errRes(400, 'bad_request', 'There is no recipe text. Type it, or paste what your phone reads from the photo.');
  if (combined.length > MAX_PARSE_TEXT_CHARS) return errRes(413, 'TOO_LARGE', `Recipe text may be at most ${MAX_PARSE_TEXT_CHARS} characters.`);

  // 1. redact first: a credential, SSN or account number never reaches the model, and costs the household nothing
  const red = redact(combined);
  if (red.redaction_status === 'quarantined') return errRes(422, 'QUARANTINED', 'That text looks like it contains a password, code or account number, so it was not read.');
  const clean = red.redacted_text;

  // 2. the photo (optional): validated before anything is spent
  let image: { bytes: Uint8Array; type: string } | null = null;
  if (body['image_b64'] !== undefined) {
    const type = typeof body['image_type'] === 'string' ? body['image_type'] : '';
    const magic = IMAGE_TYPES[type];
    if (typeof body['image_b64'] !== 'string' || magic === undefined) return errRes(415, 'BAD_IMAGE', 'The photo must be a PNG, JPEG or WebP.');
    let bytes: Uint8Array;
    try {
      bytes = fromBase64Bytes(body['image_b64']);
    } catch {
      return errRes(415, 'BAD_IMAGE', 'The photo could not be read.');
    }
    if (bytes.length === 0) return errRes(415, 'BAD_IMAGE', 'The photo is empty.');
    if (bytes.length > MAX_IMAGE_BYTES) return errRes(413, 'TOO_LARGE', 'The photo must be under 4 MB.');
    if (!magic(bytes)) return errRes(415, 'BAD_IMAGE', 'The file is not the kind of image it says it is.');
    image = { bytes, type };
  }

  // 3. the token cap, then one AI call from this month's allowance
  const system = parseSystem(appName);
  const user = `Recipe text:\n<<<\n${clean}\n>>>`;
  if (estimateTokens(system) + estimateTokens(user) > PARSE_TOKEN_CAP) return errRes(413, 'TOO_LARGE', 'That recipe is too long to read in one go.');
  if (!(await ctx.store.takeQuota())) return errRes(429, 'QUOTA_EXCEEDED', 'This recipe book has used its AI reads for the month.');

  let parsed: ParsedRecipe | null;
  try {
    parsed = normalizeParsed(await ctx.parser.parse(clean));
  } catch {
    await ctx.store.returnQuota();
    return errRes(502, 'PARSE_FAILED', 'The recipe could not be read right now. Try again.');
  }
  if (parsed === null) {
    await ctx.store.returnQuota();
    return errRes(502, 'PARSE_FAILED', 'That did not read as a recipe.');
  }

  // 4. store: the photo encrypted, the row scoped to (app_id, household_id)
  const record_id = `rec_${crypto.randomUUID().replace(/-/g, '')}`;
  const image_r2_key = image !== null && ctx.saveImage !== undefined ? await ctx.saveImage(image.bytes, image.type, record_id) : null;
  const data: RecipeData = { ingredients: parsed.ingredients, steps: parsed.steps, tags: parsed.tags, prep_minutes: parsed.prep_minutes, image_r2_key, source_quote: sourceQuote(clean, parsed.title) };
  const rec: AppRecord = { record_id, title: parsed.title, data, created_at: ctx.now().toISOString() };
  await ctx.store.insert({ ...rec, search_tokens: searchTokens(parsed) });
  return jsonRes(200, { record: rec });
}

/** The page, with <base> set so its relative api/ calls land on this app whichever URL (/ or /preview) served it. */
function renderPage(ctx: ServeContext, preview: boolean): Response {
  const base = `/apps/${ctx.app.household_id}/${ctx.app.slug}/`;
  const html = ctx.bundle.html.replace(/<head[^>]*>/i, (m) => `${m}<base href="${base}">`);
  const embed = !preview && ctx.access === 'install';
  return new Response(html, {
    status: 200,
    headers: {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'private, no-store',
      'x-content-type-options': 'nosniff',
      'referrer-policy': 'no-referrer',
      'content-security-policy':
        `default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data: blob:; connect-src 'self'; form-action 'self'; base-uri 'self'; frame-ancestors ${embed ? '*' : "'self'"}`,
      'permissions-policy': 'camera=(self)',
    },
  });
}

export async function serveApp(request: Request, path: string, ctx: ServeContext, appName: string): Promise<Response> {
  const route = (ctx.bundle.manifest as AppManifest).routes.find((r) => r.method === request.method && r.path === path);
  if (route === undefined) return errRes(404, 'not_found', 'No such page.');
  const url = new URL(request.url);
  const install = ctx.access === 'install';
  switch (route.handler) {
    case 'ui':
      return renderPage(ctx, false);
    case 'preview':
      return install ? errRes(404, 'not_found', 'No such page.') : renderPage(ctx, true);
    case 'parse_recipe':
      return parseRecipe(request, ctx, appName);
    case 'search': {
      const q = url.searchParams.get('q') ?? '';
      const tag = url.searchParams.get('tag') ?? '';
      const records = q.trim() === '' && tag === '' ? await ctx.store.recent(SEARCH_LIMIT) : await ctx.store.search(q, tag);
      return jsonRes(200, { records });
    }
    case 'suggest': {
      const [records, facts] = await Promise.all([ctx.store.recent(SUGGEST_POOL), ctx.facts()]);
      const suggestions = suggestMeals({ records, cuisines: facts.cuisines, restrictions: facts.restrictions });
      return jsonRes(200, { suggestions, used_facts: facts });
    }
    default:
      return errRes(404, 'not_found', 'No such page.');
  }
}

// ─── production wiring: D1, R2, the gateway ─────────────────────────────────

type RuntimeEnv = Pick<Env, 'DB' | 'DOCS_BUCKET' | 'KEK_B64' | 'SESSION_SECRET' | 'APP_NAME' | 'OPENAI_API_KEY' | 'ANTHROPIC_API_KEY' | 'CHIEF_MODEL' | 'WORKER_MODEL' | 'KV' | 'GOOGLE_CLIENT_ID' | 'GOOGLE_CLIENT_SECRET' | 'INGEST_EMAIL_WF'>;

const monthOf = (at: Date): string => at.toISOString().slice(0, 7);

export function d1Store(db: D1Database, app: Pick<AppRow, 'app_id' | 'household_id'>, now: () => Date): AppStore {
  const { app_id, household_id } = app;
  const rowOf = (r: { record_id: string; title: string; data_json: string; created_at: string }): AppRecord => ({ record_id: r.record_id, title: r.title, data: JSON.parse(r.data_json) as RecipeData, created_at: r.created_at });
  const cols = 'record_id, title, data_json, created_at';
  return {
    async insert(r) {
      await db
        .prepare(`INSERT INTO micro_app_records (record_id, app_id, household_id, collection, title, search_tokens, data_json, created_at) VALUES (?, ?, ?, 'recipes', ?, ?, ?, ?)`)
        .bind(r.record_id, app_id, household_id, r.title, r.search_tokens, JSON.stringify(r.data), r.created_at)
        .run();
    },
    async search(q, tag) {
      const clauses = queryWords(q).map((_, i) => `search_tokens LIKE ?${i + 4} ESCAPE '\\'`);
      const binds: unknown[] = queryWords(q).map((w) => `% ${likeEscape(w)}%`);
      if (tag !== '') {
        clauses.push(`search_tokens LIKE ?${binds.length + 4} ESCAPE '\\'`);
        binds.push(`% ${likeEscape(tagToken(tag))} %`);
      }
      const { results } = await db
        .prepare(`SELECT ${cols} FROM micro_app_records WHERE app_id = ?1 AND household_id = ?2 AND collection = ?3${clauses.map((c) => ` AND ${c}`).join('')} ORDER BY created_at DESC, record_id LIMIT ${SEARCH_LIMIT}`)
        .bind(app_id, household_id, 'recipes', ...binds)
        .all<{ record_id: string; title: string; data_json: string; created_at: string }>();
      return results.map(rowOf);
    },
    async recent(limit) {
      const { results } = await db
        .prepare(`SELECT ${cols} FROM micro_app_records WHERE app_id = ?1 AND household_id = ?2 AND collection = 'recipes' ORDER BY created_at DESC, record_id LIMIT ?3`)
        .bind(app_id, household_id, limit)
        .all<{ record_id: string; title: string; data_json: string; created_at: string }>();
      return results.map(rowOf);
    },
    async takeQuota() {
      const month = monthOf(now());
      const res = await db
        .prepare(
          `UPDATE micro_apps SET monthly_ai_calls_used = CASE WHEN COALESCE(quota_month, '') = ?3 THEN monthly_ai_calls_used + 1 ELSE 1 END, quota_month = ?3
            WHERE app_id = ?1 AND household_id = ?2 AND (CASE WHEN COALESCE(quota_month, '') = ?3 THEN monthly_ai_calls_used ELSE 0 END) < monthly_ai_calls_cap`,
        )
        .bind(app_id, household_id, month)
        .run();
      return (res.meta.changes ?? 0) === 1;
    },
    async returnQuota() {
      await db
        .prepare(`UPDATE micro_apps SET monthly_ai_calls_used = MAX(0, monthly_ai_calls_used - 1) WHERE app_id = ?1 AND household_id = ?2 AND quota_month = ?3`)
        .bind(app_id, household_id, monthOf(now()))
        .run();
    },
  };
}

/** The household's live preferences, read from facts_current at the moment of the request. */
export async function householdFacts(db: D1Database, household_id: string): Promise<Facts> {
  const { results } = await db
    .prepare(
      `SELECT f.predicate, f.value_text, o.canonical_name AS object_name FROM facts_current f LEFT JOIN entities o ON o.entity_id = f.object_id
        WHERE f.household_id = ? AND f.predicate IN ('prefers_cuisine', 'dietary_restriction') ORDER BY f.predicate, f.fact_id LIMIT 50`,
    )
    .bind(household_id)
    .all<{ predicate: string; value_text: string | null; object_name: string | null }>();
  const cuisines = results.filter((r) => r.predicate === 'prefers_cuisine').map((r) => r.object_name ?? r.value_text).filter((x): x is string => x !== null && x !== '');
  const restrictions = results.filter((r) => r.predicate === 'dietary_restriction').map((r) => r.value_text ?? r.object_name).filter((x): x is string => x !== null && x !== '');
  return { cuisines: [...new Set(cuisines)], restrictions: [...new Set(restrictions)] };
}

export function gatewayParser(gateway: Gateway, household_id: string, appName: string): RecipeParser {
  return {
    async parse(text) {
      const res = await gateway.worker({
        household_id,
        purpose: 'app.parse_recipe',
        system: parseSystem(appName),
        user: `Recipe text:\n<<<\n${text}\n>>>`,
        json_schema: {
          name: 'parsed_recipe',
          schema: {
            type: 'object',
            additionalProperties: false,
            required: ['title', 'ingredients', 'steps', 'tags', 'prep_minutes'],
            properties: {
              title: { type: 'string' },
              ingredients: { type: 'array', items: { type: 'string' } },
              steps: { type: 'array', items: { type: 'string' } },
              tags: { type: 'array', items: { type: 'string' } },
              prep_minutes: { type: 'number' },
            },
          },
        },
        max_output_tokens: 1500,
      });
      return JSON.parse(res.text) as unknown;
    },
  };
}

async function loadBundle(env: RuntimeEnv, app: AppRow): Promise<Bundle | null> {
  const raw = await getDecrypted(env, app.household_id, app.bundle_r2_key);
  if (raw === null) return null;
  try {
    return JSON.parse(new TextDecoder().decode(raw)) as Bundle;
  } catch {
    return null;
  }
}

const APP_PATH = /^\/apps\/([^/]+)\/([^/]+)(\/.*)?$/;

/** Entry point for every /apps/... request. */
export async function handleAppRequest(request: Request, env: RuntimeEnv, deps: { fetch?: typeof fetch; now?: () => Date } = {}): Promise<Response> {
  const nowFn = deps.now ?? (() => new Date());
  const now = nowFn();
  const m = APP_PATH.exec(new URL(request.url).pathname);
  if (m === null) return errRes(404, 'not_found', 'No such page.');
  const [, hhParam, slugParam, rest] = m as unknown as [string, string, string, string | undefined];
  const path = rest === undefined || rest === '' ? '/' : rest.length > 1 && rest.endsWith('/') ? rest.slice(0, -1) : rest;

  // 1. credentials, before the app is looked up (no credentials: the same 401 whether or not the app exists)
  let access: Access;
  let tokenApp: string | null = null;
  const t = new URL(request.url).searchParams.get('t');
  const claims = t === null ? null : await verifyAppToken(env.SESSION_SECRET, t, now);
  let member: { household_id: string } | null = null;
  if (claims !== null) {
    access = claims.scope === 'install' ? 'install' : 'preview';
    tokenApp = claims.app_id;
    if (claims.household_id !== hhParam) return errRes(404, 'not_found', 'No such page.');
  } else {
    const auth = await authenticate(request, env as unknown as Env, now);
    if (auth === null) return errRes(401, 'unauthorized', 'Sign in to open this app.');
    if (!passesCsrf(request, auth)) return errRes(403, 'csrf', 'Cross-site request refused.');
    member = { household_id: auth.household_id };
    access = 'member';
    if (auth.household_id !== hhParam) return errRes(404, 'not_found', 'No such page.');
  }
  const household_id = hhParam;

  // 2. the app, looked up by (household, slug) only
  const app = await env.DB.prepare('SELECT * FROM micro_apps WHERE household_id = ? AND slug = ?').bind(household_id, slugParam).first<AppRow>();
  if (app === null || (tokenApp !== null && tokenApp !== app.app_id) || (member !== null && member.household_id !== app.household_id)) return errRes(404, 'not_found', 'No such page.');
  if (app.status === 'ARCHIVED') return errRes(404, 'not_found', 'No such page.');
  if (app.status === 'STAGED' && (access === 'install' || !(path === '/preview' || path.startsWith('/api/')))) return errRes(404, 'not_found', 'No such page.');

  // 3. serve
  const bundle = await loadBundle(env, app);
  if (bundle === null) return errRes(500, 'bundle_missing', 'This app is unavailable.');
  const gateway = createGateway({
    db: env.DB,
    apiKeys: { openai: env.OPENAI_API_KEY, anthropic: env.ANTHROPIC_API_KEY },
    fetch: deps.fetch ?? createGuardedFetch({ db: env.DB }),
    now: nowFn,
    models: { chief: env.CHIEF_MODEL, worker: env.WORKER_MODEL },
  });
  const ctx: ServeContext = {
    app,
    bundle,
    access,
    store: d1Store(env.DB, app, nowFn),
    parser: gatewayParser(gateway, household_id, env.APP_NAME),
    facts: () => householdFacts(env.DB, household_id),
    saveImage: async (bytes, type, record_id) => {
      const key = `apps/${household_id}/${app.app_id}/${record_id}.${type.split('/')[1]}.enc`;
      await putEncrypted(env, household_id, key, bytes);
      return key;
    },
    now: nowFn,
  };
  return serveApp(request, path, ctx, env.APP_NAME);
}

