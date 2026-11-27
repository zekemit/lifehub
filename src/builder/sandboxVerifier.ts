/**
 * src/builder/sandboxVerifier.ts — the deterministic gate between a model-written micro-app and the vault. Nothing is staged to R2, and no goal
 * reaches STAGED, until verifyBundle() returns ok. It reads the bundle as text and never runs it.
 *
 * A bundle is { manifest, html }: the manifest names the routes and form actions the app uses, the html is its single page. The data and
 * the AI call behind those routes are the runtime's own (src/builder/microAppRuntime.ts); a generated page can only talk to them.
 *
 *   HARD REJECTS  eval(, new Function, Function(, process.env, external <script src>, external loads (link, img, iframe, form action, ...),
 *                 any fetch( whose target is not a literal relative 'api/...' (so none to an external origin, none to another route of this
 *                 origin), XMLHttpRequest / WebSocket / sendBeacon / dynamic import, cookies, navigation, a policy_blocklist domain anywhere,
 *                 and anything that names another household or hard-codes its own tenant id.
 *   STRUCTURE     manifest schema and template, the required routes and the form action, no undeclared route, and accessible HTML.
 * Every problem is returned as a structured diagnostic, in full: the repair loop in GoalExecutionWF hands the exact list back to the model.
 */

export const MAX_HTML_CHARS = 120_000;
export const MAX_MANIFEST_CHARS = 8_000;
export const MAX_APP_AI_CALLS_PER_MONTH = 100;

export interface Diagnostic {
  code: string;
  message: string;
  where?: string;
}

export interface AppRoute {
  method: string;
  path: string;
  handler: string;
}

export interface AppManifest {
  version: 1;
  template: string;
  slug: string;
  title: string;
  description?: string;
  collections: string[];
  routes: AppRoute[];
  form_actions: Array<{ id: string; method: string; path: string }>;
  limits?: { monthly_ai_calls_cap?: number };
  theme?: { color?: string };
}

export interface Bundle {
  manifest: AppManifest;
  html: string;
}

export interface TemplateSpec {
  routes: readonly AppRoute[];
  collections: readonly string[];
  form_actions: ReadonlyArray<{ id: string; method: string; path: string }>;
  /** Strings the page must contain to be wired to the runtime. */
  page_must_reference: readonly string[];
}

/** The templates the runtime can host. Adding a chore tracker or a trip hub is adding an entry here and its handlers in the runtime. */
export const APP_TEMPLATES: Readonly<Record<string, TemplateSpec>> = {
  recipe_book: {
    routes: [
      { method: 'GET', path: '/', handler: 'ui' },
      { method: 'GET', path: '/preview', handler: 'preview' },
      { method: 'POST', path: '/api/parse-recipe', handler: 'parse_recipe' },
      { method: 'GET', path: '/api/search', handler: 'search' },
      { method: 'GET', path: '/api/suggest', handler: 'suggest' },
    ],
    collections: ['recipes'],
    form_actions: [{ id: 'parse-recipe', method: 'POST', path: '/api/parse-recipe' }],
    page_must_reference: ['api/parse-recipe', 'api/search', 'api/suggest'],
  },
};

const MANIFEST_KEYS = ['version', 'template', 'slug', 'title', 'description', 'collections', 'routes', 'form_actions', 'limits', 'theme'];

const FORBIDDEN: ReadonlyArray<readonly [string, RegExp, string]> = [
  ['EVAL', /\beval\s*\(/, 'eval( is forbidden'],
  ['NEW_FUNCTION', /\bnew\s+Function\b/, 'new Function is forbidden'],
  ['FUNCTION_CTOR', /(?<![\w.$])Function\s*\(/, 'Function( is forbidden'],
  ['PROCESS_ENV', /\bprocess\s*\.\s*env\b/, 'process.env is forbidden'],
  ['EXTERNAL_SCRIPT', /<script\b[^>]*\bsrc\s*=\s*["']?\s*(?:https?:)?\/\//i, 'a <script src> pointing at another origin is forbidden'],
  ['EXTERNAL_LOAD', /<(?:link|img|iframe|source|video|audio|embed|object|input)\b[^>]*\b(?:src|href|data)\s*=\s*["']?\s*(?:https?:)?\/\//i, 'loading a resource from another origin is forbidden'],
  ['EXTERNAL_FORM', /<form\b[^>]*\baction\s*=\s*["']?\s*(?:https?:)?\/\//i, 'a form that posts to another origin is forbidden'],
  ['EXTERNAL_FETCH', /\bfetch\s*\(\s*[`'"]\s*(?:https?:)?\/\//, 'a fetch call to another origin is forbidden'],
  ['XHR', /\bXMLHttpRequest\b/, 'XMLHttpRequest is forbidden; use fetch with a relative api/ path'],
  ['WEBSOCKET', /\b(?:WebSocket|EventSource)\b/, 'WebSocket and EventSource are forbidden'],
  ['BEACON', /\bsendBeacon\b/, 'sendBeacon is forbidden'],
  ['DYNAMIC_IMPORT', /\bimport\s*\(|\bimport\s+[^;\n]*\sfrom\s/, 'imports are forbidden'],
  ['COOKIE', /\bdocument\s*\.\s*cookie\b/, 'document.cookie is forbidden'],
  ['NAVIGATION', /\b(?:window\s*\.\s*top|window\s*\.\s*parent|top\s*\.\s*location|parent\s*\.\s*location|window\s*\.\s*open|postMessage|location\s*\.\s*(?:href|replace|assign)\s*[=(]|\blocation\s*=[^=])/, 'navigating or messaging outside the page is forbidden'],
  ['DOCUMENT_WRITE', /\bdocument\s*\.\s*write(?:ln)?\s*\(/, 'document.write is forbidden'],
  ['JAVASCRIPT_URL', /\bjavascript\s*:/i, 'javascript: URLs are forbidden'],
  ['SRCDOC', /\bsrcdoc\s*=/i, 'srcdoc is forbidden'],
  ['META_REFRESH', /<meta\b[^>]*http-equiv\s*=\s*["']?refresh/i, 'meta refresh is forbidden'],
  ['BASE_TAG', /<base\b/i, 'the runtime sets <base>; the page may not'],
  // text analysis cannot follow a name built at run time, so the ways to build one are closed too (the CSP is the second line of defence)
  ['INDIRECT_GLOBAL', /\b(?:window|self|globalThis|top|parent|frames)\s*\[/, 'reaching a global by a computed name is forbidden'],
  ['CONSTRUCTOR_CHAIN', /\.\s*constructor\s*\.\s*constructor\b|\bconstructor\s*\(\s*[`'"]/, 'reaching the Function constructor through .constructor is forbidden'],
  ['STRING_TIMER', /\bset(?:Timeout|Interval)\s*\(\s*[`'"]/, 'a timer given a string runs it as code'],
  ['WORKER', /\b(?:Worker|SharedWorker|importScripts|serviceWorker)\b/, 'workers are forbidden'],
];

const escapeRe = (s: string): string => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Every `fetch(` must be given a literal that starts with api/ (so it can only reach this app's own routes). */
function fetchTargets(text: string): Diagnostic[] {
  const out: Diagnostic[] = [];
  for (const m of text.matchAll(/\bfetch\s*\(/g)) {
    const rest = text.slice(m.index + m[0].length, m.index + m[0].length + 200);
    if (/^\s*[`'"]\s*(?:https?:)?\/\//.test(rest)) continue; // reported as EXTERNAL_FETCH
    if (!/^\s*[`'"]api\//.test(rest)) {
      out.push({ code: 'FETCH_TARGET', message: "a fetch call must be given a literal relative path that starts with 'api/'", where: `fetch call at: ${rest.slice(0, 40).replace(/\s+/g, ' ')}` });
    }
  }
  return out;
}

export interface VerifyContext {
  household_id: string;
  /** policy_blocklist domains: none may appear anywhere in the bundle. */
  blocklist: readonly string[];
  /** When given, the manifest slug must equal it. */
  slug?: string;
}

function checkManifest(m: unknown, ctx: VerifyContext): Diagnostic[] {
  const out: Diagnostic[] = [];
  const d = (code: string, message: string, where?: string): void => void out.push({ code, message, ...(where === undefined ? {} : { where }) });
  if (m === null || typeof m !== 'object' || Array.isArray(m)) return [{ code: 'MANIFEST_SHAPE', message: 'manifest must be an object', where: 'manifest' }];
  const o = m as Record<string, unknown>;
  for (const k of Object.keys(o)) if (!MANIFEST_KEYS.includes(k)) d('UNKNOWN_KEY', `manifest has an unknown key '${k}'`, `manifest.${k}`);
  if (o['version'] !== 1) d('MANIFEST_VERSION', 'manifest.version must be 1', 'manifest.version');
  const spec = typeof o['template'] === 'string' ? APP_TEMPLATES[o['template']] : undefined;
  if (spec === undefined) {
    d('UNSUPPORTED_TEMPLATE', `manifest.template must be one of: ${Object.keys(APP_TEMPLATES).join(', ')}`, 'manifest.template');
    return out;
  }
  if (typeof o['slug'] !== 'string' || !/^[a-z0-9][a-z0-9-]{1,38}[a-z0-9]$/.test(o['slug'])) d('BAD_SLUG', 'manifest.slug must be 3-40 lower-case letters, digits and hyphens', 'manifest.slug');
  else if (ctx.slug !== undefined && o['slug'] !== ctx.slug) d('SLUG_MISMATCH', `manifest.slug must be '${ctx.slug}'`, 'manifest.slug');
  if (typeof o['title'] !== 'string' || o['title'].trim() === '' || o['title'].length > 80) d('BAD_TITLE', 'manifest.title must be 1-80 characters', 'manifest.title');
  const collections = o['collections'];
  if (!Array.isArray(collections) || !collections.every((c) => typeof c === 'string')) d('BAD_COLLECTIONS', 'manifest.collections must be an array of names', 'manifest.collections');
  else {
    for (const c of spec.collections) if (!collections.includes(c)) d('MISSING_COLLECTION', `manifest.collections must include '${c}'`, 'manifest.collections');
    for (const c of collections) if (!spec.collections.includes(c as string)) d('UNKNOWN_COLLECTION', `collection '${String(c)}' is not part of this template`, 'manifest.collections');
  }
  const routes = o['routes'];
  if (!Array.isArray(routes)) d('BAD_ROUTES', 'manifest.routes must be an array of { method, path, handler }', 'manifest.routes');
  else {
    const seen = new Set<string>();
    for (const r of routes as unknown[]) {
      const rr = r as Partial<AppRoute> | null;
      if (rr === null || typeof rr !== 'object' || typeof rr.method !== 'string' || typeof rr.path !== 'string' || typeof rr.handler !== 'string') {
        d('BAD_ROUTE', 'every route needs a string method, path and handler', 'manifest.routes');
        continue;
      }
      const key = `${rr.method} ${rr.path}`;
      if (seen.has(key)) d('DUPLICATE_ROUTE', `route ${key} is declared twice`, 'manifest.routes');
      seen.add(key);
      const want = spec.routes.find((x) => x.method === rr.method && x.path === rr.path);
      if (want === undefined) d('UNKNOWN_ROUTE', `route ${key} is not served by this template`, 'manifest.routes');
      else if (want.handler !== rr.handler) d('BAD_HANDLER', `route ${key} must use handler '${want.handler}', not '${rr.handler}'`, 'manifest.routes');
    }
    for (const want of spec.routes) if (!seen.has(`${want.method} ${want.path}`)) d('MISSING_ROUTE', `route ${want.method} ${want.path} (handler '${want.handler}') is not declared`, 'manifest.routes');
  }
  const forms = o['form_actions'];
  if (!Array.isArray(forms)) d('BAD_FORM_ACTIONS', 'manifest.form_actions must be an array', 'manifest.form_actions');
  else {
    for (const want of spec.form_actions) {
      if (!(forms as Array<Record<string, unknown>>).some((f) => f?.['id'] === want.id && f?.['method'] === want.method && f?.['path'] === want.path)) {
        d('MISSING_FORM_ACTION', `form action '${want.id}' (${want.method} ${want.path}) is not declared`, 'manifest.form_actions');
      }
    }
  }
  const limits = o['limits'] as { monthly_ai_calls_cap?: unknown } | undefined;
  if (limits !== undefined) {
    const cap = limits?.monthly_ai_calls_cap;
    if (cap !== undefined && (!Number.isInteger(cap) || (cap as number) < 1 || (cap as number) > MAX_APP_AI_CALLS_PER_MONTH)) {
      d('BAD_LIMIT', `limits.monthly_ai_calls_cap must be a whole number from 1 to ${MAX_APP_AI_CALLS_PER_MONTH}`, 'manifest.limits');
    }
  }
  return out;
}

function checkHtml(html: string, spec: TemplateSpec | undefined): Diagnostic[] {
  const out: Diagnostic[] = [];
  const d = (code: string, message: string, where?: string): void => void out.push({ code, message, ...(where === undefined ? {} : { where }) });
  if (!/<html\b[^>]*\blang\s*=/i.test(html)) d('A11Y_LANG', '<html> needs a lang attribute', 'html');
  if (!/<title>\s*[^<\s][^<]*<\/title>/i.test(html)) d('A11Y_TITLE', 'the page needs a non-empty <title>', 'html');
  if (!/<meta\b[^>]*name\s*=\s*["']viewport["']/i.test(html)) d('MOBILE_VIEWPORT', 'the page needs <meta name="viewport"> (mobile-first)', 'html');
  if (!/<main\b/i.test(html)) d('A11Y_MAIN', 'the page needs a <main> landmark', 'html');
  if (!/<h1\b/i.test(html)) d('A11Y_H1', 'the page needs an <h1>', 'html');
  const inputs = [...html.matchAll(/<input\b[^>]*>/gi)].map((m) => m[0]);
  const labelled = (tag: string): boolean => {
    if (/\baria-label(?:ledby)?\s*=\s*["'][^"']+["']/i.test(tag)) return true;
    const id = /\bid\s*=\s*["']([^"']+)["']/i.exec(tag)?.[1];
    return id !== undefined && new RegExp(`<label\\b[^>]*\\bfor\\s*=\\s*["']${escapeRe(id)}["']`, 'i').test(html);
  };
  const file = inputs.find((t) => /\btype\s*=\s*["']?file/i.test(t));
  if (file === undefined) d('MISSING_PHOTO_INPUT', 'the page needs a photo upload <input type="file">', 'html');
  else {
    if (!/\baccept\s*=\s*["'][^"']*image\//i.test(file)) d('PHOTO_INPUT_ACCEPT', 'the photo input must accept="image/*"', 'html');
    if (!labelled(file)) d('A11Y_LABEL', 'the photo input needs an aria-label or a <label for>', 'html');
  }
  const search = inputs.find((t) => /\btype\s*=\s*["']?search/i.test(t));
  if (search === undefined) d('MISSING_SEARCH_INPUT', 'the page needs a search box (<input type="search">)', 'html');
  else if (!labelled(search)) d('A11Y_LABEL', 'the search box needs an aria-label or a <label for>', 'html');
  for (const m of html.matchAll(/<img\b[^>]*>/gi)) if (!/\balt\s*=/i.test(m[0])) d('A11Y_IMG_ALT', 'every <img> needs an alt attribute', m[0].slice(0, 60));
  for (const m of html.matchAll(/<button\b([^>]*)>([\s\S]*?)<\/button>/gi)) {
    if (m[2]!.replace(/<[^>]*>/g, '').trim() === '' && !/\baria-label\s*=\s*["'][^"']+["']/i.test(m[1]!)) d('A11Y_BUTTON', 'every <button> needs text or an aria-label', m[0].slice(0, 60));
  }
  if (!/what\s+should\s+we\s+cook/i.test(html)) d('MISSING_SUGGEST_UI', 'the page needs the "What Should We Cook?" suggestions control', 'html');
  for (const ref of spec?.page_must_reference ?? []) if (!html.includes(ref)) d('UNWIRED_ROUTE', `the page never calls '${ref}'`, 'html');
  return out;
}

export interface VerifyResult {
  ok: boolean;
  diagnostics: Diagnostic[];
}

export function verifyBundle(bundle: unknown, ctx: VerifyContext): VerifyResult {
  const out: Diagnostic[] = [];
  if (bundle === null || typeof bundle !== 'object' || Array.isArray(bundle)) return { ok: false, diagnostics: [{ code: 'BUNDLE_SHAPE', message: 'the bundle must be { manifest, html }' }] };
  const b = bundle as { manifest?: unknown; html?: unknown };
  if (typeof b.html !== 'string' || b.html.trim() === '') out.push({ code: 'BUNDLE_SHAPE', message: 'bundle.html must be a non-empty string', where: 'html' });
  if (b.manifest === undefined) out.push({ code: 'BUNDLE_SHAPE', message: 'bundle.manifest is missing', where: 'manifest' });
  if (out.length > 0) return { ok: false, diagnostics: out };
  const html = b.html as string;
  const manifestText = JSON.stringify(b.manifest);
  if (html.length > MAX_HTML_CHARS) out.push({ code: 'TOO_LARGE', message: `html is over ${MAX_HTML_CHARS} characters`, where: 'html' });
  if (manifestText.length > MAX_MANIFEST_CHARS) out.push({ code: 'TOO_LARGE', message: `manifest is over ${MAX_MANIFEST_CHARS} characters`, where: 'manifest' });
  if (out.length > 0) return { ok: false, diagnostics: out };

  const text = `${html}\n${manifestText}`;
  for (const [code, re, message] of FORBIDDEN) {
    const hit = re.exec(text);
    if (hit !== null) out.push({ code, message, where: hit[0].slice(0, 60) });
  }
  out.push(...fetchTargets(text));
  for (const domain of ctx.blocklist) {
    if (new RegExp(`(?<![a-z0-9-])${escapeRe(domain)}(?![a-z0-9-])`, 'i').test(text)) out.push({ code: 'BLOCKLISTED_ORIGIN', message: `${domain} is on the policy blocklist and may not appear in an app`, where: domain });
  }
  for (const m of text.matchAll(/\/apps\/([^/\s"'`?#\\]+)/g)) {
    if (m[1] !== ctx.household_id) out.push({ code: 'CROSS_TENANT', message: 'the app names another household in a URL', where: m[0].slice(0, 60) });
  }
  for (const m of text.matchAll(/\bhh_[A-Za-z0-9]+/g)) {
    if (m[0] !== ctx.household_id) out.push({ code: 'CROSS_TENANT', message: 'the app names another household id', where: m[0] });
  }
  if (/["']?household_id["']?\s*[:=]/.test(text)) out.push({ code: 'TENANT_REFERENCE', message: 'an app may not set or read household_id: the runtime scopes every request to the signed-in household', where: 'household_id' });
  if (/\bapp_[A-Za-z0-9]{6,}/.test(text)) out.push({ code: 'TENANT_REFERENCE', message: 'an app may not name app ids: the runtime scopes every request to this app', where: 'app_id' });

  const spec = typeof (b.manifest as { template?: unknown } | null)?.template === 'string' ? APP_TEMPLATES[(b.manifest as { template: string }).template] : undefined;
  out.push(...checkManifest(b.manifest, ctx));
  out.push(...checkHtml(html, spec));
  return { ok: out.length === 0, diagnostics: out };
}

/** The exact trace the repair loop hands back to the model. */
export function formatDiagnostics(diagnostics: readonly Diagnostic[]): string {
  return diagnostics.map((x) => `- [${x.code}] ${x.message}${x.where === undefined ? '' : ` (at: ${x.where})`}`).join('\n');
}
