/**
 * src/builder/smokeTest.ts — an in-memory synthetic request test of a bundle. It runs the REAL runtime (serveApp) over an in-memory store, a
 * deterministic parser and fixed household facts: no D1, no R2, no model, no network, and nothing it does survives the call.
 *
 *   GET /               200 HTML that carries the bundle's own page (with the runtime's <base>)
 *   GET /preview        200 HTML
 *   POST /api/parse-recipe   200, a recipe with a title, ingredients and steps
 *   GET /api/search?q=       200, and the recipe just saved is found; a word nothing starts finds nothing
 *   GET /api/suggest         200, exactly three meals, none that breaks the household's restriction
 * A failure names the request and what came back, so the repair loop can hand it to the model verbatim.
 */

import { heuristicParse, type RecipeData } from './recipeLogic';
import { matchesQuery } from './recipeLogic';
import { serveApp, type AppRecord, type AppStore, type ServeContext } from './microAppRuntime';
import type { Bundle, Diagnostic } from './sandboxVerifier';

export const SMOKE_RECIPE_TEXT = 'Smoke Test Pad Thai\nIngredients: rice noodles, shrimp, tamarind paste, peanuts\n1. Soak the noodles.\n2. Stir-fry everything.';

function memoryStore(cap: number): AppStore {
  const rows: Array<AppRecord & { search_tokens: string }> = [];
  let used = 0;
  return {
    async insert(r) {
      rows.push(r);
    },
    async search(q, tag) {
      return rows.filter((r) => matchesQuery(r.search_tokens, q, tag)).map(({ search_tokens: _s, ...r }) => r);
    },
    async recent(limit) {
      return rows.slice(-limit).reverse().map(({ search_tokens: _s, ...r }) => r);
    },
    async takeQuota() {
      if (used >= cap) return false;
      used += 1;
      return true;
    },
    async returnQuota() {
      used = Math.max(0, used - 1);
    },
  };
}

export interface SmokeResult {
  ok: boolean;
  diagnostics: Diagnostic[];
  checks: Array<{ name: string; status: number }>;
}

export async function smokeTestBundle(bundle: Bundle, o: { household_id: string; appName: string }): Promise<SmokeResult> {
  const diagnostics: Diagnostic[] = [];
  const checks: SmokeResult['checks'] = [];
  const slug = bundle.manifest?.slug ?? 'smoke';
  const ctx: ServeContext = {
    app: { app_id: 'app_smoke', household_id: o.household_id, slug, status: 'LIVE' },
    bundle,
    access: 'member',
    store: memoryStore(5),
    parser: { parse: async (text) => heuristicParse(text) },
    facts: async () => ({ cuisines: ['Thai'], restrictions: ['gluten-free'] }),
    now: () => new Date(),
  };
  const base = `https://smoke.invalid/apps/${o.household_id}/${slug}`;
  const call = async (name: string, method: string, path: string, body?: unknown): Promise<{ status: number; text: string }> => {
    try {
      const res = await serveApp(new Request(`${base}${path}`, { method, ...(body === undefined ? {} : { body: JSON.stringify(body), headers: { 'content-type': 'application/json' } }) }), path.split('?')[0]!, ctx, o.appName);
      const text = await res.text();
      checks.push({ name, status: res.status });
      return { status: res.status, text };
    } catch (err) {
      checks.push({ name, status: 500 });
      diagnostics.push({ code: 'SMOKE_THREW', message: `${name} threw: ${err instanceof Error ? err.message.slice(0, 160) : 'error'}`, where: `${method} ${path}` });
      return { status: 500, text: '' };
    }
  };
  const expect = (name: string, r: { status: number; text: string }, method: string, path: string): boolean => {
    if (r.status === 200) return true;
    diagnostics.push({ code: 'SMOKE_STATUS', message: `${name}: expected 200, got ${r.status}${r.text === '' ? '' : ` (${r.text.slice(0, 120)})`}`, where: `${method} ${path}` });
    return false;
  };

  const ui = await call('ui', 'GET', '/');
  if (expect('ui', ui, 'GET', '/') && (!ui.text.includes('<base href=') || !ui.text.includes('<title>'))) diagnostics.push({ code: 'SMOKE_UI', message: 'the page did not render with its <base> and <title>', where: 'GET /' });
  expect('preview', await call('preview', 'GET', '/preview'), 'GET', '/preview');
  const parsed = await call('parse', 'POST', '/api/parse-recipe', { text: SMOKE_RECIPE_TEXT });
  if (expect('parse', parsed, 'POST', '/api/parse-recipe')) {
    const rec = (JSON.parse(parsed.text) as { record?: { title?: unknown; data?: Partial<RecipeData> } }).record;
    if (typeof rec?.title !== 'string' || !Array.isArray(rec.data?.ingredients) || !Array.isArray(rec.data?.steps) || rec.data.ingredients.length === 0 || rec.data.steps.length === 0) {
      diagnostics.push({ code: 'SMOKE_PARSE', message: 'parse-recipe did not return a recipe with a title, ingredients and steps', where: 'POST /api/parse-recipe' });
    }
  }
  const found = await call('search', 'GET', '/api/search?q=pad');
  if (expect('search', found, 'GET', '/api/search') && !(JSON.parse(found.text) as { records?: unknown[] }).records?.length) {
    diagnostics.push({ code: 'SMOKE_SEARCH', message: 'search did not find the recipe that was just saved', where: 'GET /api/search?q=pad' });
  }
  const suggested = await call('suggest', 'GET', '/api/suggest');
  if (expect('suggest', suggested, 'GET', '/api/suggest')) {
    const list = (JSON.parse(suggested.text) as { suggestions?: Array<{ title?: string }> }).suggestions ?? [];
    if (list.length !== 3) diagnostics.push({ code: 'SMOKE_SUGGEST', message: `suggest returned ${list.length} meals, not 3`, where: 'GET /api/suggest' });
  }
  return { ok: diagnostics.length === 0, diagnostics, checks };
}
