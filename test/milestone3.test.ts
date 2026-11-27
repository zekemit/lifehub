/**
 * Milestone 3 behavioural suite: decisions from the Milestone 2 review, Google Calendar OAuth + sync, nightly decay,
 * triggers and the cold-start guarantee, and the HTTP API. Real local D1, R2 and KV; only the network is faked.
 */

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { runCalendarSync, type CalendarSyncResult } from '../src/connectors/calendarPipeline';
import {
  CalendarAuthError,
  GOOGLE_SCOPES,
  MIN_FREE_WINDOW_MINUTES,
  ScopeViolationError,
  buildAuthUrl,
  classifyEvent,
  computeFreeWindows,
  exchangeCode,
  findCalendarConnector,
  getAccessToken,
  localToUtcMs,
  type CalendarDeps,
  type GoogleEvent,
} from '../src/connectors/googleCalendar';
import { decryptForHousehold, fromBase64Bytes, getDecrypted, putEncrypted, sha256Hex } from '../src/crypto/vault';
import { writeFact } from '../src/graph/writeFact';
import { runIngestFile } from '../src/ingest/filePipeline';
import { recomputeCreditPeriods } from '../src/ingest/spend';
import {
  DEFAULT_CHIEF_MODEL,
  DEFAULT_WORKER_MODEL,
  UnknownModelError,
  createGateway,
  resolveRoute,
} from '../src/llm/gateway';
import { runNightlySweep, type SweepResult } from '../src/maintenance/sweepPipeline';
import { decayFacts } from '../src/maintenance/decay';
import { TaxLanguageError, assertNoTaxLanguage, buildCopy } from '../src/missions/copy';
import {
  HOME_CAPACITY,
  UnsafeTriggerSqlError,
  assertSafeTriggerSql,
  evaluateTrigger,
  isBlockedByCooldown,
  loadTriggers,
  orderCandidates,
  stageCandidate,
  type Candidate,
} from '../src/missions/engine';
import { getHome } from '../src/missions/home';
import { createGuardedFetch } from '../src/net/egress';
import { handleApi, MAX_UPLOAD_BYTES } from '../src/workers/api';
import { runScheduled } from '../src/workers/scheduled';
import { issueSessionToken } from '../src/workers/session';
import { openTestRuntime, type TestRuntime } from './helpers';
import { GOOD_SCOPE, day, fakeGoogle, newGoogle, type FakeGoogleState } from './helpers/m3';
import { anthropicReply, directStep, fakeProvider, makeHousehold, openaiReply, testEnv, type Household } from './helpers/m2';

let rt: TestRuntime;
let db: D1Database;
before(async () => {
  rt = await openTestRuntime();
  db = rt.db;
});
after(async () => {
  await rt.dispose();
});

// ── shared helpers ──
const all = async <T>(sql: string, ...b: unknown[]): Promise<T[]> => (await db.prepare(sql).bind(...b).all<T>()).results;
const one = async <T>(sql: string, ...b: unknown[]): Promise<T> => (await db.prepare(sql).bind(...b).first<T>())!;
const count = async (sql: string, ...b: unknown[]): Promise<number> => (await one<{ n: number }>(`SELECT COUNT(*) AS n FROM (${sql})`, ...b)).n;

const noLlm = () =>
  createGateway({ db, apiKeys: {}, fetch: (async () => { throw new Error('the LLM must not be called in this test'); }) as unknown as typeof fetch });

function calendarDeps(env: ReturnType<typeof testEnv>['env'], google: FakeGoogleState, now: () => Date = () => new Date()): CalendarDeps {
  return { env, fetch: createGuardedFetch({ db, fetchImpl: fakeGoogle(google), ttlMs: 0 }), now };
}

async function userEpisode(h: Household, text: string, occurredAt: string | null = null): Promise<string> {
  const id = `ep_${randomUUID().slice(0, 12)}`;
  await db.prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, occurred_at, redaction_status) VALUES (?, ?, 'user_input', ?, ?, ?, 'clean')`)
    .bind(id, h.household_id, text, randomUUID(), occurredAt).run();
  return id;
}

interface Fact {
  subject: { type: 'card_account' | 'household' | 'person'; name: string };
  predicate: string;
  value_text?: string;
  value_cents?: number;
  object?: { type: 'cuisine'; name: string };
  quote: string;
}

async function writeFacts(h: Household, episodeId: string, text: string, facts: Fact[]): Promise<string[]> {
  const ids: string[] = [];
  for (const f of facts) {
    const at = text.indexOf(f.quote);
    assert.ok(at >= 0, `quote not in text: ${f.quote}`);
    const r = await writeFact(db, {
      household_id: h.household_id, source_episode_id: episodeId, subject: f.subject, predicate: f.predicate, confidence: 1,
      ...(f.value_text !== undefined ? { value_text: f.value_text } : {}), ...(f.value_cents !== undefined ? { value_cents: f.value_cents } : {}),
      ...(f.object !== undefined ? { object: f.object } : {}), quotes: [{ quote: f.quote, char_start: at, char_end: at + f.quote.length }],
    });
    assert.ok(r.status === 'written' || r.status === 'reaffirmed', JSON.stringify(r));
    ids.push((r as { fact_id: string }).fact_id);
  }
  return ids;
}

async function cardHousehold(o: { name: string; product: string; last4: string; fee?: { cents: number; postsOn: string }; openedOn?: string }): Promise<{ h: Household; cardId: string; episode: string; text: string }> {
  const h = await makeHousehold(db);
  const parts = [`${o.name} ending ${o.last4}`];
  if (o.fee) parts.push(`annual fee $${o.fee.cents / 100} posts ${o.fee.postsOn}`);
  if (o.openedOn) parts.push(`opened ${o.openedOn}`);
  const text = `${parts.join(', ')}.`;
  const episode = await userEpisode(h, text);
  const subject = { type: 'card_account', name: o.name } as const;
  const facts: Fact[] = [
    { subject, predicate: 'card_product', value_text: o.product, quote: o.name },
    { subject, predicate: 'card_last4', value_text: o.last4, quote: `ending ${o.last4}` },
  ];
  if (o.fee) {
    facts.push({ subject, predicate: 'annual_fee', value_cents: o.fee.cents, quote: `annual fee $${o.fee.cents / 100}` });
    facts.push({ subject, predicate: 'annual_fee_posts_on', value_text: o.fee.postsOn, quote: `posts ${o.fee.postsOn}` });
  }
  if (o.openedOn) facts.push({ subject, predicate: 'card_opened_on', value_text: o.openedOn, quote: `opened ${o.openedOn}` });
  await writeFacts(h, episode, text, facts);
  const card = await one<{ entity_id: string }>(`SELECT entity_id FROM entities WHERE household_id = ? AND type = 'card_account'`, h.household_id);
  return { h, cardId: card.entity_id, episode, text };
}

const q = (s: string): string => `"${s.replace(/"/g, '""')}"`;
const MONARCH_HEADER = 'Date,Merchant,Category,Account,Original Statement,Notes,Amount,Tags,Owner';
const mrow = (date: string, merchant: string, category: string, account: string, orig: string, amount: string): string =>
  `${date},${q(merchant)},${q(category)},${q(account)},${q(orig)},,${amount},,`;
const monthDay = (monthOffset: number, d: number): string => {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + monthOffset, d)).toISOString().slice(0, 10);
};
const AMEX = 'Platinum Card® (...1005)';

async function ingestCsv(h: Household, text: string): Promise<unknown> {
  const { env } = testEnv(rt);
  const bytes = new TextEncoder().encode(text);
  const sha256 = await sha256Hex(bytes);
  const r2_key = `raw/${h.household_id}/${sha256}.csv.enc`;
  await putEncrypted(env, h.household_id, r2_key, bytes);
  return runIngestFile({ env, gateway: noLlm() }, { household_id: h.household_id, r2_key, file_name: 'export.csv', kind: 'csv', sha256 }, directStep().step);
}

async function runSync(env: ReturnType<typeof testEnv>['env'], google: FakeGoogleState, h: Household, o: { thenSweep?: boolean; started?: string[]; now?: () => Date } = {}) {
  const { step, log } = directStep();
  const result: CalendarSyncResult = await runCalendarSync(
    { cal: calendarDeps(env, google, o.now), startSweep: async (id) => { o.started?.push(id); } },
    { household_id: h.household_id, then_sweep: o.thenSweep ?? false },
    step,
  );
  return { result, log };
}

async function runSweep(h: Household, asOf: Date = new Date()): Promise<{ result: SweepResult; log: string[] }> {
  const { step, log } = directStep();
  const result = await runNightlySweep({ db, now: () => asOf }, { household_id: h.household_id }, step);
  return { result, log: log.map((l) => l.name) };
}

const allDayEvent = (id: string, summary: string, start: string, end: string): GoogleEvent => ({ id, summary, start: { date: start }, end: { date: end } });

// ═══════════════ decisions from the Milestone 2 review ═══════════════

test('DECISION: product config is Hayes / in.askhayes.com and the chief model defaults to gpt-6.2-sol', () => {
  const root = resolve(process.env['PROJECT_ROOT'] ?? process.cwd());
  const toml = readFileSync(join(root, 'wrangler.toml'), 'utf8');
  assert.match(toml, /APP_NAME = "Hayes"/);
  assert.match(toml, /INBOUND_DOMAIN = "in\.askhayes\.com"/);
  assert.equal(DEFAULT_CHIEF_MODEL, 'gpt-6.2-sol');
  assert.equal(DEFAULT_WORKER_MODEL, 'gpt-6-luna');
  assert.deepEqual(resolveRoute('chief'), { primary: 'gpt-6.2-sol', failover: 'claude-opus-5-5' });
  assert.deepEqual(resolveRoute('chief', { chief: 'gpt-6.1-sol' }), { primary: 'gpt-6.1-sol', failover: 'claude-opus-5-5' });
  assert.deepEqual(resolveRoute('worker', { worker: '' }), { primary: 'gpt-6-luna', failover: 'claude-haiku-5-5' });
  assert.throws(() => resolveRoute('chief', { chief: 'gpt-6-sol' }), UnknownModelError);
  assert.throws(() => resolveRoute('worker', { worker: 'definitely-not-a-model' }), UnknownModelError);
});

test('DECISION: CHIEF_MODEL / WORKER_MODEL overrides are used, unknown ids fail before any request, and a 404 primary is never silent', async () => {
  const h = await makeHousehold(db);
  const provider = fakeProvider(() => openaiReply('ok'));
  const gw = createGateway({ db, apiKeys: { openai: 'k' }, fetch: createGuardedFetch({ db, fetchImpl: provider.fetch, ttlMs: 0 }), models: { chief: 'gpt-6.1-sol', worker: 'gpt-6-luna' } });
  assert.equal((await gw.chief({ household_id: h.household_id, purpose: 't', system: 's', user: 'u' })).model, 'gpt-6.1-sol');
  assert.equal(provider.calls[0]!.body['model'], 'gpt-6.1-sol');
  const bad = createGateway({ db, apiKeys: { openai: 'k' }, fetch: createGuardedFetch({ db, fetchImpl: provider.fetch, ttlMs: 0 }), models: { chief: 'gpt-7' } });
  await assert.rejects(() => bad.chief({ purpose: 't', system: 's', user: 'u' }), UnknownModelError);
  assert.equal(provider.calls.length, 1, 'no request was made for the unknown model');

  // the primary id does not exist at the vendor (404): the call still succeeds via failover, but it is flagged loudly
  const notFound = fakeProvider((c) => (c.host === 'api.openai.com' ? new Response('{"error":{"message":"model not found"}}', { status: 404 }) : anthropicReply({ ok: true })));
  const gw2 = createGateway({ db, apiKeys: { openai: 'k', anthropic: 'a' }, fetch: createGuardedFetch({ db, fetchImpl: notFound.fetch, ttlMs: 0 }) });
  const out = await gw2.chief({ household_id: h.household_id, purpose: 'chief.test', system: 's', user: 'u', json_schema: { name: 'x', schema: { type: 'object' } } });
  assert.deepEqual([out.model, out.failed_over], ['claude-opus-5-5', true]);
  assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'llm_model_not_found'`, h.household_id), 1);
});

test("DECISION: a card's current credit periods are 'unconfirmed' until its transactions cover them, then they resolve", async () => {
  const { h, cardId } = await cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005' });
  const now = new Date();
  const first = await recomputeCreditPeriods(db, h.household_id, { from: day(0), to: day(0) }, now);
  const rows = await all<{ benefit_key: string; status: string; used_cents: number; period_end: string }>('SELECT * FROM credit_periods WHERE household_id = ? AND card_entity_id = ?', h.household_id, cardId);
  const amexBenefits = await count(`SELECT 1 FROM benefit_catalog WHERE card_product = 'amex_platinum'`);
  assert.ok(first.updated >= amexBenefits, 'every Amex benefit gets a current period');
  assert.ok(rows.length >= amexBenefits && rows.every((r) => r.status === 'unconfirmed' && r.used_cents === 0 && r.period_end >= day(0)), 'only current periods, none claimed used or unused');
  assert.equal(await count(`SELECT 1 FROM credit_periods WHERE household_id = ? AND status IN ('open', 'expired_unused')`, h.household_id), 0);
  // once the card has history from before the period starts, the same periods are real
  await ingestCsv(h, [MONARCH_HEADER, mrow(monthDay(-3, 1), 'Netflix', 'Subscriptions', AMEX, 'NETFLIX.COM', '-15.99'), mrow(monthDay(0, 1), 'Resy', 'Restaurants & Bar', AMEX, 'RESY *CARBONE NEW YORK', '-40.00')].join('\n') + '\n');
  const after = await all<{ benefit_key: string; status: string; used_cents: number; period_start: string }>(`SELECT benefit_key, status, used_cents, period_start FROM credit_periods WHERE household_id = ? AND period_end >= ?`, h.household_id, day(0));
  const historyStart = monthDay(-3, 1);
  for (const r of after) {
    // a period is only settled when the card's history reaches back to its first day
    assert.equal(r.status === 'unconfirmed', r.period_start < historyStart && r.used_cents === 0, `${r.benefit_key} ${r.period_start} ${r.status}`);
  }
  assert.ok(after.some((r) => r.status === 'open'), 'monthly and quarterly periods inside the history are now open');
  const resy = after.find((r) => r.benefit_key === 'amex_platinum.resy_credit')!;
  assert.deepEqual([resy.status, resy.used_cents], ['partially_used', 4000]);
});

test("DECISION: data that starts mid-period leaves that period 'unconfirmed' (we cannot say nothing was used before it)", async () => {
  const { h, cardId } = await cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005' });
  await db.prepare(`INSERT INTO transactions (txn_id, household_id, episode_id, posted_on, amount_cents, descriptor_norm, card_entity_id) VALUES (?, ?, ?, '2031-03-10', -1599, 'NETFLIX', ?)`)
    .bind(`txn_${randomUUID()}`, h.household_id, await userEpisode(h, 'x'), cardId).run();
  await recomputeCreditPeriods(db, h.household_id, { from: '2031-03-20', to: '2031-03-20' }, new Date('2031-03-20T12:00:00Z'));
  const monthly = await all<{ benefit_key: string; status: string; period_start: string }>(
    `SELECT benefit_key, status, period_start FROM credit_periods WHERE household_id = ? AND benefit_key = 'amex_platinum.digital_entertainment_credit'`, h.household_id);
  assert.deepEqual(monthly, [{ benefit_key: 'amex_platinum.digital_entertainment_credit', status: 'unconfirmed', period_start: '2031-03-01' }]);
  // March 10 is after the quarter began too
  const quarterly = await one<{ status: string }>(`SELECT status FROM credit_periods WHERE household_id = ? AND benefit_key = 'amex_platinum.resy_credit'`, h.household_id);
  assert.equal(quarterly.status, 'unconfirmed');
});

test("DECISION: category-level benefit matching ('category:travel') counts sub-categories, not look-alikes", async () => {
  const opened = day(-100);
  const { h, cardId } = await cardHousehold({ name: 'Chase Sapphire Reserve', product: 'chase_sapphire_reserve', last4: '2222', openedOn: opened });
  const ep = await userEpisode(h, 'x');
  const insert = async (env: string | null, cents: number, desc: string, when: string) =>
    db.prepare(`INSERT INTO transactions (txn_id, household_id, episode_id, posted_on, amount_cents, descriptor_norm, card_entity_id, envelope_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      .bind(`txn_${randomUUID()}`, h.household_id, ep, when, -cents, desc, cardId, env).run();
  await insert('travel_and_vacation', 25000, 'AIRLINE TICKET', day(-30));
  await insert('travelers_insurance', 9000, 'INSURANCE', day(-29)); // not a sub-category of travel
  await insert('transportation', 9000, 'LOCAL BUS', day(-28)); // different category
  await insert('travel', 3000, 'HOTEL', day(-90) < opened ? opened : day(-27));
  await insert('travel', 20000, 'OUTSIDE PERIOD', day(-400));
  // coverage must reach back to the period start for the "used" figure to be trusted; period starts at the anniversary
  await insert('shopping', 100, 'OLD', opened);
  await recomputeCreditPeriods(db, h.household_id, { from: day(0), to: day(0) }, new Date());
  const travel = await one<{ used_cents: number; status: string; period_start: string }>(
    `SELECT used_cents, status, period_start FROM credit_periods WHERE household_id = ? AND benefit_key = 'chase_sapphire_reserve.travel_credit'`, h.household_id);
  assert.equal(travel.used_cents, 28000, 'travel_and_vacation + travel only, capped at the $300 benefit');
  assert.equal(travel.status, 'partially_used');
  assert.equal(travel.period_start, opened.replace(/^\d{4}/, opened.slice(0, 4)), 'the credit year is anchored on the card anniversary');
  // without an anniversary fact a cardmember-year benefit cannot be placed in time: nothing is written for it
  const bare = await cardHousehold({ name: 'Chase Sapphire Reserve', product: 'chase_sapphire_reserve', last4: '3333' });
  await recomputeCreditPeriods(db, bare.h.household_id, { from: day(0), to: day(0) }, new Date());
  assert.equal(await count(`SELECT 1 FROM credit_periods WHERE household_id = ? AND benefit_key = 'chase_sapphire_reserve.travel_credit'`, bare.h.household_id), 0);
});

// ═══════════════ Google Calendar: OAuth ═══════════════

test('CALENDAR OAUTH: the consent URL asks for exactly openid, email, calendar.events.readonly and calendar.freebusy', () => {
  assert.deepEqual([...GOOGLE_SCOPES], ['openid', 'email', 'https://www.googleapis.com/auth/calendar.events.readonly', 'https://www.googleapis.com/auth/calendar.freebusy']);
  const u = new URL(buildAuthUrl({ clientId: 'cid', redirectUri: 'https://app.example/cb', state: 'st', codeChallenge: 'chal' }));
  assert.equal(u.origin + u.pathname, 'https://accounts.google.com/o/oauth2/v2/auth');
  assert.deepEqual(u.searchParams.get('scope')!.split(' '), [...GOOGLE_SCOPES]);
  assert.deepEqual([u.searchParams.get('access_type'), u.searchParams.get('prompt'), u.searchParams.get('include_granted_scopes'), u.searchParams.get('code_challenge_method')], ['offline', 'consent', 'false', 'S256']);
  assert.ok(!/gmail|drive|contacts|calendar\.events(?!\.readonly)|auth\/calendar(?!\.)/.test(u.searchParams.get('scope')!.replace('calendar.events.readonly', '').replace('calendar.freebusy', '')));
});

test('CALENDAR OAUTH: no other Google scope appears anywhere in the source', () => {
  const root = resolve(process.env['PROJECT_ROOT'] ?? process.cwd(), 'src');
  const walk = (dir: string): string[] => readdirSync(dir).flatMap((n) => (statSync(join(dir, n)).isDirectory() ? walk(join(dir, n)) : n.endsWith('.ts') ? [join(dir, n)] : []));
  // Milestone 6's Gmail sync engine is the ONE file allowed to name gmail.readonly: it is dormant (nothing requests that scope), and
  // test/milestone6.test.ts pins what it may say and that sign-in cannot ask for it. Every other file is held to the Milestone 3 rule.
  const engine = join(root, 'connectors', 'gmailSync.ts');
  const files = walk(root).filter((f) => f !== engine);
  const scopes = new Set<string>();
  for (const f of files) for (const m of readFileSync(f, 'utf8').matchAll(/googleapis\.com\/auth\/([a-z.]+)/g)) scopes.add(m[1]!);
  assert.deepEqual([...scopes].sort(), ['calendar.events.readonly', 'calendar.freebusy', 'userinfo.email']);
  assert.ok(!files.some((f) => /gmail\.(readonly|modify|send|compose)|mail\.google\.com/.test(readFileSync(f, 'utf8'))));
});

test('CALENDAR OAUTH: tokens are exchanged, stored AES-GCM-encrypted in KV under the household key, and refreshed on demand', async () => {
  const { env } = testEnv(rt);
  const h = await makeHousehold(db);
  const google = newGoogle();
  google.scope = GOOD_SCOPE; // Google reports `email` in its long form
  const { connector_id, email } = await exchangeCode(calendarDeps(env, google), h.household_id, 'good-code', 'https://app.example/cb');
  assert.equal(email, 'member@example.org');
  const row = await one<{ status: string; kind: string; scopes: string; token_kv_key: string; sync_cursor: string | null }>('SELECT * FROM connectors WHERE connector_id = ?', connector_id);
  assert.deepEqual([row.status, row.kind, row.token_kv_key, row.sync_cursor], ['active', 'google_calendar', `gcal:${connector_id}`, null]);
  assert.equal(row.scopes, GOOD_SCOPE);

  const stored = (await env.KV.get(row.token_kv_key))!;
  const raw = new TextDecoder('latin1').decode(fromBase64Bytes(stored));
  assert.ok(!/rt-secret|at-initial|access_token/.test(stored + raw), 'no token text in KV');
  const opened = JSON.parse(new TextDecoder().decode(await decryptForHousehold(env, h.household_id, `kv:${h.household_id}:${row.token_kv_key}`, fromBase64Bytes(stored)))) as { refresh_token: string; scope: string };
  assert.deepEqual([opened.refresh_token, opened.scope], ['rt-secret', GOOD_SCOPE], 'the owner can read it with the right key and AAD');
  const other = await makeHousehold(db);
  await putEncrypted(env, other.household_id, `raw/${other.household_id}/x`, new Uint8Array([1])); // gives `other` a DEK
  await assert.rejects(() => decryptForHousehold(env, other.household_id, `kv:${h.household_id}:${row.token_kv_key}`, fromBase64Bytes(stored)), 'another household cannot read it');
  await assert.rejects(() => decryptForHousehold(env, h.household_id, 'kv:somewhere:else', fromBase64Bytes(stored)), 'it is bound to its KV key');

  const connector = (await findCalendarConnector(db, h.household_id))!;
  assert.equal(await getAccessToken(calendarDeps(env, google), connector), 'at-initial');
  assert.equal(google.refreshCount, 0);
  const later = () => new Date(Date.now() + 2 * 3600_000);
  assert.equal(await getAccessToken(calendarDeps(env, google, later), connector), 'at-refreshed-1');
  assert.equal(await getAccessToken(calendarDeps(env, google, () => new Date(later().getTime() + 60_000)), connector), 'at-refreshed-1', 'the refreshed token is cached');
  assert.equal(google.refreshCount, 1);
  // refresh requests carry the refresh token only in the POST body, never in a URL
  assert.ok(google.log.filter((l) => l.path === '/token').every((l) => l.method === 'POST' && Object.keys(l.query).length === 0));
});

test('CALENDAR OAUTH: a response with any extra scope is refused and nothing is stored', async () => {
  const { env } = testEnv(rt);
  const h = await makeHousehold(db);
  const google = newGoogle();
  google.scope += ' https://www.googleapis.com/auth/gmail.readonly';
  const err = await exchangeCode(calendarDeps(env, google), h.household_id, 'good-code', 'https://app.example/cb').catch((e: unknown) => e);
  assert.ok(err instanceof ScopeViolationError);
  assert.match(err.granted, /gmail\.readonly/);
  assert.equal(await count('SELECT 1 FROM connectors WHERE household_id = ?', h.household_id), 0);
  await assert.rejects(() => exchangeCode(calendarDeps(env, newGoogle()), h.household_id, 'bad-code', 'https://app.example/cb'), CalendarAuthError);
  assert.equal(await count('SELECT 1 FROM connectors WHERE household_id = ?', h.household_id), 0);
});

test('CALENDAR OAUTH: an expired access token is refreshed once and retried; a revoked grant deactivates the connector', async () => {
  const { env } = testEnv(rt);
  const h = await makeHousehold(db);
  const google = newGoogle();
  await exchangeCode(calendarDeps(env, google), h.household_id, 'good-code', 'https://app.example/cb');
  google.rejectNextAccessToken = true;
  const { result } = await runSync(env, google, h);
  assert.equal(result.status, 'done');
  assert.equal(google.refreshCount >= 1, true, 'the 401 forced a refresh');
  google.revokeRefresh = true;
  const active = (await findCalendarConnector(db, h.household_id))!;
  await assert.rejects(() => getAccessToken(calendarDeps(env, google), active, true), CalendarAuthError);
  const row = await one<{ status: string; last_error: string }>(`SELECT status, last_error FROM connectors WHERE household_id = ?`, h.household_id);
  assert.deepEqual([row.status, row.last_error], ['revoked', 'refresh failed: invalid_grant']);
  assert.equal(await findCalendarConnector(db, h.household_id), null);
  const again = await runSync(env, google, h);
  assert.equal(again.result.status, 'not_connected');
});

// ═══════════════ Google Calendar: extraction ═══════════════

test('CALENDAR: occasions and trips are classified deterministically; meetings, cancelled, declined and past events are ignored', () => {
  const today = '2026-12-01';
  const ev = (o: Partial<GoogleEvent> & { id: string }): GoogleEvent => ({ summary: 'x', start: { date: '2026-12-10' }, end: { date: '2026-12-11' }, ...o });
  assert.deepEqual(classifyEvent(ev({ id: 'a', summary: 'Our Anniversary' }), today), { id: 'a', kind: 'anniversary', name: 'Our Anniversary', start: '2026-12-10', end: '2026-12-10', allDay: true });
  assert.equal(classifyEvent(ev({ id: 'b', summary: "Ava's Birthday 🎂" }), today)!.kind, 'birthday');
  assert.equal(classifyEvent(ev({ id: 'b2', summary: "Ava's B-day" }), today)!.kind, 'birthday');
  const trip = classifyEvent(ev({ id: 'c', summary: 'Lisbon Vacation', start: { date: '2026-12-20' }, end: { date: '2026-12-27' } }), today)!;
  assert.deepEqual([trip.kind, trip.start, trip.end], ['trip', '2026-12-20', '2026-12-26'], 'an all-day end date is exclusive');
  assert.equal(classifyEvent(ev({ id: 'd', summary: 'Ski week', start: { date: '2026-12-20' }, end: { date: '2026-12-24' } }), today)!.kind, 'trip', 'a 3+ day all-day span is a trip');
  assert.equal(classifyEvent(ev({ id: 'e', summary: 'Team standup', start: { dateTime: '2026-12-02T09:00:00-05:00' }, end: { dateTime: '2026-12-02T09:15:00-05:00' } }), today), null);
  assert.equal(classifyEvent(ev({ id: 'f', summary: 'Our Anniversary', status: 'cancelled' }), today), null);
  assert.equal(classifyEvent(ev({ id: 'g', summary: 'Our Anniversary', attendees: [{ self: true, responseStatus: 'declined' }] }), today), null);
  assert.equal(classifyEvent(ev({ id: 'h', summary: 'Our Anniversary', start: { date: '2026-11-20' }, end: { date: '2026-11-21' } }), today), null, 'past occasion');
  assert.equal(classifyEvent(ev({ id: 'i', summary: 'Spring Vacation', start: { date: '2026-11-20' }, end: { date: '2026-12-03' } }), today)!.kind, 'trip', 'a trip in progress counts');
  assert.equal(classifyEvent(ev({ id: 'j', summary: '   ' }), today), null);
  assert.equal(classifyEvent({ id: 'k', summary: 'Birthday' }, today), null, 'no start date');
});

test('CALENDAR: free windows are Fri/Sat 18:00-22:00 local, clipped by busy time, at least 2 hours, DST-correct', () => {
  // America/New_York: EST (UTC-5) in December, EDT (UTC-4) in July
  assert.equal(new Date(localToUtcMs('2026-12-04', '18:00', 'America/New_York')).toISOString(), '2026-12-04T23:00:00.000Z');
  assert.equal(new Date(localToUtcMs('2026-07-10', '18:00', 'America/New_York')).toISOString(), '2026-07-10T22:00:00.000Z');
  assert.equal(new Date(localToUtcMs('2026-03-14', '22:00', 'America/New_York')).toISOString(), '2026-03-15T02:00:00.000Z', 'the evening before the spring-forward night is still EST');
  assert.equal(new Date(localToUtcMs('2026-12-04', '18:00', 'Asia/Tokyo')).toISOString(), '2026-12-04T09:00:00.000Z');
  const tz = 'America/New_York';
  const start = (d: string): number => localToUtcMs(d, '00:00', tz);
  const free = computeFreeWindows([], tz, '2026-12-01', 14, start('2026-12-01'));
  assert.deepEqual(free.map((w) => [w.date, w.weekday]), [['2026-12-04', 'Fri'], ['2026-12-05', 'Sat'], ['2026-12-11', 'Fri'], ['2026-12-12', 'Sat']]);
  assert.deepEqual([free[0]!.startMs, free[0]!.endMs], [localToUtcMs('2026-12-04', '18:00', tz), localToUtcMs('2026-12-04', '22:00', tz)]);
  const busy = (d: string, from: string, to: string) => ({ start: new Date(localToUtcMs(d, from, tz)).toISOString(), end: new Date(localToUtcMs(d, to, tz)).toISOString() });
  const w = (blocks: Array<{ start: string; end: string }>) => computeFreeWindows(blocks, tz, '2026-12-04', 2, start('2026-12-04'));
  assert.deepEqual(w([busy('2026-12-04', '17:00', '19:00')]).map((x) => [x.date, new Date(x.startMs).toISOString().slice(11, 16)]), [['2026-12-04', '00:00'], ['2026-12-05', '23:00']], 'free 19:00-22:00 (3h) remains; Saturday untouched');
  assert.deepEqual(w([busy('2026-12-04', '19:00', '21:00')]).map((x) => x.date), ['2026-12-05'], 'two 1h scraps are not a window');
  assert.deepEqual(w([busy('2026-12-04', '19:00', '20:00')]).map((x) => (x.endMs - x.startMs) / 60_000), [120, 240], 'the longer stretch (20:00-22:00) is kept');
  assert.deepEqual(w([busy('2026-12-04', '17:59', '22:01'), busy('2026-12-05', '08:00', '23:00')]), [], 'fully busy');
  assert.deepEqual(w([busy('2026-12-04', '19:59', '22:00')]).map((x) => x.date), ['2026-12-05'], '119 free minutes (18:00-19:59) is not enough');
  assert.deepEqual(w([busy('2026-12-04', '20:00', '22:00')]).map((x) => x.date), ['2026-12-04', '2026-12-05'], 'exactly 120 free minutes is enough');
  assert.equal(MIN_FREE_WINDOW_MINUTES, 120);
  // tonight's window never starts in the past
  const mid = computeFreeWindows([], tz, '2026-12-04', 1, localToUtcMs('2026-12-04', '19:30', tz));
  assert.deepEqual([mid.length, mid[0]!.startMs], [1, localToUtcMs('2026-12-04', '19:30', tz)]);
  assert.deepEqual(computeFreeWindows([], tz, '2026-12-04', 1, localToUtcMs('2026-12-04', '20:30', tz)), [], 'less than 2h left tonight');
  assert.deepEqual(computeFreeWindows([], tz, '2026-12-02', 2, 0), [], 'Wed/Thu have no windows');
});

async function connected(google: FakeGoogleState): Promise<{ env: ReturnType<typeof testEnv>['env']; h: Household }> {
  const { env } = testEnv(rt);
  const h = await makeHousehold(db);
  await exchangeCode(calendarDeps(env, google), h.household_id, 'good-code', 'https://app.example/cb');
  return { env, h };
}

test('CALENDAR SYNC: occasions, trips and free windows become quote-grounded facts; ordinary meetings are never stored', async () => {
  const google = newGoogle();
  const anniversary = day(10);
  google.events = [
    allDayEvent('e1', 'Our Anniversary', anniversary, day(11)),
    allDayEvent('e2', "Ava's Birthday 🎂", day(40), day(41)),
    allDayEvent('e3', 'Lisbon Vacation', day(60), day(67)),
    { id: 'e4', summary: 'Team standup', start: { dateTime: `${day(1)}T09:00:00-05:00` }, end: { dateTime: `${day(1)}T09:15:00-05:00` } },
    { id: 'e5', summary: 'Dentist', start: { dateTime: `${day(2)}T09:00:00-05:00` }, end: { dateTime: `${day(2)}T10:00:00-05:00` } },
    { id: 'e6', summary: 'Old Birthday', status: 'cancelled' },
    allDayEvent('e7', 'Last year Anniversary', day(-100), day(-99)),
  ];
  const { env, h } = await connected(google);
  const { result, log } = await runSync(env, google, h);
  assert.equal(result.status, 'done');
  assert.deepEqual([result.mode, result.occasions, result.trips, result.events_seen], ['full', 2, 1, 7]);
  assert.ok(log.some((s) => s.name === 'events') && log[log.length - 1]!.name === 'commit');

  const facts = await all<{ predicate: string; name: string; type: string; value_text: string }>(
    `SELECT f.predicate, e.canonical_name AS name, e.type, f.value_text FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id
      WHERE f.household_id = ? AND f.predicate IN ('event_date', 'event_kind', 'trip_window') ORDER BY e.canonical_name, f.predicate`, h.household_id);
  assert.deepEqual(facts, [
    { predicate: 'event_date', name: "Ava's Birthday 🎂", type: 'event', value_text: day(40) },
    { predicate: 'event_kind', name: "Ava's Birthday 🎂", type: 'event', value_text: 'birthday' },
    { predicate: 'trip_window', name: 'Lisbon Vacation', type: 'trip', value_text: `${day(60)}/${day(66)}` },
    { predicate: 'event_date', name: 'Our Anniversary', type: 'event', value_text: anniversary },
    { predicate: 'event_kind', name: 'Our Anniversary', type: 'event', value_text: 'anniversary' },
  ]);
  const names = (await all<{ canonical_name: string }>('SELECT canonical_name FROM entities WHERE household_id = ?', h.household_id)).map((e) => e.canonical_name);
  assert.ok(!names.some((n) => /standup|dentist|Last year|Old Birthday/i.test(n)), 'only occasions, trips and windows are stored');

  // quotes are exact lines of the (redacted) calendar episode, offsets are UTF-16 even after an emoji
  const quotes = await all<{ quote: string; char_start: number; char_end: number; text: string; predicate: string }>(
    `SELECT q.quote, q.char_start, q.char_end, e.redacted_text AS text, f.predicate FROM fact_quotes q JOIN facts f ON f.fact_id = q.fact_id JOIN episodes e ON e.episode_id = q.episode_id
      WHERE f.household_id = ? AND f.predicate IN ('event_date', 'trip_window')`, h.household_id);
  assert.equal(quotes.length, 3);
  for (const x of quotes) assert.equal(x.text.slice(x.char_start, x.char_end), x.quote);
  assert.ok(quotes.some((x) => x.quote === `Occasion: Our Anniversary | anniversary | ${anniversary} | yearly`));
  assert.ok(quotes.some((x) => x.quote.startsWith("Occasion: Ava's Birthday 🎂")));
  assert.equal((await one<{ source: string }>(`SELECT source FROM episodes WHERE household_id = ? LIMIT 1`, h.household_id)).source, 'calendar');

  // free windows: only Fri/Sat, each quoted by its local line, stored as a UTC interval
  const windows = await all<{ name: string; value_text: string; quote: string }>(
    `SELECT e.canonical_name AS name, f.value_text, q.quote FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id JOIN fact_quotes q ON q.fact_id = f.fact_id
      WHERE f.household_id = ? AND f.predicate = 'free_window' ORDER BY e.canonical_name`, h.household_id);
  assert.ok(windows.length >= 50, `windows: ${windows.length}`);
  for (const w of windows) {
    const date = w.name.replace('evening:', '');
    assert.ok([5, 6].includes(new Date(`${date}T00:00:00Z`).getUTCDay()), `${date} is a Fri/Sat`);
    assert.match(w.quote, new RegExp(`^Free window (Fri|Sat) ${date} \\d\\d:\\d\\d-\\d\\d:\\d\\d America/New_York$`));
    const [s, e] = w.value_text.split('/') as [string, string];
    assert.ok(Date.parse(e) - Date.parse(s) >= 120 * 60_000);
  }
  const cursor = JSON.parse((await one<{ sync_cursor: string }>(`SELECT sync_cursor FROM connectors WHERE household_id = ?`, h.household_id)).sync_cursor);
  assert.equal(cursor.syncToken, 'sync-1');
  assert.deepEqual(Object.values(cursor.tracked).map((t) => (t as { name: string }).name).sort(), ["Ava's Birthday 🎂", 'Lisbon Vacation', 'Our Anniversary']);
});

test('CALENDAR SYNC: incremental syncToken, removals close facts, 410 and a shrunken window fall back to a full sync, pagination works', async () => {
  const google = newGoogle();
  google.pageSize = 2; // force pagination
  google.events = [allDayEvent('e1', 'Our Anniversary', day(10), day(11)), allDayEvent('e2', 'Lisbon Vacation', day(60), day(63)), allDayEvent('e3', 'Dentist', day(3), day(4)), allDayEvent('e4', "Ava's Birthday", day(30), day(31)), allDayEvent('e5', 'Gym', day(5), day(6))];
  const { env, h } = await connected(google);
  await runSync(env, google, h);
  const eventCalls = () => google.log.filter((l) => l.path.endsWith('/events'));
  assert.equal(eventCalls().length, 3, '5 events at 2 per page = 3 requests');
  assert.ok(eventCalls().every((l) => l.query['timeMin'] !== undefined && l.query['syncToken'] === undefined && l.query['showDeleted'] === 'true'));
  assert.ok(Date.parse(eventCalls()[0]!.query['timeMin']!) < Date.now() - 360 * 86_400_000 && Date.parse(eventCalls()[0]!.query['timeMax']!) > Date.now() + 170 * 86_400_000, '365 days back, 180 forward');

  // incremental: the anniversary is cancelled, a new one appears
  google.log.length = 0;
  google.pageSize = 2500;
  google.delta = [{ id: 'e1', status: 'cancelled' }, allDayEvent('e8', 'Wedding Anniversary', day(20), day(21))];
  const inc = await runSync(env, google, h);
  const call = eventCalls()[0]!;
  assert.deepEqual([inc.result.mode, call.query['syncToken'], call.query['timeMin']], ['incremental', 'sync-1', undefined], 'only the delta is requested');
  assert.equal(await count(`SELECT 1 FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id WHERE f.household_id = ? AND e.canonical_name = 'Our Anniversary'`, h.household_id), 0, 'the cancelled occasion no longer counts');
  assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'calendar_event_removed'`, h.household_id) >= 2, true);
  assert.equal(await count(`SELECT 1 FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id WHERE f.household_id = ? AND e.canonical_name = 'Wedding Anniversary' AND f.predicate = 'event_date'`, h.household_id), 1);
  assert.equal(await count(`SELECT 1 FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id WHERE f.household_id = ? AND e.canonical_name = 'Lisbon Vacation'`, h.household_id), 1, 'untouched events stay');

  // 410 Gone: the token expired, so the sync restarts from scratch
  google.log.length = 0;
  google.expireSyncToken = true;
  google.events = [allDayEvent('e8', 'Wedding Anniversary', day(20), day(21))];
  const gone = await runSync(env, google, h);
  assert.equal(gone.result.mode, 'full');
  assert.deepEqual([eventCalls().length, eventCalls()[0]!.query['syncToken'], eventCalls()[1]!.query['timeMin'] !== undefined], [2, 'sync-2', true]);
  assert.equal(await count(`SELECT 1 FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id WHERE f.household_id = ? AND e.canonical_name IN ('Lisbon Vacation', "Ava's Birthday")`, h.household_id), 0, 'a full re-list closes what is no longer there');
  assert.equal(await count(`SELECT 1 FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id WHERE f.household_id = ? AND e.canonical_name = 'Wedding Anniversary'`, h.household_id) >= 1, true);

  // a forward window that has shrunk below 120 days is rebuilt
  const cursor = JSON.parse((await one<{ sync_cursor: string }>('SELECT sync_cursor FROM connectors WHERE household_id = ?', h.household_id)).sync_cursor);
  cursor.window_end = day(30);
  await db.prepare('UPDATE connectors SET sync_cursor = ? WHERE household_id = ?').bind(JSON.stringify(cursor), h.household_id).run();
  google.log.length = 0;
  assert.equal((await runSync(env, google, h)).result.mode, 'full');
  assert.equal(eventCalls()[0]!.query['syncToken'], undefined);
});

test('CALENDAR SYNC: a window that becomes busy is retired; the cursor is only committed after the writes; the chain can start the sweep', async () => {
  const google = newGoogle();
  const { env, h } = await connected(google);
  await runSync(env, google, h);
  const firstFriday = (await all<{ name: string }>(`SELECT e.canonical_name AS name FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id WHERE f.household_id = ? AND f.predicate = 'free_window' ORDER BY e.canonical_name`, h.household_id))[1]!.name.replace('evening:', '');
  const total = await count(`SELECT 1 FROM facts_current WHERE household_id = ? AND predicate = 'free_window'`, h.household_id);
  google.busy = [{ start: `${firstFriday}T00:00:00Z`, end: `${day(1, new Date(`${firstFriday}T00:00:00Z`))}T12:00:00Z` }];
  const started: string[] = [];
  const out = await runSync(env, google, h, { thenSweep: true, started });
  assert.ok(out.result.closed! >= 1);
  assert.equal(await count(`SELECT 1 FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id WHERE f.household_id = ? AND f.predicate = 'free_window' AND e.canonical_name = ?`, h.household_id, `evening:${firstFriday}`), 0);
  assert.ok((await count(`SELECT 1 FROM facts_current WHERE household_id = ? AND predicate = 'free_window'`, h.household_id)) < total);
  assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'window_no_longer_free'`, h.household_id) >= 1, true);
  assert.deepEqual(started, [h.household_id]);
  assert.ok(out.log.some((s) => s.name === 'start-sweep'));

  // a failure before commit leaves the old cursor in place (the next run re-reads the same delta)
  const before = (await one<{ sync_cursor: string }>('SELECT sync_cursor FROM connectors WHERE household_id = ?', h.household_id)).sync_cursor;
  const broken = fakeGoogle(google);
  const failing = { ...calendarDeps(env, google), fetch: ((i: RequestInfo | URL, init?: RequestInit) => (String(typeof i === 'string' ? i : (i as URL).href ?? (i as Request).url).includes('freeBusy') ? Promise.resolve(new Response('boom', { status: 500 })) : broken(i, init))) as typeof fetch };
  await assert.rejects(() => runCalendarSync({ cal: failing }, { household_id: h.household_id, then_sweep: false }, directStep().step));
  assert.equal((await one<{ sync_cursor: string }>('SELECT sync_cursor FROM connectors WHERE household_id = ?', h.household_id)).sync_cursor, before);
  // not connected: nothing happens
  const nobody = await makeHousehold(db);
  assert.equal((await runSync(env, google, nobody)).result.status, 'not_connected');
});

// ═══════════════ nightly decay ═══════════════

test("DECAY: stale habit facts close after decay_days, non-decaying predicates and re-confirmed facts survive", async () => {
  const h = await makeHousehold(db);
  const ago = (days: number) => new Date(Date.now() - days * 86_400_000).toISOString();
  const subject = { type: 'household', name: 'Smith' } as const;
  const likes = (cuisine: string): Fact => ({ subject, predicate: 'prefers_cuisine', object: { type: 'cuisine', name: cuisine }, quote: cuisine });
  const registry = await one<{ decay_days: number }>(`SELECT decay_days FROM predicate_registry WHERE predicate = 'prefers_cuisine'`);
  assert.equal(registry.decay_days, 365);

  const mk = async (text: string, days: number, facts: Fact[]) => {
    const ep = await userEpisode(h, text, ago(days));
    await writeFacts(h, ep, text, facts);
    return ep;
  };
  await mk('Italian', 400, [likes('Italian')]); // stale
  await mk('Thai', 366, [likes('Thai')]); // just past the limit
  await mk('Mexican', 364, [likes('Mexican')]); // just inside
  await mk('Korean', 100, [likes('Korean')]); // fresh
  const old = await mk('Japanese', 500, [likes('Japanese')]); // stale, but re-confirmed today
  await mk('Japanese again', 1, [likes('Japanese')].map((f) => ({ ...f, quote: 'Japanese' })));
  await mk('Ava birthday 1985-03-12', 800, [{ subject: { type: 'person', name: 'Ava' }, predicate: 'birthday', value_text: '1985-03-12', quote: '1985-03-12' }]); // decay NULL
  assert.ok(old);

  const decayDays = async (): Promise<Array<{ name: string; closed: boolean }>> =>
    (await all<{ name: string; closed: number }>(`SELECT o.canonical_name AS name, f.valid_to IS NOT NULL AS closed FROM facts f JOIN entities o ON o.entity_id = f.object_id WHERE f.household_id = ? AND f.predicate = 'prefers_cuisine' ORDER BY o.canonical_name`, h.household_id)).map((r) => ({ name: r.name, closed: r.closed === 1 }));
  assert.ok((await decayDays()).every((d) => !d.closed));

  const asOf = new Date();
  const result = await decayFacts(db, h.household_id, asOf);
  assert.deepEqual(result, { closed: 2, examined: 2 });
  assert.deepEqual(await decayDays(), [
    { name: 'Italian', closed: true }, { name: 'Japanese', closed: false }, { name: 'Korean', closed: false }, { name: 'Mexican', closed: false }, { name: 'Thai', closed: true },
  ]);
  const current = (await all<{ name: string }>(`SELECT o.canonical_name AS name FROM facts_current f JOIN entities o ON o.entity_id = f.object_id WHERE f.household_id = ? AND f.predicate = 'prefers_cuisine'`, h.household_id)).map((r) => r.name).sort();
  assert.deepEqual(current, ['Japanese', 'Korean', 'Mexican']);
  assert.equal(await count(`SELECT 1 FROM facts_current WHERE household_id = ? AND predicate = 'birthday'`, h.household_id), 1, 'birthday has no decay_days');
  const audit = await all<{ detail_json: string; created_at: string }>(`SELECT detail_json, created_at FROM audit_log WHERE household_id = ? AND action = 'decay_invalidate'`, h.household_id);
  assert.equal(audit.length, 2);
  for (const a of audit) {
    const d = JSON.parse(a.detail_json) as { predicate: string; decay_days: number; last_support: string; fact_id: string };
    assert.deepEqual([d.predicate, d.decay_days], ['prefers_cuisine', 365]);
    assert.ok(Date.parse(d.last_support) < asOf.getTime() - 365 * 86_400_000);
  }
  // the closed facts and their quotes stay as history; closing is idempotent
  assert.equal(await count(`SELECT 1 FROM facts f JOIN fact_quotes q ON q.fact_id = f.fact_id WHERE f.household_id = ? AND f.valid_to IS NOT NULL`, h.household_id), 2);
  assert.deepEqual(await decayFacts(db, h.household_id, asOf), { closed: 0, examined: 0 });
  assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'decay_invalidate'`, h.household_id), 2);
  // other households are untouched
  const other = await makeHousehold(db);
  assert.deepEqual(await decayFacts(db, other.household_id, asOf), { closed: 0, examined: 0 });
});

// ═══════════════ triggers and gates ═══════════════

test('TRIGGERS: stored SQL must be a single read-only SELECT scoped by ?1; the seeded triggers all pass and unsafe rows are never run', async () => {
  for (const t of await loadTriggers(db)) assert.doesNotThrow(() => assertSafeTriggerSql(t.sql_condition), t.trigger_key);
  assert.deepEqual((await loadTriggers(db)).map((t) => t.trigger_key), ['T_ANNIV_DINING', 'T_ANNUAL_FEE_ROI', 'T_CLOSING_RETURN_OR_WARRANTY', 'T_EXPIRING_CREDIT', 'T_FAMILY_EVENT_AUTOPILOT', 'T_OCCASION_PLAN', 'T_PRICE_DROP_OR_DELAY_CLAIM', 'T_SUBSCRIPTION_CREEP']);
  const bad = [
    'DELETE FROM facts WHERE household_id = ?1', 'SELECT 1 FROM facts WHERE household_id = ?1; DROP TABLE facts', 'SELECT * FROM facts',
    "WITH x AS (SELECT 1) INSERT INTO audit_log (id, actor, action) SELECT 'a', 'b', 'c' FROM x WHERE ?1 = ?1", 'UPDATE facts SET value_text = NULL WHERE household_id = ?1',
    'PRAGMA table_info(facts)', 'SELECT load_extension(\'x\') WHERE ?1 = ?1', 'SELECT 1 FROM facts WHERE household_id = ?1 AND 1 = (SELECT 1; )', '  insert into facts select 1 where ?1 = ?1',
  ];
  for (const sql of bad) assert.throws(() => assertSafeTriggerSql(sql), UnsafeTriggerSqlError, sql);
  assert.doesNotThrow(() => assertSafeTriggerSql("SELECT 'drop table; delete' AS note FROM facts WHERE household_id = ?1"), 'keywords inside string literals are fine');

  // a malicious row in the triggers table is rejected and audited, and does nothing
  const h = await makeHousehold(db);
  await db.prepare(`INSERT INTO triggers (trigger_key, template_key, sql_condition, cooldown_days) VALUES ('T_EVIL', 'occasion_plan', ?, 1)`)
    .bind(`INSERT INTO audit_log (id, actor, action) SELECT 'evil-' || ?1, 'evil', 'pwn' WHERE ?2 = ?2 AND ?3 = ?3`).run();
  try {
    const { result } = await runSweep(h);
    assert.ok(result.triggers.evaluated >= 0);
    assert.equal(await count(`SELECT 1 FROM audit_log WHERE actor = 'evil'`), 0);
    assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'trigger_rejected'`, h.household_id), 1);
  } finally {
    await db.prepare(`DELETE FROM triggers WHERE trigger_key = 'T_EVIL'`).run();
  }
});

test('GATES: tax language is banned (G7) and candidates are ordered credit -> fee -> flagship before the rest', () => {
  for (const bad of ['This is deductible', 'you can deduct it', 'Enter on line 12', 'a business write-off', 'Tax DEDUCTION']) assert.throws(() => assertNoTaxLanguage(bad), TaxLanguageError, bad);
  assert.doesNotThrow(() => assertNoTaxLanguage('Possible evidence — confirm with your preparer.', 'Your $100 Resy credit resets Dec 31'));
  for (const t of ['T_EXPIRING_CREDIT', 'T_ANNUAL_FEE_ROI', 'T_ANNIV_DINING', 'T_SUBSCRIPTION_CREEP', 'T_OCCASION_PLAN']) {
    const copy = buildCopy(t, { card_name: 'Amex Platinum', label: 'Resy credit: $100 per quarter', amount_cents: 10000, used_cents: 0, remaining_cents: 10000, period_end: '2026-12-31', status: 'open', fee_cents: 89500, credits_used_cents: 0, next_post: '2026-12-05', has_history: true, occasion: 'Our Anniversary', kind: 'anniversary', occasion_date: '2026-12-05', window_date: '2026-12-04', credit_label: 'Resy credit: $100', credit_remaining_cents: 10000, credit_status: 'open', subscription: 'Netflix', old_cents: 1599, new_cents: 1799, since: '2026-11-01' });
    assert.ok(copy.title.length > 10 && copy.r2_label !== '' && Object.keys(copy.claims).length >= 1, t);
  }
  const mk = (trigger_key: string, score: number, key = trigger_key + score): Candidate => ({ trigger_key, template_key: 't', cooldown_days: 1, dedupe_key: key, recoverable_cents: score, urgency: 1, confidence: 1, score, claims: [], data: {} });
  const ordered = orderCandidates([mk('T_EXPIRING_CREDIT', 100), mk('T_EXPIRING_CREDIT', 900), mk('T_ANNUAL_FEE_ROI', 50), mk('T_SUBSCRIPTION_CREEP', 500), mk('T_ANNIV_DINING', 10), mk('T_EXPIRING_CREDIT', 300)]);
  assert.deepEqual(ordered.map((c) => `${c.trigger_key}:${c.score}`), ['T_EXPIRING_CREDIT:900', 'T_ANNUAL_FEE_ROI:50', 'T_ANNIV_DINING:10', 'T_SUBSCRIPTION_CREEP:500', 'T_EXPIRING_CREDIT:300', 'T_EXPIRING_CREDIT:100'], 'the flagship beats the subscription card for slot 3 even with a lower score');
  assert.deepEqual(orderCandidates([mk('T_SUBSCRIPTION_CREEP', 5), mk('T_EXPIRING_CREDIT', 1)]).map((c) => c.trigger_key), ['T_EXPIRING_CREDIT', 'T_SUBSCRIPTION_CREEP']);
  assert.equal(HOME_CAPACITY, 3);
});

// ═══════════════ the flagship cross-domain join ═══════════════

interface Scenario {
  h: Household;
  env: ReturnType<typeof testEnv>['env'];
  cardId: string;
  google: FakeGoogleState;
  feeDate: string;
  anniversary: string | null;
  text: string;
}

async function buildScenario(o: {
  occasionInDays?: number | null;
  occasionKind?: 'anniversary' | 'birthday';
  busyAllEvenings?: boolean;
  resyCents?: number;
  extraDiningCents?: number;
  csv?: boolean;
  amexRows?: boolean;
  calendar?: boolean;
} = {}): Promise<Scenario> {
  const feeDate = day(12);
  const { h, cardId, text } = await cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005', fee: { cents: 89500, postsOn: feeDate } });
  const google = newGoogle();
  const { env } = testEnv(rt);
  const days = o.occasionInDays === undefined ? 10 : o.occasionInDays;
  const anniversary = days === null ? null : day(days);
  if (o.calendar !== false) {
    if (anniversary !== null) google.events.push(allDayEvent('occ', o.occasionKind === 'birthday' ? "Ava's Birthday" : 'Our Anniversary', anniversary, day(days! + 1)));
    google.events.push(allDayEvent('trip', 'Lisbon Vacation', day(60), day(66)));
    google.events.push({ id: 'm1', summary: 'Team standup', start: { dateTime: `${day(1)}T14:00:00Z` }, end: { dateTime: `${day(1)}T14:30:00Z` } });
    if (o.busyAllEvenings) google.busy = [{ start: new Date(Date.now() - 86_400_000).toISOString(), end: new Date(Date.now() + 200 * 86_400_000).toISOString() }];
    await exchangeCode(calendarDeps(env, google), h.household_id, 'good-code', 'https://app.example/cb');
    await runSync(env, google, h);
  }
  if (o.csv !== false) {
    const rows = [
      mrow(monthDay(-3, 10), 'Carbone', 'Restaurants & Bar', 'Checking (...9999)', 'CARBONE NEW YORK', '-400.00'),
      mrow(monthDay(-2, 10), 'Carbone', 'Restaurants & Bar', 'Checking (...9999)', 'CARBONE NEW YORK', '-500.00'),
      mrow(monthDay(-1, 10), 'Carbone', 'Restaurants & Bar', 'Checking (...9999)', 'CARBONE NEW YORK', '-600.00'),
      mrow(monthDay(-1, 12), 'Paycheck', 'Paychecks', 'Checking (...9999)', 'ACME PAYROLL', '3000.00'),
    ];
    if (o.amexRows !== false) {
      for (const m of [-3, -2, -1, 0]) rows.push(mrow(monthDay(m, 1), 'Netflix', 'Subscriptions', AMEX, 'NETFLIX.COM', '-15.99'));
      rows.push(mrow(monthDay(0, 1), 'Uber', 'Taxi & Ride Shares', AMEX, 'UBER *TRIP', '-15.00'));
    }
    if ((o.resyCents ?? 0) > 0) rows.push(mrow(monthDay(0, 1), 'Resy', 'Restaurants & Bar', o.amexRows === false ? 'Checking (...9999)' : AMEX, 'RESY *CARBONE NEW YORK', `-${((o.resyCents ?? 0) / 100).toFixed(2)}`));
    if ((o.extraDiningCents ?? 0) > 0) rows.push(mrow(monthDay(0, 1), 'Wine Bar', 'Restaurants & Bar', 'Checking (...9999)', 'WINE BAR', `-${((o.extraDiningCents ?? 0) / 100).toFixed(2)}`));
    await ingestCsv(h, [MONARCH_HEADER, ...rows].join('\n') + '\n');
  } else {
    await recomputeCreditPeriods(db, h.household_id, { from: day(0), to: day(0) }, new Date());
  }
  return { h, env, cardId, google, feeDate, anniversary, text };
}

const flagship = async (h: Household, at: Date = new Date()): Promise<Candidate[]> => {
  const t = (await loadTriggers(db)).find((x) => x.trigger_key === 'T_ANNIV_DINING')!;
  return (await evaluateTrigger(db, h.household_id, t, at)).candidates;
};

test('T_ANNIV_DINING fires when all four domains join, and says what it joined', async () => {
  const s = await buildScenario({});
  const found = await flagship(s.h);
  assert.equal(found.length, 1);
  const c = found[0]!;
  assert.equal(c.trigger_key, 'T_ANNIV_DINING');
  const d = c.data as Record<string, unknown>;
  assert.deepEqual([d['occasion'], d['occasion_date'], d['kind'], d['credit_remaining_cents'], d['credit_status'], d['card_name']], ['Our Anniversary', s.anniversary, 'anniversary', 10000, 'open', 'Amex Platinum']);
  const gap = Math.abs(Date.parse(String(d['window_date'])) - Date.parse(s.anniversary!)) / 86_400_000;
  assert.ok(gap <= 3, `the free window is within 3 days of the occasion (${gap})`);
  assert.ok(Number(d['headroom_cents']) >= 18000);
  assert.equal(c.recoverable_cents, 10000);
  assert.equal(c.confidence, 1);
  assert.deepEqual(c.claims.map((x) => x.key), ['occasion', 'window', 'credit']);
  assert.ok(c.claims.every((x) => x.facts.length >= 1 && x.facts.every((f) => typeof f === 'string')));
  assert.ok(c.score > 0 && c.urgency > 0.3);
  // a birthday counts as an occasion too
  assert.equal((await flagship((await buildScenario({ occasionKind: 'birthday', occasionInDays: 5 })).h)).length, 1);
});

test('T_ANNIV_DINING is suppressed when ANY single domain fails (and at the exact boundaries)', async () => {
  const cases: Array<[string, Parameters<typeof buildScenario>[0], number]> = [
    ['occasion 15 days away (outside 14)', { occasionInDays: 15 }, 0],
    ['occasion 14 days away (inside)', { occasionInDays: 14 }, 1],
    ['no occasion on the calendar', { occasionInDays: null }, 0],
    ['calendar not connected at all', { calendar: false }, 0],
    ['every evening busy: no free window', { busyAllEvenings: true }, 0],
    ['Resy credit used down to $49.99 left', { resyCents: 5001 }, 0],
    ['Resy credit with exactly $50 left', { resyCents: 5000 }, 1],
    ['Resy credit fully used', { resyCents: 10000 }, 0],
    ['dining headroom $179.99', { extraDiningCents: 32001 }, 0],
    ['dining headroom exactly $180', { extraDiningCents: 32000 }, 1],
    ['no spend data, so no dining headroom', { csv: false }, 0],
  ];
  for (const [name, options, expected] of cases) {
    const s = await buildScenario(options);
    assert.equal((await flagship(s.h)).length, expected, name);
  }
  // credit known only as 'unconfirmed' (no Amex history): fires, at half confidence, and the card says so
  const unconfirmed = await buildScenario({ amexRows: false });
  const [c] = await flagship(unconfirmed.h);
  assert.deepEqual([c!.confidence, (c!.data as Record<string, unknown>)['credit_status']], [0.5, 'unconfirmed']);
  assert.match(buildCopy('T_ANNIV_DINING', c!.data).why, /possibly/);
});

test('T_ANNIV_DINING respects cooldown_days and does not duplicate an active card (Gate G9)', async () => {
  const s = await buildScenario({});
  const first = await runSweep(s.h);
  assert.deepEqual(first.log, ['decay', 'recompute', 'triggers', 'cold_start']);
  const runs = () => all<{ run_id: string; template_key: string; state: string }>(`SELECT run_id, template_key, state FROM mission_runs WHERE household_id = ? ORDER BY created_at, run_id`, s.h.household_id);
  const staged = await runs();
  assert.deepEqual(staged.map((r) => r.template_key).sort(), ['anniv_dining', 'annual_fee_roi', 'expiring_credit']);
  assert.ok(staged.every((r) => r.state === 'R1_STAGED'));

  const second = await runSweep(s.h);
  assert.equal(second.result.triggers.staged.length, 0);
  assert.ok(second.result.triggers.suppressed >= 3, `suppressed ${second.result.triggers.suppressed}`);
  assert.equal((await runs()).length, 3, 'a second sweep stages nothing new');

  // "Not for me": the run ends DECLINED, but its cooldown keeps the same occasion from coming straight back
  const flagshipRun = staged.find((r) => r.template_key === 'anniv_dining')!;
  await db.prepare(`UPDATE mission_runs SET state = 'DECLINED' WHERE run_id = ?`).bind(flagshipRun.run_id).run();
  const third = await runSweep(s.h);
  assert.equal((await runs()).filter((r) => r.template_key === 'anniv_dining').length, 1);
  assert.ok(third.result.triggers.suppressed >= 1);

  // direct engine checks with explicit clocks: the cooldown is a window, then it opens
  await db.prepare(`UPDATE triggers SET cooldown_days = 3 WHERE trigger_key = 'T_ANNIV_DINING'`).run();
  try {
    const def = (await loadTriggers(db)).find((t) => t.trigger_key === 'T_ANNIV_DINING')!;
    const at = (days: number) => new Date(Date.now() + days * 86_400_000);
    const [c2] = (await evaluateTrigger(db, s.h.household_id, def, at(2))).candidates;
    assert.equal(await isBlockedByCooldown(db, s.h.household_id, c2!, at(2)), true, 'day 2: inside the 3-day cooldown');
    const [c4] = (await evaluateTrigger(db, s.h.household_id, def, at(4))).candidates;
    assert.equal(await isBlockedByCooldown(db, s.h.household_id, c4!, at(4)), false, 'day 4: the cooldown has passed');
    const outcome = await stageCandidate(db, s.h.household_id, c4!, at(4));
    assert.equal(outcome.staged, true);
    assert.equal((await runs()).filter((r) => r.template_key === 'anniv_dining').length, 2);
    // and now the new run is active, so it blocks a repeat regardless of the clock
    assert.equal(await isBlockedByCooldown(db, s.h.household_id, c4!, at(30)), true);
  } finally {
    await db.prepare(`UPDATE triggers SET cooldown_days = 14 WHERE trigger_key = 'T_ANNIV_DINING'`).run();
  }
});

test('GATE G1: a card whose supporting quote no longer verifies is never staged, and the rejection is audited', async () => {
  const s = await buildScenario({});
  const fact = await one<{ fact_id: string }>(`SELECT f.fact_id FROM facts f WHERE f.household_id = ? AND f.predicate = 'card_product'`, s.h.household_id);
  // corrupt the stored offsets so the quote is no longer a substring at that position
  await db.prepare('UPDATE fact_quotes SET char_start = char_start + 1, char_end = char_end + 1 WHERE fact_id = ?').bind(fact.fact_id).run();
  const { result } = await runSweep(s.h);
  const templates = (await all<{ template_key: string }>('SELECT template_key FROM mission_runs WHERE household_id = ?', s.h.household_id)).map((r) => r.template_key);
  assert.ok(!templates.includes('expiring_credit') && !templates.includes('anniv_dining'), templates.join());
  assert.ok(templates.includes('annual_fee_roi'), 'cards that rest on other, intact facts are unaffected');
  assert.ok(result.triggers.ungrounded >= 2);
  const audits = await all<{ detail_json: string }>(`SELECT detail_json FROM audit_log WHERE household_id = ? AND action = 'mission_ungrounded'`, s.h.household_id);
  assert.ok(audits.length >= 2 && audits.every((a) => /no verified quote/.test(a.detail_json)));
});

// ═══════════════ Day-1 cold start + Home ═══════════════

async function sessionFor(env: ReturnType<typeof testEnv>['env'], h: Household, ttl = 3600): Promise<string> {
  return issueSessionToken(env.SESSION_SECRET, { household_id: h.household_id, member_id: `mem_${h.household_id}` }, ttl, new Date());
}
const api = (env: ReturnType<typeof testEnv>['env'], path: string, o: { method?: string; token?: string | null; body?: BodyInit; headers?: Record<string, string> } = {}) =>
  handleApi(new Request(`https://api.example${path}`, { method: o.method ?? 'GET', headers: { ...(o.token === null ? {} : { authorization: `Bearer ${o.token}` }), ...o.headers }, ...(o.body ? { body: o.body } : {}) }), env);

test('COLD START: Calendar + Cards + a sample Monarch CSV yield 3 quote-grounded cards, and /api/home returns the exact quotes', async () => {
  const s = await buildScenario({});
  const { result } = await runSweep(s.h);
  assert.equal(result.triggers.staged.length + result.cold_start.staged.length, 3);
  assert.deepEqual(result.cold_start, { staged: [], unlock_scan: false });

  const token = await sessionFor(s.env, s.h);
  const res = await api(s.env, '/api/home', { token });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('cache-control'), 'no-store');
  const home = (await res.json()) as Awaited<ReturnType<typeof getHome>>;
  assert.equal(home.cards.length, 3);
  assert.deepEqual(home.cards.map((c) => c.trigger_key).sort(), ['T_ANNIV_DINING', 'T_ANNUAL_FEE_ROI', 'T_EXPIRING_CREDIT']);
  assert.deepEqual(home.cards.map((c) => c.score), [...home.cards.map((c) => c.score)].sort((a, b) => b - a), 'ranked by recoverable x urgency');
  assert.deepEqual([home.header.waiting_count, home.header.filed_count], [3, 0]);
  const expectedFound = home.cards.reduce((sum, c) => sum + Math.trunc(c.recoverable_cents * c.confidence), 0);
  assert.equal(home.header.found_cents, expectedFound);
  assert.ok(home.header.found_cents > 0);

  const text = (id: string) => one<{ redacted_text: string }>('SELECT redacted_text FROM episodes WHERE episode_id = ?', id).then((r) => r.redacted_text);
  for (const card of home.cards) {
    assert.ok(card.evidence.length >= 1, `${card.trigger_key} has evidence`);
    assert.ok(card.title !== '' && card.why !== '' && card.r2_label !== '');
    for (const chip of card.evidence) {
      assert.equal((await text(chip.episode_id)).slice(chip.char_start, chip.char_end), chip.quote, `${card.trigger_key}: chip quote is verbatim`);
      assert.ok(chip.claim.length > 5);
    }
  }
  const byTrigger = (k: string) => home.cards.find((c) => c.trigger_key === k)!;
  const dining = byTrigger('T_ANNIV_DINING');
  assert.deepEqual(dining.evidence.map((e) => e.predicate).sort(), ['card_product', 'event_date', 'event_kind', 'free_window']);
  assert.ok(dining.evidence.some((e) => e.quote === `Occasion: Our Anniversary | anniversary | ${s.anniversary} | yearly` && e.source === 'calendar'));
  assert.ok(dining.evidence.some((e) => /^Free window (Fri|Sat) \d{4}-\d{2}-\d{2} 18:00-22:00 America\/New_York$/.test(e.quote)));
  assert.ok(dining.evidence.some((e) => e.quote === 'Amex Platinum' && e.source === 'user_input'));
  const fee = byTrigger('T_ANNUAL_FEE_ROI');
  assert.deepEqual(fee.evidence.map((e) => e.quote).sort(), ['annual fee $895', `posts ${s.feeDate}`]);
  assert.match(fee.title, /\$895 fee posts/);
  assert.equal(fee.r2_label, 'Stage retention-call script');
  assert.equal(fee.unconfirmed, false, 'the card has statement history from the CSV');
  for (const c of home.cards) assert.doesNotMatch(`${c.title} ${c.why}`, /deductib|deduct/i);
  // every staged card is private, free, and recorded with its gates
  const runs = await all<{ state: string; cost_usd: number; origin: string; expires_at: string | null }>('SELECT state, cost_usd, origin, expires_at FROM mission_runs WHERE household_id = ?', s.h.household_id);
  assert.ok(runs.every((r) => r.state === 'R1_STAGED' && r.cost_usd === 0 && r.origin === 'trigger' && r.expires_at !== null));
  const gates = await all<{ gate_name: string }>(`SELECT gate_name FROM steps s JOIN mission_runs r ON r.run_id = s.run_id WHERE r.household_id = ? AND s.kind = 'gate'`, s.h.household_id);
  assert.equal(gates.filter((g) => g.gate_name === 'G1_grounding').length, 3);
  assert.equal(await count(`SELECT 1 FROM claims c JOIN mission_runs r ON r.run_id = c.run_id WHERE r.household_id = ? AND c.status = 'grounded'`, s.h.household_id) >= 5, true);
  assert.equal(await count(`SELECT 1 FROM llm_usage WHERE household_id = ?`, s.h.household_id), 0, 'no model was involved');

  // header follows the cards: confirming one moves it from waiting to filed
  await db.prepare(`UPDATE mission_runs SET state = 'CONFIRMED', updated_at = ? WHERE run_id = ?`).bind(new Date().toISOString(), dining.run_id).run();
  const after = (await (await api(s.env, '/api/home', { token })).json()) as typeof home;
  assert.deepEqual([after.header.waiting_count, after.header.filed_count, after.cards.length], [2, 1, 2]);
  assert.equal(after.header.found_cents, expectedFound, 'confirmed work still counts as found this week');
});

test('COLD START: Calendar + Cards only (no CSV) still gives 3 cards, marked unconfirmed, with the occasion fallback', async () => {
  const s = await buildScenario({ csv: false, occasionInDays: 20 });
  const { result } = await runSweep(s.h);
  assert.deepEqual(result.triggers.staged.map((c) => c.trigger_key).sort(), ['T_ANNUAL_FEE_ROI', 'T_EXPIRING_CREDIT']);
  assert.deepEqual(result.cold_start.staged.map((c) => c.trigger_key), ['T_OCCASION_PLAN']);
  assert.equal(result.cold_start.unlock_scan, false);
  const home = await getHome(db, s.h.household_id, new Date());
  assert.equal(home.cards.length, 3);
  const credit = home.cards.find((c) => c.trigger_key === 'T_EXPIRING_CREDIT')!;
  assert.equal(credit.unconfirmed, true);
  assert.match(credit.title, /can't see this card's spending yet/);
  assert.equal(credit.confidence, 0.5);
  assert.equal(home.cards.find((c) => c.trigger_key === 'T_ANNUAL_FEE_ROI')!.unconfirmed, true);
  const plan = home.cards.find((c) => c.trigger_key === 'T_OCCASION_PLAN')!;
  assert.deepEqual(plan.evidence.map((e) => e.predicate).sort(), ['event_date', 'event_kind']);
  assert.match(plan.title, /Our Anniversary is /);
  // nothing here overstates: unconfirmed money is counted at half weight
  const full = home.cards.reduce((sum, c) => sum + c.recoverable_cents, 0);
  assert.ok(home.header.found_cents < full);
});

test('COLD START: a household with nothing yet gets the "forward receipts" card once, never a repeat', async () => {
  const h = await makeHousehold(db);
  const first = await runSweep(h);
  assert.deepEqual([first.result.triggers.staged.length, first.result.cold_start.staged.length, first.result.cold_start.unlock_scan], [0, 0, true]);
  const home = await getHome(db, h.household_id, new Date());
  assert.equal(home.cards.length, 1);
  assert.match(home.cards[0]!.title, /Forward 3 receipts/);
  assert.deepEqual([home.cards[0]!.evidence.length, home.cards[0]!.recoverable_cents], [0, 0]);
  const second = await runSweep(h);
  assert.equal(second.result.cold_start.unlock_scan, false);
  assert.equal(await count(`SELECT 1 FROM mission_runs WHERE household_id = ?`, h.household_id), 1);
  // once the household has spend data, the placeholder is not offered again
  const other = await makeHousehold(db);
  await ingestCsv(other, `${MONARCH_HEADER}\n${mrow(monthDay(-1, 5), 'Shop', 'Shopping', 'Checking (...1)', 'SHOP', '-10.00')}\n`);
  assert.equal((await runSweep(other)).result.cold_start.unlock_scan, false);
});

test('COLD START: a subscription price increase from a CSV becomes the third card, with both charges as evidence', async () => {
  const { h, cardId } = await cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005', fee: { cents: 89500, postsOn: day(12) } });
  void cardId;
  const rows = [
    mrow(monthDay(-3, 1), 'Streamly', 'Subscriptions', AMEX, 'STREAMLY.COM', '-15.99'),
    mrow(monthDay(-2, 1), 'Streamly', 'Subscriptions', AMEX, 'STREAMLY.COM', '-15.99'),
    mrow(monthDay(-1, 1), 'Streamly', 'Subscriptions', AMEX, 'STREAMLY.COM', '-16.99'),
    mrow(monthDay(0, 1), 'Streamly', 'Subscriptions', AMEX, 'STREAMLY.COM', '-16.99'),
  ];
  await ingestCsv(h, [MONARCH_HEADER, ...rows].join('\n') + '\n');
  const { result } = await runSweep(h);
  assert.ok(result.triggers.staged.some((c) => c.trigger_key === 'T_SUBSCRIPTION_CREEP'), JSON.stringify(result.triggers.staged));
  const home = await getHome(db, h.household_id, new Date());
  const card = home.cards.find((c) => c.trigger_key === 'T_SUBSCRIPTION_CREEP')!;
  assert.match(card.title, /Streamly went from \$15\.99 to \$16\.99/);
  assert.equal(card.recoverable_cents, 100 * 12);
  const quotes = card.evidence.map((e) => e.quote);
  assert.equal(quotes.length, 2);
  assert.ok(quotes.every((qt) => /STREAMLY\.COM/.test(qt)), quotes.join(' | '));
  assert.notEqual(quotes[0], quotes[1], 'one quote per charge: the old price and the new one');
  assert.equal(card.r2_label, 'Stage cancel script');
});

// ═══════════════ HTTP API ═══════════════

test('API: every endpoint requires a valid session for a current member; the household comes from the token only', async () => {
  const { env } = testEnv(rt);
  const h = await makeHousehold(db);
  const token = await sessionFor(env, h);
  assert.equal((await api(env, '/api/home', { token: null })).status, 401);
  assert.equal((await api(env, '/api/home', { token: 'garbage' })).status, 401);
  assert.equal((await api(env, '/api/home', { token: `${token.slice(0, -2)}xx` })).status, 401, 'tampered signature');
  const [body] = token.split('.') as [string, string];
  assert.equal((await api(env, '/api/home', { token: `${body}.` })).status, 401);
  assert.equal((await api(env, '/api/home', { token: await sessionFor(env, h, -10) })).status, 401, 'expired');
  assert.equal((await api(env, '/api/home', { token: await issueSessionToken('another-secret-0123456789', { household_id: h.household_id, member_id: `mem_${h.household_id}` }, 3600, new Date()) })).status, 401, 'wrong key');
  assert.equal((await api(env, '/api/home', { token: await issueSessionToken(env.SESSION_SECRET, { household_id: h.household_id, member_id: 'mem_someone_else' }, 3600, new Date()) })).status, 401, 'member does not exist');
  assert.equal((await api(env, '/api/home', { token })).status, 200);
  assert.equal((await api(env, '/api/home', { token, headers: { authorization: `Token ${token}` } })).status, 401, 'not a Bearer header');
  assert.equal((await api(env, '/api/home', { token, headers: { authorization: `Bearer ${token} extra` } })).status, 401, 'malformed header');
  // routing
  assert.equal((await api(env, '/api/nope', { token })).status, 404);
  const wrong = await api(env, '/api/ingest/file', { token });
  assert.deepEqual([wrong.status, wrong.headers.get('allow')], [405, 'POST']);
  assert.equal((await api(env, '/api/home', { token, method: 'POST' })).status, 405);
  // a member who left the household loses access immediately
  await db.prepare('DELETE FROM members WHERE household_id = ?').bind(h.household_id).run();
  assert.equal((await api(env, '/api/home', { token })).status, 401);
  // and a soft-deleted household has no API
  const gone = await makeHousehold(db);
  const goneToken = await sessionFor(env, gone);
  await db.prepare(`UPDATE households SET deleted_at = '2026-01-01T00:00:00.000Z' WHERE household_id = ?`).bind(gone.household_id).run();
  assert.equal((await api(env, '/api/home', { token: goneToken })).status, 401);
});

test("API: /api/home never shows another household's cards", async () => {
  const a = await buildScenario({});
  const b = await buildScenario({});
  await runSweep(a.h);
  const tokenB = await sessionFor(b.env, b.h);
  const home = (await (await api(b.env, '/api/home', { token: tokenB })).json()) as Awaited<ReturnType<typeof getHome>>;
  assert.deepEqual([home.cards.length, home.header.found_cents, home.header.waiting_count], [0, 0, 0]);
  const homeA = await getHome(db, a.h.household_id, new Date());
  assert.equal(homeA.cards.length, 3);
});

test('API: POST /api/ingest/file stores the upload encrypted, starts IngestFileWF, and the workflow can read it back', async () => {
  const { env, workflows } = testEnv(rt);
  const { h, cardId } = await cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005' });
  const token = await sessionFor(env, h);
  const csv = `${MONARCH_HEADER}\n${mrow(monthDay(-1, 3), 'Resy', 'Restaurants & Bar', AMEX, 'RESY *SECRET-MARKER NEW YORK', '-12.50')}\n`;
  const upload = (fields: Record<string, string | File>, tok: string | null = token) => {
    const form = new FormData();
    for (const [k, v] of Object.entries(fields)) form.set(k, v);
    return api(env, '/api/ingest/file', { method: 'POST', token: tok, body: form });
  };
  const other = await makeHousehold(db);
  const res = await upload({ file: new File([csv], 'monarch.csv', { type: 'text/csv' }), default_card_entity_id: cardId, household_id: other.household_id });
  assert.equal(res.status, 202);
  const accepted = (await res.json()) as { status: string; instance_id: string; sha256: string; kind: string };
  assert.deepEqual([accepted.status, accepted.kind], ['accepted', 'csv']);
  assert.equal(workflows.file.length, 1);
  const { params, id } = workflows.file[0]!;
  assert.equal(id, accepted.instance_id);
  assert.deepEqual([params.household_id, params.kind, params.file_name, params.sha256, params.default_card_entity_id, params.sign_convention], [h.household_id, 'csv', 'monarch.csv', await sha256Hex(new TextEncoder().encode(csv)), cardId, 'auto']);
  assert.equal(params.household_id === other.household_id, false, 'a household id in the form is ignored');
  const stored = new Uint8Array(await (await rt.bucket.get(params.r2_key))!.arrayBuffer());
  assert.ok(!new TextDecoder('latin1').decode(stored).includes('SECRET-MARKER'), 'encrypted at rest');
  assert.match(params.r2_key, new RegExp(`^upload/${h.household_id}/`));
  assert.equal(new TextDecoder().decode((await getDecrypted(env, h.household_id, params.r2_key))!), csv);

  // the very params the endpoint produced drive the real pipeline
  const run = await runIngestFile({ env, gateway: noLlm() }, params, directStep().step);
  assert.equal(run.status, 'done');
  assert.equal(await count('SELECT 1 FROM transactions WHERE household_id = ? AND card_entity_id = ?', h.household_id, cardId), 1);

  // same file again: accepted once
  const again = await upload({ file: new File([csv], 'monarch.csv') });
  assert.deepEqual([again.status, ((await again.json()) as { status: string }).status], [200, 'duplicate']);
  assert.equal(workflows.file.length, 1);
});

test('API: upload validation — size, PDFs, kinds, options, ownership of the card', async () => {
  const { env, workflows } = testEnv(rt);
  const h = await makeHousehold(db);
  const token = await sessionFor(env, h);
  const other = await cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005' });
  const post = (form: FormData | null, headers: Record<string, string> = {}) => api(env, '/api/ingest/file', { method: 'POST', token, ...(form ? { body: form } : {}), headers });
  const f = (fields: Record<string, string | File>): FormData => {
    const form = new FormData();
    for (const [k, v] of Object.entries(fields)) form.set(k, v);
    return form;
  };
  const code = async (r: Response) => ((await r.json()) as { error?: string }).error;

  assert.equal((await post(null, { 'content-type': 'application/json' })).status, 415);
  assert.equal(await code(await post(f({ nofile: 'x' }))), 'file_required');
  assert.equal(await code(await post(f({ file: 'not a file' }))), 'file_required');
  assert.equal(await code(await post(f({ file: new File([], 'a.csv') }))), 'empty_file');
  const pdf = await post(f({ file: new File([`%PDF-1.7\n1 0 obj`], 'statement.pdf', { type: 'application/pdf' }) }));
  assert.deepEqual([pdf.status, await code(pdf)], [415, 'pdf_text_required']);
  assert.equal(await code(await post(f({ file: new File(['a,b'], 'data.xlsx') }))), 'kind_required');
  assert.equal(await code(await post(f({ file: new File(['a,b'], 'data.csv'), kind: 'spreadsheet' }))), 'kind_required');
  assert.equal((await post(f({ file: new File([new Uint8Array([0xff, 0xfe, 0xfd, 0x00])], 'bin.csv') }))).status, 415);
  assert.equal(await code(await post(f({ file: new File(['Date,Amount\n'], 'a.csv'), sign_convention: 'sideways' }))), 'bad_sign_convention');
  assert.equal(await code(await post(f({ file: new File(['Date,Amount\n'], 'a.csv'), statement_year: '1999' }))), 'bad_statement_year');
  assert.equal(await code(await post(f({ file: new File(['Date,Amount\n'], 'a.csv'), default_card_entity_id: other.cardId }))), 'unknown_card', "another household's card");
  assert.equal(await code(await post(f({ file: new File(['Date,Amount\n'], 'a.csv'), default_card_entity_id: 'ent_missing' }))), 'unknown_card');
  assert.equal(workflows.file.length, 0, 'nothing was started by a rejected upload');
  const big = await post(f({ file: new File(['x'], 'a.csv') }), { 'content-length': String(MAX_UPLOAD_BYTES + 200_000) });
  assert.deepEqual([big.status, await code(big)], [413, 'too_large']);

  // statement text from a PDF is accepted as kind=pdf_text, with a statement year and sign convention
  const ok = await post(f({ file: new File(['11/03 RESY *CARBONE 62.50\n'], 'statement.txt'), sign_convention: 'positive_is_outflow', statement_year: '2026' }));
  assert.equal(ok.status, 202);
  assert.deepEqual([workflows.file[0]!.params.kind, workflows.file[0]!.params.sign_convention, workflows.file[0]!.params.statement_year], ['pdf_text', 'positive_is_outflow', 2026]);
});

test('API: POST /api/calendar/sync needs a connected calendar and starts CalendarSyncWF with the sweep chained', async () => {
  const google = newGoogle();
  const { env, workflows } = testEnv(rt);
  const h = await makeHousehold(db);
  const token = await sessionFor(env, h);
  const none = await api(env, '/api/calendar/sync', { method: 'POST', token });
  assert.deepEqual([none.status, ((await none.json()) as { error: string }).error], [409, 'calendar_not_connected']);
  await exchangeCode(calendarDeps(env, google), h.household_id, 'good-code', 'https://app.example/cb');
  const res = await api(env, '/api/calendar/sync', { method: 'POST', token });
  assert.equal(res.status, 202);
  assert.equal(workflows.calendar.length, 1);
  assert.deepEqual(workflows.calendar[0]!.params, { household_id: h.household_id, then_sweep: true });
  const again = await api(env, '/api/calendar/sync', { method: 'POST', token });
  assert.deepEqual([again.status, ((await again.json()) as { status: string }).status], [200, 'already_running'], 'a double tap in the same minute does not start two syncs');
  assert.equal(workflows.calendar.length, 1);
  // a revoked connector counts as not connected
  await db.prepare(`UPDATE connectors SET status = 'revoked' WHERE household_id = ?`).bind(h.household_id).run();
  assert.equal((await api(env, '/api/calendar/sync', { method: 'POST', token })).status, 409);
});

// ═══════════════ nightly scheduling ═══════════════

test('SCHEDULED: the hourly cron starts the nightly chain only for households at 03:00 local, once per local day', async () => {
  const google = newGoogle();
  const { env, workflows } = testEnv(rt);
  const mk = async (timezone: string, o: { plan?: string; deleted?: boolean; calendar?: boolean } = {}) => {
    const h = await makeHousehold(db);
    await db.prepare('UPDATE households SET timezone = ?, plan = ?, deleted_at = ? WHERE household_id = ?').bind(timezone, o.plan ?? 'trial', o.deleted ? '2026-01-01T00:00:00.000Z' : null, h.household_id).run();
    if (o.calendar) await exchangeCode(calendarDeps(env, google), h.household_id, 'good-code', 'https://app.example/cb');
    return h;
  };
  const nyPlain = await mk('America/New_York');
  const nyCalendar = await mk('America/New_York', { calendar: true });
  const nyCanceled = await mk('America/New_York', { plan: 'canceled' });
  const nyDeleted = await mk('America/New_York', { deleted: true });
  const tokyo = await mk('Asia/Tokyo');
  const london = await mk('Europe/London');
  const bogus = await mk('Mars/Phobos');
  const at = new Date('2026-12-10T08:00:00Z'); // 03:00 EST, 17:00 Tokyo, 08:00 London
  const first = await runScheduled(env, at);
  const swept = new Set(workflows.sweep.map((w) => w.params.household_id));
  const synced = new Set(workflows.calendar.map((w) => w.params.household_id));
  assert.ok(swept.has(nyPlain.household_id) && !synced.has(nyPlain.household_id));
  assert.ok(synced.has(nyCalendar.household_id) && !swept.has(nyCalendar.household_id), 'a connected calendar goes through CalendarSyncWF, which chains the sweep');
  for (const skipped of [nyCanceled, nyDeleted, tokyo, london, bogus]) {
    assert.ok(!swept.has(skipped.household_id) && !synced.has(skipped.household_id), skipped.household_id);
  }
  assert.equal(workflows.sweep.find((w) => w.params.household_id === nyPlain.household_id)!.id, `sweep_${nyPlain.household_id}_2026-12-10`);
  const cal = workflows.calendar.find((w) => w.params.household_id === nyCalendar.household_id)!;
  assert.deepEqual([cal.id, cal.params.then_sweep], [`cal_${nyCalendar.household_id}_2026-12-10`, true]);
  assert.ok(first.started >= 2 && first.due === first.started + first.skipped);
  // the next hour starts nothing for them; a repeat of the same hour is idempotent
  const second = await runScheduled(env, at);
  assert.equal(second.started, 0);
  assert.equal(second.skipped, first.due);
  const total = workflows.sweep.length + workflows.calendar.length;
  await runScheduled(env, new Date('2026-12-10T09:00:00Z'));
  assert.equal(workflows.sweep.length + workflows.calendar.length, total);
  // Tokyo's 03:00 is 18:00 UTC the day before
  await runScheduled(env, new Date('2026-12-09T18:00:00Z'));
  assert.ok(workflows.sweep.some((w) => w.params.household_id === tokyo.household_id && w.id === `sweep_${tokyo.household_id}_2026-12-10`));
  // London in winter: 03:00 local = 03:00 UTC
  await runScheduled(env, new Date('2026-12-10T03:00:00Z'));
  assert.ok(workflows.sweep.some((w) => w.params.household_id === london.household_id));
});
