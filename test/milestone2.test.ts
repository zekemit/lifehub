/**
 * Milestone 2 behavioural suite: metered LLM gateway, egress policy, email routing + ingestion, CSV/PDF spend engine.
 * Real local D1 + R2 (miniflare/workerd). Faked: only the LLM providers' network and the Workflows engine.
 */

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { getDecrypted, putEncrypted, sha256Hex } from '../src/crypto/vault';
import { writeFact } from '../src/graph/writeFact';
import { runIngestEmail, type EmailPipelineResult } from '../src/ingest/emailPipeline';
import { extractFacts, EpisodeNotSafeError } from '../src/ingest/extract';
import { runIngestFile, type FilePipelineResult } from '../src/ingest/filePipeline';
import {
  CsvFormatError,
  categoryToEnvelope,
  normalizeDescriptor,
  parseCsv,
  parseDate,
  parseSpendCsv,
  parseStatementText,
  toCents,
} from '../src/ingest/csv';
import {
  CADENCES,
  cadenceTolerance,
  detectRecurring,
  median,
  periodsFor,
  type HistoryTxn,
} from '../src/ingest/spend';
import { MAX_FACTS_PER_EPISODE, MAX_FACTS_PER_STEP, chunk, writeFactsChunk } from '../src/ingest/steps';
import {
  LlmUnavailableError,
  MODEL_ROUTES,
  TokenCliffError,
  UnredactedInputError,
  WORKER_INPUT_TOKEN_CEILING,
  createGateway,
  estimateTokens,
  splitToTokenBudget,
  type Gateway,
} from '../src/llm/gateway';
import { MODEL_PRICING, costUsd } from '../src/llm/pricing';
import { EgressError, PolicyBlockedError, createGuardedFetch, matchBlocklist } from '../src/net/egress';
import {
  MAX_EMAIL_BYTES,
  authenticationVerdict,
  handleEmail,
  parseRecipient,
} from '../src/workers/emailRouter';
import { countingDb, openTestRuntime, type TestRuntime } from './helpers';
import {
  PASSING_AUTH,
  anthropicReply,
  directStep,
  fakeProvider,
  inboundMessage,
  makeHousehold,
  openaiReply,
  randomKek,
  rfc822,
  testEnv,
  type Household,
  type ProviderCall,
  type ProviderHandler,
} from './helpers/m2';

let rt: TestRuntime;
let db: D1Database;
before(async () => {
  rt = await openTestRuntime();
  db = rt.db;
});
after(async () => {
  await rt.dispose();
});

// ── shared fixtures ──
const all = async <T>(sql: string, ...b: unknown[]): Promise<T[]> => (await db.prepare(sql).bind(...b).all<T>()).results;
const one = async <T>(sql: string, ...b: unknown[]): Promise<T> => (await db.prepare(sql).bind(...b).first<T>())!;
const count = async (sql: string, ...b: unknown[]): Promise<number> => (await one<{ n: number }>(`SELECT COUNT(*) AS n FROM (${sql})`, ...b)).n;

function gatewayWith(handler: ProviderHandler, o: { db?: D1Database } = {}): { gateway: Gateway; calls: ProviderCall[] } {
  const provider = fakeProvider(handler);
  const store = o.db ?? db;
  const gateway = createGateway({
    db: store,
    apiKeys: { openai: 'sk-test', anthropic: 'ak-test' },
    fetch: createGuardedFetch({ db: store, fetchImpl: provider.fetch, ttlMs: 0 }),
  });
  return { gateway, calls: provider.calls };
}

const NEVER: ProviderHandler = () => {
  throw new Error('the LLM must not be called in this test');
};

const llmRows = (household_id: string) =>
  all<{ model: string; input_tokens: number; cached_tokens: number; output_tokens: number; cost_usd: number }>(
    'SELECT model, input_tokens, cached_tokens, output_tokens, cost_usd FROM llm_usage WHERE household_id = ? ORDER BY created_at, id',
    household_id,
  );

const close = (a: number, b: number): void => assert.ok(Math.abs(a - b) < 1e-9, `${a} !~ ${b}`);

// ═════════════════════════════ egress policy ═════════════════════════════

test('EGRESS: every policy_blocklist domain, its subdomains and spelling tricks are rejected before the network is touched', async () => {
  const provider = fakeProvider(() => new Response('ok'));
  const guarded = createGuardedFetch({ db, fetchImpl: provider.fetch, ttlMs: 0 });
  const seeded = await all<{ domain: string }>('SELECT domain FROM policy_blocklist');
  assert.deepEqual(
    seeded.map((d) => d.domain).sort(),
    ['api.monarch.com', 'api.resy.com', 'exploretock.com', 'opentable.com', 'resy.com', 'sevenrooms.com'],
  );
  const urls = [
    'https://resy.com/cities/ny',
    'https://api.resy.com/4/find',
    'https://opentable.com/',
    'https://www.opentable.com/r/carbone',
    'https://exploretock.com/x',
    'https://sevenrooms.com/reservations',
    'https://API.MONARCH.COM/graphql',
    'https://resy.com./trailing-dot',
    'http://us.resy.com:8443/port',
    'https://user:pw@resy.com/creds',
  ];
  for (const url of urls) {
    await assert.rejects(() => guarded(url), (e: unknown) => e instanceof PolicyBlockedError, url);
  }
  assert.equal(provider.calls.length, 0, 'blocked requests must never reach the network layer');
  // look-alikes are different domains
  await guarded('https://notresy.com/');
  await guarded('https://resy.com.example.org/');
  assert.equal(provider.calls.length, 2);
  assert.equal(matchBlocklist('monarch.com', [{ domain: 'api.monarch.com', reason: 'x' }]), null);
  await assert.rejects(() => guarded('file:///etc/passwd'), EgressError);
  await assert.rejects(() => guarded('ftp://example.com/x'), EgressError);
});

test('EGRESS: redirects are re-checked on every hop, the blocklist is live, and it fails closed', async () => {
  const hops: string[] = [];
  const provider = fakeProvider((call) => {
    hops.push(call.url);
    return call.url.includes('start')
      ? new Response(null, { status: 302, headers: { location: 'https://www.opentable.com/steal' } })
      : new Response('ok');
  });
  // fakeProvider parses JSON bodies; bodies are empty here, which parses as {}
  const guarded = createGuardedFetch({ db, fetchImpl: provider.fetch, ttlMs: 0 });
  await assert.rejects(() => guarded('https://example.org/start'), PolicyBlockedError);
  assert.deepEqual(hops, ['https://example.org/start']);

  // a new row takes effect without redeploying
  await db.prepare(`INSERT INTO policy_blocklist (domain, reason) VALUES ('blocked-later.example', 'test')`).run();
  try {
    await assert.rejects(() => guarded('https://shop.blocked-later.example/'), PolicyBlockedError);
  } finally {
    await db.prepare(`DELETE FROM policy_blocklist WHERE domain = 'blocked-later.example'`).run();
  }
  await guarded('https://shop.blocked-later.example/');

  // fail closed: if the policy cannot be read, nothing is fetched
  const broken = { prepare: () => { throw new Error('d1 down'); } } as unknown as D1Database;
  const before = provider.calls.length;
  await assert.rejects(() => createGuardedFetch({ db: broken, fetchImpl: provider.fetch })('https://example.org/x'), /d1 down/);
  assert.equal(provider.calls.length, before);
});

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const p = join(dir, name);
    return statSync(p).isDirectory() ? sourceFiles(p) : p.endsWith('.ts') ? [p] : [];
  });
}

test('EGRESS: no source file calls the global fetch() except the guarded wrapper', () => {
  const root = resolve(process.env['PROJECT_ROOT'] ?? process.cwd(), 'src');
  const offenders = sourceFiles(root)
    .filter((f) => !f.endsWith(join('net', 'egress.ts')))
    .filter((f) => /(^|[^.\w$])fetch\s*\(/.test(readFileSync(f, 'utf8').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '')));
  assert.deepEqual(offenders, []);
});

test('CONFIG: no brand name or domain is hardcoded in source (they come from env vars)', () => {
  const root = resolve(process.env['PROJECT_ROOT'] ?? process.cwd());
  const hits = sourceFiles(join(root, 'src')).filter((f) => /lifehub\.app|\bKeel\b/i.test(readFileSync(f, 'utf8')));
  assert.deepEqual(hits, []);
  const toml = readFileSync(join(root, 'wrangler.toml'), 'utf8');
  assert.match(toml, /APP_NAME = "Keel"/);
  assert.match(toml, /INBOUND_DOMAIN = "in\.lifehub\.app"/);
  assert.match(toml, /binding = "INGEST_EMAIL_WF"[\s\S]*class_name = "IngestEmailWF"/);
  assert.match(toml, /binding = "INGEST_FILE_WF"[\s\S]*class_name = "IngestFileWF"/);
});

// ═════════════════════════════ LLM gateway ═════════════════════════════

const SCHEMA = { name: 'echo', schema: { type: 'object', additionalProperties: false, required: ['ok'], properties: { ok: { type: 'boolean' } } } };

test('GATEWAY: routes chief -> gpt-6-sol and worker -> gpt-6-luna, and meters every call into llm_usage', async () => {
  const h = await makeHousehold(db);
  const { gateway, calls } = gatewayWith(() => openaiReply({ ok: true }, { prompt_tokens: 2000, completion_tokens: 500, cached: 400 }));
  assert.deepEqual(MODEL_ROUTES, {
    chief: { primary: 'gpt-6-sol', failover: 'claude-opus-5-5' },
    worker: { primary: 'gpt-6-luna', failover: 'claude-haiku-5-5' },
  });
  const w = await gateway.worker({ household_id: h.household_id, purpose: 't.worker', system: 'You are a test.', user: 'hello', json_schema: SCHEMA });
  const c = await gateway.chief({ household_id: h.household_id, purpose: 't.chief', system: 'You are a test.', user: 'hello' });
  assert.equal(calls[0]!.host, 'api.openai.com');
  assert.equal(calls[0]!.body['model'], 'gpt-6-luna');
  assert.equal(calls[1]!.body['model'], 'gpt-6-sol');
  assert.deepEqual((calls[0]!.body['response_format'] as { json_schema: { strict: boolean } }).json_schema.strict, true);
  assert.equal(w.failed_over, false);
  assert.equal(c.model, 'gpt-6-sol');
  const rows = await llmRows(h.household_id);
  assert.equal(rows.length, 2);
  assert.deepEqual(rows[0] && { m: rows[0].model, i: rows[0].input_tokens, c: rows[0].cached_tokens, o: rows[0].output_tokens }, { m: 'gpt-6-luna', i: 2000, c: 400, o: 500 });
  close(rows[0]!.cost_usd, (1600 * 0.1 + 400 * 0.01 + 500 * 0.5) / 1e6);
  close(rows[1]!.cost_usd, (1600 * 2 + 400 * 0.1 + 500 * 10) / 1e6);
  assert.equal(w.cost_usd, rows[0]!.cost_usd);
});

test('GATEWAY: automatic failover (worker -> haiku, chief -> opus) on HTTP errors, network errors and refusals', async () => {
  const h = await makeHousehold(db);
  // worker: OpenAI 500 -> Anthropic tool_use
  const a = gatewayWith((call) =>
    call.host === 'api.openai.com'
      ? new Response('upstream exploded', { status: 500 })
      : anthropicReply({ ok: true }, { input_tokens: 1000, output_tokens: 300, cache_read_input_tokens: 200 }),
  );
  const w = await a.gateway.worker({ household_id: h.household_id, purpose: 't.fail', system: 'sys', user: 'u', json_schema: SCHEMA });
  assert.equal(w.model, 'claude-haiku-5-5');
  assert.equal(w.failed_over, true);
  assert.equal(JSON.parse(w.text).ok, true);
  assert.equal(a.calls[1]!.host, 'api.anthropic.com');
  assert.equal(a.calls[1]!.body['model'], 'claude-haiku-5-5');
  assert.deepEqual(a.calls[1]!.body['tool_choice'], { type: 'tool', name: 'echo' });
  // chief: network exception -> Opus, plain text
  const b = gatewayWith((call) => {
    if (call.host === 'api.openai.com') throw new TypeError('fetch failed');
    return new Response(JSON.stringify({ stop_reason: 'end_turn', content: [{ type: 'text', text: 'hi' }], usage: { input_tokens: 10, output_tokens: 2 } }), { status: 200 });
  });
  const c = await b.gateway.chief({ household_id: h.household_id, purpose: 't.chief', system: 'sys', user: 'u' });
  assert.deepEqual([c.model, c.text, c.failed_over], ['claude-opus-5-5', 'hi', true]);
  // refusal and truncation also fail over
  const d = gatewayWith((call) =>
    call.host === 'api.openai.com'
      ? new Response(JSON.stringify({ choices: [{ finish_reason: 'stop', message: { content: null, refusal: 'no' } }], usage: {} }), { status: 200 })
      : anthropicReply({ ok: true }),
  );
  assert.equal((await d.gateway.worker({ household_id: h.household_id, purpose: 't.refuse', system: 's', user: 'u', json_schema: SCHEMA })).model, 'claude-haiku-5-5');
  const rows = await llmRows(h.household_id);
  assert.deepEqual(rows.map((r) => r.model), ['claude-haiku-5-5', 'claude-opus-5-5', 'claude-haiku-5-5']);
  // Anthropic usage: total prompt tokens = uncached + cache read; cached tokens are priced at the cache-hit rate
  assert.deepEqual([rows[0]!.input_tokens, rows[0]!.cached_tokens], [1200, 200]);
  close(rows[0]!.cost_usd, (1000 * 0.1 + 200 * 0.01 + 300 * 0.5) / 1e6);
  assert.ok((await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'llm_attempt_failed'`, h.household_id)) >= 3);
});

test('GATEWAY: when every model fails it throws LlmUnavailableError and meters nothing', async () => {
  const h = await makeHousehold(db);
  const { gateway } = gatewayWith(() => new Response('down', { status: 503 }));
  const err = await gateway.worker({ household_id: h.household_id, purpose: 't', system: 's', user: 'u' }).catch((e: unknown) => e);
  assert.ok(err instanceof LlmUnavailableError);
  assert.deepEqual(err.attempts.map((a) => a.model), ['gpt-6-luna', 'claude-haiku-5-5']);
  assert.equal((await llmRows(h.household_id)).length, 0);
  // a missing API key is a failed attempt too, not a crash
  const noKeys = createGateway({ db, apiKeys: {}, fetch: fakeProvider(NEVER).fetch });
  await assert.rejects(() => noKeys.chief({ purpose: 't', system: 's', user: 'u' }), LlmUnavailableError);
});

test('GATEWAY: a worker prompt over 90,000 tokens throws TokenCliffError, sends nothing and logs $0', async () => {
  const h = await makeHousehold(db);
  const { gateway, calls } = gatewayWith(() => openaiReply('x'));
  const huge = 'word '.repeat(50_000); // 250k chars => 100k estimated tokens
  assert.ok(estimateTokens(huge) > WORKER_INPUT_TOKEN_CEILING);
  const err = await gateway.worker({ household_id: h.household_id, purpose: 'big.worker', system: 'sys', user: huge }).catch((e: unknown) => e);
  assert.ok(err instanceof TokenCliffError);
  assert.equal(err.ceiling, 90_000);
  assert.ok(err.estimatedTokens > 90_000);
  assert.equal(calls.length, 0);
  assert.equal((await llmRows(h.household_id)).length, 0);
  const spend = await one<{ usd: number | null }>('SELECT SUM(cost_usd) AS usd FROM llm_usage WHERE household_id = ?', h.household_id);
  assert.equal(spend.usd ?? 0, 0);
  const audit = await all<{ detail_json: string }>(`SELECT detail_json FROM audit_log WHERE household_id = ? AND action = 'llm_token_cliff'`, h.household_id);
  assert.equal(audit.length, 1);
  assert.equal(JSON.parse(audit[0]!.detail_json).purpose, 'big.worker');

  // just under the ceiling is accepted (and the schema counts toward the budget)
  const under = 'word '.repeat(42_000); // 210k chars => 84k estimated tokens + overhead
  assert.ok(estimateTokens(under) < WORKER_INPUT_TOKEN_CEILING - 1000);
  await gateway.worker({ household_id: h.household_id, purpose: 'ok.worker', system: 'sys', user: under });
  assert.equal(calls.length, 1);
  // the ceiling is a worker guard (Luna/Haiku price cliff); the chief route is not capped by it
  await gateway.chief({ household_id: h.household_id, purpose: 'big.chief', system: 'sys', user: huge });
  assert.equal(calls.length, 2);
});

test('GATEWAY: splitToTokenBudget windows are contiguous exact substrings, each within budget', () => {
  const text = Array.from({ length: 4000 }, (_, i) => `line ${i} ${'x'.repeat(i % 70)}`).join('\n');
  const windows = splitToTokenBudget(text, 20_000);
  assert.ok(windows.length > 1);
  let at = 0;
  for (const w of windows) {
    assert.equal(w.start, at);
    assert.ok(estimateTokens(text.slice(w.start, w.end)) <= 20_000);
    at = w.end;
  }
  assert.equal(at, text.length);
  assert.deepEqual(splitToTokenBudget('', 10), []);
  assert.deepEqual(splitToTokenBudget('short', 10_000), [{ start: 0, end: 5 }]);
  const noNewlines = 'y'.repeat(100_000);
  assert.ok(splitToTokenBudget(noNewlines, 10_000).every((w) => estimateTokens(noNewlines.slice(w.start, w.end)) <= 10_000));
});

test('GATEWAY: raw SSNs, card numbers and credentials never leave the Worker (UnredactedInputError, zero calls)', async () => {
  const { gateway, calls } = gatewayWith(() => openaiReply('x'));
  const dirty = [
    'my ssn is 123-45-6789',
    'card 4111 1111 1111 1111 expires soon',
    'ITIN 912-70-1234',
    'password: Xk39!aZq',
    'Account number: 123456789012',
  ];
  for (const text of dirty) {
    await assert.rejects(() => gateway.worker({ purpose: 't', system: 'sys', user: text }), UnredactedInputError, text);
    await assert.rejects(() => gateway.chief({ purpose: 't', system: text, user: 'u' }), UnredactedInputError, text);
  }
  assert.equal(calls.length, 0);
  await gateway.worker({ purpose: 't', system: 'sys', user: 'card [ACCT ****1111] and [SSN] are masked' });
  assert.equal(calls.length, 1);
});

test('PRICING: costs follow the published tables, including the Haiku 100K cliff', () => {
  assert.deepEqual(Object.keys(MODEL_PRICING).sort(), ['claude-haiku-5-5', 'claude-opus-5-5', 'gpt-6-luna', 'gpt-6-sol']);
  close(costUsd('claude-haiku-5-5', { input_tokens: 100_000, cached_tokens: 0, output_tokens: 0 }), 0.01);
  close(costUsd('claude-haiku-5-5', { input_tokens: 100_001, cached_tokens: 0, output_tokens: 0 }), 100_001 * 0.5 / 1e6);
  close(costUsd('claude-opus-5-5', { input_tokens: 1_000_000, cached_tokens: 0, output_tokens: 1_000_000 }), 24);
});

// ═════════════════════════════ encrypted storage ═════════════════════════════

test('VAULT: raw bytes are AES-GCM encrypted in R2, round-trip, and are bound to household and key', async () => {
  const { env } = testEnv(rt);
  const hh = (await makeHousehold(db)).household_id;
  const other = (await makeHousehold(db)).household_id;
  const plain = new TextEncoder().encode('Subject: very secret receipt $12.34 SECRET-MARKER');
  await putEncrypted(env, hh, `raw/${hh}/a.eml.enc`, plain);
  const stored = new Uint8Array(await (await rt.bucket.get(`raw/${hh}/a.eml.enc`))!.arrayBuffer());
  assert.ok(!new TextDecoder('latin1').decode(stored).includes('SECRET-MARKER'));
  assert.equal(stored.length, plain.length + 12 + 16);
  assert.deepEqual(await getDecrypted(env, hh, `raw/${hh}/a.eml.enc`), plain);
  assert.equal(await getDecrypted(env, hh, 'raw/none'), null);

  // an object copied under another key or into another household does not decrypt
  await rt.bucket.put(`raw/${hh}/b.eml.enc`, stored);
  await assert.rejects(() => getDecrypted(env, hh, `raw/${hh}/b.eml.enc`));
  await putEncrypted(env, other, `raw/${other}/z.enc`, plain);
  await rt.bucket.put(`raw/${other}/stolen.enc`, stored);
  await assert.rejects(() => getDecrypted(env, other, `raw/${other}/stolen.enc`));
  // a flipped bit fails authentication
  const tampered = stored.slice();
  tampered[tampered.length - 1] = tampered[tampered.length - 1]! ^ 1;
  await rt.bucket.put(`raw/${hh}/c.eml.enc`, tampered);
  await assert.rejects(() => getDecrypted(env, hh, `raw/${hh}/c.eml.enc`));
  // the DEK is stored wrapped, never in the clear, and a wrong KEK cannot unwrap it
  const dek = new Uint8Array(await (await rt.bucket.get(`keys/${hh}.dek`))!.arrayBuffer());
  assert.equal(dek.length, 12 + 32 + 16);
  await assert.rejects(() => getDecrypted({ ...env, KEK_B64: randomKek() }, hh, `raw/${hh}/a.eml.enc`));
  await assert.rejects(() => getDecrypted({ ...env, KEK_B64: 'AAAA' }, hh, `raw/${hh}/a.eml.enc`), /32 bytes/);
});

test('VAULT: concurrent first writes for a new household converge on a single data key', async () => {
  const { env } = testEnv(rt);
  const hh = (await makeHousehold(db)).household_id;
  const blobs = [1, 2, 3, 4, 5, 6].map((i) => new TextEncoder().encode(`document ${i}`));
  await Promise.all(blobs.map((b, i) => putEncrypted(env, hh, `raw/${hh}/${i}.enc`, b)));
  for (const [i, b] of blobs.entries()) assert.deepEqual(await getDecrypted(env, hh, `raw/${hh}/${i}.enc`), b);
});

// ═════════════════════════════ email router ═════════════════════════════

test('EMAIL: recipient parsing and Authentication-Results trust', () => {
  assert.equal(parseRecipient('H_abcdefgh23@Inbound.Test', 'inbound.test'), 'h_abcdefgh23');
  assert.equal(parseRecipient('h_abcdefgh23@in.lifehub.app', 'inbound.test'), null);
  for (const bad of ['h_short@inbound.test', 'x_abcdefgh23@inbound.test', 'h_abcdefgh2!@inbound.test', 'h_abcdefgh18@inbound.test', 'nope', '@inbound.test']) {
    assert.equal(parseRecipient(bad, 'inbound.test'), null, bad);
  }
  const raw = (headers: string[]) => new TextEncoder().encode(`${headers.join('\r\n')}\r\n\r\nbody`);
  assert.equal(authenticationVerdict(raw(['Authentication-Results: mx; spf=pass smtp.mailfrom=a.com', 'Subject: x'])).pass, true);
  assert.equal(authenticationVerdict(raw(['Authentication-Results: mx; spf=fail; dkim=pass header.d=a.com'])).pass, true);
  assert.equal(authenticationVerdict(raw(['Authentication-Results: mx; spf=fail; dkim=none'])).pass, false);
  assert.equal(authenticationVerdict(raw(['Authentication-Results: mx;\r\n spf=pass'])).pass, true, 'folded header');
  assert.equal(authenticationVerdict(raw(['Subject: x'])).pass, false);
  // only the topmost header counts; a forged one lower down is sender-controlled
  assert.equal(authenticationVerdict(raw(['Authentication-Results: mx; spf=fail; dkim=fail', 'Authentication-Results: forged; spf=pass'])).pass, false);
  // text in the body is not a header
  assert.equal(authenticationVerdict(new TextEncoder().encode('Subject: x\r\n\r\nAuthentication-Results: spf=pass')).pass, false);
});

test('EMAIL: unknown or malformed recipients are rejected with setReject()', async () => {
  const { env, workflows } = testEnv(rt);
  const h = await makeHousehold(db);
  const body = rfc822({ to: 'x', subject: 's', body: 'b', authResults: PASSING_AUTH });
  for (const to of ['h_aaaaaaaaaa@inbound.test', `${h.alias_local}@in.lifehub.app`, 'whoever@inbound.test', 'h_bad@inbound.test']) {
    const msg = inboundMessage(body, { from: 'a@example.com', to });
    const out = await handleEmail(msg, env);
    assert.equal(out.status, 'rejected', to);
    assert.equal(msg.rejected, 'Unknown recipient', to);
  }
  // a soft-deleted household no longer receives mail
  const gone = await makeHousehold(db);
  await db.prepare(`UPDATE households SET deleted_at = '2026-01-01T00:00:00.000Z' WHERE household_id = ?`).bind(gone.household_id).run();
  const msg = inboundMessage(body, { from: 'a@example.com', to: `${gone.alias_local}@inbound.test` });
  assert.equal((await handleEmail(msg, env)).status, 'rejected');
  assert.equal(workflows.email.length, 0);
});

test('EMAIL: 25 MiB cap (declared and actual size), message names the product from env', async () => {
  const { env, workflows } = testEnv(rt);
  const h = await makeHousehold(db);
  const small = rfc822({ to: 'x', subject: 's', body: 'b', authResults: PASSING_AUTH });
  const declared = inboundMessage(small, { from: 'a@example.com', to: `${h.alias_local}@inbound.test`, rawSize: MAX_EMAIL_BYTES + 1 });
  const out = await handleEmail(declared, env);
  assert.equal(out.status, 'rejected');
  assert.match(declared.rejected!, /25 MiB/);
  assert.match(declared.rejected!, /TestApp/);
  assert.doesNotMatch(declared.rejected!, /Keel/);
  const big = new Uint8Array(MAX_EMAIL_BYTES + 10).fill(65);
  const lying = inboundMessage(big, { from: 'a@example.com', to: `${h.alias_local}@inbound.test`, rawSize: 100 });
  assert.equal((await handleEmail(lying, env)).status, 'rejected');
  // exactly at the cap is fine
  const atCap = new Uint8Array(MAX_EMAIL_BYTES).fill(66);
  atCap.set(small, 0);
  const ok = inboundMessage(atCap, { from: h.member_email, to: `${h.alias_local}@inbound.test` });
  assert.equal((await handleEmail(ok, env)).status, 'accepted');
  assert.equal(workflows.email.length, 1);
});

test('EMAIL: accepted only with SPF/DKIM pass or a known member sender; encrypted at rest; duplicates processed once', async () => {
  const { env, workflows } = testEnv(rt);
  const h = await makeHousehold(db);
  const to = `${h.alias_local}@inbound.test`;
  const send = (raw: Uint8Array, from: string) => {
    const msg = inboundMessage(raw, { from, to });
    return handleEmail(msg, env).then((out) => ({ out, msg }));
  };
  const plain = (o: { authResults?: string | null; extraTopHeaders?: string[]; body?: string }) =>
    rfc822({ to, subject: 'Receipt', body: o.body ?? 'Total $12.34 SECRET-MARKER', authResults: o.authResults ?? null, ...(o.extraTopHeaders ? { extraTopHeaders: o.extraTopHeaders } : {}) });

  const noAuth = await send(plain({ body: 'one' }), 'stranger@example.com');
  assert.equal(noAuth.out.status, 'rejected');
  assert.match(noAuth.msg.rejected!, /could not be verified/);
  const forged = await send(plain({ body: 'two', extraTopHeaders: ['Authentication-Results: mx; spf=fail; dkim=none'], authResults: 'forged; spf=pass' }), 'stranger@example.com');
  assert.equal(forged.out.status, 'rejected');
  assert.equal(workflows.email.length, 0);

  const threeBytes = plain({ body: 'three SECRET-MARKER', authResults: PASSING_AUTH });
  const viaSpf = await send(threeBytes, 'shop@example.com');
  assert.equal(viaSpf.out.status, 'accepted');
  const viaMember = await send(plain({ body: 'four' }), h.member_email.toUpperCase());
  assert.equal(viaMember.out.status, 'accepted');
  assert.equal(workflows.email.length, 2);

  const queued = workflows.email[0]!;
  assert.equal(queued.id, (viaSpf.out as { episode_id: string }).episode_id);
  assert.equal(queued.params.household_id, h.household_id);
  assert.equal(queued.params.envelope_from, 'shop@example.com');
  const stored = new Uint8Array(await (await rt.bucket.get(queued.params.r2_key))!.arrayBuffer());
  assert.ok(!new TextDecoder('latin1').decode(stored).includes('SECRET-MARKER'), 'plaintext must not be in R2');
  assert.match(new TextDecoder().decode((await getDecrypted(env, h.household_id, queued.params.r2_key))!), /three SECRET-MARKER/);
  assert.equal(queued.params.sha256, await sha256Hex((await getDecrypted(env, h.household_id, queued.params.r2_key))!));

  // the same message delivered again is accepted silently and not processed twice
  const again = await send(threeBytes, 'shop@example.com');
  assert.equal(again.out.status, 'duplicate');
  assert.equal(again.msg.rejected, null);
  assert.equal(workflows.email.length, 2);
});

// ═════════════════════════════ email pipeline ═════════════════════════════

function extractionHandler(facts: (call: ProviderCall) => unknown[]): ProviderHandler {
  return (call) => openaiReply({ facts: facts(call) });
}

const fact = (o: Record<string, unknown>) => ({
  subject_type: 'card_account', subject_name: 'Amex Platinum', predicate: 'annual_fee', object_type: null, object_name: null,
  value_text: null, value_num: null, value_cents: null, currency: null, valid_from: null, valid_to: null, confidence: 0.9, quote: '', ...o,
});

async function deliver(h: Household, raw: Uint8Array, o: { deleteObject?: boolean } = {}) {
  const { env } = testEnv(rt);
  const sha256 = await sha256Hex(raw);
  const r2_key = `raw/${h.household_id}/${sha256}.eml.enc`;
  await putEncrypted(env, h.household_id, r2_key, raw);
  if (o.deleteObject) await rt.bucket.delete(r2_key);
  return { env, params: { household_id: h.household_id, r2_key, sha256, size_bytes: raw.length, envelope_from: 'shop@example.com' } };
}

async function runEmail(h: Household, raw: Uint8Array, handler: ProviderHandler, o: { counter?: ReturnType<typeof countingDb>; deleteObject?: boolean } = {}) {
  const { env, params } = await deliver(h, raw, o.deleteObject ? { deleteObject: true } : {});
  const store = o.counter?.db ?? db;
  const { gateway, calls } = gatewayWith(handler, { db: store });
  const { step, log } = directStep(o.counter);
  const result: EmailPipelineResult = await runIngestEmail({ env: { ...env, DB: store }, gateway }, params, step);
  return { result, calls, log, params };
}

test('EMAIL PIPELINE: PII is redacted before the model sees anything; facts are grounded in redacted text and written', async () => {
  const h = await makeHousehold(db);
  const raw = rfc822({
    to: `${h.alias_local}@inbound.test`,
    subject: 'Statement for SSN 123-45-6789',
    body: 'Your Amex Platinum card annual fee is $895. Card number 4111 1111 1111 1111. Taxpayer ID number: 912701234.',
  });
  const out = await runEmail(h, raw, extractionHandler(() => [
    fact({ value_cents: 89500, currency: 'USD', quote: 'annual fee is $895' }),
    fact({ predicate: 'card_last4', value_text: '1111', quote: '[ACCT ****1111]' }),
    fact({ predicate: 'annual_fee', value_cents: 1, quote: 'text the model invented' }),
  ]));
  assert.equal(out.result.status, 'done');
  if (out.result.status !== 'done') return;
  assert.equal(out.calls.length, 1);
  const sent = out.calls[0]!.sentText;
  assert.doesNotMatch(sent, /4111|123-45-6789|912701234/);
  assert.match(sent, /\[ACCT \*\*\*\*1111\]/);
  assert.match(sent, /\[SSN\]/);
  assert.match(sent, /TestApp/);
  assert.equal(out.result.redaction_status, 'redacted');
  assert.deepEqual([out.result.writes.written, out.result.dropped.quote_not_found], [2, 1]);
  const ep = await one<{ redacted_text: string; redaction_status: string; source: string }>('SELECT * FROM episodes WHERE household_id = ?', h.household_id);
  assert.doesNotMatch(ep.redacted_text, /4111|123-45-6789|912701234/);
  assert.equal(ep.source, 'email');
  const facts = await all<{ predicate: string; value_cents: number | null; value_text: string | null }>(
    'SELECT predicate, value_cents, value_text FROM facts WHERE household_id = ? ORDER BY predicate', h.household_id);
  assert.deepEqual(facts, [{ predicate: 'annual_fee', value_cents: 89500, value_text: null }, { predicate: 'card_last4', value_cents: null, value_text: '1111' }]);
  const rows = await llmRows(h.household_id);
  assert.deepEqual(rows.map((r) => r.model), ['gpt-6-luna']);
  assert.equal(await count(`SELECT 1 FROM fact_quotes q JOIN facts f ON f.fact_id = q.fact_id WHERE f.household_id = ?`, h.household_id), 2);

  // re-running the whole pipeline (a Workflow retry) is idempotent: same episode, facts reaffirmed, none duplicated
  const again = await runEmail(h, raw, extractionHandler(() => [fact({ value_cents: 89500, currency: 'USD', quote: 'annual fee is $895' })]));
  assert.equal(again.result.status === 'done' && again.result.writes.written, 0);
  assert.equal(again.result.status === 'done' && again.result.writes.reaffirmed, 1);
  assert.equal(await count('SELECT 1 FROM episodes WHERE household_id = ?', h.household_id), 1);
  assert.equal(await count('SELECT 1 FROM facts WHERE household_id = ?', h.household_id), 2);
});

test('EMAIL PIPELINE: a quarantined episode is stored with NULL text, halts, and never reaches the gateway', async () => {
  const h = await makeHousehold(db);
  for (const body of ['Your temporary password: Xk39!aZq', 'Your PIN is 4821.', 'verification code: 482913']) {
    const raw = rfc822({ to: `${h.alias_local}@inbound.test`, subject: 'Account notice', body });
    const out = await runEmail(h, raw, NEVER);
    assert.equal(out.result.status, 'quarantined', body);
    assert.equal(out.calls.length, 0, body);
    assert.deepEqual(out.log.map((s) => s.name), ['ingest']);
  }
  const episodes = await all<{ redacted_text: string | null; redaction_status: string }>('SELECT redacted_text, redaction_status FROM episodes WHERE household_id = ?', h.household_id);
  assert.equal(episodes.length, 3);
  assert.ok(episodes.every((e) => e.redacted_text === null && e.redaction_status === 'quarantined'));
  assert.equal((await llmRows(h.household_id)).length, 0);
  assert.equal(await count('SELECT 1 FROM facts WHERE household_id = ?', h.household_id), 0);
  assert.equal(await count(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'episode_quarantined'`, h.household_id), 3);
  // the audit trail names the reason but never the secret
  const detail = (await all<{ detail_json: string }>(`SELECT detail_json FROM audit_log WHERE household_id = ? AND action = 'episode_quarantined'`, h.household_id)).map((a) => a.detail_json).join();
  assert.doesNotMatch(detail, /Xk39|4821|482913/);
});

test('EMAIL PIPELINE: even a pipeline bug cannot leak — extraction refuses unsafe episodes and the gateway refuses raw PII', async () => {
  const h = await makeHousehold(db);
  const insert = (id: string, status: string, text: string | null) =>
    db.prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, redaction_status) VALUES (?, ?, 'email', ?, ?, ?)`).bind(id, h.household_id, text, randomUUID(), status).run();
  await insert('ep_q', 'quarantined', null);
  await insert('ep_p', 'pending', 'anything');
  await insert('ep_leak', 'redacted', 'my ssn is 123-45-6789 and fee is $895');
  const { gateway, calls } = gatewayWith(NEVER);
  const deps = { db, gateway, appName: 'TestApp' };
  for (const id of ['ep_q', 'ep_p']) {
    await assert.rejects(() => extractFacts(deps, { household_id: h.household_id, episode_id: id, window: { start: 0, end: 10 } }), EpisodeNotSafeError, id);
  }
  await assert.rejects(() => extractFacts(deps, { household_id: h.household_id, episode_id: 'ep_leak', window: { start: 0, end: 50 } }), UnredactedInputError);
  await assert.rejects(() => extractFacts(deps, { household_id: 'other', episode_id: 'ep_q', window: { start: 0, end: 1 } }), EpisodeNotSafeError);
  assert.equal(calls.length, 0);
});

test('EMAIL PIPELINE: missing object, empty body and oversized bodies', async () => {
  const h = await makeHousehold(db);
  const raw = rfc822({ to: `${h.alias_local}@inbound.test`, subject: 'x', body: 'Hello' });
  const missing = await runEmail(h, raw, NEVER, { deleteObject: true });
  assert.equal(missing.result.status, 'missing_object');
  // 400K chars (the per-email cap) is ~160K estimated tokens: three windows, each far below the 90K ceiling, no TokenCliffError
  const big = rfc822({ to: `${h.alias_local}@inbound.test`, subject: 'long newsletter', body: Array.from({ length: 8000 }, (_, i) => `Paragraph ${i} lorem ipsum dolor sit amet consectetur`).join('\n') });
  const out = await runEmail(h, big, extractionHandler(() => []));
  assert.equal(out.result.status, 'done');
  if (out.result.status !== 'done') return;
  assert.ok(out.result.windows >= 2, `windows=${out.result.windows}`);
  assert.equal(out.calls.length, out.result.windows);
  for (const c of out.calls) assert.ok(estimateTokens(c.sentText) < WORKER_INPUT_TOKEN_CEILING);
  assert.equal(out.result.proposed, 0);
  assert.deepEqual(out.log.map((s) => s.name).filter((n) => n.startsWith('write')), []);
});

test('BATCHING: >50 extracted facts split across workflow steps of <=45, each far under the D1 1,000-query limit', async () => {
  const h = await makeHousehold(db);
  // 120 venues spread over a ~240K-character email => two extraction windows of <=100 facts each
  const lines = Array.from({ length: 120 }, (_, i) => `We dined at Venue ${String(i + 1).padStart(3, '0')} ${'.'.repeat(2000)}`);
  const raw = rfc822({ to: `${h.alias_local}@inbound.test`, subject: 'Dining history', body: lines.join('\n') });
  const counter = countingDb(db);
  const out = await runEmail(
    h,
    raw,
    extractionHandler((call) =>
      [...new Set([...call.sentText.matchAll(/Venue (\d{3})/g)].map((m) => m[1]!))].map((n) =>
        fact({ subject_type: 'household', subject_name: 'Smith', predicate: 'dined_at', object_type: 'venue', object_name: `Venue ${n}`, quote: `Venue ${n}` }),
      ),
    ),
    { counter },
  );
  assert.equal(out.result.status, 'done');
  if (out.result.status !== 'done') return;
  assert.equal(out.result.windows, 2);
  assert.equal(out.result.proposed, 120);
  assert.deepEqual(out.result.writes, { written: 120, reaffirmed: 0, rejected: {} });
  const writeSteps = out.log.filter((s) => s.name.startsWith('write-'));
  assert.deepEqual(writeSteps.map((s) => s.name), ['write-0', 'write-1', 'write-2']);
  const sizes = chunk(Array.from({ length: 120 }), MAX_FACTS_PER_STEP).map((c) => c.length);
  assert.deepEqual(sizes, [45, 45, 30]);
  const worst = Math.max(...writeSteps.map((s) => s.queries));
  assert.ok(worst < 1000, `a write step used ${worst} D1 queries`);
  assert.ok(worst <= 45 * 18, `a write step used ${worst} queries; the budget is 45 facts x 18`);
  assert.equal(await count('SELECT 1 FROM facts WHERE household_id = ? AND predicate = ?', h.household_id, 'dined_at'), 120);
  console.log(`      write steps: ${writeSteps.map((s) => `${s.name}=${s.queries}q`).join(', ')}`);
});

test('BATCHING: a step refuses >45 facts, and one episode cannot flood the graph past 200 facts', async () => {
  const h = await makeHousehold(db);
  const ep = `ep_${randomUUID().slice(0, 8)}`;
  await db.prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, redaction_status) VALUES (?, ?, 'email', 'Venue', ?, 'redacted')`).bind(ep, h.household_id, randomUUID()).run();
  const proposals = Array.from({ length: 46 }, (_, i) => ({ household_id: h.household_id, source_episode_id: ep, subject: { type: 'household', name: 'S' } as const, predicate: 'dined_at', object: { type: 'venue', name: `V${i}` } as const, confidence: 0.5, quotes: [{ quote: 'Venue', char_start: 0, char_end: 5 }] }));
  await assert.rejects(() => writeFactsChunk(db, proposals), RangeError);
  assert.equal(await count('SELECT 1 FROM facts WHERE household_id = ?', h.household_id), 0);
  assert.equal(MAX_FACTS_PER_EPISODE, 200);
  // 260 distinct facts from the model are capped at 200 => 5 steps (45x4 + 20)
  const raw = rfc822({ to: `${h.alias_local}@inbound.test`, subject: 'flood', body: Array.from({ length: 260 }, (_, i) => `Venue ${String(i).padStart(3, '0')}`).join('\n') });
  const out = await runEmail(h, raw, extractionHandler((call) => {
    const venues = [...new Set([...call.sentText.matchAll(/Venue (\d{3})/g)].map((m) => m[1]!))];
    return venues.slice(0, 100).map((n) => fact({ subject_type: 'household', subject_name: 'Smith', predicate: 'dined_at', object_type: 'venue', object_name: `Venue ${n}`, quote: `Venue ${n}` }));
  }));
  assert.equal(out.result.status, 'done');
  if (out.result.status === 'done') assert.ok(out.result.proposed <= 200);
});

// ═════════════════════════════ spend parsing (pure) ═════════════════════════════

const AS_OF = new Date('2026-11-26T12:00:00Z');

test('PARSE: CSV tokenizer handles quotes, embedded commas/newlines, BOM, CRLF, and records exact offsets', () => {
  const text = '﻿a,b,c\r\n1,"two, with comma","line\nbreak ""q"""\r\n\r\n4,5,6';
  const recs = parseCsv(text);
  assert.deepEqual(recs.map((r) => r.cells), [['a', 'b', 'c'], ['1', 'two, with comma', 'line\nbreak "q"'], ['4', '5', '6']]);
  assert.equal(text.slice(recs[1]!.start, recs[1]!.end), '1,"two, with comma","line\nbreak ""q"""');
  assert.equal(text.slice(recs[2]!.start, recs[2]!.end), '4,5,6');
  assert.deepEqual(parseCsv(''), []);
});

test('PARSE: amounts, dates, descriptors, categories', () => {
  const cases: Array<[string, number | null]> = [['$1,234.56', 123456], ['-12.30', -1230], ['(12.30)', -1230], ['12.30-', -1230], ['12.30 CR', -1230], ['−12.30', -1230], ['+5', 500], ['7.5', 750], ['abc', null], ['', null], ['1.234', null], ['0.07', 7]];
  for (const [input, want] of cases) assert.equal(toCents(input), want, input);
  const ctx = { asOf: AS_OF };
  assert.equal(parseDate('2026-11-03', ctx), '2026-11-03');
  assert.equal(parseDate('2026-11-03T10:00:00Z', ctx), '2026-11-03');
  assert.equal(parseDate('11/3/2026', ctx), '2026-11-03');
  assert.equal(parseDate('11/03/26', ctx), '2026-11-03');
  assert.equal(parseDate('12/30', ctx), '2025-12-30', 'MM/DD without a year never lands in the future');
  assert.equal(parseDate('12/30', { asOf: AS_OF, year: 2026 }), '2026-12-30');
  assert.equal(parseDate('02/30/2026', ctx), null);
  assert.equal(parseDate('13/01/2026', ctx), null);
  assert.equal(normalizeDescriptor('SQ *Blue Bottle #1234 NYC 10001'), 'BLUE BOTTLE NYC');
  assert.equal(normalizeDescriptor('RESY *CARBONE  New York, NY'), 'RESY CARBONE NEW YORK NY');
  assert.equal(normalizeDescriptor('WALMART+ 8005551234'), 'WALMART+');
  assert.equal(normalizeDescriptor('***'), 'UNKNOWN');
  assert.equal(categoryToEnvelope('Restaurants & Bar'), 'restaurants_and_bar');
  for (const c of ['Transfer', 'Credit Card Payment', 'Paychecks', 'Interest Income', null, '']) assert.equal(categoryToEnvelope(c), null, String(c));
});

test('PARSE: formats and sign conventions (Monarch, Amex-style, Debit/Credit, generic, PDF statement text)', () => {
  const monarch = parseSpendCsv('Date,Merchant,Category,Account,Original Statement,Notes,Amount,Tags,Owner\n2026-11-03,Resy,Restaurants & Bar,Platinum (...1005),RESY *CARBONE NY,,-62.50,,\n2026-11-04,Employer,Paychecks,Checking (...9),ACME PAYROLL,,3000.00,,\n', { asOf: AS_OF });
  assert.deepEqual([monarch.format, monarch.convention], ['monarch', 'negative_is_outflow']);
  assert.deepEqual(monarch.rows.map((r) => [r.amount_cents, r.merchant_hint, r.category_hint, r.descriptor_norm]), [[-6250, 'Resy', 'Restaurants & Bar', 'RESY CARBONE NY'], [300000, 'Employer', 'Paychecks', 'ACME PAYROLL']]);
  const amex = parseSpendCsv('Date,Description,Card Member,Account #,Amount\n11/03/2026,RESY *CARBONE,A B,-12345,62.50\n11/04/2026,PAYMENT RECEIVED,A B,-12345,-500.00\n', { asOf: AS_OF });
  assert.deepEqual([amex.format, amex.convention, amex.rows.map((r) => r.amount_cents)], ['amex', 'positive_is_outflow', [-6250, 50000]]);
  const generic = parseSpendCsv('Transaction Date,Post Date,Description,Category,Type,Amount\n11/03/2026,11/04/2026,COFFEE,Food,Sale,-4.50\n11/05/2026,11/06/2026,BOOKS,Shopping,Sale,-20.00\n', { asOf: AS_OF });
  assert.deepEqual([generic.convention, generic.rows[0]!.posted_on, generic.rows.map((r) => r.amount_cents)], ['negative_is_outflow', '2026-11-04', [-450, -2000]], 'posted date wins over transaction date');
  const mostlyPositive = parseSpendCsv('Date,Description,Amount\n2026-11-03,A,10.00\n2026-11-04,B,20.00\n2026-11-05,C,30.00\n2026-11-06,D,-5.00\n', { asOf: AS_OF });
  assert.equal(mostlyPositive.convention, 'positive_is_outflow');
  const forced = parseSpendCsv('Date,Description,Amount\n2026-11-03,A,10.00\n2026-11-04,B,20.00\n', { asOf: AS_OF, sign_convention: 'negative_is_outflow' });
  assert.deepEqual(forced.rows.map((r) => r.amount_cents), [1000, 2000]);
  const dc = parseSpendCsv('Transaction Date,Posted Date,Card No.,Description,Category,Debit,Credit\n2026-11-03,2026-11-04,1234,UBER *TRIP,Travel,15.00,\n2026-11-05,2026-11-06,1234,PAYMENT,Payment,,200.00\n', { asOf: AS_OF });
  assert.deepEqual([dc.format, dc.rows.map((r) => r.amount_cents), dc.rows[0]!.account_hint], ['debit_credit', [-1500, 20000], '1234']);
  assert.throws(() => parseSpendCsv('foo,bar\n1,2\n', { asOf: AS_OF }), CsvFormatError);
  assert.throws(() => parseSpendCsv('Date,Amount\n2026-01-01,5\n', { asOf: AS_OF }), CsvFormatError);
  const dupes = parseSpendCsv('Date,Description,Amount\n2026-11-03,COFFEE,-4.50\n2026-11-03,COFFEE,-4.50\n2026-11-03,COFFEE,-4.50\n', { asOf: AS_OF });
  assert.deepEqual(dupes.rows.map((r) => r.descriptor_norm), ['COFFEE', 'COFFEE ~2', 'COFFEE ~3']);
  const bad = parseSpendCsv('Date,Description,Amount\nnot a date,X,1\n2026-11-03,,2\n2026-11-03,Y,n/a\n2026-11-03,OK,-1\n', { asOf: AS_OF });
  assert.deepEqual([bad.rows.length, bad.skipped], [1, 3]);

  const pdf = parseStatementText('Account Summary  Previous balance 1,000.00\n11/03 11/04 RESY *CARBONE NEW YORK NY 62.50\n11/05 PAYMENT THANK YOU 500.00CR\nTotal fees 0.00\n11/06 REFUND STORE (12.00)\n', { asOf: AS_OF, statement_year: 2026 });
  assert.deepEqual(pdf.rows.map((r) => [r.posted_on, r.amount_cents, r.descriptor_norm]), [['2026-11-03', -6250, 'RESY CARBONE NEW YORK NY'], ['2026-11-05', 50000, 'PAYMENT THANK YOU'], ['2026-11-06', 1200, 'REFUND STORE']]);
});

test('PARSE: periodsFor and median', () => {
  assert.deepEqual(periodsFor('quarterly', '2026-11-10', '2026-11-26', null), [{ start: '2026-10-01', end: '2026-12-31' }]);
  assert.deepEqual(periodsFor('monthly', '2026-02-20', '2026-03-05', null), [{ start: '2026-02-01', end: '2026-02-28' }, { start: '2026-03-01', end: '2026-03-31' }]);
  assert.deepEqual(periodsFor('semiannual', '2026-06-30', '2026-07-01', null), [{ start: '2026-01-01', end: '2026-06-30' }, { start: '2026-07-01', end: '2026-12-31' }]);
  assert.deepEqual(periodsFor('annual', '2026-05-05', '2026-05-06', null), [{ start: '2026-01-01', end: '2026-12-31' }]);
  assert.deepEqual(periodsFor('cardmember_year', '2026-11-26', '2026-11-26', { month: 3, day: 15 }), [{ start: '2026-03-15', end: '2027-03-14' }]);
  assert.deepEqual(periodsFor('cardmember_year', '2026-11-26', '2026-11-26', null), []);
  assert.deepEqual([median([3, 1, 2]), median([1, 2, 3, 4]), median([5]), median([])], [2, 3, 5, 0]);
});

const tx = (date: string, cents: number, merchant: string | null = 'm1', i = 0): HistoryTxn => ({ txn_id: `t${date}${i}`, posted_on: date, amount_cents: cents, merchant_entity_id: merchant, descriptor_norm: 'X', card_entity_id: null, episode_id: 'e' });

test('RECURRING: >=2 occurrences, +/-10% amount (chained), +/-5-day cadence', () => {
  const monthly = detectRecurring([tx('2026-01-15', -1599), tx('2026-02-14', -1599), tx('2026-03-17', -1599)]);
  assert.deepEqual(monthly.map((s) => [s.cadence, s.amount_cents, s.valid_from, s.occurrences]), [['monthly', 1599, '2026-01-15', 3]]);
  assert.equal(detectRecurring([tx('2026-01-15', -1599)]).length, 0, 'one charge is not a series');
  assert.equal(detectRecurring([tx('2026-01-15', -1599), tx('2026-02-14', -1599)]).length, 1, 'two charges are');
  assert.equal(detectRecurring([tx('2026-01-15', -1599), tx('2026-02-14', 1599)]).length, 0, 'refunds are not charges');
  // +/-5 days around 30 is accepted, 6 is not
  assert.equal(detectRecurring([tx('2026-01-01', -1000), tx('2026-02-05', -1000)]).length, 1); // 35 days
  assert.equal(detectRecurring([tx('2026-01-01', -1000), tx('2026-02-07', -1000)]).length, 0); // 37 days
  assert.equal(detectRecurring([tx('2026-01-01', -1000), tx('2026-01-26', -1000)]).length, 1); // 25 days
  assert.equal(detectRecurring([tx('2026-01-01', -1000), tx('2026-01-25', -1000)]).length, 0); // 24 days
  // amounts: within 10% links, beyond does not
  assert.equal(detectRecurring([tx('2026-01-15', -1000), tx('2026-02-14', -1100)]).length, 1);
  assert.equal(detectRecurring([tx('2026-01-15', -1000), tx('2026-02-14', -1101)]).length, 0);
  // a price step inside the 10% band keeps one series; valid_from is the first charge at the CURRENT amount
  const stepped = detectRecurring([tx('2026-01-15', -1599), tx('2026-02-14', -1599), tx('2026-03-16', -1699), tx('2026-04-15', -1699)]);
  assert.deepEqual(stepped.map((s) => [s.amount_cents, s.valid_from, s.occurrences]), [[1699, '2026-03-16', 4]]);
  // other cadences and tolerances
  assert.equal(detectRecurring([tx('2026-01-01', -500), tx('2026-01-08', -500)])[0]?.cadence, 'weekly');
  assert.equal(detectRecurring([tx('2026-01-01', -500), tx('2026-01-11', -500)]).length, 0, 'weekly tolerance is tight (1 day)');
  assert.equal(detectRecurring([tx('2026-01-01', -500), tx('2026-01-15', -500)])[0]?.cadence, 'biweekly');
  assert.equal(detectRecurring([tx('2026-01-01', -9900), tx('2026-04-02', -9900)])[0]?.cadence, 'quarterly');
  assert.equal(detectRecurring([tx('2025-01-10', -12000), tx('2026-01-10', -12000)])[0]?.cadence, 'annual');
  assert.deepEqual(CADENCES.map((c) => cadenceTolerance(c.days)), [1, 2, 5, 5, 5]);
  // separate merchants stay separate; the latest run wins after a break
  const two = detectRecurring([tx('2026-01-15', -1000, 'a'), tx('2026-02-14', -1000, 'a'), tx('2026-01-20', -5000, 'b'), tx('2026-02-19', -5000, 'b')]);
  assert.deepEqual(two.map((s) => s.key), ['a', 'b']);
  const resumed = detectRecurring([tx('2026-01-15', -1000), tx('2026-02-14', -1000), tx('2026-05-01', -1000), tx('2026-05-31', -1000)]);
  assert.deepEqual(resumed.map((s) => [s.valid_from, s.occurrences]), [['2026-05-01', 2]]);
  // unresolved merchants group by descriptor, ignoring the ' ~2' repeat suffix
  const byDescriptor = detectRecurring([{ ...tx('2026-01-15', -1000, null), descriptor_norm: 'GYM' }, { ...tx('2026-02-14', -1000, null, 1), descriptor_norm: 'GYM ~2' }]);
  assert.equal(byDescriptor.length, 1);
});

// ═════════════════════════════ spend pipeline (real D1) ═════════════════════════════

const NOW = new Date();
/** Date in the month `monthOffset` from now (UTC), clamped to a day that has already happened for the current month. */
const on = (monthOffset: number, day: number): string => new Date(Date.UTC(NOW.getUTCFullYear(), NOW.getUTCMonth() + monthOffset, day)).toISOString().slice(0, 10);

const q = (s: string): string => `"${s.replace(/"/g, '""')}"`;
const MONARCH_HEADER = 'Date,Merchant,Category,Account,Original Statement,Notes,Amount,Tags,Owner';
const mrow = (date: string, merchant: string, category: string, account: string, orig: string, amount: string): string => `${date},${q(merchant)},${q(category)},${q(account)},${q(orig)},,${amount},,`;
const AMEX_ACCOUNT = 'Platinum Card® (...1005)';

async function amexHousehold(): Promise<{ h: Household; cardId: string }> {
  const h = await makeHousehold(db);
  const text = 'Amex Platinum card ending 1005 opened for the household';
  const ep = `ep_${randomUUID().slice(0, 8)}`;
  await db.prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, redaction_status) VALUES (?, ?, 'user_input', ?, ?, 'clean')`).bind(ep, h.household_id, text, randomUUID()).run();
  const base = { household_id: h.household_id, source_episode_id: ep, subject: { type: 'card_account', name: 'Amex Platinum' } as const, confidence: 1 };
  for (const [predicate, value, quote] of [['card_product', 'amex_platinum', 'Amex Platinum'], ['card_last4', '1005', 'ending 1005']] as const) {
    const r = await writeFact(db, { ...base, predicate, value_text: value, quotes: [{ quote, char_start: text.indexOf(quote), char_end: text.indexOf(quote) + quote.length }] });
    assert.equal(r.status, 'written');
  }
  const card = await one<{ entity_id: string }>(`SELECT entity_id FROM entities WHERE household_id = ? AND type = 'card_account'`, h.household_id);
  return { h, cardId: card.entity_id };
}

async function storeFile(h: Household, text: string, ext = 'csv') {
  const { env } = testEnv(rt);
  const bytes = new TextEncoder().encode(text);
  const sha256 = await sha256Hex(bytes);
  const r2_key = `raw/${h.household_id}/${sha256}.${ext}.enc`;
  await putEncrypted(env, h.household_id, r2_key, bytes);
  return { env, sha256, r2_key };
}

async function runFile(h: Household, text: string, handler: ProviderHandler, o: { kind?: 'csv' | 'pdf_text'; card?: string | null; counter?: ReturnType<typeof countingDb>; sign?: 'auto' | 'negative_is_outflow' | 'positive_is_outflow'; year?: number } = {}) {
  const stored = await storeFile(h, text, o.kind === 'pdf_text' ? 'txt' : 'csv');
  const store = o.counter?.db ?? db;
  const { gateway, calls } = gatewayWith(handler, { db: store });
  const { step, log } = directStep(o.counter);
  const result: FilePipelineResult = await runIngestFile(
    { env: { ...stored.env, DB: store }, gateway },
    {
      household_id: h.household_id, r2_key: stored.r2_key, file_name: o.kind === 'pdf_text' ? 'statement.txt' : 'export.csv', kind: o.kind ?? 'csv', sha256: stored.sha256,
      default_card_entity_id: o.card ?? null, sign_convention: o.sign ?? 'auto', ...(o.year ? { statement_year: o.year } : {}),
    },
    step,
  );
  return { result, calls, log };
}

function monarchFile(): { text: string; rows: number } {
  const rows = [
    mrow(on(-3, 10), 'Carbone', 'Restaurants & Bar', 'Checking (...9999)', 'CARBONE NEW YORK', '-200.00'),
    mrow(on(-2, 10), 'Carbone', 'Restaurants & Bar', 'Checking (...9999)', 'CARBONE NEW YORK', '-300.00'),
    mrow(on(-1, 10), 'Carbone', 'Restaurants & Bar', 'Checking (...9999)', 'CARBONE NEW YORK', '-250.00'),
    mrow(on(0, 1), 'Resy', 'Restaurants & Bar', AMEX_ACCOUNT, 'RESY *CARBONE NEW YORK NY', '-62.50'),
    mrow(on(0, 1), 'Uber', 'Taxi & Ride Shares', AMEX_ACCOUNT, 'UBER *TRIP HELP.UBER.COM', '-15.00'),
    ...[-3, -2, -1, 0].map((m) => mrow(on(m, 1), 'Netflix', 'Subscriptions', AMEX_ACCOUNT, 'NETFLIX.COM 866-579-7172', '-15.99')),
    mrow(on(-1, 12), 'Blue Bottle', 'Coffee Shops', 'Checking (...9999)', 'SQ *BLUE BOTTLE', '-4.50'),
    mrow(on(-1, 12), 'Blue Bottle', 'Coffee Shops', 'Checking (...9999)', 'SQ *BLUE BOTTLE', '-4.50'),
    mrow(on(-1, 15), 'Acme Payroll', 'Paychecks', 'Checking (...9999)', 'ACME PAYROLL', '3000.00'),
    mrow(on(-1, 16), 'Transfer', 'Transfer', 'Checking (...9999)', 'TRANSFER TO SAVINGS', '-500.00'),
    mrow(on(-2, 20), 'Smith, Jones & Co', 'Shopping', 'Checking (...9999)', 'SMITH JONES AND CO', '-80.00'),
  ];
  return { text: [MONARCH_HEADER, ...rows].join('\n') + '\n', rows: rows.length };
}

const snapshot = async (h: Household) => ({
  txns: await all<Record<string, unknown>>('SELECT * FROM transactions WHERE household_id = ? ORDER BY txn_id', h.household_id),
  periods: await all<Record<string, unknown>>('SELECT * FROM credit_periods WHERE household_id = ? ORDER BY benefit_key, period_start', h.household_id),
  envelopes: await all<Record<string, unknown>>('SELECT * FROM envelopes WHERE household_id = ? ORDER BY envelope_key', h.household_id),
  facts: await all<Record<string, unknown>>('SELECT fact_id, predicate, value_cents, value_text, valid_from, valid_to, expired_at FROM facts WHERE household_id = ? ORDER BY fact_id', h.household_id),
  episodes: await all<Record<string, unknown>>('SELECT episode_id, sha256 FROM episodes WHERE household_id = ? ORDER BY episode_id', h.household_id),
});

test('SPEND: a Monarch export is ingested deterministically (no LLM), budgets and credit periods follow', async () => {
  const { h, cardId } = await amexHousehold();
  const file = monarchFile();
  const run = await runFile(h, file.text, NEVER);
  assert.equal(run.result.status, 'done');
  if (run.result.status !== 'done') return;
  assert.equal(run.calls.length, 0, 'Monarch names its merchants; no model call is needed');
  assert.deepEqual([run.result.format, run.result.convention, run.result.rows, run.result.inserted, run.result.duplicates], ['monarch', 'negative_is_outflow', file.rows, file.rows, 0]);

  // transactions: exact amounts, both identical coffees kept, quoted commas intact, card matched by account last4
  const txns = await all<{ posted_on: string; amount_cents: number; descriptor_norm: string; card_entity_id: string | null; envelope_key: string | null; merchant_entity_id: string | null }>(
    'SELECT * FROM transactions WHERE household_id = ?', h.household_id);
  assert.equal(txns.length, file.rows);
  assert.deepEqual(txns.filter((t) => t.descriptor_norm.startsWith('BLUE BOTTLE')).map((t) => t.descriptor_norm).sort(), ['BLUE BOTTLE', 'BLUE BOTTLE ~2']);
  assert.equal(txns.filter((t) => t.card_entity_id === cardId).length, 6, 'Resy, Uber and 4x Netflix are on the Amex; checking rows are not');
  assert.ok(txns.filter((t) => t.card_entity_id === null).every((t) => t.amount_cents !== 0));
  assert.ok(txns.every((t) => t.merchant_entity_id !== null));
  assert.equal(txns.find((t) => t.descriptor_norm === 'SMITH JONES AND CO')!.amount_cents, -8000);
  assert.deepEqual(txns.filter((t) => t.envelope_key === null).map((t) => t.descriptor_norm).sort(), ['ACME PAYROLL', 'TRANSFER TO SAVINGS']);

  // envelopes: trailing-3-month median (200, 300, 250 => 250.00) and live headroom for the current month
  assert.deepEqual(await all('SELECT envelope_key, monthly_target_cents FROM envelopes WHERE household_id = ? ORDER BY envelope_key', h.household_id), [
    { envelope_key: 'restaurants_and_bar', monthly_target_cents: 25000 },
    { envelope_key: 'subscriptions', monthly_target_cents: 1599 },
  ]);
  const headroom = await one<{ headroom_cents: number }>(`SELECT headroom_cents FROM envelope_headroom WHERE household_id = ? AND envelope_key = 'restaurants_and_bar'`, h.household_id);
  assert.equal(headroom.headroom_cents, 25000 - 6250, 'the $62.50 Resy charge this month reduces headroom');

  // credit periods: the $62.50 Resy charge counts against the $100 quarterly credit; Uber Cash is fully used
  const periods = await all<{ benefit_key: string; period_start: string; used_cents: number; status: string }>(
    `SELECT benefit_key, period_start, used_cents, status FROM credit_periods WHERE household_id = ? AND card_entity_id = ?`, h.household_id, cardId);
  const resy = periods.filter((p) => p.benefit_key === 'amex_platinum.resy_credit' && p.status !== 'expired_unused');
  assert.equal(resy.length, 1);
  assert.deepEqual([resy[0]!.used_cents, resy[0]!.status], [6250, 'partially_used']);
  const uber = periods.find((p) => p.benefit_key === 'amex_platinum.uber_cash' && p.period_start === on(0, 1))!;
  assert.deepEqual([uber.used_cents, uber.status], [1500, 'used']);
  const digital = periods.filter((p) => p.benefit_key === 'amex_platinum.digital_entertainment_credit');
  assert.ok(digital.some((p) => p.status === 'open' && p.period_start === on(0, 1)), 'current month is open');
  assert.ok(digital.every((p) => p.used_cents === 0));
  assert.ok(digital.every((p) => p.period_start >= on(-3, 1)), 'nothing is reported for months before the first Amex transaction');
  assert.ok(digital.some((p) => p.status === 'expired_unused' && p.period_start === on(-2, 1)), 'a finished month with full data and no spend is expired_unused');
  assert.ok(periods.every((p) => !p.benefit_key.startsWith('chase_') && !p.benefit_key.startsWith('capital_one_')));

  // recurring: Netflix is a monthly $15.99 series on the Amex, written through writeFact with a grounded quote
  const recurring = await all<{ predicate: string; value_cents: number | null; value_text: string | null; valid_from: string; object_id: string | null }>(
    `SELECT f.predicate, f.value_cents, f.value_text, f.valid_from, f.object_id FROM facts_current f JOIN entities e ON e.entity_id = f.subject_id
      WHERE f.household_id = ? AND e.type = 'recurring_charge' AND e.canonical_name = 'Netflix' ORDER BY f.predicate`, h.household_id);
  assert.deepEqual(recurring.map((r) => [r.predicate, r.value_cents, r.value_text]), [['charged_to_card', null, null], ['recurring_amount', 1599, null], ['recurring_cadence', null, 'monthly'], ['recurring_merchant', null, null]]);
  assert.equal(recurring.find((r) => r.predicate === 'charged_to_card')!.object_id, cardId);
  assert.equal(recurring[0]!.valid_from.slice(0, 10), on(-3, 1));
  const quote = await one<{ quote: string }>(`SELECT q.quote FROM fact_quotes q JOIN facts f ON f.fact_id = q.fact_id WHERE f.household_id = ? AND f.predicate = 'recurring_amount'`, h.household_id);
  assert.match(quote.quote, /NETFLIX\.COM/);
  assert.equal(await count(`SELECT 1 FROM facts f JOIN entities e ON e.entity_id = f.subject_id WHERE f.household_id = ? AND e.type = 'recurring_charge' AND e.canonical_name <> 'Netflix'`, h.household_id), 0, 'one-off merchants are not recurring');
});

test('SPEND: re-importing the exact same Monarch CSV is 100% idempotent', async () => {
  const { h } = await amexHousehold();
  const file = monarchFile();
  const first = await runFile(h, file.text, NEVER);
  const before = await snapshot(h);
  assert.ok(before.txns.length === file.rows && before.periods.length > 0 && before.envelopes.length > 0);
  const second = await runFile(h, file.text, NEVER);
  assert.equal(second.result.status === 'done' && second.result.inserted, 0);
  assert.equal(second.result.status === 'done' && second.result.duplicates, file.rows);
  assert.deepEqual(await snapshot(h), before, 'transactions, credit periods, envelopes, facts and episodes are byte-for-byte unchanged');
  assert.ok(first.result.status === 'done' && second.result.status === 'done');
  const writes = second.result.status === 'done' ? second.result.recurring_writes : null;
  assert.deepEqual([writes?.written, writes?.reaffirmed], [0, 4], 'recurring facts are reaffirmed, not duplicated');
  // an overlapping export (all old rows + one new charge) adds exactly one transaction
  const overlap = file.text + mrow(on(0, 1), 'Resy', 'Restaurants & Bar', AMEX_ACCOUNT, 'RESY *CARBONE NEW YORK NY', '-30.00') + '\n';
  const third = await runFile(h, overlap, NEVER);
  assert.deepEqual(third.result.status === 'done' && [third.result.inserted, third.result.duplicates], [1, file.rows]);
  // ...and used_cents is recomputed from stored spend, not incremented: 62.50 + 30.00 => 92.50, then idempotent again
  const used = async () => (await all<{ used_cents: number; status: string }>(`SELECT used_cents, status FROM credit_periods WHERE household_id = ? AND benefit_key = 'amex_platinum.resy_credit' AND status <> 'expired_unused'`, h.household_id));
  assert.deepEqual(await used(), [{ used_cents: 9250, status: 'partially_used' }]);
  await runFile(h, overlap, NEVER);
  assert.deepEqual(await used(), [{ used_cents: 9250, status: 'partially_used' }]);
  // pushing it past the cap marks the credit used, capped at the benefit amount
  await runFile(h, overlap + mrow(on(0, 1), 'Resy', 'Restaurants & Bar', AMEX_ACCOUNT, 'RESY *OTHER PLACE NEW YORK NY', '-80.00') + '\n', NEVER);
  assert.deepEqual(await used(), [{ used_cents: 10000, status: 'used' }]);
});

test('SPEND: a Resy charge on an Amex Platinum increments credit_periods.used_cents and lowers envelope_headroom', async () => {
  const { h, cardId } = await amexHousehold();
  const history = [-3, -2, -1].map((m, i) => mrow(on(m, 5), 'Dinner Spot', 'Restaurants & Bar', 'Checking (...9999)', 'DINNER SPOT', `-${[400, 500, 600][i]}.00`));
  const before = await runFile(h, [MONARCH_HEADER, ...history].join('\n') + '\n', NEVER);
  assert.equal(before.result.status, 'done');
  const headroom = async () => (await one<{ headroom_cents: number }>(`SELECT headroom_cents FROM envelope_headroom WHERE household_id = ? AND envelope_key = 'restaurants_and_bar'`, h.household_id)).headroom_cents;
  assert.equal(await headroom(), 50000, 'median(400, 500, 600) = 500.00 and nothing spent yet this month');
  assert.equal(await count('SELECT 1 FROM credit_periods WHERE household_id = ?', h.household_id), 0, 'no Amex spend yet, so no period is reported');
  const resy = mrow(on(0, 1), 'Resy', 'Restaurants & Bar', AMEX_ACCOUNT, 'RESY *L ARTUSI NEW YORK NY', '-112.40');
  await runFile(h, [MONARCH_HEADER, ...history, resy].join('\n') + '\n', NEVER);
  const period = await one<{ used_cents: number; status: string; period_start: string; period_end: string }>(
    `SELECT used_cents, status, period_start, period_end FROM credit_periods WHERE household_id = ? AND card_entity_id = ? AND benefit_key = 'amex_platinum.resy_credit' AND period_start <= ? AND period_end >= ?`,
    h.household_id, cardId, on(0, 1), on(0, 1));
  assert.equal(period.used_cents, 10000, 'capped at the $100 quarterly credit');
  assert.equal(period.status, 'used');
  assert.ok(period.period_start <= on(0, 1) && period.period_end >= on(0, 1));
  assert.equal(await headroom(), 50000 - 11240, 'the full $112.40 reduces monthly dining headroom');
});

test('SPEND: bank CSV — unknown merchants go to the worker model in batches, known ones never do, PII never reaches it', async () => {
  const { h, cardId } = await amexHousehold();
  const lines = Array.from({ length: 320 }, (_, i) => `${on(-1, 1 + (i % 25))},STORE${String(i).padStart(3, '0')} ANYTOWN ST #${100 + i},-${(5 + (i % 40)).toFixed(2)}`);
  lines.push(`${on(-1, 3)},WIRE REF ACCT 4111111111111111 SSN 123-45-6789,-9.99`);
  const csv = ['Transaction Date,Description,Amount', ...lines].join('\n') + '\n';
  const handler: ProviderHandler = (call) => {
    const items = [...call.sentText.matchAll(/^\d+\. (.+)$/gm)].map((m) => m[1]!);
    return openaiReply({ items: items.map((d) => ({ descriptor: d, merchant_name: d.split(' ')[0]!.replace('STORE', 'Store '), envelope_key: 'shopping' })).concat([{ descriptor: 'NOT SENT', merchant_name: 'Ghost', envelope_key: 'dining' }]) });
  };
  const counter = countingDb(db);
  const first = await runFile(h, csv, handler, { card: cardId, counter });
  assert.equal(first.result.status, 'done');
  if (first.result.status !== 'done') return;
  assert.equal(first.result.rows, 321);
  assert.equal(first.calls.length, 3, '321 unknown descriptors => ceil(321 / 150) = 3 calls');
  for (const c of first.calls) assert.doesNotMatch(c.sentText, /4111|123-45-6789/);
  assert.equal(await count(`SELECT 1 FROM entities WHERE household_id = ? AND canonical_name = 'Ghost'`, h.household_id), 0, 'descriptors the model invented are discarded');
  const stored = await all<{ descriptor_norm: string; card_entity_id: string | null; envelope_key: string | null; merchant_entity_id: string | null }>('SELECT * FROM transactions WHERE household_id = ?', h.household_id);
  assert.equal(stored.length, 321);
  assert.ok(stored.every((t) => t.card_entity_id === cardId), 'no account column => the default card');
  assert.ok(stored.every((t) => t.envelope_key === 'shopping' && t.merchant_entity_id !== null));
  const wire = stored.find((t) => t.descriptor_norm.startsWith('WIRE REF'))!;
  assert.equal(wire.descriptor_norm, 'WIRE REF ACCT ACCT SSN SSN', 'the card number and SSN were masked before anything was stored');
  assert.ok(!stored.some((t) => /4111/.test(t.descriptor_norm)));
  assert.match((await one<{ redacted_text: string }>(`SELECT redacted_text FROM episodes WHERE household_id = ? AND source = 'csv' LIMIT 1`, h.household_id)).redacted_text, /\[ACCT \*\*\*\*1111\].*\[SSN\]/);
  // worst step stays far below the D1 query ceiling
  const worst = Math.max(...first.log.map((s) => s.queries));
  assert.ok(worst < 1000, `a step used ${worst} queries`);
  console.log(`      steps: ${first.log.map((s) => `${s.name}=${s.queries}q`).join(', ')}`);
  // the same file again: every merchant is now known => zero model calls, zero new rows
  const second = await runFile(h, csv, NEVER, { card: cardId });
  assert.equal(second.calls.length, 0);
  assert.equal(second.result.status === 'done' && second.result.inserted, 0);
});

test('SPEND: 500 distinct new merchants in one step stay well under the D1 query limit', async () => {
  const { h } = await amexHousehold();
  const rows = Array.from({ length: 500 }, (_, i) => mrow(on(-1, 1 + (i % 25)), `Merchant ${i}`, 'Shopping', 'Checking (...9999)', `MERCHANT ${i} LLC`, `-${(1 + i / 100).toFixed(2)}`));
  const counter = countingDb(db);
  const run = await runFile(h, [MONARCH_HEADER, ...rows].join('\n') + '\n', NEVER, { counter });
  assert.equal(run.result.status === 'done' && run.result.inserted, 500);
  const worst = Math.max(...run.log.map((s) => s.queries));
  assert.ok(worst < 400, `worst step used ${worst} queries`);
  console.log(`      500 new merchants: worst step ${worst} queries (${run.log.filter((s) => s.queries > 50).map((s) => `${s.name}=${s.queries}`).join(', ')})`);
  assert.equal(await count(`SELECT 1 FROM entities WHERE household_id = ? AND type = 'merchant'`, h.household_id), 500);
});

test('SPEND: Amex-style CSV (positive = charge) and PDF statement text', async () => {
  const { h, cardId } = await amexHousehold();
  const csv = `Date,Description,Card Member,Account #,Amount\n${on(0, 1)},RESY *CARBONE NEW YORK NY,A SMITH,-11005,40.00\n${on(0, 1)},PAYMENT RECEIVED - THANK YOU,A SMITH,-11005,-500.00\n`;
  const handler: ProviderHandler = (call) => openaiReply({ items: [...call.sentText.matchAll(/^\d+\. (.+)$/gm)].map((m) => ({ descriptor: m[1]!, merchant_name: m[1]!.startsWith('RESY') ? 'Resy' : 'Card payment', envelope_key: 'dining' })) });
  const a = await runFile(h, csv, handler, { card: cardId });
  assert.equal(a.result.status === 'done' && a.result.convention, 'positive_is_outflow');
  assert.deepEqual((await all<{ amount_cents: number }>('SELECT amount_cents FROM transactions WHERE household_id = ? ORDER BY amount_cents', h.household_id)).map((r) => r.amount_cents), [-4000, 50000]);
  assert.deepEqual(await all(`SELECT used_cents, status FROM credit_periods WHERE household_id = ? AND benefit_key = 'amex_platinum.resy_credit' AND status <> 'expired_unused'`, h.household_id), [{ used_cents: 4000, status: 'partially_used' }]);

  const h2 = (await amexHousehold());
  const statement = `Account Summary\nPrevious balance 1,000.00\n${on(0, 1).slice(5, 7)}/01 RESY *CARBONE NEW YORK NY 62.50\n${on(0, 1).slice(5, 7)}/02 PAYMENT THANK YOU 500.00CR\n`;
  const b = await runFile(h2.h, statement, handler, { kind: 'pdf_text', card: h2.cardId, year: Number(on(0, 1).slice(0, 4)) });
  assert.equal(b.result.status === 'done' && b.result.format, 'statement_text');
  assert.deepEqual((await all<{ amount_cents: number }>('SELECT amount_cents FROM transactions WHERE household_id = ? ORDER BY amount_cents', h2.h.household_id)).map((r) => r.amount_cents), [-6250, 50000]);
  assert.equal((await one<{ source: string }>(`SELECT source FROM episodes WHERE household_id = ? AND source <> 'user_input'`, h2.h.household_id)).source, 'pdf');
});

test('SPEND: credit periods are only reported unused where the card has data for the whole period', async () => {
  const { h } = await amexHousehold();
  // the card's first transaction is on the 20th of two months ago: that month is only partly covered
  const csv = [MONARCH_HEADER, mrow(on(-2, 20), 'Uber', 'Taxi & Ride Shares', AMEX_ACCOUNT, 'UBER *TRIP', '-15.00'), mrow(on(0, 1), 'Uber', 'Taxi & Ride Shares', AMEX_ACCOUNT, 'UBER *TRIP', '-15.00')].join('\n') + '\n';
  await runFile(h, csv, NEVER);
  const rows = await all<{ benefit_key: string; period_start: string; status: string; used_cents: number }>(
    `SELECT benefit_key, period_start, status, used_cents FROM credit_periods WHERE household_id = ?`, h.household_id);
  const digital = rows.filter((r) => r.benefit_key === 'amex_platinum.digital_entertainment_credit').map((r) => r.period_start).sort();
  assert.deepEqual(digital, [on(-1, 1), on(0, 1)], 'the partly covered month and everything before it are not reported');
  // spend is still counted where it is seen, even in a partly covered month (it can only understate)
  assert.ok(rows.some((r) => r.benefit_key === 'amex_platinum.uber_cash' && r.period_start === on(-2, 1) && r.status === 'used'));
  assert.ok(rows.some((r) => r.benefit_key === 'amex_platinum.uber_cash' && r.period_start === on(-1, 1) && r.status === 'expired_unused' && r.used_cents === 0));
});

test('SPEND: a file containing a credential is quarantined whole — NULL text, no transactions, no model call', async () => {
  const { h } = await amexHousehold();
  const csv = `${MONARCH_HEADER}\n${on(0, 1)},"Shop","Shopping","Checking (...9)","SHOP","password: Xk39!aZq",-5.00,,\n`;
  const run = await runFile(h, csv, NEVER);
  assert.equal(run.result.status, 'quarantined');
  assert.equal(run.calls.length, 0);
  assert.deepEqual(run.log.map((s) => s.name), ['clock', 'prepare']);
  assert.deepEqual(await all('SELECT redacted_text, redaction_status, source FROM episodes WHERE household_id = ? AND source = ?', h.household_id, 'csv'), [{ redacted_text: null, redaction_status: 'quarantined', source: 'csv' }]);
  assert.equal(await count('SELECT 1 FROM transactions WHERE household_id = ?', h.household_id), 0);
  assert.equal((await llmRows(h.household_id)).length, 0);
});

test('SPEND: unreadable files fail soft (missing object, empty file) and never write rows', async () => {
  const { h } = await amexHousehold();
  const { env } = testEnv(rt);
  const { gateway } = gatewayWith(NEVER);
  const { step } = directStep();
  const missing = await runIngestFile({ env, gateway }, { household_id: h.household_id, r2_key: 'raw/nope', file_name: 'x.csv', kind: 'csv', sha256: 'f'.repeat(64) }, step);
  assert.deepEqual(missing, { status: 'missing_object' });
  const empty = await runFile(h, `${MONARCH_HEADER}\nbad,row\n`, NEVER);
  assert.equal(empty.result.status, 'empty');
  await assert.rejects(() => runFile(h, 'this,is,not,a,statement\n1,2,3,4,5\n', NEVER), CsvFormatError);
  assert.equal(await count('SELECT 1 FROM transactions WHERE household_id = ?', h.household_id), 0);
});

test('SPEND: other households are untouched — every table is keyed by household_id', async () => {
  const a = await amexHousehold();
  const b = await amexHousehold();
  await runFile(a.h, monarchFile().text, NEVER);
  const bSnapshot = await snapshot(b.h);
  assert.deepEqual([bSnapshot.txns.length, bSnapshot.periods.length, bSnapshot.envelopes.length], [0, 0, 0]);
  await runFile(b.h, monarchFile().text, NEVER);
  const aTxns = await count('SELECT 1 FROM transactions WHERE household_id = ?', a.h.household_id);
  const bTxns = await count('SELECT 1 FROM transactions WHERE household_id = ?', b.h.household_id);
  assert.equal(aTxns, bTxns, 'identical files in two households each store their own rows (txn ids and episode ids are household-scoped)');
});
