/**
 * Milestone 4 behavioural suite: Google OAuth sign-in and the sealed session cookie, the middleware that protects /api, the
 * MissionRunWF state machine (compile -> debit -> R0 read/fill -> G1-G9 -> R1 stage -> R2 approval -> R3 deep link), the gates
 * one by one, credits, Web Push, and the member-facing mission endpoints.
 * Real local D1, R2 and KV; only the network (Google, model vendors, push services) and the Workflows engine are faked.
 */

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { getAccessToken, findCalendarConnector, type CalendarDeps } from '../src/connectors/googleCalendar';
import { buildCopy } from '../src/missions/copy';
import { ISSUER_DOMAINS, MERCHANT_DOMAINS, buildDeepLink, parseAffiliateTags, resolveDomain } from '../src/missions/deeplinks';
import { CAPACITY_STATES, activeStagedCount, stageUnlockScan } from '../src/missions/engine';
import { runGates, type GateInput } from '../src/missions/gates';
import { getHome, type HomeFeed } from '../src/missions/home';
import { creditBalance, debitCredit, refundCredit } from '../src/missions/ledger';
import { getRun } from '../src/missions/runState';
import { canonicalJson, missionDepsFromEnv, runMission, type ApprovalEvent } from '../src/missions/runPipeline';
import { createGuardedFetch } from '../src/net/egress';
import { isAllowedPushEndpoint, notifyHousehold, parseSubscription, vapidAuthorization } from '../src/notify/webPush';
import type { Env } from '../src/types';
import { handleApi } from '../src/workers/api';
import { SESSION_COOKIE, clearCookieHeader, issueSessionToken, openCookie, readCookie, sealCookie, setCookieHeader, timingSafeEqual } from '../src/workers/session';
import { openTestRuntime, type TestRuntime } from './helpers';
import { TIMEOUT, directStep, makeHousehold, openaiReply, testEnv } from './helpers/m2';
import { day, fakeGoogle, newGoogle, type FakeGoogleState } from './helpers/m3';
import { MONARCH_HEADER, PUSH_ENDPOINT, SAFE_SCRIPT, fixtures, monthDay, mrow, newNetwork, vapidKeys } from './helpers/m4';

let rt: TestRuntime;
let fx: ReturnType<typeof fixtures>;
before(async () => {
  rt = await openTestRuntime();
  fx = fixtures(rt);
});
after(async () => {
  await rt.dispose();
});

const approveWith = (run_id: string, hh: string): (() => Promise<ApprovalEvent>) => async () => {
  const run = (await getRun(rt.db, hh, run_id))!;
  return { decision: 'approve', payload_hash: String(run.params['payload_hash']), member_id: `mem_${hh}` };
};

// ═══════════════ MissionRunWF: the happy path ═══════════════

test('MissionRunWF: the flagship card runs compile -> debit -> R0 -> gates -> R1 -> R2 approval -> R3 deep link -> confirmed', async () => {
  const keys = await vapidKeys();
  const s = await fx.buildScenario({ envExtra: { VAPID_PUBLIC_KEY: keys.publicKey, VAPID_PRIVATE_KEY: keys.privateKey, VAPID_SUBJECT: keys.subject } });
  await fx.grant(s.h, 3);
  await rt.db.prepare('UPDATE members SET push_endpoint = ? WHERE household_id = ?').bind(JSON.stringify({ endpoint: PUSH_ENDPOINT }), s.h.household_id).run();
  const runs = await fx.stage(s.h);
  const run_id = runs['anniv_dining']!;
  assert.ok(run_id, `the flagship card was staged: ${Object.keys(runs).join()}`);

  const { step, log, waits } = directStep(undefined, {
    [`approve:${run_id}`]: approveWith(run_id, s.h.household_id),
    [`confirmed:${run_id}`]: {},
  });
  const outcome = await runMission(step, fx.deps(s, run_id), { run_id, household_id: s.h.household_id });

  assert.equal(outcome.status, 'confirmed');
  assert.deepEqual(
    log.map((l) => l.name),
    ['compile', 'debit_credit', 'r0_read', 'r0_fill', 'gates', 'r1_stage', 'notify', 'r3_regate', 'r3_link', 'deliver_link', 'confirm'],
  );
  assert.deepEqual(waits.map((w) => [w.name, w.type]), [['await_approval', `approve:${run_id}`], ['await_confirmation', `confirmed:${run_id}`]]);
  assert.equal(waits[1]!.timeout, '72 hours', 'confirmation waits 72 hours');
  assert.ok(typeof waits[0]!.timeout === 'number' && waits[0]!.timeout >= 60_000, 'approval always has an explicit timeout');

  const run = (await getRun(rt.db, s.h.household_id, run_id))!;
  assert.equal(run.state, 'CONFIRMED');
  assert.equal(await fx.balance(s.h), 2, 'one credit debited, none refunded');
  const ledger = await fx.all<{ delta: number; reason: string }>(`SELECT delta, reason FROM credit_ledger WHERE run_id = ?`, run_id);
  assert.deepEqual(ledger, [{ delta: -1, reason: 'mission_debit' }]);

  // the link: a deep link to the merchant, never fetched
  const link = await fx.one<{ merchant_domain: string; url: string; affiliate_program: string | null }>(`SELECT merchant_domain, url, affiliate_program FROM deeplinks WHERE run_id = ?`, run_id);
  assert.equal(link.merchant_domain, 'resy.com');
  assert.match(link.url, /^https:\/\/resy\.com\/\?/);
  assert.equal(new URL(link.url).searchParams.get('seats'), '2');
  assert.equal(link.affiliate_program, null);
  assert.ok(!s.net.hosts.some((h) => h.endsWith('resy.com')), 'resy.com was never contacted');

  // push: two payload-less tickles (card ready, link ready), with a VAPID header
  assert.equal(s.net.push.length, 2);
  assert.ok(s.net.push.every((p) => p.bodyLength === 0 && /^vapid t=[\w-]+\.[\w-]+\.[\w-]+, k=/.test(p.authorization ?? '')));

  // the run's steps are recorded, and all nine gates passed twice (R1 and R3)
  const steps = await fx.all<{ step_no: number; kind: string; status: string }>(`SELECT step_no, kind, status FROM steps WHERE run_id = ? AND step_no >= 10 ORDER BY step_no`, run_id);
  assert.ok(steps.every((x) => x.status === 'ok'), JSON.stringify(steps));
  assert.ok(steps.some((x) => x.kind === 'deeplink') && steps.some((x) => x.kind === 'gate'));
  assert.equal(JSON.parse((await fx.one<{ gate_result_json: string }>(`SELECT gate_result_json FROM steps WHERE run_id = ? AND step_no = 14`, run_id)).gate_result_json).length, 9);
  assert.equal(canonicalJson({ b: 1, a: { d: 1, c: [{ z: 1, y: 2 }] } }), '{"a":{"c":[{"y":2,"z":1}],"d":1},"b":1}');
});

// ── lifecycle helpers ──

type Scenario = Awaited<ReturnType<typeof fx.buildScenario>>;

async function runWith(s: Scenario, run_id: string, events: Record<string, unknown> = {}) {
  const { step, log, waits } = directStep(undefined, events);
  const outcome = await runMission(step, fx.deps(s, run_id), { run_id, household_id: s.h.household_id });
  return { outcome, log: log.map((l) => l.name), waits };
}
const stateOf = async (s: Scenario, run_id: string): Promise<string> => (await getRun(rt.db, s.h.household_id, run_id))!.state;
const ledgerOf = (run_id: string) => fx.all<{ delta: number; reason: string }>(`SELECT delta, reason FROM credit_ledger WHERE run_id = ? ORDER BY entry_id`, run_id);
const failureOf = async (s: Scenario, run_id: string): Promise<{ stage: string; gates?: string[]; reason?: string; error?: string }> =>
  (await getRun(rt.db, s.h.household_id, run_id))!.params['failure'] as { stage: string; gates?: string[]; reason?: string; error?: string };
const DECLINE: ApprovalEvent = { decision: 'decline', payload_hash: 'irrelevant' };

test('MissionRunWF: an R2 template ends at CONFIRMED with the worker-written script on the card, one model call, and no link', async () => {
  const s = await fx.buildScenario();
  await fx.grant(s.h, 2);
  const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
  let during: HomeFeed | undefined;
  const { outcome, log, waits } = await runWith(s, run_id, {
    [`approve:${run_id}`]: async () => {
      during = await getHome(rt.db, s.h.household_id, new Date());
      return approveWith(run_id, s.h.household_id)();
    },
  });
  assert.deepEqual(outcome, { status: 'confirmed', link: null });
  assert.deepEqual(log, ['compile', 'debit_credit', 'r0_read', 'r0_fill', 'gates', 'r1_stage', 'notify', 'r3_regate', 'confirm']);
  assert.deepEqual(waits.map((w) => w.name), ['await_approval']);
  assert.equal(await stateOf(s, run_id), 'CONFIRMED');
  assert.equal(await fx.balance(s.h), 1);

  const card = during!.cards.find((c) => c.run_id === run_id)!;
  assert.equal(card.state, 'R2_AWAITING');
  assert.match(card.payload_hash ?? '', /^[0-9a-f]{64}$/);
  assert.equal(card.script, SAFE_SCRIPT);
  assert.equal(card.link, null);

  assert.equal(s.net.llm.length, 1);
  assert.equal(s.net.llm[0]!.host, 'api.openai.com');
  assert.match(s.net.llm[0]!.sentText, /Amex Platinum/);
  assert.match(s.net.llm[0]!.sentText, /\$895\.00/);
  assert.ok(!/1005/.test(s.net.llm[0]!.sentText), 'no card digits are sent to the model');
  const usage = await fx.one<{ n: number; cost: number }>(`SELECT COUNT(*) AS n, SUM(cost_usd) AS cost FROM llm_usage WHERE run_id = ?`, run_id);
  assert.equal(usage.n, 1);
  const run = (await getRun(rt.db, s.h.household_id, run_id))!;
  assert.ok(usage.cost > 0 && Math.abs(run.cost_usd - usage.cost) < 1e-9, 'the run records what the model cost');
  assert.equal((await fx.all('SELECT 1 FROM deeplinks WHERE run_id = ?', run_id)).length, 0);
});

test('MissionRunWF: affiliate tags go on tagged merchant links only, and never on a blocklisted domain', async () => {
  const tags: Record<string, unknown> = {};
  for (const d of [...Object.values(MERCHANT_DOMAINS), ...Object.values(ISSUER_DOMAINS)]) tags[d] = { program: 'prog', param: 'aff', value: 'hayes-1' };
  const s = await fx.buildScenario({ envExtra: { AFFILIATE_TAGS: JSON.stringify(tags) } });
  await fx.grant(s.h, 3);
  const runs = await fx.stage(s.h);
  const blocked = new Set((await fx.all<{ domain: string }>('SELECT domain FROM policy_blocklist')).map((r) => r.domain));
  for (const key of ['anniv_dining', 'expiring_credit']) {
    const run_id = runs[key]!;
    const { outcome } = await runWith(s, run_id, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id), [`confirmed:${run_id}`]: {} });
    assert.equal(outcome.status, 'confirmed', key);
    const link = await fx.one<{ merchant_domain: string; url: string; affiliate_program: string | null }>('SELECT merchant_domain, url, affiliate_program FROM deeplinks WHERE run_id = ?', run_id);
    const params = new URL(link.url).searchParams;
    if (blocked.has(link.merchant_domain)) {
      assert.equal(link.affiliate_program, null, `${key}: ${link.merchant_domain} is blocklisted, so its link is never tagged`);
      assert.equal(params.get('aff'), null);
    } else {
      assert.equal(link.affiliate_program, 'prog', key);
      assert.equal(params.get('aff'), 'hayes-1');
    }
  }
  const resy = await fx.one<{ url: string }>('SELECT url FROM deeplinks WHERE run_id = ?', runs['anniv_dining']!);
  assert.equal(new URL(resy.url).hostname, 'resy.com');
  assert.equal(new URL(resy.url).searchParams.get('aff'), null, 'a Resy link is untagged even though a tag was configured for it');
});

test('MissionRunWF: a wrong hash or a decline ends DECLINED, an unanswered card EXPIRED; the credit stays spent and no link is issued', async () => {
  const s = await fx.buildScenario();
  await fx.grant(s.h, 3);
  const runs = await fx.stage(s.h);
  const cases: Array<[string, unknown, string, string | null]> = [
    ['anniv_dining', { decision: 'approve', payload_hash: 'f'.repeat(64) }, 'DECLINED', 'payload_hash_mismatch'],
    ['annual_fee_roi', DECLINE, 'DECLINED', 'declined'],
    ['expiring_credit', TIMEOUT, 'EXPIRED', null],
  ];
  for (const [key, event, state, reason] of cases) {
    const run_id = runs[key]!;
    const { outcome, log } = await runWith(s, run_id, { [`approve:${run_id}`]: event });
    assert.equal(await stateOf(s, run_id), state, key);
    assert.equal(outcome.status, state.toLowerCase(), key);
    if (reason !== null) assert.equal((await getRun(rt.db, s.h.household_id, run_id))!.params['declined_reason'], reason);
    assert.ok(!log.includes('r3_regate') && !log.includes('r3_link'), `${key}: nothing after the decision runs`);
    assert.deepEqual((await ledgerOf(run_id)).map((l) => l.reason), ['mission_debit'], `${key}: no refund on ${state}`);
    assert.equal((await fx.all('SELECT 1 FROM deeplinks WHERE run_id = ?', run_id)).length, 0);
  }
  assert.equal(await fx.balance(s.h), 0);
});

test('MissionRunWF: a link nobody confirms stays R3_LINK_ISSUED; a confirmation moves it to CONFIRMED', async () => {
  const s = await fx.buildScenario();
  await fx.grant(s.h, 2);
  const runs = await fx.stage(s.h);
  const a = runs['anniv_dining']!;
  const unconfirmed = await runWith(s, a, { [`approve:${a}`]: approveWith(a, s.h.household_id), [`confirmed:${a}`]: TIMEOUT });
  assert.equal(unconfirmed.outcome.status, 'link_issued');
  assert.deepEqual(unconfirmed.log.slice(-3), ['r3_link', 'deliver_link', 'unconfirmed']);
  assert.equal(await stateOf(s, a), 'R3_LINK_ISSUED');
  assert.equal((await fx.all('SELECT 1 FROM deeplinks WHERE run_id = ?', a)).length, 1);
  assert.deepEqual((await ledgerOf(a)).map((l) => l.reason), ['mission_debit']);
  const home = await getHome(rt.db, s.h.household_id, new Date());
  const card = home.cards.find((c) => c.run_id === a)!;
  assert.equal(card.state, 'R3_LINK_ISSUED');
  assert.match(card.link?.url ?? '', /^https:\/\/resy\.com\//);
  assert.equal(home.cards[0]!.run_id, a, 'a card holding a link is shown first');
});

// ═══════════════ credits ═══════════════

test('CREDITS: a household with no credit halts as INSUFFICIENT_CREDITS at debit_credit, takes nothing, calls nothing', async () => {
  const s = await fx.buildScenario();
  const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
  s.net.hosts.length = 0;
  const { outcome, log, waits } = await runWith(s, run_id);
  assert.deepEqual(outcome, { status: 'insufficient_credits', auto_recharge: 'disabled' });
  assert.deepEqual(log, ['compile', 'debit_credit']);
  assert.equal(waits.length, 0);
  assert.equal(await stateOf(s, run_id), 'INSUFFICIENT_CREDITS');
  assert.deepEqual(await ledgerOf(run_id), []);
  assert.equal(await fx.balance(s.h), 0, 'the balance never goes negative');
  assert.deepEqual(s.net.hosts, [], 'no model, calendar or push call was made');
  assert.equal((await failureOf(s, run_id)).reason, 'INSUFFICIENT_CREDITS');
});

test('CREDITS: a finished run cannot be started again, so it cannot be charged twice', async () => {
  const s = await fx.buildScenario();
  await fx.grant(s.h, 5);
  const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
  await runWith(s, run_id, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id) });
  assert.equal(await fx.balance(s.h), 4);
  await assert.rejects(runWith(s, run_id), { name: 'IllegalTransitionError' });
  assert.equal(await fx.balance(s.h), 4);
  assert.equal((await ledgerOf(run_id)).length, 1);
});

// ═══════════════ gate failures refund the credit and halt ═══════════════

/** A run that failed a gate: FAILED_GATE, debit then refund, nothing staged or sent, no approval awaited, no link. */
async function assertRefundedAndHalted(s: Scenario, run_id: string, granted: number, stage: 'r1' | 'r3'): Promise<string[]> {
  assert.equal(await stateOf(s, run_id), 'FAILED_GATE');
  assert.deepEqual((await ledgerOf(run_id)).map((l) => l.reason), ['mission_debit', 'mission_refund']);
  assert.equal(await fx.balance(s.h), granted, 'the household is made whole');
  assert.equal((await fx.all('SELECT 1 FROM deeplinks WHERE run_id = ?', run_id)).length, 0);
  const failure = await failureOf(s, run_id);
  assert.equal(failure.stage, stage);
  return failure.gates ?? [];
}

test('GATE FAILURE G3: not enough dining headroom refunds the credit and halts before anything is staged or sent', async () => {
  const s = await fx.buildScenario();
  await fx.grant(s.h, 2);
  const run_id = (await fx.stage(s.h))['anniv_dining']!;
  // the household spends most of the dining envelope after the card was staged
  await fx.ingestCsv(s.env, s.h, [MONARCH_HEADER, mrow(monthDay(0, 1), 'Wine Bar', 'Restaurants & Bar', 'Checking (...9999)', 'WINE BAR LATE', '-420.00')].join('\n') + '\n');
  const headroom = (await fx.one<{ h: number }>(`SELECT MAX(headroom_cents) AS h FROM envelope_headroom WHERE household_id = ? AND envelope_key LIKE '%dining%'`, s.h.household_id)).h;
  assert.ok(headroom < 16_500, `headroom is now ${headroom}`);

  s.net.push.length = 0;
  const { outcome, log, waits } = await runWith(s, run_id, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id) });
  assert.equal(outcome.status, 'failed_gate');
  const gates = await assertRefundedAndHalted(s, run_id, 2, 'r1');
  assert.ok(gates.includes('G3'), gates.join());
  assert.deepEqual(log, ['compile', 'debit_credit', 'r0_read', 'r0_fill', 'gates', 'refund']);
  assert.equal(waits.length, 0, 'no approval is awaited');
  assert.equal(s.net.push.length, 0, 'the member is not notified');
  const stored = (await getRun(rt.db, s.h.household_id, run_id))!;
  assert.equal(stored.params['payload'], undefined, 'no card was staged');
  assert.equal(stored.params['payload_hash'], undefined);
});

test('GATE FAILURE G7: tax language in the model-written script refuses the card, refunds, and halts (only G7 fails)', async () => {
  for (const phrase of ['This fee is deductible, so keep the card.', 'You can deduct the fee, so keep it.', 'Enter on line 8 of Schedule C and keep it.', 'Treat the fee as a write-off.']) {
    const s = await fx.buildScenario();
    await fx.grant(s.h, 1);
    const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
    s.net.workerReply = () => openaiReply({ script: phrase });
    const { outcome, log, waits } = await runWith(s, run_id, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id) });
    assert.deepEqual(outcome, { status: 'failed_gate', stage: 'r1', failed: ['G7'] }, phrase);
    assert.deepEqual(await assertRefundedAndHalted(s, run_id, 1, 'r1'), ['G7']);
    assert.equal(log.at(-1), 'refund');
    assert.equal(waits.length, 0);
    const home = await getHome(rt.db, s.h.household_id, new Date());
    assert.ok(!JSON.stringify(home).includes(phrase), 'the blocked text never reaches Home');
    assert.equal(s.net.push.length, 0);
  }
});

test('GATE FAILURE G8: a run over its per-credit cost cap, or a worker budget the input exceeds, refunds and halts', async () => {
  // spend already booked against the run pushes it over the cap
  const over = await fx.buildScenario();
  await fx.grant(over.h, 1);
  const overRun = (await fx.stage(over.h))['annual_fee_roi']!;
  await rt.db.prepare(`INSERT INTO llm_usage (id, household_id, run_id, model, input_tokens, cached_tokens, output_tokens, cost_usd) VALUES (?, ?, ?, 'gpt-6-luna', 1000, 0, 100, 0.7)`).bind(`u_${overRun}`, over.h.household_id, overRun).run();
  const a = await runWith(over, overRun, { [`approve:${overRun}`]: approveWith(overRun, over.h.household_id) });
  assert.deepEqual(a.outcome, { status: 'failed_gate', stage: 'r1', failed: ['G8'] });
  assert.deepEqual(await assertRefundedAndHalted(over, overRun, 1, 'r1'), ['G8']);

  // the template's own input budget is smaller than the prompt: the model is never called
  const small = await fx.buildScenario();
  await fx.grant(small.h, 1);
  const smallRun = (await fx.stage(small.h))['annual_fee_roi']!;
  const original = (await fx.one<{ plan_json: string }>(`SELECT plan_json FROM mission_templates WHERE template_key = 'annual_fee_roi'`)).plan_json;
  await rt.db.prepare(`UPDATE mission_templates SET plan_json = ? WHERE template_key = 'annual_fee_roi'`).bind(original.replace('"max_input_tokens":60000', '"max_input_tokens":5')).run();
  try {
    const b = await runWith(small, smallRun, { [`approve:${smallRun}`]: approveWith(smallRun, small.h.household_id) });
    assert.deepEqual(b.outcome, { status: 'failed_gate', stage: 'r1', failed: ['G8'] });
    assert.deepEqual(await assertRefundedAndHalted(small, smallRun, 1, 'r1'), ['G8']);
    assert.equal(small.net.llm.length, 0, 'the oversized call was refused before it was made');
  } finally {
    await rt.db.prepare(`UPDATE mission_templates SET plan_json = ? WHERE template_key = 'annual_fee_roi'`).bind(original).run();
  }
});

test('GATE FAILURE G4/G2: a calendar with no free evening, or one that cannot be read, refunds and halts', async () => {
  const busy = await fx.buildScenario();
  await fx.grant(busy.h, 1);
  const busyRun = (await fx.stage(busy.h))['anniv_dining']!;
  busy.net.google.busy = [{ start: new Date(Date.now() - 86_400_000).toISOString(), end: new Date(Date.now() + 200 * 86_400_000).toISOString() }];
  const a = await runWith(busy, busyRun, { [`approve:${busyRun}`]: approveWith(busyRun, busy.h.household_id) });
  assert.deepEqual(a.outcome, { status: 'failed_gate', stage: 'r1', failed: ['G4'] });
  assert.deepEqual(await assertRefundedAndHalted(busy, busyRun, 1, 'r1'), ['G4']);
  assert.ok(busy.net.google.log.some((l) => l.path.endsWith('/freeBusy')), 'free windows were fetched in this run, not trusted from the sync');

  const gone = await fx.buildScenario();
  await fx.grant(gone.h, 1);
  const goneRun = (await fx.stage(gone.h))['anniv_dining']!;
  await rt.db.prepare(`UPDATE connectors SET status = 'revoked' WHERE household_id = ?`).bind(gone.h.household_id).run();
  const b = await runWith(gone, goneRun, { [`approve:${goneRun}`]: approveWith(goneRun, gone.h.household_id) });
  assert.equal(b.outcome.status, 'failed_gate');
  const gates = await assertRefundedAndHalted(gone, goneRun, 1, 'r1');
  assert.ok(gates.includes('G2') && gates.includes('G4'), gates.join());
});

test('GATE FAILURE at R3: the gates run again after approval, and a calendar that filled up in between refunds and issues no link', async () => {
  const s = await fx.buildScenario();
  await fx.grant(s.h, 1);
  const run_id = (await fx.stage(s.h))['anniv_dining']!;
  const { outcome, log, waits } = await runWith(s, run_id, {
    [`approve:${run_id}`]: async () => {
      // the member approves; before the link is issued the whole calendar fills up
      s.net.google.busy = [{ start: new Date(Date.now() - 86_400_000).toISOString(), end: new Date(Date.now() + 200 * 86_400_000).toISOString() }];
      return approveWith(run_id, s.h.household_id)();
    },
  });
  assert.deepEqual(outcome, { status: 'failed_gate', stage: 'r3', failed: ['G4'] });
  assert.deepEqual(log, ['compile', 'debit_credit', 'r0_read', 'r0_fill', 'gates', 'r1_stage', 'notify', 'r3_regate', 'refund']);
  assert.deepEqual(waits.map((w) => w.name), ['await_approval']);
  assert.deepEqual(await assertRefundedAndHalted(s, run_id, 1, 'r3'), ['G4']);
});

test('GATE FAILURE at R3: a card that changed after the member approved it is not delivered', async () => {
  const s = await fx.buildScenario();
  await fx.grant(s.h, 1);
  const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
  const { outcome } = await runWith(s, run_id, {
    [`approve:${run_id}`]: async () => {
      const hash = (await approveWith(run_id, s.h.household_id)()).payload_hash;
      // a newer statement says the fee is $950, not $895
      const text = 'Amex Platinum annual fee $950 posts soon.';
      const ep = await fx.userEpisode(s.h, text);
      await fx.writeFacts(s.h, ep, text, [{ subject: { type: 'card_account', name: 'Amex Platinum' }, predicate: 'annual_fee', value_cents: 95_000, quote: 'annual fee $950' }]);
      return { decision: 'approve', payload_hash: hash } satisfies ApprovalEvent;
    },
  });
  assert.equal(outcome.status, 'failed_gate');
  const gates = await assertRefundedAndHalted(s, run_id, 1, 'r3');
  assert.ok(gates.includes('G2'), gates.join());
});

test('SYSTEM ERROR: when the model vendors are down the run is CANCELED and the credit refunded', async () => {
  const s = await fx.buildScenario();
  await fx.grant(s.h, 1);
  const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
  s.net.workerReply = () => new Response('upstream down', { status: 503 });
  const { outcome, log } = await runWith(s, run_id, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id) });
  assert.deepEqual(outcome, { status: 'canceled', reason: 'system_error' });
  assert.equal(log.at(-1), 'system_error');
  assert.equal(await stateOf(s, run_id), 'CANCELED');
  assert.deepEqual((await ledgerOf(run_id)).map((l) => l.reason), ['mission_debit', 'mission_refund']);
  assert.equal(await fx.balance(s.h), 1);
  const failure = await failureOf(s, run_id);
  assert.deepEqual([failure.stage, failure.reason], ['r0', 'system_error']);
  assert.ok((failure.error ?? '').length <= 200);
});

// ═══════════════ the gates, one at a time ═══════════════

type Household = Awaited<ReturnType<typeof fx.cardHousehold>>['h'];
const newRunId = (): string => `run_${randomUUID().replace(/-/g, '')}`;

async function seedRun(h: Household, o: { run_id?: string; state?: string; template_key?: string; dedupe_key?: string; created_at?: string } = {}): Promise<string> {
  const run_id = o.run_id ?? newRunId();
  await rt.db
    .prepare(
      `INSERT INTO mission_runs (run_id, household_id, template_key, template_version, origin, params_json, state, cost_usd, created_at, updated_at)
       VALUES (?, ?, ?, 1, 'trigger', ?, ?, 0, ?, ?)`,
    )
    .bind(run_id, h.household_id, o.template_key ?? 'gate_unit', JSON.stringify({ dedupe_key: o.dedupe_key ?? 'dk' }), o.state ?? 'R1_STAGED', o.created_at ?? new Date().toISOString(), new Date().toISOString())
    .run();
  return run_id;
}

const HOUR = 3_600_000;

/** A baseline that passes all nine gates: a $150 dinner, $200 of headroom, a 4-hour free window holding the proposed 2 hours. */
async function gateBase(): Promise<{ input: GateInput; h: Household; feeFact: string }> {
  const { h } = await fx.cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005', fee: { cents: 89_500, postsOn: day(12) } });
  const feeFact = (await fx.one<{ fact_id: string }>(`SELECT fact_id FROM facts WHERE household_id = ? AND predicate = 'annual_fee'`, h.household_id)).fact_id;
  const now = new Date();
  const run_id = newRunId();
  const dedupe_key = `dk_${randomUUID()}`;
  const start = new Date(now.getTime() + 5 * 86_400_000).toISOString();
  const end = new Date(Date.parse(start) + 2 * HOUR).toISOString();
  const data = { card_name: 'Amex Platinum', fee_cents: 89_500, next_post: day(12), credits_used_cents: 0, has_history: true };
  const input: GateInput = {
    db: rt.db,
    now,
    household_id: h.household_id,
    run_id,
    template_key: 'gate_unit',
    dedupe_key,
    cooldown_days: 14,
    plan: [
      { id: 'read', kind: 'read', tier: 'R0', worker_prompt_id: null, max_input_tokens: 0 },
      { id: 'stage', kind: 'stage', tier: 'R1', worker_prompt_id: null, max_input_tokens: 0 },
      { id: 'approve', kind: 'approve', tier: 'R2', worker_prompt_id: null, max_input_tokens: 0 },
      { id: 'link', kind: 'deeplink', tier: 'R3', worker_prompt_id: null, max_input_tokens: 0 },
    ],
    max_tier: 'R3',
    read: {
      candidate_found: true,
      data,
      claims: [{ key: 'fee', facts: [feeFact] }],
      facts: [{ fact_id: feeFact, predicate: 'annual_fee', value_type: 'money', current: true, last_support: now.toISOString() }],
      headroom_cents: 20_000,
      envelope_key: 'dining',
      spend_data_at: now.toISOString(),
      calendar: { required: true, fetched_at: now.toISOString(), windows: [{ date: start.slice(0, 10), start, end: new Date(Date.parse(start) + 4 * HOUR).toISOString() }], error: null },
      fetched_at: now.toISOString(),
    },
    payload: {
      version: 1,
      run_id,
      template_key: 'gate_unit',
      trigger_key: 'T_ANNUAL_FEE_ROI',
      dedupe_key,
      kind: 'dinner_plan',
      copy: buildCopy('T_ANNUAL_FEE_ROI', data),
      script: 'A polite script.',
      price_cents: 15_000,
      envelope_key: 'dining',
      proposed_range: { start, end },
      action: { type: 'deeplink', merchant_domain: 'uber.com', path: '/', query: {} },
      expires_on: null,
    },
    r3: true,
    credit_cost: 1,
  };
  return { input, h, feeFact };
}

const failedOf = async (input: GateInput): Promise<string[]> => (await runGates(input)).failed;

test('GATES: the baseline passes G1-G9 in order, and every result says why', async () => {
  const { input } = await gateBase();
  const out = await runGates(input);
  assert.equal(out.ok, true);
  assert.deepEqual(out.failed, []);
  assert.deepEqual(out.results.map((r) => r.gate), ['G1', 'G2', 'G3', 'G4', 'G5', 'G6', 'G7', 'G8', 'G9']);
  assert.ok(out.results.every((r) => r.ok && r.detail.length > 0 && r.name.length > 0));
});

test('GATES: each gate fails when its rule is broken, and only the gates that break implies fail', async () => {
  const stale = (iso: string, ms: number): string => new Date(Date.parse(iso) - ms).toISOString();
  const cases: Array<[string | string[], (i: GateInput, ctx: { feeFact: string; h: Household }) => void | Promise<void>]> = [
    // a claim resting on a fact the graph does not hold has no verified quote (G1) and no current supporting fact (G2)
    [['G1', 'G2'], (i) => { i.read.claims = [{ key: 'fee', facts: ['fact_does_not_exist'] }]; }],
    ['G1', (i) => { i.read.claims = []; }],
    [['G1', 'G2'], (i) => { i.read.claims = [{ key: 'fee', facts: [] }]; }],
    ['G2', (i) => { i.read.candidate_found = false; }],
    ['G2', (i) => { i.read.facts = i.read.facts.map((f) => ({ ...f, current: false })); }],
    ['G2', (i) => { i.read.facts = i.read.facts.map((f) => ({ ...f, last_support: stale(i.now.toISOString(), 36 * 86_400_000) })); }],
    ['G2', (i) => { i.read.spend_data_at = stale(i.now.toISOString(), 36 * 86_400_000); }],
    ['G2', (i) => { i.read.spend_data_at = null; }],
    ['G2', (i) => { i.read.calendar.fetched_at = stale(i.now.toISOString(), 16 * 60_000); }],
    ['G3', (i) => { i.read.headroom_cents = 16_499; }],
    ['G3', (i) => { i.read.headroom_cents = null; }],
    ['G4', (i) => { i.payload.proposed_range = { start: stale(i.payload.proposed_range!.start, 5 * HOUR), end: stale(i.payload.proposed_range!.end, 5 * HOUR) }; }],
    ['G4', (i) => { i.read.calendar.windows = []; }],
    ['G4', (i) => { i.payload.proposed_range = null; }],
    ['G6', (i) => { i.plan = [...i.plan, { id: 'oops', kind: 'stage', tier: 'R3', worker_prompt_id: null, max_input_tokens: 0 }]; }],
    ['G6', (i) => { i.max_tier = 'R2'; }],
    ['G6', (i) => { i.payload.action = { type: 'none' }; }],
    ['G7', (i) => { i.payload.script = 'That is a deductible expense.'; }],
    ['G8', async (i) => { await rt.db.prepare(`INSERT INTO llm_usage (id, household_id, run_id, model, input_tokens, cached_tokens, output_tokens, cost_usd) VALUES (?, ?, ?, 'm', 10, 0, 10, 0.2501)`).bind(`u_${randomUUID()}`, i.household_id, i.run_id).run(); }],
    ['G8', async (i) => { await rt.db.prepare(`INSERT INTO llm_usage (id, household_id, run_id, model, input_tokens, cached_tokens, output_tokens, cost_usd) VALUES (?, ?, ?, 'm', 90001, 0, 10, 0.01)`).bind(`u_${randomUUID()}`, i.household_id, i.run_id).run(); }],
    ['G8', (i) => { i.fill_failure = 'worker input exceeds the template budget'; }],
    ['G9', async (i, ctx) => { await seedRun(ctx.h, { dedupe_key: i.dedupe_key, template_key: i.template_key, state: 'R2_AWAITING' }); }],
  ];
  for (const [gate, mutate] of cases) {
    const { input, h, feeFact } = await gateBase();
    await mutate(input, { feeFact, h });
    assert.deepEqual(await failedOf(input), Array.isArray(gate) ? gate : [gate], `${gate}: ${mutate.toString().slice(0, 90)}`);
  }
});

test('GATES: all nine are always evaluated; a card that breaks several reports every one', async () => {
  const { input } = await gateBase();
  input.read.claims = [{ key: 'fee', facts: ['fact_does_not_exist'] }];
  input.read.headroom_cents = 1;
  input.payload.script = 'You can deduct it.';
  const out = await runGates(input);
  assert.deepEqual(out.failed, ['G1', 'G2', 'G3', 'G7']);
  assert.equal(out.results.length, 9);
  assert.ok(out.results.filter((r) => !r.ok).every((r) => r.detail.length > 0));
});

test('GATES: boundaries are inclusive exactly where the spec says (35 days, 15 minutes, price + 10%, $0.60, 90,000 tokens, window edges)', async () => {
  const ok = async (mutate: (i: GateInput) => void | Promise<void>): Promise<string[]> => {
    const { input } = await gateBase();
    await mutate(input);
    return failedOf(input);
  };
  const at = (i: GateInput, ms: number): string => new Date(i.now.getTime() - ms).toISOString();
  assert.deepEqual(await ok((i) => { i.read.headroom_cents = 16_500; }), [], 'price 15000 + 10% = 16500 fits exactly');
  assert.deepEqual(await ok((i) => { i.read.facts = i.read.facts.map((f) => ({ ...f, last_support: at(i, 35 * 86_400_000) })); }), [], 'money exactly 35 days old is current');
  assert.deepEqual(await ok((i) => { i.read.facts = i.read.facts.map((f) => ({ ...f, last_support: at(i, 35 * 86_400_000 + 1) })); }), ['G2'], '35 days + 1 ms is stale');
  assert.deepEqual(await ok((i) => { i.read.calendar.fetched_at = at(i, 15 * 60_000); }), [], 'calendar exactly 15 minutes old is current');
  assert.deepEqual(await ok((i) => { i.read.calendar.fetched_at = at(i, 15 * 60_000 + 1); }), ['G2'], '15 minutes + 1 ms is stale');
  assert.deepEqual(await ok((i) => { i.payload.proposed_range = { start: i.read.calendar.windows[0]!.start, end: i.read.calendar.windows[0]!.end }; }), [], 'the proposed time may fill the window exactly');
  assert.deepEqual(await ok((i) => { const w = i.read.calendar.windows[0]!; i.payload.proposed_range = { start: w.start, end: new Date(Date.parse(w.end) + 1).toISOString() }; }), ['G4'], 'one millisecond past the window end');
  assert.deepEqual(await ok((i) => { const w = i.read.calendar.windows[0]!; i.payload.proposed_range = { start: new Date(Date.parse(w.start) - 1).toISOString(), end: w.end }; }), ['G4'], 'one millisecond before the window start');
  assert.deepEqual(await ok(async (i) => { await rt.db.prepare(`INSERT INTO llm_usage (id, household_id, run_id, model, input_tokens, cached_tokens, output_tokens, cost_usd) VALUES (?, ?, ?, 'm', 90000, 0, 10, 0.25)`).bind(`u_${randomUUID()}`, i.household_id, i.run_id).run(); }), [], '$0.25 (one credit) and 90,000 tokens are allowed');
  // a card that spends no money needs no headroom, and one without an envelope cannot be measured
  assert.deepEqual(await ok((i) => { i.payload.price_cents = 0; i.read.headroom_cents = null; }), []);
  assert.deepEqual(await ok((i) => { i.payload.envelope_key = null; i.read.headroom_cents = null; i.read.envelope_key = null; }), []);
});

test('GATE G9: a duplicate blocks only while it is open or inside the cooldown, only for the same template, card and household', async () => {
  const { input, h } = await gateBase();
  const other = await fx.cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005' });
  const old = new Date(Date.now() - 20 * 86_400_000).toISOString();
  const recent = new Date(Date.now() - 2 * 86_400_000).toISOString();
  await seedRun(h, { run_id: input.run_id, dedupe_key: input.dedupe_key, template_key: input.template_key });
  assert.deepEqual(await failedOf(input), [], 'the run itself is not its own duplicate');
  await seedRun(h, { dedupe_key: 'different', template_key: input.template_key, state: 'R2_AWAITING' });
  await seedRun(h, { dedupe_key: input.dedupe_key, template_key: 'some_other_template', state: 'R2_AWAITING' });
  await seedRun(other.h, { dedupe_key: input.dedupe_key, template_key: input.template_key, state: 'R2_AWAITING' });
  assert.deepEqual(await failedOf(input), [], 'other card, other template and other household do not collide');
  await seedRun(h, { dedupe_key: input.dedupe_key, template_key: input.template_key, state: 'DECLINED', created_at: old });
  assert.deepEqual(await failedOf(input), [], 'a run that ended before the cooldown began does not block');
  await seedRun(h, { dedupe_key: input.dedupe_key, template_key: input.template_key, state: 'DECLINED', created_at: recent });
  assert.deepEqual(await failedOf(input), ['G9'], 'a recent run blocks whatever state it ended in');
});

test('GATE G5: a mission host on policy_blocklist, or a blocked fetch recorded against the run, fails the gate', async () => {
  const { input } = await gateBase();
  const { input: other } = await gateBase();
  const addBlock = (domain: string) => rt.db.prepare(`INSERT INTO policy_blocklist (domain, reason) VALUES (?, 'test')`).bind(domain).run();
  const dropBlock = (domain: string) => rt.db.prepare(`DELETE FROM policy_blocklist WHERE domain = ?`).bind(domain).run();
  for (const domain of ['api.openai.com', 'googleapis.com', 'oauth2.googleapis.com']) {
    await addBlock(domain);
    try {
      assert.deepEqual(await failedOf(input), ['G5'], domain);
    } finally {
      await dropBlock(domain);
    }
  }
  assert.deepEqual(await failedOf(input), []);
  const audit = (run_id: string, household_id: string) =>
    rt.db.prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json) VALUES (?, ?, 'egress', 'egress_blocked', ?)`).bind(`aud_${randomUUID()}`, household_id, JSON.stringify({ run_id, host: 'resy.com', domain: 'resy.com' })).run();
  await audit(input.run_id, input.household_id);
  assert.deepEqual(await failedOf(input), ['G5'], 'an attempt to reach Resy during this run fails it');
  assert.deepEqual(await failedOf(other), [], 'another run is unaffected');
});

test('GATE G7: every banned phrase fails the gate wherever it appears on the card', async () => {
  const phrases = ['deductible', 'You can deduct this', 'enter on line 12', 'it is a write-off', 'tax deductions'];
  const places: Array<(i: GateInput, text: string) => void> = [
    (i, t) => { i.payload.copy = { ...i.payload.copy, title: t }; },
    (i, t) => { i.payload.copy = { ...i.payload.copy, why: t }; },
    (i, t) => { i.payload.copy = { ...i.payload.copy, r2_label: t }; },
    (i, t) => { i.payload.copy = { ...i.payload.copy, claims: { fee: t } }; },
    (i, t) => { i.payload.script = t; },
  ];
  for (const phrase of phrases) {
    for (const place of places) {
      const { input } = await gateBase();
      place(input, phrase);
      assert.deepEqual(await failedOf(input), ['G7'], phrase);
    }
  }
  const { input } = await gateBase();
  input.payload.script = 'The annual fee is high. Ask about retention offers and decide afterwards.';
  assert.deepEqual(await failedOf(input), []);
});

test('GATE G1: a quote that no longer matches its episode, or a fact that was retired, fails the gate', async () => {
  const { input, feeFact } = await gateBase();
  await rt.db.prepare('UPDATE fact_quotes SET char_start = char_start + 1, char_end = char_end + 1 WHERE fact_id = ?').bind(feeFact).run();
  assert.deepEqual(await failedOf(input), ['G1']);
  await rt.db.prepare('UPDATE fact_quotes SET char_start = char_start - 1, char_end = char_end - 1 WHERE fact_id = ?').bind(feeFact).run();
  assert.deepEqual(await failedOf(input), []);
  await rt.db.prepare(`UPDATE facts SET expired_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE fact_id = ?`).bind(feeFact).run();
  assert.deepEqual(await failedOf(input), ['G1']);
});

// ═══════════════ credits ledger ═══════════════

test('LEDGER: debit is atomic and conditional, so racing missions can never overdraw; refund is once-only and household-scoped', async () => {
  const h = await makeHousehold(rt.db);
  const at = new Date().toISOString();
  await fx.grant(h, 3);
  const runs: string[] = [];
  for (let i = 0; i < 10; i++) runs.push(await seedRun(h));
  const results = await Promise.all(runs.map((id) => debitCredit(rt.db, h.household_id, id, 1, at)));
  assert.equal(results.filter((r) => r.ok).length, 3, 'exactly the credits that exist were spent');
  assert.equal(results.filter((r) => !r.ok).length, 7);
  assert.equal(await creditBalance(rt.db, h.household_id), 0);
  assert.equal((await fx.all(`SELECT 1 FROM credit_ledger WHERE household_id = ? AND reason = 'mission_debit'`, h.household_id)).length, 3);

  // a cost larger than the balance takes nothing, and a zero-cost run takes nothing and succeeds
  const poor = await makeHousehold(rt.db);
  await fx.grant(poor, 1);
  const big = await seedRun(poor);
  assert.deepEqual(await debitCredit(rt.db, poor.household_id, big, 2, at), { ok: false, balance: 1 });
  assert.equal(await creditBalance(rt.db, poor.household_id), 1);
  assert.deepEqual(await debitCredit(rt.db, poor.household_id, await seedRun(poor), 0, at), { ok: true, debited: false, balance: 1, from_subscription: 0, from_purchased: 0 });

  // the same run debits once: a retried step is a no-op that still reports success
  const run = await seedRun(poor);
  assert.deepEqual(await debitCredit(rt.db, poor.household_id, run, 1, at), { ok: true, debited: true, balance: 0, from_subscription: 1, from_purchased: 0 });
  assert.deepEqual(await debitCredit(rt.db, poor.household_id, run, 1, at), { ok: true, debited: false, balance: 0, from_subscription: 1, from_purchased: 0 });

  // refund: once, only against an existing debit, never for another household's run
  const stranger = await makeHousehold(rt.db);
  assert.equal(await refundCredit(rt.db, stranger.household_id, run, at), false);
  assert.equal(await refundCredit(rt.db, poor.household_id, run, at), true);
  assert.equal(await refundCredit(rt.db, poor.household_id, run, at), false);
  assert.equal(await creditBalance(rt.db, poor.household_id), 1);
  assert.equal(await refundCredit(rt.db, poor.household_id, await seedRun(poor), at), false, 'nothing was debited for it');
  assert.equal(await creditBalance(rt.db, stranger.household_id), 0);
  await assert.rejects(rt.db.prepare(`UPDATE credit_ledger SET delta = 99 WHERE household_id = ?`).bind(poor.household_id).run(), /append-only/);
});

// ═══════════════ deep links ═══════════════

test('DEEP LINKS: domains come from fixed tables, tags only from env for non-blocklisted domains, and a merchant parameter cannot spoof the tag', () => {
  assert.equal(resolveDomain('RESY|OPENTABLE|category:dining', 'amex_platinum'), 'resy.com');
  assert.equal(resolveDomain('category:dining', 'amex_platinum'), 'americanexpress.com', 'no merchant token falls back to the issuer');
  assert.equal(resolveDomain('category:dining', 'chase_sapphire_reserve'), 'chase.com');
  assert.equal(resolveDomain('category:dining', null), null);
  assert.equal(resolveDomain('UNKNOWN MERCHANT', 'some_new_card'), null, 'an unknown merchant and card yield no link rather than a guess');
  assert.equal(resolveDomain('uber one', null), 'uber.com', 'matching ignores case');

  const raw = JSON.stringify({
    'UBER.com': { program: 'impact', param: 'irclickid', value: 'abc' },
    'resy.com': { program: 'bad', param: 'aff', value: 'x' },
    'clearme.com': { program: 'p', param: 'bad param!', value: 'x' },
    'oura.com': { program: 'p', param: 'aff', value: '' },
  });
  const tags = parseAffiliateTags(raw);
  assert.deepEqual(Object.keys(tags).sort(), ['resy.com', 'uber.com'], 'malformed tags are dropped, domains lower-cased');
  assert.deepEqual([parseAffiliateTags(undefined), parseAffiliateTags(''), parseAffiliateTags('not json'), parseAffiliateTags('[1,2]')], [{}, {}, {}, {}]);

  const blocked = (d: string): boolean => d === 'resy.com';
  const tagged = buildDeepLink({ merchant_domain: 'uber.com', path: '/go', query: { irclickid: 'spoof', ref: '1' } }, tags, blocked);
  assert.deepEqual(tagged, { merchant_domain: 'uber.com', url: 'https://uber.com/go?irclickid=abc&ref=1', affiliate_program: 'impact' });
  const resy = buildDeepLink({ merchant_domain: 'resy.com', path: '/', query: { date: '2026-12-05', seats: '2' } }, tags, blocked);
  assert.deepEqual(resy, { merchant_domain: 'resy.com', url: 'https://resy.com/?date=2026-12-05&seats=2', affiliate_program: null });
  const untagged = buildDeepLink({ merchant_domain: 'lyft.com', path: '/', query: {} }, tags, blocked);
  assert.equal(untagged.affiliate_program, null);
  assert.equal(untagged.url, 'https://lyft.com/');
});

// ═══════════════ Web Push ═══════════════

test('WEB PUSH: only push-service hosts are accepted (no SSRF), and lookalikes, credentials and plain http are refused', () => {
  for (const ok of ['https://fcm.googleapis.com/fcm/send/x', 'https://updates.push.services.mozilla.com/wpush/v2/x', 'https://web.push.apple.com/x', 'https://wns2-par02p.notify.windows.com/?token=x']) {
    assert.equal(isAllowedPushEndpoint(ok), true, ok);
  }
  for (const bad of [
    'http://fcm.googleapis.com/x', 'https://fcm.googleapis.com.evil.test/x', 'https://evilfcm.googleapis.com/x', 'https://evil.test/fcm.googleapis.com',
    'https://user:pw@fcm.googleapis.com/x', 'https://localhost/x', 'https://169.254.169.254/latest', 'https://127.0.0.1/x', 'ftp://fcm.googleapis.com/x', 'not a url', '',
  ]) {
    assert.equal(isAllowedPushEndpoint(bad), false, bad);
  }
  assert.equal(parseSubscription(JSON.stringify({ endpoint: PUSH_ENDPOINT, keys: { p256dh: 'x', auth: 'y' } }))?.endpoint, PUSH_ENDPOINT);
  assert.equal(parseSubscription(JSON.stringify({ endpoint: 'https://evil.test/x' })), null);
  assert.deepEqual([parseSubscription(null), parseSubscription('nope'), parseSubscription('{"endpoint":5}')], [null, null, null]);
});

test('WEB PUSH: the VAPID header is a valid ES256 JWT for the push service origin, good for at most 12 hours', async () => {
  const keys = await vapidKeys();
  const now = new Date();
  const header = await vapidAuthorization(PUSH_ENDPOINT, keys, now);
  const m = /^vapid t=([\w-]+)\.([\w-]+)\.([\w-]+), k=([\w-]+)$/.exec(header)!;
  assert.ok(m, header);
  const [, h64, c64, s64, k] = m as unknown as [string, string, string, string, string];
  assert.equal(k, keys.publicKey);
  const dec = (b: string): Uint8Array => Uint8Array.from(Buffer.from(b, 'base64url'));
  assert.deepEqual(JSON.parse(Buffer.from(h64, 'base64url').toString()), { typ: 'JWT', alg: 'ES256' });
  const claims = JSON.parse(Buffer.from(c64, 'base64url').toString()) as { aud: string; exp: number; sub: string };
  assert.equal(claims.aud, 'https://fcm.googleapis.com');
  assert.equal(claims.sub, keys.subject);
  assert.ok(claims.exp * 1000 - now.getTime() <= 12 * HOUR && claims.exp * 1000 > now.getTime());
  const pub = await crypto.subtle.importKey('raw', dec(keys.publicKey), { name: 'ECDSA', namedCurve: 'P-256' }, false, ['verify']);
  assert.equal(await crypto.subtle.verify({ name: 'ECDSA', hash: 'SHA-256' }, pub, dec(s64), new TextEncoder().encode(`${h64}.${c64}`)), true, 'the signature verifies with the public key');
  assert.equal(await crypto.subtle.verify({ name: 'ECDSA', hash: 'SHA-256' }, pub, dec(s64), new TextEncoder().encode(`${h64}.${c64}x`)), false);
  await assert.rejects(vapidAuthorization(PUSH_ENDPOINT, { ...keys, publicKey: 'AAAA' }, now), /65-byte/);
});

test('WEB PUSH: notifyHousehold sends an empty, payload-less tickle and handles every way a push can go wrong', async () => {
  const keys = await vapidKeys();
  const env = { VAPID_PUBLIC_KEY: keys.publicKey, VAPID_PRIVATE_KEY: keys.privateKey, VAPID_SUBJECT: keys.subject };
  const net = newNetwork();
  const guarded = createGuardedFetch({ db: rt.db, fetchImpl: net.fetch, ttlMs: 0 });
  const notify = (h: Household, e: Partial<typeof env> = env) => notifyHousehold({ db: rt.db, fetch: guarded, env: e, now: () => new Date() }, h.household_id, 'card_ready');
  const subscribe = (h: Household, endpoint: string) => rt.db.prepare('UPDATE members SET push_endpoint = ? WHERE household_id = ?').bind(JSON.stringify({ endpoint }), h.household_id).run();
  const audits = (h: Household) => fx.all<{ action: string }>(`SELECT action FROM audit_log WHERE household_id = ? AND actor = 'web-push' ORDER BY id`, h.household_id).then((r) => r.map((x) => x.action));

  const h = await makeHousehold(rt.db);
  assert.deepEqual(await notify(h, {}), { sent: 0, expired: 0, failed: 0, skipped: 'no_vapid' });
  assert.deepEqual(await audits(h), ['push_skipped']);
  assert.deepEqual(await notify(h), { sent: 0, expired: 0, failed: 0, skipped: 'no_subscribers' });
  assert.equal(net.push.length, 0);

  await subscribe(h, PUSH_ENDPOINT);
  assert.deepEqual(await notify(h), { sent: 1, expired: 0, failed: 0, skipped: null });
  assert.equal(net.push.length, 1);
  assert.deepEqual([net.push[0]!.bodyLength, net.push[0]!.ttl], [0, '3600']);
  assert.ok(net.push[0]!.authorization?.startsWith('vapid t='));

  net.pushStatus = 500;
  assert.deepEqual(await notify(h), { sent: 0, expired: 0, failed: 1, skipped: null });
  assert.ok((await audits(h)).includes('push_failed'));

  net.pushStatus = 410;
  assert.deepEqual(await notify(h), { sent: 0, expired: 1, failed: 0, skipped: null });
  assert.equal((await fx.one<{ p: string | null }>('SELECT push_endpoint AS p FROM members WHERE household_id = ?', h.household_id)).p, null, 'a gone subscription is forgotten');
  assert.ok((await audits(h)).includes('push_endpoint_expired'));

  // a stored endpoint that is not on a push service is never contacted
  await subscribe(h, 'https://evil.test/collect');
  net.hosts.length = 0;
  assert.deepEqual(await notify(h), { sent: 0, expired: 0, failed: 1, skipped: null });
  assert.ok(!net.hosts.includes('evil.test'));

  // a network failure is counted, never thrown
  await subscribe(h, PUSH_ENDPOINT);
  const down = createGuardedFetch({ db: rt.db, fetchImpl: (async () => { throw new Error('connection reset'); }) as unknown as typeof fetch, ttlMs: 0 });
  assert.deepEqual(await notifyHousehold({ db: rt.db, fetch: down, env, now: () => new Date() }, h.household_id, 'x'), { sent: 0, expired: 0, failed: 1, skipped: null });
});

// ═══════════════ sealed cookies ═══════════════

test('COOKIES: a sealed cookie round-trips, expires exactly on time, and is bound to its secret and purpose', async () => {
  const secret = 'a-test-secret-of-sufficient-length';
  const now = new Date('2026-12-01T12:00:00Z');
  const sealed = await sealCookie(secret, 'session', { household_id: 'hh_1', member_id: 'mem_1' }, 600, now);
  assert.match(sealed, /^v1\.[\w-]{16}\.[\w-]+$/);
  const open = await openCookie(secret, 'session', sealed, now);
  assert.deepEqual([open?.['household_id'], open?.['member_id'], open?.['exp']], ['hh_1', 'mem_1', Math.floor(now.getTime() / 1000) + 600]);
  assert.notEqual(await openCookie(secret, 'session', sealed, new Date(now.getTime() + 599_000)), null);
  assert.equal(await openCookie(secret, 'session', sealed, new Date(now.getTime() + 600_000)), null, 'expired at exp');
  assert.equal(await openCookie(secret, 'oauth', sealed, now), null, 'a session cookie is not an OAuth cookie');
  assert.equal(await openCookie('another-secret-of-sufficient-length', 'session', sealed, now), null);
  assert.notEqual(sealed, await sealCookie(secret, 'session', { household_id: 'hh_1', member_id: 'mem_1' }, 600, now), 'a fresh IV every time');
  assert.ok(!Buffer.from(sealed.split('.')[2]!, 'base64url').toString('latin1').includes('hh_1'), 'the contents are encrypted, not just signed');
  await assert.rejects(sealCookie('short', 'session', {}, 60, now), /at least 16/);
});

test('COOKIES: any tampering, truncation or malformed value is rejected', async () => {
  const secret = 'a-test-secret-of-sufficient-length';
  const now = new Date();
  const sealed = await sealCookie(secret, 'session', { household_id: 'hh_1', member_id: 'mem_1' }, 600, now);
  const [v, iv, ct] = sealed.split('.') as [string, string, string];
  const flip = (s: string, at: number): string => s.slice(0, at) + (s[at] === 'A' ? 'B' : 'A') + s.slice(at + 1);
  const bad = [
    `${v}.${flip(iv, 3)}.${ct}`, `${v}.${iv}.${flip(ct, 0)}`, `${v}.${iv}.${flip(ct, ct.length - 3)}`, `${v}.${iv}.${ct.slice(0, -4)}`, `v2.${iv}.${ct}`,
    `${iv}.${ct}`, `${sealed}.extra`, `${v}..${ct}`, `${v}.${iv}.`, '', 'garbage', `${v}.${iv.slice(0, 8)}.${ct}`, `${v}.!!!.${ct}`,
  ];
  for (const b of bad) assert.equal(await openCookie(secret, 'session', b, now), null, b.slice(0, 40));
  assert.notEqual(await openCookie(secret, 'session', sealed, now), null);
});

test('COOKIES: header helpers set __Host- cookies correctly and read the right one out of many', () => {
  const header = setCookieHeader(SESSION_COOKIE, 'v1.a.b', 1209600);
  assert.equal(header, '__Host-session=v1.a.b; Max-Age=1209600; Path=/; Secure; HttpOnly; SameSite=Lax');
  assert.ok(!/domain/i.test(header), '__Host- cookies carry no Domain');
  assert.equal(clearCookieHeader(SESSION_COOKIE), '__Host-session=; Max-Age=0; Path=/; Secure; HttpOnly; SameSite=Lax');
  const req = (cookie?: string): Request => new Request('https://x.example/', cookie === undefined ? {} : { headers: { cookie } });
  assert.equal(readCookie(req('a=1; __Host-session=abc; b=2'), '__Host-session'), 'abc');
  assert.equal(readCookie(req('x__Host-session=evil; __Host-session2=evil2'), '__Host-session'), null, 'names match exactly');
  assert.equal(readCookie(req('  __Host-session = spaced ;b=2'), '__Host-session'), 'spaced');
  assert.equal(readCookie(req(), '__Host-session'), null);
  assert.equal(readCookie(req(''), '__Host-session'), null);
  assert.deepEqual([timingSafeEqual('abc', 'abc'), timingSafeEqual('abc', 'abd'), timingSafeEqual('abc', 'abcd'), timingSafeEqual('', '')], [true, false, false, true]);
});

// ═══════════════ Google sign-in, the session cookie, and the auth middleware ═══════════════

const ORIGIN = 'https://app.example';
const CALLBACK = `${ORIGIN}/api/auth/google/callback`;
const newEmail = (): string => `user-${randomUUID()}@example.org`;

interface Rig {
  env: Env;
  google: FakeGoogleState;
  call: (path: string, o?: { method?: string; cookie?: string; headers?: Record<string, string>; body?: string; now?: Date }) => Promise<Response>;
}

function rig(envExtra: Partial<Env> = {}): Rig {
  const { env } = testEnv(rt, envExtra);
  const google = newGoogle();
  google.identity.email = newEmail();
  const guarded = createGuardedFetch({ db: rt.db, fetchImpl: fakeGoogle(google), ttlMs: 0 });
  return {
    env,
    google,
    call: (path, o = {}) =>
      handleApi(
        new Request(`${ORIGIN}${path}`, { method: o.method ?? 'GET', headers: { ...(o.cookie ? { cookie: o.cookie } : {}), ...o.headers }, ...(o.body === undefined ? {} : { body: o.body }) }),
        env,
        { now: () => o.now ?? new Date(), fetch: guarded },
      ),
  };
}

function cookiesOf(res: Response): Record<string, string> {
  const out: Record<string, string> = {};
  for (const c of res.headers.getSetCookie()) {
    const pair = c.split(';')[0]!;
    out[pair.slice(0, pair.indexOf('='))] = pair.slice(pair.indexOf('=') + 1);
  }
  return out;
}

/** Runs GET /login and returns what the browser would hold: the oauth cookie, the state, the PKCE verifier and the redirect. */
async function startLogin(r: Rig, o: { attest18?: boolean; now?: Date } = {}) {
  const res = await r.call(`/api/auth/google/login${o.attest18 === false ? '' : '?attest_18=1'}`, o.now ? { now: o.now } : {});
  const oauth = cookiesOf(res)['__Host-oauth']!;
  const sealed = (await openCookie(r.env.SESSION_SECRET, 'oauth', oauth, o.now ?? new Date()))!;
  return { res, oauth, state: String(sealed['state']), verifier: String(sealed['verifier']), location: new URL(res.headers.get('location')!) };
}

async function callback(r: Rig, login: Awaited<ReturnType<typeof startLogin>>, o: { code?: string; state?: string; cookie?: string | null; extra?: string; now?: Date } = {}): Promise<Response> {
  const query = o.extra ?? `code=${o.code ?? 'good-code'}&state=${encodeURIComponent(o.state ?? login.state)}`;
  const cookie = o.cookie === null ? undefined : (o.cookie ?? `__Host-oauth=${login.oauth}`);
  return r.call(`/api/auth/google/callback?${query}`, { ...(cookie === undefined ? {} : { cookie }), ...(o.now ? { now: o.now } : {}) });
}

const countMembers = async (email: string): Promise<number> => (await fx.one<{ n: number }>('SELECT COUNT(*) AS n FROM members WHERE lower(email) = ?', email.toLowerCase())).n;

/** A fully signed-in member: the session cookie header value, plus the ids. */
async function signIn(r: Rig): Promise<{ cookie: string; household_id: string; member_id: string }> {
  const login = await startLogin(r);
  const res = await callback(r, login);
  assert.equal(res.status, 302, await res.clone().text());
  const session = cookiesOf(res)['__Host-session']!;
  const claims = (await openCookie(r.env.SESSION_SECRET, 'session', session, new Date()))!;
  return { cookie: `__Host-session=${session}`, household_id: String(claims['household_id']), member_id: String(claims['member_id']) };
}

test('AUTH LOGIN: redirects to Google with the four scopes, PKCE S256 and a state bound to a sealed, HttpOnly, 10-minute cookie', async () => {
  const r = rig();
  const a = await startLogin(r);
  assert.equal(a.res.status, 302);
  assert.equal(a.res.headers.get('cache-control'), 'no-store');
  const u = a.location;
  assert.equal(`${u.origin}${u.pathname}`, 'https://accounts.google.com/o/oauth2/v2/auth');
  assert.equal(u.searchParams.get('client_id'), 'client-id.apps.test');
  assert.equal(u.searchParams.get('redirect_uri'), CALLBACK);
  assert.equal(u.searchParams.get('response_type'), 'code');
  assert.deepEqual(u.searchParams.get('scope')!.split(' '), ['openid', 'email', 'https://www.googleapis.com/auth/calendar.events.readonly', 'https://www.googleapis.com/auth/calendar.freebusy']);
  assert.deepEqual([u.searchParams.get('access_type'), u.searchParams.get('prompt'), u.searchParams.get('include_granted_scopes')], ['offline', 'consent', 'false']);
  assert.equal(u.searchParams.get('state'), a.state);
  assert.equal(u.searchParams.get('code_challenge_method'), 'S256');
  assert.equal(u.searchParams.get('code_challenge'), Buffer.from(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(a.verifier))).toString('base64url'), 'the challenge is the S256 of the sealed verifier');
  assert.ok(![...u.searchParams.values()].some((v) => v.includes('client-secret')), 'the client secret never goes to the browser');
  assert.ok(a.state.length >= 30 && a.verifier.length >= 40);

  const setCookie = a.res.headers.get('set-cookie')!;
  assert.match(setCookie, /^__Host-oauth=v1\.[\w-]+\.[\w-]+; Max-Age=600; Path=\/; Secure; HttpOnly; SameSite=Lax$/);
  assert.ok(!setCookie.includes(a.state) && !setCookie.includes(a.verifier), 'neither the state nor the verifier is readable in the cookie');
  const b = await startLogin(r);
  assert.notEqual(b.state, a.state);
  assert.notEqual(b.verifier, a.verifier);

  assert.equal((await openCookie(r.env.SESSION_SECRET, 'oauth', a.oauth, new Date()))!['attest18'], true);
  assert.equal((await openCookie(r.env.SESSION_SECRET, 'oauth', (await startLogin(r, { attest18: false })).oauth, new Date()))!['attest18'], false);
});

test('AUTH LOGIN: unconfigured Google credentials answer 503, and only GET is accepted', async () => {
  const r = rig({ GOOGLE_CLIENT_ID: '' } as Partial<Env>);
  assert.equal((await r.call('/api/auth/google/login')).status, 503);
  assert.equal((await r.call('/api/auth/google/callback?code=x&state=y')).status, 503);
  const ok = rig();
  const wrong = await ok.call('/api/auth/google/login', { method: 'POST' });
  assert.deepEqual([wrong.status, wrong.headers.get('allow')], [405, 'GET']);
  assert.equal((await ok.call('/api/auth/google/callback', { method: 'POST' })).status, 405);
});

test('AUTH CALLBACK: a new member gets a household, encrypted calendar tokens, and a sealed HttpOnly session cookie that unlocks /api', async () => {
  const r = rig();
  r.google.identity = { email: 'New.Member@Example.org', sub: 'sub-new', name: 'New Member', email_verified: true };
  const login = await startLogin(r);
  const now = new Date();
  const res = await callback(r, login);

  assert.equal(res.status, 302);
  assert.equal(res.headers.get('location'), '/?signed_in=1');
  assert.equal(res.headers.get('cache-control'), 'no-store');
  const raw = res.headers.getSetCookie();
  assert.equal(raw.length, 2);
  const session = raw.find((c) => c.startsWith('__Host-session='))!;
  assert.match(session, /^__Host-session=v1\.[\w-]+\.[\w-]+; Max-Age=1209600; Path=\/; Secure; HttpOnly; SameSite=Lax$/);
  assert.ok(raw.some((c) => c === '__Host-oauth=; Max-Age=0; Path=/; Secure; HttpOnly; SameSite=Lax'), 'the one-time OAuth cookie is cleared');

  // the token request: authorization_code grant with the secret and the PKCE verifier from the sealed cookie
  const tokenCall = r.google.log.find((l) => l.path === '/token')!;
  const form = new URLSearchParams(tokenCall.body);
  assert.deepEqual(
    [form.get('grant_type'), form.get('code'), form.get('client_id'), form.get('client_secret'), form.get('redirect_uri'), form.get('code_verifier')],
    ['authorization_code', 'good-code', 'client-id.apps.test', 'client-secret', CALLBACK, login.verifier],
  );

  // the household and its owner
  const member = await fx.one<{ member_id: string; household_id: string; email: string; role: string; age_attested_18: number; display_name: string }>(`SELECT * FROM members WHERE lower(email) = 'new.member@example.org'`);
  assert.deepEqual([member.email, member.role, member.age_attested_18, member.display_name], ['new.member@example.org', 'owner', 1, 'New Member']);
  const hh = await fx.one<{ plan: string; shard: number; alias_local: string; timezone: string }>('SELECT plan, shard, alias_local, timezone FROM households WHERE household_id = ?', member.household_id);
  assert.deepEqual([hh.plan, hh.shard, hh.timezone], ['trial', 0, 'America/New_York']);
  assert.match(hh.alias_local, /^h_[a-z2-7]{10}$/);
  assert.equal(await creditBalance(rt.db, member.household_id), 5, 'a new household gets the Day-1 trial grant');
  const audits = (await fx.all<{ action: string; detail_json: string }>(`SELECT action, detail_json FROM audit_log WHERE household_id = ? AND actor = 'auth' ORDER BY action`, member.household_id));
  assert.deepEqual(audits.map((a) => a.action), ['household_created', 'signin']);
  assert.ok(audits.every((a) => !/rt-secret|at-initial|good-code/.test(a.detail_json)), 'no token or code is logged');

  // the calendar tokens: connector active, KV value is ciphertext, and the tokens are usable
  const conn = await findCalendarConnector(rt.db, member.household_id);
  assert.ok(conn !== null && conn.status === 'active');
  const stored = (await r.env.KV.get(conn!.token_kv_key!))!;
  assert.ok(!/rt-secret|at-initial/.test(stored) && !/rt-secret|at-initial/.test(Buffer.from(stored, 'base64').toString('latin1')), 'tokens are encrypted at rest');
  const cal: CalendarDeps = { env: r.env, fetch: createGuardedFetch({ db: rt.db, fetchImpl: fakeGoogle(r.google), ttlMs: 0 }), now: () => new Date() };
  assert.equal(await getAccessToken(cal, conn!), 'at-initial');

  // the cookie
  const sealed = (await openCookie(r.env.SESSION_SECRET, 'session', cookiesOf(res)['__Host-session']!, now))!;
  assert.deepEqual([sealed['household_id'], sealed['member_id']], [member.household_id, member.member_id]);
  assert.ok(Math.abs(Number(sealed['exp']) - (Math.floor(now.getTime() / 1000) + 14 * 86_400)) <= 5);

  // and it unlocks the API
  const cookie = `__Host-session=${cookiesOf(res)['__Host-session']}`;
  const home = await r.call('/api/home', { cookie });
  assert.equal(home.status, 200);
  const me = (await (await r.call('/api/auth/me', { cookie })).json()) as Record<string, unknown>;
  assert.deepEqual([me['member_id'], me['household_id'], me['email'], me['display_name'], me['role'], me['plan'], me['calendar_connected']], [member.member_id, member.household_id, 'new.member@example.org', 'New Member', 'owner', 'trial', true]);
  assert.ok(!JSON.stringify(me).includes('rt-secret'));
});

test('AUTH CALLBACK: a returning member (any e-mail casing) is signed into the same household without attestation or a new refresh token', async () => {
  const r = rig();
  const first = await signIn(r);
  r.google.identity.email = r.google.identity.email.toUpperCase();
  r.google.omitRefreshToken = true;
  const login = await startLogin(r, { attest18: false });
  const res = await callback(r, login);
  assert.equal(res.status, 302);
  assert.equal(res.headers.get('location'), '/?signed_in=1', 'the stored refresh token is kept, so the calendar stays connected');
  const claims = (await openCookie(r.env.SESSION_SECRET, 'session', cookiesOf(res)['__Host-session']!, new Date()))!;
  assert.deepEqual([claims['household_id'], claims['member_id']], [first.household_id, first.member_id]);
  assert.equal(await countMembers(r.google.identity.email), 1);
  assert.equal((await fx.all('SELECT 1 FROM households h JOIN members m ON m.household_id = h.household_id WHERE lower(m.email) = ?', r.google.identity.email.toLowerCase())).length, 1);
});

test('AUTH CALLBACK: a brand-new member who did not confirm they are 18 is refused and nothing is created', async () => {
  const r = rig();
  const login = await startLogin(r, { attest18: false });
  const res = await callback(r, login);
  assert.equal(res.status, 403);
  assert.equal(((await res.json()) as { error: string }).error, 'age_attestation_required');
  assert.equal(await countMembers(r.google.identity.email), 0);
  assert.deepEqual(Object.keys(cookiesOf(res)), ['__Host-oauth'], 'no session cookie is issued');
});

test('AUTH CALLBACK: every way the sign-in can be forged or fail ends with no session cookie, no household and no stored tokens', async () => {
  type Mut = (r: Rig, login: Awaited<ReturnType<typeof startLogin>>) => Promise<Response> | Response;
  const cases: Array<[string, number, string, Mut]> = [
    ['state does not match', 400, 'bad_state', (r, l) => callback(r, l, { state: `${l.state}x` })],
    ['no oauth cookie at all', 400, 'bad_state', (r, l) => callback(r, l, { cookie: null })],
    ['oauth cookie expired', 400, 'bad_state', (r, l) => callback(r, l, { now: new Date(Date.now() + 11 * 60_000) })],
    ['a session cookie replayed as the oauth cookie', 400, 'bad_state', async (r, l) => callback(r, l, { cookie: `__Host-oauth=${await sealCookie(r.env.SESSION_SECRET, 'session', { state: l.state, verifier: l.verifier }, 600, new Date())}` })],
    ['an oauth cookie sealed with another secret', 400, 'bad_state', async (r, l) => callback(r, l, { cookie: `__Host-oauth=${await sealCookie('some-other-secret-0123456789', 'oauth', { state: l.state, verifier: l.verifier }, 600, new Date())}` })],
    ['the member denied access at Google', 400, 'oauth_denied', (r, l) => callback(r, l, { extra: `error=access_denied&state=${l.state}` })],
    ['no code', 400, 'bad_request', (r, l) => callback(r, l, { extra: `state=${l.state}` })],
    ['no state', 400, 'bad_request', (r, l) => callback(r, l, { extra: 'code=good-code' })],
    ['Google rejects the code', 502, 'token_exchange_failed', (r, l) => callback(r, l, { code: 'bad-code' })],
    ['Google grants an extra scope', 403, 'scope_violation', (r, l) => { r.google.scope += ' https://www.googleapis.com/auth/gmail.readonly'; return callback(r, l); }],
    ['id_token minted for another client', 401, 'identity_rejected', (r, l) => { r.google.idClaims = { aud: 'someone-elses-client' }; return callback(r, l); }],
    ['id_token from another issuer', 401, 'identity_rejected', (r, l) => { r.google.idClaims = { iss: 'https://evil.example' }; return callback(r, l); }],
    ['id_token already expired', 401, 'identity_rejected', (r, l) => { r.google.idClaims = { exp: Math.floor(Date.now() / 1000) - 60 }; return callback(r, l); }],
    ['e-mail not verified by Google', 401, 'identity_rejected', (r, l) => { r.google.identity.email_verified = false; return callback(r, l); }],
    ['id_token without an e-mail', 401, 'identity_rejected', (r, l) => { r.google.idClaims = { email: undefined }; return callback(r, l); }],
    ['id_token without a subject', 401, 'identity_rejected', (r, l) => { r.google.idClaims = { sub: undefined }; return callback(r, l); }],
  ];
  for (const [name, status, code, run] of cases) {
    const r = rig();
    const login = await startLogin(r);
    const households = (await fx.one<{ n: number }>('SELECT COUNT(*) AS n FROM households')).n;
    const res = await run(r, login);
    assert.equal(res.status, status, name);
    assert.equal(((await res.json()) as { error: string }).error, code, name);
    assert.equal(res.headers.get('set-cookie'), '__Host-oauth=; Max-Age=0; Path=/; Secure; HttpOnly; SameSite=Lax', `${name}: only the oauth cookie is cleared`);
    assert.equal((await fx.one<{ n: number }>('SELECT COUNT(*) AS n FROM households')).n, households, `${name}: no household was created`);
    assert.equal(await countMembers(r.google.identity.email), 0, name);
    assert.equal((await fx.all(`SELECT 1 FROM connectors WHERE kind = 'google_calendar' AND created_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 minute') AND household_id NOT IN (SELECT household_id FROM members)`)).length, 0, `${name}: no tokens were stored`);
  }
});

test('AUTH CALLBACK: without a refresh token a new member is still signed in and told to reconnect the calendar', async () => {
  const r = rig();
  r.google.omitRefreshToken = true;
  const res = await callback(r, await startLogin(r));
  assert.equal(res.status, 302);
  assert.equal(res.headers.get('location'), '/?signed_in=1&calendar=reconnect');
  const session = cookiesOf(res)['__Host-session']!;
  const claims = (await openCookie(r.env.SESSION_SECRET, 'session', session, new Date()))!;
  assert.equal(await findCalendarConnector(rt.db, String(claims['household_id'])), null);
  const me = (await (await r.call('/api/auth/me', { cookie: `__Host-session=${session}` })).json()) as { calendar_connected: boolean };
  assert.equal(me.calendar_connected, false);
});

test('AUTH CALLBACK: two sign-ins racing for the same new address create exactly one household', async () => {
  const r = rig();
  const [a, b] = await Promise.all([startLogin(r), startLogin(r)]);
  const [ra, rb] = await Promise.all([callback(r, a), callback(r, b)]);
  assert.deepEqual([ra.status, rb.status], [302, 302]);
  assert.equal(await countMembers(r.google.identity.email), 1);
  const household = async (res: Response): Promise<unknown> => (await openCookie(r.env.SESSION_SECRET, 'session', cookiesOf(res)['__Host-session']!, new Date()))!['household_id'];
  assert.equal(await household(ra), await household(rb));
});

test('MIDDLEWARE: every protected route answers 401 without a valid session, and unknown routes 404 before any auth', async () => {
  const r = rig();
  const routes: Array<[string, string]> = [
    ['GET', '/api/home'], ['GET', '/api/auth/me'], ['POST', '/api/auth/logout'], ['POST', '/api/ingest/file'], ['POST', '/api/calendar/sync'],
    ['POST', '/api/push/subscribe'], ['POST', `/api/missions/${newRunId()}/start`], ['POST', `/api/missions/${newRunId()}/approve`],
    ['POST', `/api/missions/${newRunId()}/decline`], ['POST', `/api/missions/${newRunId()}/confirm`],
  ];
  for (const [method, path] of routes) {
    const res = await r.call(path, { method });
    assert.equal(res.status, 401, `${method} ${path}`);
    assert.equal(((await res.json()) as { error: string }).error, 'unauthorized');
  }
  assert.equal((await r.call('/api/nope')).status, 404);
  assert.equal((await r.call('/api/missions/not-a-run/start', { method: 'POST' })).status, 404);
  assert.equal((await r.call(`/api/missions/${newRunId()}/explode`, { method: 'POST' })).status, 404);
  const wrong = await r.call('/api/auth/me', { method: 'POST' });
  assert.deepEqual([wrong.status, wrong.headers.get('allow')], [405, 'GET']);
});

test('MIDDLEWARE: a forged, tampered, expired, mis-purposed or mis-keyed session cookie is rejected', async () => {
  const r = rig();
  const me = await signIn(r);
  const value = me.cookie.slice('__Host-session='.length);
  const get = (cookie: string, now?: Date) => r.call('/api/home', { cookie, ...(now ? { now } : {}) });
  assert.equal((await get(me.cookie)).status, 200);
  assert.equal((await get(`__Host-session=${value.slice(0, -2)}xx`)).status, 401, 'tampered');
  assert.equal((await get('__Host-session=')).status, 401);
  assert.equal((await get('__Host-session=garbage')).status, 401);
  assert.equal((await get(me.cookie, new Date(Date.now() + 15 * 86_400_000))).status, 401, 'expired after 14 days');
  assert.equal((await get(me.cookie, new Date(Date.now() + 13 * 86_400_000))).status, 200, 'still good on day 13');
  const claims = { household_id: me.household_id, member_id: me.member_id };
  assert.equal((await get(`__Host-session=${await sealCookie(r.env.SESSION_SECRET, 'oauth', claims, 600, new Date())}`)).status, 401, 'an oauth-purpose cookie is not a session');
  assert.equal((await get(`__Host-session=${await sealCookie('some-other-secret-0123456789', 'session', claims, 600, new Date())}`)).status, 401, 'sealed with another secret');
  assert.equal((await get(`__Host-session=${await sealCookie(r.env.SESSION_SECRET, 'session', { household_id: me.household_id, member_id: 'mem_nobody' }, 600, new Date())}`)).status, 401, 'a member that does not exist');
  const stranger = await signIn(rig());
  assert.equal((await get(`__Host-session=${await sealCookie(r.env.SESSION_SECRET, 'session', { household_id: stranger.household_id, member_id: me.member_id }, 600, new Date())}`)).status, 401, 'a member id paired with someone else\'s household');
});

test('MIDDLEWARE: a deleted household or a removed member loses access immediately', async () => {
  const r = rig();
  const a = await signIn(r);
  assert.equal((await r.call('/api/home', { cookie: a.cookie })).status, 200);
  await rt.db.prepare(`UPDATE households SET deleted_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE household_id = ?`).bind(a.household_id).run();
  assert.equal((await r.call('/api/home', { cookie: a.cookie })).status, 401);
  const b = await signIn(rig());
  await rt.db.prepare('DELETE FROM members WHERE member_id = ?').bind(b.member_id).run();
  assert.equal((await r.call('/api/home', { cookie: b.cookie })).status, 401);
});

test('MIDDLEWARE: cookie-authenticated writes need a same-origin Origin header (CSRF); Bearer tokens do not', async () => {
  const r = rig();
  const me = await signIn(r);
  const post = (headers: Record<string, string>) => r.call('/api/calendar/sync', { method: 'POST', cookie: me.cookie, headers });
  // 409 = authenticated and routed (this fresh household has a connected calendar from sign-in, so it is accepted, not blocked)
  const noOrigin = await post({});
  assert.deepEqual([noOrigin.status, ((await noOrigin.json()) as { error: string }).error], [403, 'csrf']);
  assert.equal((await post({ origin: 'https://evil.example' })).status, 403);
  assert.equal((await post({ origin: `${ORIGIN}.evil.example` })).status, 403);
  assert.equal((await post({ origin: 'null' })).status, 403);
  assert.equal((await post({ origin: ORIGIN })).status, 202, 'same origin passes');
  assert.equal((await r.call('/api/home', { cookie: me.cookie })).status, 200, 'safe methods never need an Origin');
  const bearer = await issueSessionToken(r.env.SESSION_SECRET, { household_id: me.household_id, member_id: me.member_id }, 3600, new Date());
  const viaBearer = await r.call('/api/calendar/sync', { method: 'POST', headers: { authorization: `Bearer ${bearer}` } });
  assert.notEqual(viaBearer.status, 403, 'a bearer token is not an ambient credential, so no Origin is required');
  assert.equal(viaBearer.status === 202 || viaBearer.status === 200, true);
});

test('MIDDLEWARE: an Authorization header that is wrong is never rescued by a valid cookie; a valid Bearer token still works', async () => {
  const r = rig();
  const me = await signIn(r);
  const bearer = await issueSessionToken(r.env.SESSION_SECRET, { household_id: me.household_id, member_id: me.member_id }, 3600, new Date());
  const withHeaders = (authorization: string) => r.call('/api/home', { cookie: me.cookie, headers: { authorization } });
  assert.equal((await withHeaders('Token abc')).status, 401);
  assert.equal((await withHeaders('Bearer')).status, 401);
  assert.equal((await withHeaders(`Bearer ${bearer}x`)).status, 401);
  assert.equal((await withHeaders(`Bearer ${bearer} trailing`)).status, 401);
  assert.equal((await withHeaders(`Bearer ${bearer}`)).status, 200);
  assert.equal((await r.call('/api/home', { headers: { authorization: `Bearer ${bearer}` } })).status, 200);
  const expired = await issueSessionToken(r.env.SESSION_SECRET, { household_id: me.household_id, member_id: me.member_id }, -1, new Date());
  assert.equal((await r.call('/api/home', { headers: { authorization: `Bearer ${expired}` } })).status, 401);
});

test('AUTH: logout clears the session cookie, and /me never reveals tokens or secrets', async () => {
  const r = rig();
  const me = await signIn(r);
  const res = await r.call('/api/auth/logout', { method: 'POST', cookie: me.cookie, headers: { origin: ORIGIN } });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('set-cookie'), '__Host-session=; Max-Age=0; Path=/; Secure; HttpOnly; SameSite=Lax');
  assert.equal((await r.call('/api/auth/logout', { method: 'POST', cookie: me.cookie })).status, 403, 'logout is CSRF-protected too');
  const body = JSON.stringify(await (await r.call('/api/auth/me', { cookie: me.cookie })).json());
  for (const secret of ['rt-secret', 'at-initial', 'client-secret', r.env.SESSION_SECRET, r.env.KEK_B64]) assert.ok(!body.includes(secret));
});

// ═══════════════ the member's side: mission endpoints ═══════════════

const bearerFor = (s: Scenario, household_id = s.h.household_id): Promise<string> =>
  issueSessionToken(s.env.SESSION_SECRET, { household_id, member_id: `mem_${household_id}` }, 3600, new Date());

async function post(s: Scenario, path: string, o: { body?: unknown; raw?: string; token?: string | null } = {}): Promise<Response> {
  const token = o.token === undefined ? await bearerFor(s) : o.token;
  return handleApi(
    new Request(`${ORIGIN}${path}`, {
      method: 'POST',
      headers: { ...(token === null ? {} : { authorization: `Bearer ${token}` }), 'content-type': 'application/json' },
      ...(o.raw !== undefined ? { body: o.raw } : o.body !== undefined ? { body: JSON.stringify(o.body) } : {}),
    }),
    s.env,
  );
}
const bodyOf = async (res: Response): Promise<Record<string, unknown>> => (await res.json()) as Record<string, unknown>;
const setRun = (run_id: string, state: string, patch: Record<string, unknown> = {}) =>
  rt.db.prepare(`UPDATE mission_runs SET state = ?, params_json = json_patch(params_json, ?) WHERE run_id = ?`).bind(state, JSON.stringify(patch), run_id).run();

test('API start: a tap starts MissionRunWF once (instance id = run id); no credit, no card, a foreign card, or the wrong state is refused', async () => {
  const s = await fx.buildScenario();
  const runs = await fx.stage(s.h);
  const run_id = runs['annual_fee_roi']!;
  const start = (id: string, token?: string | null) => post(s, `/api/missions/${id}/start`, token === undefined ? {} : { token });

  const broke = await start(run_id);
  assert.deepEqual([broke.status, (await bodyOf(broke))['error']], [402, 'insufficient_credits']);
  assert.equal(s.workflows.mission.length, 0);

  await fx.grant(s.h, 1);
  const ok = await start(run_id);
  assert.deepEqual([ok.status, await bodyOf(ok)], [202, { status: 'accepted', run_id }]);
  assert.deepEqual(s.workflows.mission, [{ id: run_id, params: { run_id, household_id: s.h.household_id } }]);
  const again = await start(run_id);
  assert.deepEqual([again.status, (await bodyOf(again))['status']], [200, 'already_started']);
  assert.equal(s.workflows.mission.length, 1, 'a double tap starts one instance');
  assert.equal(await fx.balance(s.h), 1, 'starting takes nothing: the Workflow debits at compile');

  await setRun(runs['expiring_credit']!, 'R2_AWAITING');
  const running = await start(runs['expiring_credit']!);
  assert.deepEqual([running.status, (await bodyOf(running))['state']], [200, 'R2_AWAITING']);
  await setRun(runs['anniv_dining']!, 'DECLINED');
  assert.deepEqual([(await start(runs['anniv_dining']!)).status], [409]);
  await setRun(runs['anniv_dining']!, 'R1_STAGED', {});
  await rt.db.prepare(`UPDATE mission_runs SET expires_at = ? WHERE run_id = ?`).bind(new Date(Date.now() - 86_400_000).toISOString(), runs['anniv_dining']!).run();
  const expired = await start(runs['anniv_dining']!);
  assert.deepEqual([expired.status, (await bodyOf(expired))['error']], [409, 'expired']);

  // the "forward receipts" card has nothing to run
  const fresh = await makeHousehold(rt.db);
  const scan = await stageUnlockScan(rt.db, fresh.household_id, new Date());
  const refused = await start(scan, await bearerFor(s, fresh.household_id));
  assert.deepEqual([refused.status, (await bodyOf(refused))['error']], [409, 'not_runnable']);

  // another household's card, a malformed id, no session
  const stranger = await start(run_id, await bearerFor(s, fresh.household_id));
  assert.equal(stranger.status, 404, 'a foreign run id looks like no run at all');
  assert.equal((await start('run_nothing')).status, 404);
  assert.equal((await start(run_id, null)).status, 401);
  assert.equal(s.workflows.mission.length, 1);
});

test('API approve/decline: only the exact staged card can be approved; decisions are recorded once and sent to the waiting Workflow', async () => {
  const s = await fx.buildScenario();
  const runs = await fx.stage(s.h);
  const run_id = runs['annual_fee_roi']!;
  const decide = (id: string, action: 'approve' | 'decline', o: { body?: unknown; raw?: string; token?: string | null } = {}) => post(s, `/api/missions/${id}/${action}`, o);
  const approvals = () => fx.all<{ member_id: string; decision: string; payload_hash: string }>('SELECT member_id, decision, payload_hash FROM approvals WHERE run_id = ?', run_id);

  // not waiting yet
  assert.deepEqual([(await decide(run_id, 'approve', { body: { payload_hash: 'abc' } })).status], [409]);
  // waiting, but not staged by the Workflow yet (no hash)
  await setRun(run_id, 'R2_AWAITING');
  assert.deepEqual([(await decide(run_id, 'approve', { body: { payload_hash: 'abc' } })).status], [409]);
  await setRun(run_id, 'R2_AWAITING', { payload_hash: 'abc' });

  for (const bad of [{ raw: 'not json' }, { raw: '[]' }, { body: {} }, { body: { payload_hash: 5 } }]) {
    assert.equal((await decide(run_id, 'approve', bad)).status, 400, JSON.stringify(bad));
  }
  const stale = await decide(run_id, 'approve', { body: { payload_hash: 'zzz' } });
  assert.deepEqual([stale.status, (await bodyOf(stale))['error']], [409, 'payload_changed']);
  assert.deepEqual([await approvals(), s.workflows.events], [[], []], 'a stale approval records and sends nothing');

  const ok = await decide(run_id, 'approve', { body: { payload_hash: 'abc' } });
  assert.deepEqual([ok.status, (await bodyOf(ok))['status']], [202, 'approval_sent']);
  assert.deepEqual(await approvals(), [{ member_id: `mem_${s.h.household_id}`, decision: 'approve', payload_hash: 'abc' }]);
  assert.deepEqual(s.workflows.events, [{ id: run_id, type: `approve:${run_id}`, payload: { decision: 'approve', payload_hash: 'abc', member_id: `mem_${s.h.household_id}` } }]);
  assert.equal((await decide(run_id, 'approve', { body: { payload_hash: 'abc' } })).status, 202, 'a retry re-sends the event');
  assert.equal((await approvals()).length, 1, 'but the decision is recorded once');

  await setRun(run_id, 'CONFIRMED');
  const late = await decide(run_id, 'approve', { body: { payload_hash: 'abc' } });
  assert.deepEqual([late.status, (await bodyOf(late))['status']], [200, 'already_decided']);

  // decline: a waiting card goes through the Workflow; a staged card that was never started is declined directly
  const waiting = runs['expiring_credit']!;
  await setRun(waiting, 'R2_AWAITING', { payload_hash: 'def' });
  const d1 = await decide(waiting, 'decline');
  assert.deepEqual([d1.status, (await bodyOf(d1))['status']], [202, 'decline_sent']);
  assert.deepEqual(s.workflows.events.at(-1), { id: waiting, type: `approve:${waiting}`, payload: { decision: 'decline', payload_hash: 'def', member_id: `mem_${s.h.household_id}` } });
  const staged = runs['anniv_dining']!;
  const events = s.workflows.events.length;
  const d2 = await decide(staged, 'decline');
  assert.deepEqual([d2.status, (await bodyOf(d2))['status']], [200, 'declined']);
  assert.equal(await stateOf(s, staged), 'DECLINED');
  assert.equal(s.workflows.events.length, events, 'no Workflow exists for it, so no event is sent');
  assert.deepEqual(await ledgerOf(staged), [], 'declining a staged card costs nothing');
  assert.equal((await decide(staged, 'decline')).status, 200, 'declining twice is harmless');

  // a foreign household cannot touch it
  const fresh = await makeHousehold(rt.db);
  assert.equal((await decide(run_id, 'approve', { body: { payload_hash: 'abc' }, token: await bearerFor(s, fresh.household_id) })).status, 404);
  assert.equal((await decide(run_id, 'decline', { token: await bearerFor(s, fresh.household_id) })).status, 404);
});

test('API confirm: only an issued link can be confirmed, and the confirmation goes to the waiting Workflow', async () => {
  const s = await fx.buildScenario();
  const run_id = (await fx.stage(s.h))['anniv_dining']!;
  const confirm = () => post(s, `/api/missions/${run_id}/confirm`);
  const none = await confirm();
  assert.deepEqual([none.status, (await bodyOf(none))['error']], [409, 'no_link_issued']);
  await setRun(run_id, 'R3_LINK_ISSUED');
  const ok = await confirm();
  assert.deepEqual([ok.status, (await bodyOf(ok))['status']], [202, 'confirmation_sent']);
  assert.deepEqual(s.workflows.events, [{ id: run_id, type: `confirmed:${run_id}`, payload: {} }]);
  await setRun(run_id, 'CONFIRMED');
  const done = await confirm();
  assert.deepEqual([done.status, (await bodyOf(done))['status']], [200, 'already_confirmed']);
});

test('API push/subscribe: only push-service endpoints are stored, and only the endpoint (no keys) is kept', async () => {
  const s = await fx.buildScenario();
  const sub = (body: unknown, token?: string | null) => post(s, '/api/push/subscribe', token === undefined ? { body } : { body, token });
  const stored = async (): Promise<string | null> => (await fx.one<{ p: string | null }>('SELECT push_endpoint AS p FROM members WHERE household_id = ?', s.h.household_id)).p;
  assert.equal(await stored(), null);
  const ok = await sub({ endpoint: PUSH_ENDPOINT, keys: { p256dh: 'secret-key', auth: 'secret-auth' } });
  assert.deepEqual([ok.status, (await bodyOf(ok))['status']], [200, 'subscribed']);
  assert.equal(await stored(), JSON.stringify({ endpoint: PUSH_ENDPOINT }));
  for (const bad of [{ endpoint: 'https://evil.test/x' }, { endpoint: 'http://fcm.googleapis.com/x' }, { endpoint: 'https://169.254.169.254/' }, { endpoint: 5 }, {}, { endpoint: `https://fcm.googleapis.com/${'a'.repeat(3000)}` }]) {
    assert.equal((await sub(bad)).status, 400, JSON.stringify(bad).slice(0, 60));
  }
  assert.equal(await stored(), JSON.stringify({ endpoint: PUSH_ENDPOINT }), 'a rejected subscription leaves the old one in place');
  assert.equal((await sub({ endpoint: PUSH_ENDPOINT }, null)).status, 401);
});

test('CONTRACT: the API and MissionRunWF agree on instance ids, event types and payloads, end to end', async () => {
  const s = await fx.buildScenario();
  await fx.grant(s.h, 1);
  const run_id = (await fx.stage(s.h))['anniv_dining']!;
  assert.equal((await post(s, `/api/missions/${run_id}/start`)).status, 202);
  const created = s.workflows.mission[0]!;
  const { step, waits } = directStep(undefined, {
    [`approve:${run_id}`]: async () => {
      const hash = (await getRun(rt.db, s.h.household_id, run_id))!.params['payload_hash'];
      assert.equal((await post(s, `/api/missions/${run_id}/approve`, { body: { payload_hash: hash } })).status, 202);
      return s.workflows.events.find((e) => e.type === `approve:${run_id}`)!.payload;
    },
    [`confirmed:${run_id}`]: async () => {
      assert.equal((await post(s, `/api/missions/${run_id}/confirm`)).status, 202);
      return s.workflows.events.find((e) => e.type === `confirmed:${run_id}`)!.payload;
    },
  });
  const outcome = await runMission(step, fx.deps(s, run_id), created.params);
  assert.equal(outcome.status, 'confirmed');
  assert.deepEqual(waits.map((w) => w.type), s.workflows.events.map((e) => e.type), 'the Workflow waits for exactly the events the API sends');
  assert.deepEqual((await fx.all<{ decision: string }>('SELECT decision FROM approvals WHERE run_id = ?', run_id)).map((a) => a.decision), ['approve']);
  assert.equal(await stateOf(s, run_id), 'CONFIRMED');
});

test('CAPACITY: a card in flight or awaiting a decision holds a Home slot; one that is finished does not', async () => {
  const h = await makeHousehold(rt.db);
  for (const state of CAPACITY_STATES) await seedRun(h, { state });
  assert.equal(await activeStagedCount(rt.db, h.household_id), CAPACITY_STATES.length);
  assert.deepEqual([...CAPACITY_STATES], ['COMPILED', 'R0_READ', 'GATED', 'R1_STAGED', 'R2_AWAITING', 'R2_APPROVED', 'INSUFFICIENT_CREDITS']);
  for (const state of ['R3_LINK_ISSUED', 'CONFIRMED', 'EXPIRED', 'DECLINED', 'FAILED_GATE', 'CANCELED']) await seedRun(h, { state });
  assert.equal(await activeStagedCount(rt.db, h.household_id), CAPACITY_STATES.length);
});

// ═══════════════ policy ═══════════════

test('EGRESS + G5: a blocked fetch is audited against the run, never reaches the network, and fails the next gate pass', async () => {
  const { input, h } = await gateBase();
  const { env } = testEnv(rt);
  let reached = 0;
  const deps = missionDepsFromEnv(env, input.run_id, h.household_id, { network: (async () => { reached += 1; return new Response('ok'); }) as unknown as typeof fetch });
  await assert.rejects(deps.fetch('https://resy.com/cities/ny'), { name: 'PolicyBlockedError' });
  await assert.rejects(deps.fetch('https://api.resy.com/3/find'), { name: 'PolicyBlockedError' });
  await assert.rejects(deps.fetch('https://www.opentable.com/x'), { name: 'PolicyBlockedError' });
  assert.equal(reached, 0, 'nothing blocked ever reached the network');
  const rows = await fx.all<{ detail_json: string }>(`SELECT detail_json FROM audit_log WHERE household_id = ? AND action = 'egress_blocked'`, h.household_id);
  assert.equal(rows.length, 3);
  assert.deepEqual(rows.map((r) => (JSON.parse(r.detail_json) as { run_id: string }).run_id), [input.run_id, input.run_id, input.run_id]);
  assert.deepEqual(await failedOf(input), ['G5']);
  assert.equal((await deps.fetch('https://api.openai.com/v1/x')).status, 200);
  assert.equal(reached, 1);
  assert.equal((await fx.all(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'egress_blocked'`, h.household_id)).length, 3, 'allowed hosts write nothing');
});

test('STRUCTURE: MissionRunWF is bound and exported, the Workflow class is a thin wrapper, secrets stay out of [vars], and no payment API is referenced', () => {
  const root = resolve(process.env['PROJECT_ROOT'] ?? process.cwd());
  const read = (p: string): string => readFileSync(join(root, p), 'utf8');
  const toml = read('wrangler.toml');
  assert.match(toml, /\[\[workflows\]\]\s*\nname = "mission-run"\s*\nbinding = "MISSION_RUN_WF"\s*\nclass_name = "MissionRunWF"/);
  assert.match(read('src/index.ts'), /export \{ MissionRunWF \} from '\.\/workflows\/missionRunWF';/);
  const wrapper = read('src/workflows/missionRunWF.ts');
  assert.ok(wrapper.split('\n').length < 30, 'all logic lives in src/missions/runPipeline.ts, where the tests can reach it');
  assert.match(wrapper, /runMission\(/);
  assert.ok(!/runGates|debitCredit|transitionRun/.test(wrapper));
  const vars = /\[vars\]\n([\s\S]*?)(?=\n\[|\n#|$)/.exec(toml)![1]!;
  assert.deepEqual([...vars.matchAll(/^(\w+) = /gm)].map((m) => m[1]), ['APP_NAME', 'INBOUND_DOMAIN'], 'no secret or key is committed in [vars]');
  const files = ['src/missions/gates.ts', 'src/missions/runPipeline.ts', 'src/missions/ledger.ts', 'src/missions/runState.ts', 'src/missions/deeplinks.ts', 'src/notify/webPush.ts', 'src/workers/auth.ts', 'src/workers/missions.ts', 'src/workflows/missionRunWF.ts'];
  for (const f of files) {
    const code = read(f).replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
    assert.ok(!/api\.stripe\.com|api\.duffel\.com|payment_intent|paymentMethod|card_number|cvv|routing_number/i.test(code), `${f} must not touch payment APIs or card data (Stripe lives in src/billing, Duffel in src/travel)`);
    assert.ok(!/(?<!async\s)(?<![.\w$])fetch\s*\(/.test(code), `${f} must not call the global fetch()`);
  }
});

// ── END ──
