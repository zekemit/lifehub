/**
 * Milestone 5 behavioural suite: the two-bucket credit ledger, Stripe top-ups and auto-recharge, multi-credit missions with a
 * per-credit Gate G8 cap, the weekly Life Receipt and its shareable card, DropAlarmWF and PriceWatchWF.
 * Real local D1, R2 and KV; only the network (Stripe, Duffel, Google, models, push) and the Workflows engine are faked.
 */

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { attemptAutoRecharge } from '../src/billing/autoRecharge';
import { creditBalances, debitCredit, grantAnnualDrips, grantMonthly, grantPack, grantTrial, refundCredit, trialGrantStatement } from '../src/billing/credits';
import { PACKS, ROLLOVER_CAP } from '../src/billing/packs';
import { stripeClient } from '../src/billing/stripe';
import { buildCopy } from '../src/missions/copy';
import { activeStagedCount, insertRun } from '../src/missions/engine';
import { maxRunCostUsd, runGates } from '../src/missions/gates';
import { getHome } from '../src/missions/home';
import { getRun } from '../src/missions/runState';
import { missionDepsFromEnv, runMission, type ApprovalEvent } from '../src/missions/runPipeline';
import { createGuardedFetch } from '../src/net/egress';
import { issueSessionToken } from '../src/workers/session';
import { alarmRunId, runDropAlarm } from '../src/concierge/dropAlarmPipeline';
import { CHECKS_PER_DAY, createPriceWatch, parseWatchParams } from '../src/concierge/flights';
import { dailyCheck, runPriceWatch, type Check, type PriceWatchDeps } from '../src/concierge/priceWatchPipeline';
import { tripSpan } from '../src/concierge/readers';
import { amountToCents, searchFlights } from '../src/travel/duffel';
import { RELEASE_LEAD_MS, addDays, createDropAlarm, loadRule, releaseAtMs, venueLinkTarget, type ReleaseRule } from '../src/concierge/venues';
import { exchangeCode } from '../src/connectors/googleCalendar';
import { writeFact } from '../src/graph/writeFact';
import { decayFacts } from '../src/maintenance/decay';
import { groundReceipt, runLifeReceipt, type ReceiptDeps, type ReceiptPayload } from '../src/receipt/pipeline';
import { weekStartOf, type ReceiptDraft } from '../src/receipt/assemble';
import { escapeHtml, renderReceiptHtml, renderReceiptText } from '../src/receipt/render';
import { bucketDollars, renderShareCard } from '../src/receipt/shareCard';
import type { EmailSender, Env } from '../src/types';
import { runScheduled } from '../src/workers/scheduled';
import { handleApi } from '../src/workers/api';
import { openTestRuntime, type TestRuntime } from './helpers';
import { directStep, makeHousehold, openaiReply, testEnv } from './helpers/m2';
import { day } from './helpers/m3';
import { PUSH_ENDPOINT, PUSH_HOST, fixtures, newNetwork, vapidKeys, type DuffelOffer } from './helpers/m4';
import { APP_ORIGIN, WEBHOOK_SECRET, makeVenue, invoicePaid, packIntent, packSession, postWebhook, stripeEvent, stripeSignature, type StripeEvent } from './helpers/m5';

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
const AT = (): string => new Date().toISOString();
const newRunId = (): string => `run_${randomUUID().replace(/-/g, '')}`;

async function seedRun(h: Household, o: { run_id?: string; state?: string; template_key?: string; credit_cost?: number } = {}): Promise<string> {
  const run_id = o.run_id ?? newRunId();
  await rt.db
    .prepare(`INSERT INTO mission_runs (run_id, household_id, template_key, template_version, origin, params_json, state, credit_cost) VALUES (?, ?, ?, 1, 'trigger', '{}', ?, ?)`)
    .bind(run_id, h.household_id, o.template_key ?? 'unit', o.state ?? 'R1_STAGED', o.credit_cost ?? 1)
    .run();
  return run_id;
}

interface RawRow {
  delta: number;
  sub?: number;
  pur?: number;
  reason: string;
  event?: string | null;
}
const rawLedger = (h: Household, r: RawRow) =>
  rt.db
    .prepare(`INSERT INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id, stripe_event_id) VALUES (?, ?, ?, ?, ?, ?, NULL, ?)`)
    .bind(`led_raw_${randomUUID()}`, h.household_id, r.delta, r.sub ?? r.delta, r.pur ?? 0, r.reason, r.event ?? null)
    .run();

// ═══════════════ schema (migration 0003) ═══════════════

test('SCHEMA: credit_ledger takes trial_grant and enforces the two-bucket rules in the table itself', async () => {
  const h = await makeHousehold(rt.db);
  await rawLedger(h, { delta: 5, reason: 'trial_grant', event: `trial:${h.household_id}` });
  await rawLedger(h, { delta: 10, sub: 0, pur: 10, reason: 'pack_purchase', event: `pack:${randomUUID()}` });
  await assert.rejects(rawLedger(h, { delta: 5, reason: 'trial_grant', event: `trial:${h.household_id}` }), /UNIQUE/i, 'one trial grant per household, by stripe_event_id');
  const bad: Array<[string, RawRow]> = [
    ['delta must equal sub + pur', { delta: 5, sub: 2, pur: 2, reason: 'admin_adjust' }],
    ['a pack lands in purchased credits only', { delta: 10, sub: 10, pur: 0, reason: 'pack_purchase' }],
    ['a pack must add credits', { delta: 0, sub: 0, pur: 0, reason: 'pack_purchase' }],
    ['a grant lands in subscription credits only', { delta: 10, sub: 0, pur: 10, reason: 'monthly_grant' }],
    ['a trial grant lands in subscription credits only', { delta: 5, sub: 0, pur: 5, reason: 'trial_grant' }],
    ['expiry can never touch purchased credits', { delta: -3, sub: 0, pur: -3, reason: 'expiry' }],
    ['expiry only removes', { delta: 3, sub: 3, pur: 0, reason: 'expiry' }],
    ['an unknown reason', { delta: 1, reason: 'bonus' }],
  ];
  for (const [why, row] of bad) await assert.rejects(rawLedger(h, row), /constraint/i, why);
  const view = await fx.one<{ balance: number; subscription_credits: number; purchased_credits: number }>('SELECT balance, subscription_credits, purchased_credits FROM credit_balance WHERE household_id = ?', h.household_id);
  assert.deepEqual(view, { balance: 15, subscription_credits: 5, purchased_credits: 10 });
  await assert.rejects(rt.db.prepare('UPDATE credit_ledger SET delta = 99 WHERE household_id = ?').bind(h.household_id).run(), /append-only/);
  await assert.rejects(rt.db.prepare('DELETE FROM credit_ledger WHERE household_id = ?').bind(h.household_id).run(), /append-only/);
});

test('SCHEMA: runs carry credit_cost and the INSUFFICIENT_CREDITS state; households carry opt-in auto-recharge defaults', async () => {
  const h = await makeHousehold(rt.db);
  const run_id = await seedRun(h);
  assert.equal((await fx.one<{ credit_cost: number }>('SELECT credit_cost FROM mission_runs WHERE run_id = ?', run_id)).credit_cost, 1, 'default 1');
  await rt.db.prepare(`UPDATE mission_runs SET state = 'INSUFFICIENT_CREDITS', credit_cost = 5 WHERE run_id = ?`).bind(run_id).run();
  await assert.rejects(rt.db.prepare('UPDATE mission_runs SET credit_cost = -1 WHERE run_id = ?').bind(run_id).run(), /constraint/i);
  await assert.rejects(rt.db.prepare(`UPDATE mission_runs SET state = 'EXPLODED' WHERE run_id = ?`).bind(run_id).run(), /constraint/i);
  const d = await fx.one<Record<string, unknown>>('SELECT stripe_default_payment_method_id AS pm, auto_recharge_enabled AS enabled, auto_recharge_pack_sku AS sku, auto_recharge_monthly_cap_cents AS cap FROM households WHERE household_id = ?', h.household_id);
  assert.deepEqual(d, { pm: null, enabled: 0, sku: 'pack_10', cap: 2000 });
  for (const sql of [`UPDATE households SET auto_recharge_enabled = 2`, `UPDATE households SET auto_recharge_pack_sku = 'pack_999'`, `UPDATE households SET auto_recharge_monthly_cap_cents = -5`]) {
    await assert.rejects(rt.db.prepare(`${sql} WHERE household_id = ?`).bind(h.household_id).run(), /constraint/i, sql);
  }
});

test('SCHEMA: at most two active flight watches per household, by trigger, however the row arrives', async () => {
  const h = await makeHousehold(rt.db);
  const insert = (active = 1, perDay = 1) =>
    rt.db.prepare(`INSERT INTO price_watches (watch_id, household_id, kind, params_json, ceiling_cents, checks_per_day, active) VALUES (?, ?, 'flight', '{}', 50000, ?, ?)`).bind(`pwt_${randomUUID()}`, h.household_id, perDay, active).run();
  await insert();
  await insert();
  await assert.rejects(insert(), /at most 2 active/);
  await insert(0); // an inactive one is not counted
  await assert.rejects(insert(1, 2), /constraint/i, 'checks_per_day is at most 1');
  const inactive = await fx.one<{ watch_id: string }>('SELECT watch_id FROM price_watches WHERE household_id = ? AND active = 0', h.household_id);
  await assert.rejects(rt.db.prepare('UPDATE price_watches SET active = 1 WHERE watch_id = ?').bind(inactive.watch_id).run(), /at most 2 active/, 'reactivating counts too');
  const one = await fx.one<{ watch_id: string }>('SELECT watch_id FROM price_watches WHERE household_id = ? AND active = 1 LIMIT 1', h.household_id);
  await rt.db.prepare('UPDATE price_watches SET active = 0 WHERE watch_id = ?').bind(one.watch_id).run();
  await rt.db.prepare('UPDATE price_watches SET active = 1 WHERE watch_id = ?').bind(inactive.watch_id).run(); // a slot freed up
  const other = await makeHousehold(rt.db);
  await rt.db.prepare(`INSERT INTO price_watches (watch_id, household_id, kind, params_json, ceiling_cents) VALUES (?, ?, 'flight', '{}', 1)`).bind(`pwt_${randomUUID()}`, other.household_id).run(); // the cap is per household
});

// ═══════════════ the two-bucket ledger ═══════════════

test('LEDGER: missions spend subscription credits FIRST, then purchased ones; a refund puts back exactly what was taken', async () => {
  const h = await makeHousehold(rt.db);
  await fx.grant(h, 3);
  await grantPack(rt.db, h.household_id, { key: `pack:${randomUUID()}`, sku: 'pack_10' }, AT());
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 13, subscription: 3, purchased: 10 });

  const [r1, r2, r3] = [await seedRun(h), await seedRun(h), await seedRun(h)] as [string, string, string];
  assert.deepEqual(await debitCredit(rt.db, h.household_id, r1, 2, AT()), { ok: true, debited: true, balance: 11, from_subscription: 2, from_purchased: 0 });
  assert.deepEqual(await debitCredit(rt.db, h.household_id, r2, 3, AT()), { ok: true, debited: true, balance: 8, from_subscription: 1, from_purchased: 2 }, 'the last subscription credit, then two purchased');
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 8, subscription: 0, purchased: 8 });
  assert.deepEqual(await debitCredit(rt.db, h.household_id, r3, 4, AT()), { ok: true, debited: true, balance: 4, from_subscription: 0, from_purchased: 4 });

  const row = await fx.one<{ delta: number; sub_delta: number; pur_delta: number }>(`SELECT delta, sub_delta, pur_delta FROM credit_ledger WHERE entry_id = ?`, `led_debit_${r2}`);
  assert.deepEqual(row, { delta: -3, sub_delta: -1, pur_delta: -2 }, 'one ledger row records both buckets');

  assert.equal(await refundCredit(rt.db, h.household_id, r2, AT()), true);
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 7, subscription: 1, purchased: 6 }, 'one subscription and two purchased credits come back');
  assert.equal(await refundCredit(rt.db, h.household_id, r2, AT()), false, 'a refund happens once');
  assert.equal(await refundCredit(rt.db, (await makeHousehold(rt.db)).household_id, r2, AT()), false, 'and only for its own household');
  assert.equal((await debitCredit(rt.db, h.household_id, r2, 3, AT())).ok, true, 'a run id debits once: a retry is a no-op that still succeeds');
});

test('LEDGER: a multi-credit debit is all or nothing across the two buckets', async () => {
  const h = await makeHousehold(rt.db);
  await fx.grant(h, 2);
  await grantPack(rt.db, h.household_id, { key: `pack:${randomUUID()}`, sku: 'pack_10' }, AT());
  const run = await seedRun(h, { credit_cost: 5 });
  assert.deepEqual(await debitCredit(rt.db, h.household_id, run, 13, AT()), { ok: false, balance: 12 }, 'one credit short: nothing is taken');
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 12, subscription: 2, purchased: 10 });
  assert.equal((await fx.all('SELECT 1 FROM credit_ledger WHERE run_id = ?', run)).length, 0);
  assert.deepEqual(await debitCredit(rt.db, h.household_id, run, 5, AT()), { ok: true, debited: true, balance: 7, from_subscription: 2, from_purchased: 3 });
  await assert.rejects(debitCredit(rt.db, h.household_id, run, -1, AT()), /non-negative integer/);
  await assert.rejects(debitCredit(rt.db, h.household_id, run, 1.5, AT()), /non-negative integer/);
});

test('LEDGER: racing multi-credit debits can never overdraw either bucket', async () => {
  const h = await makeHousehold(rt.db);
  await fx.grant(h, 5);
  await grantPack(rt.db, h.household_id, { key: `pack:${randomUUID()}`, sku: 'pack_10' }, AT());
  // 15 credits, ten missions of 3 racing: exactly five can win
  const runs = await Promise.all(Array.from({ length: 10 }, () => seedRun(h, { credit_cost: 3 })));
  const results = await Promise.all(runs.map((id) => debitCredit(rt.db, h.household_id, id, 3, AT())));
  const won = results.filter((r) => r.ok);
  assert.equal(won.length, 5);
  assert.equal(results.filter((r) => !r.ok).length, 5);
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 0, subscription: 0, purchased: 0 });
  const took = won.reduce((a, r) => ({ s: a.s + (r.ok ? r.from_subscription : 0), p: a.p + (r.ok ? r.from_purchased : 0) }), { s: 0, p: 0 });
  assert.deepEqual(took, { s: 5, p: 10 }, 'every credit was spent once, from the right bucket');
});

test('ROLLOVER: a renewal expires unused subscription credits above 15, then grants; purchased credits are never touched', async () => {
  const h = await makeHousehold(rt.db);
  await fx.grant(h, 40);
  for (const [i, sku] of (['pack_10', 'pack_25'] as const).entries()) await grantPack(rt.db, h.household_id, { key: `pack:${h.household_id}:${i}`, sku }, AT());
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 75, subscription: 40, purchased: 35 });

  assert.deepEqual(await grantMonthly(rt.db, h.household_id, { key: 'evt_renewal_1', credits: 15 }, AT()), { granted: true, expired: 25 });
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 65, subscription: 30, purchased: 35 }, '40 -> 15 (25 expire) + 15 granted; the packs are untouched');
  const expiry = await fx.one<{ delta: number; sub_delta: number; pur_delta: number }>(`SELECT delta, sub_delta, pur_delta FROM credit_ledger WHERE stripe_event_id = 'expiry:evt_renewal_1'`);
  assert.deepEqual(expiry, { delta: -25, sub_delta: -25, pur_delta: 0 });

  assert.deepEqual(await grantMonthly(rt.db, h.household_id, { key: 'evt_renewal_1', credits: 15 }, AT()), { granted: false, expired: 0 }, 'a replay changes nothing, and expires nothing twice');
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 65, subscription: 30, purchased: 35 });
  assert.deepEqual(await grantMonthly(rt.db, h.household_id, { key: 'evt_renewal_2', credits: 15 }, AT()), { granted: true, expired: 15 });
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 65, subscription: 30, purchased: 35 });
});

test('ROLLOVER: the cap is exact (15 stays, 16 loses one), and packs of every size survive any number of renewals', async () => {
  assert.equal(ROLLOVER_CAP, 15);
  const at15 = await makeHousehold(rt.db);
  await fx.grant(at15, 15);
  assert.deepEqual(await grantMonthly(rt.db, at15.household_id, { key: `evt_${randomUUID()}`, credits: 15 }, AT()), { granted: true, expired: 0 });
  assert.equal((await creditBalances(rt.db, at15.household_id)).subscription, 30);
  const at16 = await makeHousehold(rt.db);
  await fx.grant(at16, 16);
  assert.deepEqual(await grantMonthly(rt.db, at16.household_id, { key: `evt_${randomUUID()}`, credits: 15 }, AT()), { granted: true, expired: 1 });
  assert.equal((await creditBalances(rt.db, at16.household_id)).subscription, 30);

  const trial = await makeHousehold(rt.db);
  await grantTrial(rt.db, trial.household_id, AT());
  assert.deepEqual(await grantMonthly(rt.db, trial.household_id, { key: `evt_${randomUUID()}`, credits: 15 }, AT()), { granted: true, expired: 0 });
  assert.equal((await creditBalances(rt.db, trial.household_id)).subscription, 20, 'trial credits count as subscription credits and are under the cap');

  const packs = await makeHousehold(rt.db);
  for (const sku of ['pack_10', 'pack_25', 'pack_60'] as const) await grantPack(rt.db, packs.household_id, { key: `pack:${packs.household_id}:${sku}`, sku }, AT());
  const bought = PACKS.pack_10.credits + PACKS.pack_25.credits + PACKS.pack_60.credits;
  assert.equal(bought, 95);
  for (let month = 0; month < 6; month++) await grantMonthly(rt.db, packs.household_id, { key: `evt_${packs.household_id}_${month}`, credits: 15 }, AT());
  const final = await creditBalances(rt.db, packs.household_id);
  assert.equal(final.purchased, 95, 'six renewals later every purchased credit is still there');
  assert.equal(final.subscription, 30);
});

test('PACKS: three verified SKUs at the specified credits and prices', () => {
  assert.deepEqual(PACKS, { pack_10: { credits: 10, price_cents: 999 }, pack_25: { credits: 25, price_cents: 1999 }, pack_60: { credits: 60, price_cents: 3999 } });
});

test('TRIAL: the Day-1 grant is +5 subscription credits, once, and only for the creation that won', async () => {
  const h = await makeHousehold(rt.db);
  assert.equal(await grantTrial(rt.db, h.household_id, AT()), true);
  assert.equal(await grantTrial(rt.db, h.household_id, AT()), false);
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 5, subscription: 5, purchased: 0 });
  const row = await fx.one<{ reason: string; stripe_event_id: string }>(`SELECT reason, stripe_event_id FROM credit_ledger WHERE household_id = ?`, h.household_id);
  assert.deepEqual(row, { reason: 'trial_grant', stripe_event_id: `trial:${h.household_id}` });
  // riding in the creation batch: no member, no grant
  const ghost = `hh_ghost_${randomUUID().slice(0, 8)}`;
  await trialGrantStatement(rt.db, ghost, AT(), `mem_${ghost}`).run();
  assert.equal((await fx.all('SELECT 1 FROM credit_ledger WHERE household_id = ?', ghost)).length, 0);
});

test('ANNUAL: annual plans are granted +12 each month while paid; the drip is once a month, and never for monthly, canceled or lapsed plans', async () => {
  const mk = async (plan: string, periodEnd: string | null, grantedThisMonth = false): Promise<Household> => {
    const h = await makeHousehold(rt.db);
    await rt.db.prepare('UPDATE households SET plan = ?, plan_period_end = ? WHERE household_id = ?').bind(plan, periodEnd, h.household_id).run();
    if (grantedThisMonth) await grantMonthly(rt.db, h.household_id, { key: `evt_${randomUUID()}`, credits: 12 }, AT());
    return h;
  };
  const future = new Date(Date.now() + 200 * 86_400_000).toISOString();
  const past = new Date(Date.now() - 86_400_000).toISOString();
  const paying = await mk('annual', future);
  const lapsed = await mk('annual', past);
  const monthly = await mk('monthly', future);
  const canceled = await mk('canceled', future);
  const already = await mk('annual', future, true);
  const at = new Date();
  await grantAnnualDrips(rt.db, at, 12);
  const sub = async (h: Household): Promise<number> => (await creditBalances(rt.db, h.household_id)).subscription;
  assert.equal(await sub(paying), 12);
  assert.equal(await sub(lapsed), 0);
  assert.equal(await sub(monthly), 0);
  assert.equal(await sub(canceled), 0);
  assert.equal(await sub(already), 12, 'a household already granted this month (by its invoice) gets nothing more');
  await grantAnnualDrips(rt.db, at, 12);
  assert.equal(await sub(paying), 12, 'running again in the same month is a no-op');
  const key = `annual_drip:${paying.household_id}:${at.toISOString().slice(0, 7)}`;
  assert.equal((await fx.all('SELECT 1 FROM credit_ledger WHERE stripe_event_id = ?', key)).length, 1);
  // next month, the plan still running: another 12, with the rollover cap applied first
  const nextMonth = new Date(Date.UTC(at.getUTCFullYear(), at.getUTCMonth() + 1, 15, 3));
  await fx.grant(paying, 10); // 22 unused: above the cap of 15
  await grantAnnualDrips(rt.db, nextMonth, 12);
  assert.equal(await sub(paying), 27, '22 -> 15 (7 expire) + 12');
});

// ═══════════════ Stripe webhook ═══════════════

const billingEnv = (extra: Partial<Env> = {}): Env => testEnv(rt, { STRIPE_WEBHOOK_SECRET: WEBHOOK_SECRET, STRIPE_SECRET_KEY: 'sk_test_0123456789', ...extra }).env;

async function customerHousehold(o: { customer?: string | null } = {}): Promise<Household & { customer: string }> {
  const h = await makeHousehold(rt.db);
  const customer = o.customer === undefined ? `cus_${randomUUID().replace(/-/g, '').slice(0, 14)}` : (o.customer ?? '');
  if (o.customer !== null) await rt.db.prepare('UPDATE households SET stripe_customer_id = ? WHERE household_id = ?').bind(customer, h.household_id).run();
  return { ...h, customer };
}
const bodyOf = async (res: Response): Promise<Record<string, unknown>> => (await res.clone().json()) as Record<string, unknown>;
const ledgerCount = async (h: Household): Promise<number> => (await fx.one<{ n: number }>('SELECT COUNT(*) AS n FROM credit_ledger WHERE household_id = ?', h.household_id)).n;
const eventRows = async (id: string): Promise<number> => (await fx.all('SELECT 1 FROM stripe_events WHERE event_id = ?', id)).length;

test('WEBHOOK signature: the raw body must verify; tampering, replays outside 300 s, and missing or malformed signatures are all refused', async () => {
  const env = billingEnv();
  const at = new Date(1_800_000_000_000);
  const sec = Math.floor(at.getTime() / 1000);
  const ping = stripeEvent('ping.test', { note: 'hello' });
  const raw = JSON.stringify(ping);
  const send = (body: StripeEvent | string, o: Parameters<typeof postWebhook>[2] = {}) => postWebhook(env, body, { now: at, ...o });
  const code = async (r: Response): Promise<unknown> => (await bodyOf(r))['error'];

  const ok = await send(raw, { t: sec });
  assert.deepEqual([ok.status, (await bodyOf(ok))['outcome']], [200, 'ignored_type']);
  assert.equal(await eventRows(ping.id), 1);

  const other = stripeEvent('ping.test', { note: 'forged' });
  const tampered = await send(JSON.stringify(other), { signature: stripeSignature(raw, { t: sec }) });
  assert.deepEqual([tampered.status, await code(tampered)], [400, 'bad_signature'], 'a signature for one body does not cover another');
  assert.equal(await eventRows(other.id), 0, 'nothing was processed or recorded');
  const respaced = await send(JSON.stringify(ping, null, 2), { signature: stripeSignature(raw, { t: sec }) });
  assert.deepEqual([respaced.status, await code(respaced)], [400, 'bad_signature'], 'the RAW bytes are verified, not a re-serialisation');
  const wrongSecret = await send(raw, { secret: 'whsec_somebody_else', t: sec });
  assert.deepEqual([wrongSecret.status, await code(wrongSecret)], [400, 'bad_signature']);
  const missing = await send(raw, { signature: null });
  assert.deepEqual([missing.status, await code(missing)], [400, 'missing_signature']);
  for (const header of ['garbage', 't=abc,v1=ff', 'v1=deadbeef', `t=${sec}`, `t=${sec},v1=`, '']) {
    const r = await send(raw, { signature: header });
    assert.equal(r.status, 400, header);
    assert.ok(['malformed_signature', 'missing_signature'].includes(String(await code(r))), header);
  }
  const notHex = await send(raw, { signature: `t=${sec},v1=${'zz'.repeat(32)}` });
  assert.deepEqual([notHex.status, await code(notHex)], [400, 'bad_signature']);

  // 300 seconds is the tolerance, in both directions
  assert.equal((await send(raw, { t: sec - 300 })).status, 200, 'exactly 300 s old is accepted');
  for (const t of [sec - 301, sec + 301]) {
    const r = await send(JSON.stringify(stripeEvent('ping.test', {})), { t });
    assert.deepEqual([r.status, await code(r)], [400, 'stale_timestamp'], String(t - sec));
  }

  // two v1 signatures (a secret being rolled): any one matching is enough
  const good = stripeSignature(raw, { t: sec });
  const rolled = await send(raw, { signature: `${good.replace('v1=', 'v1=' + '00'.repeat(32) + ',v1=')}` });
  assert.equal(rolled.status, 200);

  // signed but not JSON / not an event
  const garbage = 'not json at all';
  const g = await send(garbage, { t: sec });
  assert.deepEqual([g.status, await code(g)], [400, 'bad_json']);
  const noId = await send(JSON.stringify({ type: 'invoice.paid', data: { object: {} } }), { t: sec });
  assert.deepEqual([noId.status, await code(noId)], [400, 'bad_event']);

  // not configured: refuse rather than accept unverifiable events
  const bare = testEnv(rt).env;
  assert.equal((await postWebhook(bare, raw)).status, 503);
  const get = await handleApi(new Request(`${APP_ORIGIN}/api/webhooks/stripe`), env);
  assert.deepEqual([get.status, get.headers.get('allow')], [405, 'POST']);
});

test('WEBHOOK invoice.paid: +15 for a monthly plan, expiring only subscription credits above the cap; a replay never grants twice', async () => {
  const env = billingEnv();
  const h = await customerHousehold();
  await fx.grant(h, 40);
  await grantPack(rt.db, h.household_id, { key: `pack:${randomUUID()}`, sku: 'pack_25' }, AT());
  const event = invoicePaid({ customer: h.customer, interval: 'month' });
  const first = await postWebhook(env, event);
  assert.deepEqual([first.status, (await bodyOf(first))['outcome']], [200, 'granted_15_expired_25']);
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 55, subscription: 30, purchased: 25 }, '40 -> 15, +15; the 25 purchased credits are untouched');
  assert.equal((await fx.one<{ plan: string }>('SELECT plan FROM households WHERE household_id = ?', h.household_id)).plan, 'monthly');
  assert.equal((await fx.one<{ stripe_event_id: string }>(`SELECT stripe_event_id FROM credit_ledger WHERE household_id = ? AND reason = 'monthly_grant'`, h.household_id)).stripe_event_id, event.id);

  const replay = await postWebhook(env, event);
  assert.deepEqual([replay.status, (await bodyOf(replay))['duplicate']], [200, true]);
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 55, subscription: 30, purchased: 25 });

  // the SAME event delivered twice at once still grants once
  const racer = await customerHousehold();
  const ev = invoicePaid({ customer: racer.customer, interval: 'month' });
  await Promise.all([postWebhook(env, ev), postWebhook(env, ev), postWebhook(env, ev)]);
  assert.equal((await creditBalances(rt.db, racer.household_id)).subscription, 15);
  assert.equal((await fx.all(`SELECT 1 FROM credit_ledger WHERE household_id = ? AND reason = 'monthly_grant'`, racer.household_id)).length, 1);

  // next month's invoice is a new event: grants again, with the cap applied again
  const next = await postWebhook(env, invoicePaid({ customer: h.customer, interval: 'month' }));
  assert.equal((await bodyOf(next))['outcome'], 'granted_15_expired_15');
  assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: 55, subscription: 30, purchased: 25 });
});

test('WEBHOOK invoice.paid: annual plans get +12 and a period end; $0 invoices, unknown customers and odd intervals grant nothing', async () => {
  const env = billingEnv();
  const h = await customerHousehold();
  const periodEnd = Math.floor(Date.now() / 1000) + 365 * 86_400;
  const annual = await postWebhook(env, invoicePaid({ customer: h.customer, interval: 'year', periodEnd }));
  assert.equal((await bodyOf(annual))['outcome'], 'granted_12_expired_0');
  const row = await fx.one<{ plan: string; plan_period_end: string }>('SELECT plan, plan_period_end FROM households WHERE household_id = ?', h.household_id);
  assert.deepEqual([row.plan, row.plan_period_end], ['annual', new Date(periodEnd * 1000).toISOString()]);
  assert.equal((await creditBalances(rt.db, h.household_id)).subscription, 12);

  const trial = await customerHousehold();
  const zero = await postWebhook(env, invoicePaid({ customer: trial.customer, interval: 'month', amount_paid: 0 }));
  assert.equal((await bodyOf(zero))['outcome'], 'zero_amount_no_grant');
  assert.equal((await creditBalances(rt.db, trial.household_id)).total, 0, 'starting and cancelling a trial prints no credits');
  assert.equal((await fx.one<{ plan: string }>('SELECT plan FROM households WHERE household_id = ?', trial.household_id)).plan, 'monthly', 'but the plan is recorded');

  const stranger = await postWebhook(env, invoicePaid({ customer: 'cus_nobody_knows', interval: 'month' }));
  assert.deepEqual([stranger.status, (await bodyOf(stranger))['outcome']], [200, 'unknown_household']);

  const byMeta = await customerHousehold({ customer: null });
  const linked = await postWebhook(env, invoicePaid({ customer: 'cus_new_one', interval: 'month', household_id: byMeta.household_id }));
  assert.equal((await bodyOf(linked))['outcome'], 'granted_15_expired_0', 'found through the household id in the subscription metadata');
  assert.equal((await fx.one<{ c: string }>('SELECT stripe_customer_id AS c FROM households WHERE household_id = ?', byMeta.household_id)).c, 'cus_new_one');

  const weekly = await customerHousehold();
  const odd = await postWebhook(env, stripeEvent('invoice.paid', { customer: weekly.customer, lines: { data: [{ price: { recurring: { interval: 'week' } } }] } }));
  assert.equal((await bodyOf(odd))['outcome'], 'ignored_interval');
  assert.equal(await ledgerCount(weekly), 0);
});

test('WEBHOOK top-up packs: every SKU grants its credits as purchased credits, once, whichever of the two events comes first', async () => {
  const env = billingEnv();
  for (const [sku, credits, cents] of [['pack_10', 10, 999], ['pack_25', 25, 1999], ['pack_60', 60, 3999]] as const) {
    const h = await customerHousehold();
    const pi = `pi_${randomUUID().replace(/-/g, '').slice(0, 14)}`;
    const session = packSession({ household_id: h.household_id, sku, pi, amount: cents, customer: h.customer });
    const res = await postWebhook(env, session);
    assert.equal((await bodyOf(res))['outcome'], `granted_${credits}`, sku);
    assert.deepEqual(await creditBalances(rt.db, h.household_id), { total: credits, subscription: 0, purchased: credits }, sku);
    const row = await fx.one<{ reason: string; stripe_event_id: string; sub_delta: number }>(`SELECT reason, stripe_event_id, sub_delta FROM credit_ledger WHERE household_id = ?`, h.household_id);
    assert.deepEqual(row, { reason: 'pack_purchase', stripe_event_id: `pack:${pi}`, sub_delta: 0 });
    // the payment_intent.succeeded for the same purchase is a no-op, and a replay of either is too
    const intent = await postWebhook(env, packIntent({ household_id: h.household_id, sku, pi, amount: cents, customer: h.customer }));
    assert.equal((await bodyOf(intent))['outcome'], 'already_granted', sku);
    assert.equal((await postWebhook(env, session)).status, 200);
    assert.equal((await creditBalances(rt.db, h.household_id)).purchased, credits, `${sku}: still ${credits}`);
  }
  // the other order: the payment intent first
  const h = await customerHousehold();
  const pi = `pi_${randomUUID().replace(/-/g, '').slice(0, 14)}`;
  assert.equal((await bodyOf(await postWebhook(env, packIntent({ household_id: h.household_id, sku: 'pack_25', pi, amount: 1999 }))))['outcome'], 'granted_25');
  assert.equal((await bodyOf(await postWebhook(env, packSession({ household_id: h.household_id, sku: 'pack_25', pi, amount: 1999 }))))['outcome'], 'already_granted');
  assert.equal((await creditBalances(rt.db, h.household_id)).purchased, 25);
});

test('WEBHOOK top-up packs: the SKU table decides what was bought; wrong amounts, unpaid sessions and unknown SKUs grant nothing', async () => {
  const env = billingEnv();
  const h = await customerHousehold();
  const pi = () => `pi_${randomUUID().replace(/-/g, '').slice(0, 14)}`;
  const out = async (e: StripeEvent): Promise<unknown> => (await bodyOf(await postWebhook(env, e)))['outcome'];
  assert.equal(await out(packSession({ household_id: h.household_id, sku: 'pack_25', pi: pi(), amount: 999 })), 'amount_mismatch_no_grant', 'pack_25 is $19.99, not $9.99');
  assert.equal((await fx.all(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'pack_amount_mismatch'`, h.household_id)).length, 1);
  assert.equal(await out(packSession({ household_id: h.household_id, sku: 'pack_10', pi: pi(), amount: 999, paid: false })), 'not_paid_yet');
  assert.equal(await out(packSession({ household_id: h.household_id, sku: 'pack_1000', pi: pi(), amount: 999 })), 'ignored_unknown_sku');
  assert.equal(await out(packIntent({ household_id: h.household_id, sku: 'pack_60', pi: pi(), amount: 1 })), 'amount_mismatch_no_grant');
  assert.equal(await ledgerCount(h), 0);
  // a metadata "credits" field cannot raise the grant
  assert.equal(await out(packSession({ household_id: h.household_id, sku: 'pack_10', pi: pi(), amount: 999, credits: 9999 })), 'granted_10');
  assert.equal((await creditBalances(rt.db, h.household_id)).purchased, 10);
  // an unknown household is acknowledged, not retried forever
  assert.equal(await out(packSession({ household_id: 'hh_nope', sku: 'pack_10', pi: pi(), amount: 999 })), 'unknown_household');
  // a payment intent that is not a pack (a subscription's, say) is ignored
  assert.equal(await out(stripeEvent('payment_intent.succeeded', { id: pi(), metadata: {} })), 'ignored_not_a_pack');
});

test('WEBHOOK: the saved card is kept for auto-recharge only when the member agreed (setup_future_usage) or it already is one', async () => {
  const env = billingEnv();
  const h = await customerHousehold();
  const pm = async (): Promise<string | null> => (await fx.one<{ p: string | null }>('SELECT stripe_default_payment_method_id AS p FROM households WHERE household_id = ?', h.household_id)).p;
  const pi = () => `pi_${randomUUID().replace(/-/g, '').slice(0, 14)}`;
  await postWebhook(env, packIntent({ household_id: h.household_id, sku: 'pack_10', pi: pi(), amount: 999, customer: h.customer, paymentMethod: 'pm_one_off' }));
  assert.equal(await pm(), null, 'a one-off card is not kept');
  await postWebhook(env, packIntent({ household_id: h.household_id, sku: 'pack_10', pi: pi(), amount: 999, customer: h.customer, paymentMethod: 'pm_saved', saveCard: true }));
  assert.equal(await pm(), 'pm_saved');
  await postWebhook(env, packIntent({ household_id: h.household_id, sku: 'pack_10', pi: pi(), amount: 999, customer: h.customer, paymentMethod: 'pm_recharge', kind: 'auto_recharge' }));
  assert.equal(await pm(), 'pm_recharge');
});

test('WEBHOOK customer.subscription.deleted: the plan is canceled and purchased credits stay', async () => {
  const env = billingEnv();
  const h = await customerHousehold();
  await grantPack(rt.db, h.household_id, { key: `pack:${randomUUID()}`, sku: 'pack_60' }, AT());
  await rt.db.prepare(`UPDATE households SET plan = 'annual', plan_period_end = ? WHERE household_id = ?`).bind(new Date(Date.now() + 1e10).toISOString(), h.household_id).run();
  const res = await postWebhook(env, stripeEvent('customer.subscription.deleted', { id: 'sub_1', customer: h.customer }));
  assert.equal((await bodyOf(res))['outcome'], 'plan_canceled');
  const row = await fx.one<{ plan: string; plan_period_end: string | null }>('SELECT plan, plan_period_end FROM households WHERE household_id = ?', h.household_id);
  assert.deepEqual(row, { plan: 'canceled', plan_period_end: null });
  assert.equal((await creditBalances(rt.db, h.household_id)).purchased, 60, 'what the member bought is theirs to keep');
  assert.equal((await bodyOf(await postWebhook(env, stripeEvent('customer.subscription.deleted', { customer: 'cus_unknown' }))))['outcome'], 'unknown_household');
  // a canceled household is not dripped annual credits
  await grantAnnualDrips(rt.db, new Date(), 12);
  assert.equal((await creditBalances(rt.db, h.household_id)).subscription, 0);
});

// ═══════════════ credits API: balance, checkout, auto-recharge ═══════════════

type Net = ReturnType<typeof newNetwork>;
const bearer = (env: Env, h: Household, member_id = `mem_${h.household_id}`): Promise<string> => issueSessionToken(env.SESSION_SECRET, { household_id: h.household_id, member_id }, 3600, new Date());

async function api(env: Env, net: Net, path: string, o: { method?: string; token?: string | null; h?: Household; body?: unknown; raw?: string } = {}): Promise<Response> {
  const token = o.token === undefined ? (o.h === undefined ? null : await bearer(env, o.h)) : o.token;
  const method = o.method ?? (o.body !== undefined || o.raw !== undefined ? 'POST' : 'GET');
  return handleApi(
    new Request(`${APP_ORIGIN}${path}`, {
      method,
      headers: { 'content-type': 'application/json', ...(token === null ? {} : { authorization: `Bearer ${token}` }) },
      ...(o.raw !== undefined ? { body: o.raw } : o.body !== undefined ? { body: JSON.stringify(o.body) } : {}),
    }),
    env,
    { fetch: createGuardedFetch({ db: rt.db, fetchImpl: net.fetch, ttlMs: 0 }) },
  );
}

test('API credits/balance: both buckets, the pack catalogue, and the auto-recharge settings; sign-in required', async () => {
  const env = billingEnv();
  const net = newNetwork();
  const h = await makeHousehold(rt.db);
  assert.equal((await api(env, net, '/api/credits/balance', { token: null })).status, 401);
  await fx.grant(h, 7);
  await grantPack(rt.db, h.household_id, { key: `pack:${randomUUID()}`, sku: 'pack_25' }, AT());
  const res = await api(env, net, '/api/credits/balance', { h });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('cache-control'), 'no-store');
  const body = await bodyOf(res);
  assert.deepEqual([body['balance'], body['subscription_credits'], body['purchased_credits'], body['plan']], [32, 7, 25, 'trial']);
  assert.deepEqual(body['packs'], [{ sku: 'pack_10', credits: 10, price_cents: 999 }, { sku: 'pack_25', credits: 25, price_cents: 1999 }, { sku: 'pack_60', credits: 60, price_cents: 3999 }]);
  assert.deepEqual(body['auto_recharge'], { enabled: false, pack_sku: 'pack_10', monthly_cap_cents: 2000, spent_this_month_cents: 0, has_payment_method: false });
  // one household never sees another's credits
  const other = await makeHousehold(rt.db);
  assert.equal((await bodyOf(await api(env, net, '/api/credits/balance', { h: other })))['balance'], 0);
});

test('API credits/checkout: validates the SKU, asks Stripe for exactly that pack through the guarded egress, and returns the checkout URL', async () => {
  const env = billingEnv();
  const net = newNetwork();
  const h = await makeHousehold(rt.db);
  const post = (body: unknown, raw?: string) => api(env, net, '/api/credits/checkout', { h, ...(raw === undefined ? { body } : { raw }) });

  for (const bad of [{}, { pack_sku: 'pack_1000' }, { pack_sku: 10 }, { pack_sku: null }, { pack_sku: '__proto__' }, { pack_sku: 'toString' }]) {
    const r = await post(bad);
    assert.deepEqual([r.status, (await bodyOf(r))['error']], [400, 'bad_pack_sku'], JSON.stringify(bad));
  }
  assert.equal((await post(null, 'not json')).status, 400);
  assert.equal(net.stripe.calls.length, 0, 'a bad request never reaches Stripe');
  const bare = testEnv(rt).env;
  assert.equal((await api(bare, net, '/api/credits/checkout', { h, body: { pack_sku: 'pack_10' } })).status, 503, 'no Stripe key: billing is off, not broken');

  const ok = await post({ pack_sku: 'pack_25', price_cents: 1, credits: 1000, unit_amount: 1 });
  assert.equal(ok.status, 200);
  assert.deepEqual(await bodyOf(ok), { checkout_url: 'https://checkout.stripe.com/c/pay/cs_test_1', session_id: 'cs_test_1' });
  assert.ok(net.hosts.includes('api.stripe.com'));
  const call = net.stripe.calls[0]!;
  const f = call.form;
  assert.equal(call.path, '/v1/checkout/sessions');
  assert.equal(call.headers['authorization'], 'Bearer sk_test_0123456789');
  assert.equal(call.headers['stripe-version'], '2024-06-20');
  assert.deepEqual(
    [f.get('mode'), f.get('client_reference_id'), f.get('metadata[household_id]'), f.get('metadata[pack_sku]'), f.get('metadata[credits]')],
    ['payment', h.household_id, h.household_id, 'pack_25', '25'],
  );
  assert.deepEqual([f.get('line_items[0][price_data][unit_amount]'), f.get('line_items[0][price_data][currency]'), f.get('line_items[0][quantity]')], ['1999', 'usd', '1'], 'the price comes from the SKU table, never from the request');
  assert.equal(f.get('line_items[0][price_data][product_data][name]'), 'TestApp credits (25)', 'the product name comes from APP_NAME');
  assert.deepEqual([f.get('payment_intent_data[metadata][pack_sku]'), f.get('payment_intent_data[setup_future_usage]')], ['pack_25', 'off_session']);
  assert.equal(f.get('customer_creation'), 'always');
  assert.equal(f.get('customer'), null);
  assert.equal(f.get('success_url'), `${APP_ORIGIN}/?checkout=success&session_id={CHECKOUT_SESSION_ID}`);
  assert.equal(f.get('cancel_url'), `${APP_ORIGIN}/?checkout=canceled`);
  // a household that already has a Stripe customer reuses it
  const known = await customerHousehold();
  await api(env, net, '/api/credits/checkout', { h: known, body: { pack_sku: 'pack_10' } });
  const second = net.stripe.calls.at(-1)!.form;
  assert.deepEqual([second.get('customer'), second.get('customer_creation'), second.get('line_items[0][price_data][unit_amount]')], [known.customer, null, '999']);
  // Stripe being down is a 502 with nothing leaked
  net.stripe.failWith = null;
  const failing = newNetwork();
  failing.fetch = (async (i: RequestInfo | URL) => (String(i).includes('stripe.com') ? new Response('{"error":{"message":"secret internal detail"}}', { status: 500 }) : new Response('{}'))) as typeof fetch;
  const bad = await api(env, failing, '/api/credits/checkout', { h, body: { pack_sku: 'pack_10' } });
  assert.deepEqual([bad.status, (await bodyOf(bad))['error']], [502, 'stripe_error']);
  assert.ok(!JSON.stringify(await bodyOf(bad)).includes('secret internal detail'));
});

test('API credits/auto-recharge: owner only, needs a saved card, and its cap must be a real amount', async () => {
  const env = billingEnv();
  const net = newNetwork();
  const h = await customerHousehold({ customer: null });
  const adult = `mem_adult_${randomUUID().slice(0, 8)}`;
  await rt.db.prepare(`INSERT INTO members (member_id, household_id, email, role, age_attested_18) VALUES (?, ?, ?, 'adult', 1)`).bind(adult, h.household_id, `adult-${randomUUID()}@example.org`).run();
  const set = async (body: unknown, who: 'owner' | 'adult' = 'owner') => api(env, net, '/api/credits/auto-recharge', { token: await bearer(env, h, who === 'owner' ? `mem_${h.household_id}` : adult), body });
  assert.equal((await api(env, net, '/api/credits/auto-recharge', { token: null, body: { enabled: true } })).status, 401);

  const denied = await set({ enabled: true }, 'adult');
  assert.deepEqual([denied.status, (await bodyOf(denied))['error']], [403, 'owner_only']);
  for (const bad of [{}, { enabled: 'yes' }, { enabled: 1 }]) assert.equal((await set(bad)).status, 400, JSON.stringify(bad));

  const noCard = await set({ enabled: true, pack_sku: 'pack_25', monthly_cap_cents: 5000 });
  assert.deepEqual([noCard.status, (await bodyOf(noCard))['error'], (await bodyOf(noCard))['checkout']], [409, 'no_payment_method', '/api/credits/checkout']);
  assert.equal((await fx.one<{ e: number }>('SELECT auto_recharge_enabled AS e FROM households WHERE household_id = ?', h.household_id)).e, 0, 'refused: nothing changed');
  assert.equal((await set({ enabled: false })).status, 200, 'turning it off never needs a card');

  // the card arrives the way it really does: through a pack purchase the member agreed to keep it for
  await postWebhook(env, packIntent({ household_id: h.household_id, sku: 'pack_10', pi: `pi_${randomUUID().slice(0, 12)}`, amount: 999, customer: 'cus_member', paymentMethod: 'pm_member', saveCard: true }));
  const view = async (): Promise<unknown> => (await bodyOf(await api(env, net, '/api/credits/balance', { h })))['auto_recharge'];
  assert.equal(((await view()) as { has_payment_method: boolean }).has_payment_method, true);

  const bads: Array<[unknown, string]> = [
    [{ enabled: true, pack_sku: 'pack_9' }, 'bad_pack_sku'],
    [{ enabled: true, pack_sku: 'pack_25', monthly_cap_cents: 1998 }, 'bad_cap'],
    [{ enabled: true, monthly_cap_cents: 998 }, 'bad_cap'],
    [{ enabled: true, monthly_cap_cents: 50_001 }, 'bad_cap'],
    [{ enabled: true, monthly_cap_cents: 2000.5 }, 'bad_cap'],
    [{ enabled: true, monthly_cap_cents: '2000' }, 'bad_cap'],
    [{ enabled: true, monthly_cap_cents: -1 }, 'bad_cap'],
  ];
  for (const [body, code] of bads) assert.equal(((await bodyOf(await set(body)))['error']), code, JSON.stringify(body));
  const on = await set({ enabled: true, pack_sku: 'pack_25', monthly_cap_cents: 1999 });
  assert.equal(on.status, 200, 'a cap equal to the pack price is the smallest allowed');
  assert.deepEqual(await view(), { enabled: true, pack_sku: 'pack_25', monthly_cap_cents: 1999, spent_this_month_cents: 0, has_payment_method: true });
  const limit = await set({ enabled: true, monthly_cap_cents: 50_000, pack_sku: 'pack_60' });
  assert.equal(limit.status, 200);
  assert.deepEqual(await view(), { enabled: true, pack_sku: 'pack_60', monthly_cap_cents: 50_000, spent_this_month_cents: 0, has_payment_method: true });
  assert.equal((await set({ enabled: true })).status, 200);
  assert.deepEqual(await view(), { enabled: true, pack_sku: 'pack_60', monthly_cap_cents: 50_000, spent_this_month_cents: 0, has_payment_method: true }, 'omitted settings are kept');
  assert.equal((await set({ enabled: false })).status, 200);
  assert.equal(((await view()) as { enabled: boolean }).enabled, false);
});

// ═══════════════ auto-recharge and multi-credit missions in MissionRunWF ═══════════════

const approveWith = (run_id: string, hh: string): (() => Promise<ApprovalEvent>) => async () => {
  const run = (await getRun(rt.db, hh, run_id))!;
  return { decision: 'approve', payload_hash: String(run.params['payload_hash']), member_id: `mem_${hh}` };
};
type Scenario = Awaited<ReturnType<typeof fx.buildScenario>>;
/** A flagship household whose pipeline has a Stripe client (STRIPE_SECRET_KEY set), so auto-recharge can actually charge. */
const stripeScenario = (): Promise<Scenario> => fx.buildScenario({ envExtra: { STRIPE_SECRET_KEY: 'sk_test_0123456789' } });
async function runWith(s: Scenario, run_id: string, events: Record<string, unknown> = {}) {
  const { step, log, waits } = directStep(undefined, events);
  const outcome = await runMission(step, fx.deps(s, run_id), { run_id, household_id: s.h.household_id });
  return { outcome, log: log.map((l) => l.name), waits };
}
const stateOf = async (s: Scenario, run_id: string): Promise<string> => (await getRun(rt.db, s.h.household_id, run_id))!.state;
const ledgerOf = (run_id: string) => fx.all<{ reason: string; sub_delta: number; pur_delta: number }>('SELECT reason, sub_delta, pur_delta FROM credit_ledger WHERE run_id = ? ORDER BY entry_id', run_id);
const autoRows = (h: Household) => fx.all<{ status: string; amount_cents: number; payment_intent_id: string | null; error: string | null }>('SELECT status, amount_cents, payment_intent_id, error FROM auto_recharges WHERE household_id = ? ORDER BY created_at, recharge_id', h.household_id);
const optIn = (h: Household, o: { sku?: string; cap?: number; pm?: string | null } = {}) =>
  rt.db
    .prepare(`UPDATE households SET stripe_customer_id = 'cus_auto', stripe_default_payment_method_id = ?, auto_recharge_enabled = 1, auto_recharge_pack_sku = ?, auto_recharge_monthly_cap_cents = ? WHERE household_id = ?`)
    .bind(o.pm === undefined ? 'pm_saved' : o.pm, o.sku ?? 'pack_10', o.cap ?? 2000, h.household_id)
    .run();

test('INSUFFICIENT_CREDITS: the run halts cleanly, shows on Home pointing at checkout, and a top-up plus a new tap starts it again', async () => {
  const s = await fx.buildScenario();
  const runs = await fx.stage(s.h);
  const run_id = runs['annual_fee_roi']!;
  const first = await runWith(s, run_id);
  assert.deepEqual(first.outcome, { status: 'insufficient_credits', auto_recharge: 'disabled' });
  assert.equal(await stateOf(s, run_id), 'INSUFFICIENT_CREDITS');
  assert.deepEqual(first.log, ['compile', 'debit_credit']);
  const failure = (await getRun(rt.db, s.h.household_id, run_id))!.params['failure'] as Record<string, unknown>;
  assert.deepEqual([failure['reason'], failure['top_up'], failure['auto_recharge'], failure['cost']], ['INSUFFICIENT_CREDITS', '/api/credits/checkout', 'disabled', 1]);
  const card = (await getHome(rt.db, s.h.household_id, new Date())).cards.find((c) => c.run_id === run_id)!;
  assert.deepEqual([card.state, card.top_up], ['INSUFFICIENT_CREDITS', { endpoint: '/api/credits/checkout', auto_recharge: 'disabled' }]);
  assert.equal(await activeStagedCount(rt.db, s.h.household_id), 3, 'the halted card still holds its Home slot');

  // the API tells the member the same before starting anything
  const tap = (id: string) => api(s.env, s.net, `/api/missions/${id}/start`, { h: s.h, body: {} });
  const broke = await tap(run_id);
  assert.deepEqual([broke.status, (await bodyOf(broke))['top_up'], (await bodyOf(broke))['needed']], [402, '/api/credits/checkout', 1]);

  // the member tops up and taps again: a NEW Workflow instance takes over the same run
  await grantPack(rt.db, s.h.household_id, { key: `pack:${randomUUID()}`, sku: 'pack_10' }, AT());
  const retried = await tap(run_id);
  assert.deepEqual([retried.status, (await bodyOf(retried))['status']], [202, 'accepted']);
  assert.deepEqual(s.workflows.mission.map((w) => w.id), [`${run_id}~1`]);
  assert.equal((await getRun(rt.db, s.h.household_id, run_id))!.params['attempt'], 1);
  assert.equal((await tap(run_id)).status, 200, 'a double tap starts one instance');
  assert.equal(s.workflows.mission.length, 1);

  const created = s.workflows.mission[0]!;
  const { step } = directStep(undefined, {
    [`approve:${run_id}`]: async () => {
      const hash = (await getRun(rt.db, s.h.household_id, run_id))!.params['payload_hash'];
      assert.equal((await api(s.env, s.net, `/api/missions/${run_id}/approve`, { h: s.h, body: { payload_hash: hash } })).status, 202);
      return s.workflows.events.find((e) => e.type === `approve:${run_id}`)!.payload;
    },
  });
  const outcome = await runMission(step, fx.deps(s, run_id), created.params);
  assert.equal(outcome.status, 'confirmed');
  assert.equal(s.workflows.events[0]!.id, `${run_id}~1`, 'the approval goes to the NEW instance');
  assert.equal((await getRun(rt.db, s.h.household_id, run_id))!.params['failure'], undefined, 'the old failure is cleared');
  assert.deepEqual(await creditBalances(rt.db, s.h.household_id), { total: 9, subscription: 0, purchased: 9 });
});

test('INSUFFICIENT_CREDITS: a halted card can be declined directly, costing nothing', async () => {
  const s = await fx.buildScenario();
  const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
  await runWith(s, run_id);
  const res = await api(s.env, s.net, `/api/missions/${run_id}/decline`, { h: s.h, body: {} });
  assert.deepEqual([res.status, (await bodyOf(res))['status']], [200, 'declined']);
  assert.equal(await stateOf(s, run_id), 'DECLINED');
  assert.deepEqual(await ledgerOf(run_id), []);
});

test('AUTO-RECHARGE: an opted-in household with a saved card is topped up off-session, the mission carries on, and the pack is granted once', async () => {
  const s = await stripeScenario();
  await optIn(s.h, { sku: 'pack_25' });
  const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
  const { outcome, log } = await runWith(s, run_id, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id) });
  assert.equal(outcome.status, 'confirmed');
  assert.deepEqual(log.slice(0, 3), ['compile', 'debit_credit', 'r0_read']);

  assert.equal(s.net.stripe.calls.length, 1);
  const call = s.net.stripe.calls[0]!;
  assert.equal(call.path, '/v1/payment_intents');
  assert.equal(call.headers['idempotency-key'], `rch_${run_id}_0`);
  assert.deepEqual(
    [call.form.get('amount'), call.form.get('currency'), call.form.get('customer'), call.form.get('payment_method'), call.form.get('off_session'), call.form.get('confirm')],
    ['1999', 'usd', 'cus_auto', 'pm_saved', 'true', 'true'],
  );
  assert.deepEqual([call.form.get('metadata[household_id]'), call.form.get('metadata[pack_sku]'), call.form.get('metadata[kind]'), call.form.get('metadata[recharge_id]')], [s.h.household_id, 'pack_25', 'auto_recharge', `rch_${run_id}_0`]);
  const rows = await autoRows(s.h);
  assert.equal(rows.length, 1);
  assert.deepEqual([rows[0]!.status, rows[0]!.amount_cents, rows[0]!.error], ['succeeded', 1999, null]);
  const pi = rows[0]!.payment_intent_id!;
  assert.match(pi, /^pi_test_/);
  assert.deepEqual(await creditBalances(rt.db, s.h.household_id), { total: 24, subscription: 0, purchased: 24 }, '+25 purchased, -1 for the mission');
  assert.deepEqual(await ledgerOf(run_id), [{ reason: 'mission_debit', sub_delta: 0, pur_delta: -1 }]);
  const step = await fx.one<{ gate_result_json: string }>('SELECT gate_result_json FROM steps WHERE run_id = ? AND step_no = 11', run_id);
  assert.equal(JSON.parse(step.gate_result_json).auto_recharged, true);

  // Stripe's own payment_intent.succeeded for that charge must not grant the pack a second time
  const env = billingEnv();
  const hook = await postWebhook(env, packIntent({ household_id: s.h.household_id, sku: 'pack_25', pi, amount: 1999, customer: 'cus_auto', paymentMethod: 'pm_saved', kind: 'auto_recharge', recharge_id: `rch_${run_id}_0` }));
  assert.equal((await bodyOf(hook))['outcome'], 'already_granted');
  assert.equal((await creditBalances(rt.db, s.h.household_id)).purchased, 24);
});

test('AUTO-RECHARGE: it is never attempted unless the household opted in, has a saved card, and has cap room', async () => {
  const cases: Array<[string, (h: Household) => Promise<unknown>, string]> = [
    ['not opted in', async () => undefined, 'disabled'],
    ['opted in without a saved card', (h) => optIn(h, { pm: null }), 'no_payment_method'],
    ['the cap is below the pack price', (h) => optIn(h, { sku: 'pack_25', cap: 1998 }), 'monthly_cap'],
  ];
  for (const [name, setup, reason] of cases) {
    const s = await fx.buildScenario();
    await setup(s.h);
    if (name.startsWith('opted in without')) await rt.db.prepare('UPDATE households SET stripe_customer_id = NULL WHERE household_id = ?').bind(s.h.household_id).run();
    const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
    const { outcome } = await runWith(s, run_id);
    assert.deepEqual(outcome, { status: 'insufficient_credits', auto_recharge: reason }, name);
    assert.equal(await stateOf(s, run_id), 'INSUFFICIENT_CREDITS', name);
    assert.equal(s.net.stripe.calls.length, 0, `${name}: Stripe was never called`);
    assert.deepEqual(await autoRows(s.h), [], name);
    assert.equal((await creditBalances(rt.db, s.h.household_id)).total, 0, name);
  }
});

test('AUTO-RECHARGE monthly cap: this month\'s spend plus the pack price must fit; pending counts, failed and last month do not', async () => {
  const s = await stripeScenario();
  await optIn(s.h, { sku: 'pack_10', cap: 2000 });
  const runs = await fx.stage(s.h);
  const row = (id: string, status: string, cents: number, when: Date) =>
    rt.db.prepare(`INSERT INTO auto_recharges (recharge_id, household_id, pack_sku, credits, amount_cents, status, created_at, updated_at) VALUES (?, ?, 'pack_10', 10, ?, ?, ?, ?)`).bind(id, s.h.household_id, cents, status, when.toISOString(), when.toISOString()).run();
  const lastMonth = new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth() - 1, 15));
  await row('rch_old', 'succeeded', 1999, lastMonth); // last month: ignored
  await row('rch_failed', 'failed', 1999, new Date()); // failed: nothing was charged
  const first = await runWith(s, runs['annual_fee_roi']!, { [`approve:${runs['annual_fee_roi']!}`]: approveWith(runs['annual_fee_roi']!, s.h.household_id) });
  assert.equal(first.outcome.status, 'confirmed', '$9.99 fits a $20.00 cap; neither old row counted');
  assert.equal(s.net.stripe.calls.length, 1);
  // $9.99 is spent. A second pack ($19.98 in total) still fits $20.00, a third ($29.97) does not.
  assert.equal((await creditBalances(rt.db, s.h.household_id)).total, 9, '10 bought, 1 spent');
  const spend = async (runKey: string): Promise<{ status: string; reason: string | null }> => {
    // spend the remaining 9 so the next run is short again
    const left = (await creditBalances(rt.db, s.h.household_id)).total;
    if (left > 0) await debitCredit(rt.db, s.h.household_id, await seedRun(s.h), left, AT());
    const r = await runWith(s, runs[runKey]!, {});
    return r.outcome.status === 'insufficient_credits' ? { status: r.outcome.status, reason: r.outcome.auto_recharge } : { status: r.outcome.status, reason: null };
  };
  const second = await spend('expiring_credit');
  assert.equal(second.status, 'expired', 'a second pack fits ($19.98 <= $20.00) and the run proceeds to wait for approval, which never comes');
  assert.equal(s.net.stripe.calls.length, 2);
  assert.equal(((await autoRows(s.h)).filter((r) => r.status === 'succeeded').length), 3, 'the old one, plus this month\'s two');
  const third = await spend('anniv_dining');
  assert.deepEqual(third, { status: 'insufficient_credits', reason: 'monthly_cap' }, '$29.97 would exceed $20.00');
  assert.equal(s.net.stripe.calls.length, 2, 'no third charge');
  assert.equal(await stateOf(s, runs['anniv_dining']!), 'INSUFFICIENT_CREDITS');
});

test('AUTO-RECHARGE failures: a declined card, a card that needs the member, or Stripe being down halts the run and never counts against the cap', async () => {
  for (const [name, arrange] of [
    ['declined', (n: Net) => { n.stripe.failWith = { status: 402, code: 'card_declined' }; }],
    ['needs authentication', (n: Net) => { n.stripe.intentStatus = 'requires_action'; }],
    ['Stripe is down', (n: Net) => { n.stripe.failWith = { status: 500, code: 'api_error' }; }],
  ] as const) {
    const s = await stripeScenario();
    await optIn(s.h, { sku: 'pack_10', cap: 1500 });
    arrange(s.net);
    const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
    const { outcome } = await runWith(s, run_id);
    assert.deepEqual(outcome, { status: 'insufficient_credits', auto_recharge: 'payment_failed' }, name);
    const rows = await autoRows(s.h);
    assert.equal(rows.length, 1, name);
    assert.equal(rows[0]!.status, 'failed', name);
    assert.ok(rows[0]!.error !== null && rows[0]!.error.length <= 200, name);
    assert.equal((await creditBalances(rt.db, s.h.household_id)).total, 0, `${name}: nothing was granted`);
    assert.equal(await stateOf(s, run_id), 'INSUFFICIENT_CREDITS', name);
  }
  // the failed attempt left the cap untouched: the next try (card fixed) is allowed
  const s = await stripeScenario();
  await optIn(s.h, { sku: 'pack_10', cap: 1500 });
  s.net.stripe.failWith = { status: 402, code: 'card_declined' };
  const runs = await fx.stage(s.h);
  await runWith(s, runs['annual_fee_roi']!);
  s.net.stripe.failWith = null;
  const again = await runWith(s, runs['expiring_credit']!, {});
  assert.equal(again.outcome.status, 'expired', 'the second run was charged and proceeded to wait for approval');
  assert.deepEqual((await autoRows(s.h)).map((r) => r.status), ['failed', 'succeeded']);
});

test('AUTO-RECHARGE is idempotent per run and attempt, and racing runs cannot both fit under one cap', async () => {
  const s = await fx.buildScenario();
  await optIn(s.h, { sku: 'pack_10', cap: 1500 });
  const deps = { db: rt.db, stripe: stripeClient({ apiKey: 'sk_test', fetch: createGuardedFetch({ db: rt.db, fetchImpl: s.net.fetch, ttlMs: 0 }) }), now: () => new Date() };
  const run = await seedRun(s.h);
  const a = await attemptAutoRecharge(deps, s.h.household_id, run, 0);
  const b = await attemptAutoRecharge(deps, s.h.household_id, run, 0);
  assert.deepEqual([a.status, b.status], ['recharged', 'recharged']);
  assert.equal(s.net.stripe.calls.length, 1, 'a retried step does not charge twice');
  assert.equal((await creditBalances(rt.db, s.h.household_id)).purchased, 10, 'and grants the pack once');
  assert.equal((await autoRows(s.h)).length, 1);

  // two different runs racing for the last room under the cap: one wins
  const racer = await fx.buildScenario();
  await optIn(racer.h, { sku: 'pack_10', cap: 1500 });
  const rdeps = { db: rt.db, stripe: stripeClient({ apiKey: 'sk_test', fetch: createGuardedFetch({ db: rt.db, fetchImpl: racer.net.fetch, ttlMs: 0 }) }), now: () => new Date() };
  const [r1, r2] = await Promise.all([seedRun(racer.h), seedRun(racer.h)]);
  const results = await Promise.all([attemptAutoRecharge(rdeps, racer.h.household_id, r1, 0), attemptAutoRecharge(rdeps, racer.h.household_id, r2, 0)]);
  assert.deepEqual(results.map((r) => r.status).sort(), ['declined', 'recharged']);
  assert.equal(racer.net.stripe.calls.length, 1, 'exactly one charge');
  assert.deepEqual(results.filter((r) => r.status === 'declined').map((r) => (r as { reason: string }).reason), ['monthly_cap']);
});

test('API start: a household short of credits may still start when auto-recharge is allowed right now', async () => {
  const s = await fx.buildScenario();
  const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
  const tap = () => api(s.env, s.net, `/api/missions/${run_id}/start`, { h: s.h, body: {} });
  assert.equal((await tap()).status, 402);
  await optIn(s.h, { sku: 'pack_10', cap: 2000 });
  const started = await tap();
  assert.deepEqual([started.status, (await bodyOf(started))['status']], [202, 'accepted']);
  assert.equal(s.workflows.mission.length, 1);
  assert.equal(s.net.stripe.calls.length, 0, 'starting charges nothing; the Workflow does, if it must');
});

// ── multi-credit missions ──

async function withTemplateCost(cost: number, body: () => Promise<void>): Promise<void> {
  await rt.db.prepare(`UPDATE mission_templates SET credits_cost = ? WHERE template_key = 'annual_fee_roi'`).bind(cost).run();
  try {
    await body();
  } finally {
    await rt.db.prepare(`UPDATE mission_templates SET credits_cost = 1 WHERE template_key = 'annual_fee_roi'`).run();
  }
}

test('MULTI-CREDIT: a 3-credit mission debits 3 (subscription first), records credit_cost on the run, and a gate failure refunds all 3 bucket for bucket', async () => {
  await withTemplateCost(3, async () => {
    const s = await fx.buildScenario();
    await fx.grant(s.h, 2);
    await grantPack(rt.db, s.h.household_id, { key: `pack:${randomUUID()}`, sku: 'pack_10' }, AT());
    const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
    s.net.workerReply = () => openaiReply({ script: 'This fee is deductible, so keep the card.' });
    const { outcome } = await runWith(s, run_id, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id) });
    assert.deepEqual(outcome, { status: 'failed_gate', stage: 'r1', failed: ['G7'] });
    assert.equal((await fx.one<{ credit_cost: number }>('SELECT credit_cost FROM mission_runs WHERE run_id = ?', run_id)).credit_cost, 3);
    assert.deepEqual(await ledgerOf(run_id), [{ reason: 'mission_debit', sub_delta: -2, pur_delta: -1 }, { reason: 'mission_refund', sub_delta: 2, pur_delta: 1 }]);
    assert.deepEqual(await creditBalances(rt.db, s.h.household_id), { total: 12, subscription: 2, purchased: 10 }, 'made whole in the same buckets');
  });
});

test('MULTI-CREDIT: a successful 3-credit run keeps its debit, and the API refuses to start one the balance cannot cover', async () => {
  await withTemplateCost(3, async () => {
    const s = await fx.buildScenario();
    await fx.grant(s.h, 2);
    const run_id = (await fx.stage(s.h))['annual_fee_roi']!;
    const short = await api(s.env, s.net, `/api/missions/${run_id}/start`, { h: s.h, body: {} });
    assert.deepEqual([short.status, (await bodyOf(short))['needed'], (await bodyOf(short))['balance']], [402, 3, 2]);
    await fx.grant(s.h, 2);
    const { outcome } = await runWith(s, run_id, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id) });
    assert.equal(outcome.status, 'confirmed');
    assert.deepEqual(await creditBalances(rt.db, s.h.household_id), { total: 1, subscription: 1, purchased: 0 });
  });
});

test('GATE G8: the model-spend ceiling is $0.25 per credit charged (1 = $0.25, 3 = $0.75, 5 = $1.25), exact to the cent', async () => {
  assert.deepEqual([1, 3, 5].map(maxRunCostUsd), [0.25, 0.75, 1.25]);
  const h = await makeHousehold(rt.db);
  const g8 = async (credit_cost: number, spend: number, tokens = 100): Promise<{ ok: boolean; detail: string }> => {
    const run_id = newRunId();
    await rt.db.prepare(`INSERT INTO llm_usage (id, household_id, run_id, model, input_tokens, cached_tokens, output_tokens, cost_usd) VALUES (?, ?, ?, 'm', ?, 0, 10, ?)`).bind(`u_${randomUUID()}`, h.household_id, run_id, tokens, spend).run();
    const out = await runGates({
      db: rt.db, now: new Date(), household_id: h.household_id, run_id, template_key: 'unit', dedupe_key: 'dk', cooldown_days: 14, plan: [], max_tier: 'R2',
      read: { candidate_found: true, data: {}, claims: [], facts: [], headroom_cents: null, envelope_key: null, spend_data_at: null, calendar: { required: false, fetched_at: null, windows: [], error: null }, fetched_at: new Date().toISOString() },
      payload: { version: 1, run_id, template_key: 'unit', trigger_key: 'T', dedupe_key: 'dk', kind: 'plan', copy: { title: 't', why: 'w', r2_label: 'r', unconfirmed: false, claims: {} }, script: null, price_cents: 0, envelope_key: null, proposed_range: null, action: { type: 'none' }, expires_on: null },
      r3: false, credit_cost,
    });
    return out.results.find((r) => r.gate === 'G8')!;
  };
  for (const [credits, cap] of [[1, 0.25], [3, 0.75], [5, 1.25]] as const) {
    assert.equal((await g8(credits, cap)).ok, true, `${credits} credit(s): $${cap} is allowed`);
    assert.equal((await g8(credits, cap + 0.0001)).ok, false, `${credits} credit(s): one hundredth of a cent more is not`);
  }
  assert.equal((await g8(1, 0.75)).ok, false, 'a 1-credit run may not spend a 3-credit budget');
  assert.equal((await g8(5, 0.26)).ok, true, 'while a 5-credit run may');
  assert.match((await g8(3, 0.9)).detail, /cap \$0\.75 for 3 credits/);
  assert.equal((await g8(5, 0.01, 90_001)).ok, false, 'the 90,000-token ceiling is independent of the credit cost');
});

test('MULTI-CREDIT: a 3-credit run may spend up to $0.75 on models; one that spends more is refused and refunded in full', async () => {
  await withTemplateCost(3, async () => {
    const over = await fx.buildScenario();
    await fx.grant(over.h, 3);
    const overRun = (await fx.stage(over.h))['annual_fee_roi']!;
    await rt.db.prepare(`INSERT INTO llm_usage (id, household_id, run_id, model, input_tokens, cached_tokens, output_tokens, cost_usd) VALUES (?, ?, ?, 'm', 1000, 0, 100, 0.76)`).bind(`u_${overRun}`, over.h.household_id, overRun).run();
    const a = await runWith(over, overRun, { [`approve:${overRun}`]: approveWith(overRun, over.h.household_id) });
    assert.deepEqual(a.outcome, { status: 'failed_gate', stage: 'r1', failed: ['G8'] });
    assert.equal((await creditBalances(rt.db, over.h.household_id)).total, 3, 'all three credits are back');

    const within = await fx.buildScenario();
    await fx.grant(within.h, 3);
    const withinRun = (await fx.stage(within.h))['annual_fee_roi']!;
    await rt.db.prepare(`INSERT INTO llm_usage (id, household_id, run_id, model, input_tokens, cached_tokens, output_tokens, cost_usd) VALUES (?, ?, ?, 'm', 1000, 0, 100, 0.7)`).bind(`u_${withinRun}`, within.h.household_id, withinRun).run();
    const b = await runWith(within, withinRun, { [`approve:${withinRun}`]: approveWith(withinRun, within.h.household_id) });
    assert.equal(b.outcome.status, 'confirmed', '$0.70 would have failed under the old $0.60 cap, and is fine for 3 credits');
  });
});

// ═══════════════ LifeReceiptWF and the share card ═══════════════

interface FakeEmail {
  sent: Array<{ to: string; from: string; subject: string; text: string; html: string }>;
  failFor: Set<string>;
  sender: EmailSender;
}
function fakeEmail(): FakeEmail {
  const e: FakeEmail = { sent: [], failFor: new Set(), sender: undefined as unknown as EmailSender };
  e.sender = {
    send: async (m) => {
      if (e.failFor.has(m.to)) throw new Error('mailbox unavailable');
      e.sent.push(m);
      return { ok: true };
    },
  };
  return e;
}

async function receiptDeps(s: Scenario, o: { email?: FakeEmail | null; now?: () => Date; vapid?: boolean } = {}): Promise<ReceiptDeps> {
  const keys = await vapidKeys();
  return {
    db: rt.db,
    fetch: createGuardedFetch({ db: rt.db, fetchImpl: s.net.fetch, ttlMs: 0 }),
    env: {
      APP_NAME: 'TestApp',
      INBOUND_DOMAIN: 'inbound.test',
      ...(o.vapid === false ? {} : { VAPID_PUBLIC_KEY: keys.publicKey, VAPID_PRIVATE_KEY: keys.privateKey, VAPID_SUBJECT: keys.subject }),
      ...(o.email === null || o.email === undefined ? {} : { RECEIPT_EMAIL: o.email.sender }),
    },
    now: o.now ?? (() => new Date()),
  };
}

async function generate(s: Scenario, o: { notify?: boolean; as_of?: string; email?: FakeEmail | null } = {}) {
  const { step, log } = directStep();
  const deps = await receiptDeps(s, o.email === undefined ? {} : { email: o.email });
  const outcome = await runLifeReceipt(step, deps, { household_id: s.h.household_id, notify: o.notify ?? false, ...(o.as_of === undefined ? {} : { as_of: o.as_of }) });
  const row = await fx.one<{ payload_json: string }>('SELECT payload_json FROM life_receipts WHERE receipt_id = ?', outcome.receipt_id);
  return { outcome, log: log.map((l) => l.name), payload: JSON.parse(row.payload_json) as ReceiptPayload };
}

const daysFromNow = (n: number): string => new Date(Date.now() + n * 86_400_000).toISOString();

test('LIFE RECEIPT: assembles the five sections from the household\'s own record, each line pointing at a claim', async () => {
  const s = await fx.buildScenario();
  const runs = await fx.stage(s.h);
  const { outcome, log, payload } = await generate(s);
  assert.deepEqual(log, ['assemble', 'ground', 'store'], 'no e-mail or push step when not notifying');
  assert.equal(outcome.status, 'stored');
  assert.deepEqual(Object.keys(payload).sort(), ['as_of', 'done', 'dropped', 'found', 'household_id', 'next_week', 'proof', 'receipt_id', 'version', 'waiting', 'week_start']);

  // 1 Found: money on the table now, weighted by confidence, summing to the total
  assert.ok(payload.found.lines.length >= 1);
  assert.equal(payload.found.total_cents, payload.found.lines.reduce((n, l) => n + (l.cents ?? 0), 0));
  assert.ok(payload.found.total_cents > 0);
  assert.ok(payload.found.lines.every((l) => l.text.length > 0 && typeof l.claim_id === 'string'));
  // 2 Done: this week's documents and facts
  assert.ok(payload.done.counts.facts_updated > 0);
  assert.ok(payload.done.lines.some((l) => /^Updated \d+ facts? about your household$/.test(l.text)));
  // 3 Waiting on one tap: the staged cards, at most 3
  assert.equal(payload.waiting.length, 3);
  assert.deepEqual(payload.waiting.map((w) => w.run_id).sort(), Object.values(runs).sort());
  assert.ok(payload.waiting.every((w) => w.state === 'R1_STAGED' && w.title.length > 0));
  // 5 Proof: every claim a line uses, with its facts and verbatim quotes
  const used = new Set([...payload.found.lines, ...payload.done.lines, ...payload.next_week].map((l) => l.claim_id).concat(payload.waiting.flatMap((w) => (w.claim_id === null ? [] : [w.claim_id]))));
  assert.deepEqual(payload.proof.map((p) => p.claim_id).sort(), [...used].sort());
  for (const p of payload.proof) {
    assert.ok(p.facts.length > 0 && p.facts.every((f) => f.quotes.length > 0), p.text);
    for (const f of p.facts) {
      for (const q of f.quotes) {
        const ep = await fx.one<{ redacted_text: string }>('SELECT redacted_text FROM episodes WHERE episode_id = ?', q.episode_id);
        assert.equal(ep.redacted_text.slice(q.char_start, q.char_end), q.quote, 'the quote is a verbatim slice of its episode');
      }
    }
  }
  assert.equal(payload.week_start, weekStartOf(new Date(), 'America/New_York'));
  assert.equal(new Date(`${payload.week_start}T00:00:00Z`).getUTCDay(), 1, 'weeks start on Monday');
});

test('LIFE RECEIPT: "Next week" lists annual fees about to post and trips about to start', async () => {
  const s = await fx.buildScenario();
  await fx.stage(s.h);
  // a trip as the calendar sync records it: a trip_window fact on a trip entity
  const tripText = `Trip: Lisbon Vacation | ${day(60)} to ${day(66)}`;
  const tripEpisode = await fx.userEpisode(s.h, tripText);
  const wrote = await writeFact(rt.db, { household_id: s.h.household_id, source_episode_id: tripEpisode, subject: { type: 'trip', name: 'Lisbon Vacation' }, predicate: 'trip_window', value_text: `${day(60)}/${day(66)}`, confidence: 1, quotes: [{ quote: tripText, char_start: 0, char_end: tripText.length }] });
  assert.equal(wrote.status, 'written');
  // the fee posts in 12 days and the Lisbon trip starts in 60: look from the right moments
  const fee = await generate(s, { as_of: daysFromNow(8) });
  assert.ok(fee.payload.next_week.some((l) => /^Amex Platinum \$895 annual fee posts /.test(l.text)), JSON.stringify(fee.payload.next_week));
  const none = await generate(s, { as_of: daysFromNow(1) });
  assert.ok(!none.payload.next_week.some((l) => /annual fee posts/.test(l.text)), 'twelve days out is not next week');
  const trip = await generate(s, { as_of: daysFromNow(56) });
  assert.ok(trip.payload.next_week.some((l) => /^Lisbon Vacation starts /.test(l.text)), JSON.stringify(trip.payload.next_week));
});

test('LIFE RECEIPT: a decayed habit is reported as "Stopped assuming you prefer <cuisine>"', async () => {
  const { h } = await fx.cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005' });
  const text = 'We love Thai food.';
  const ep = await fx.userEpisode(h, text);
  const r = await writeFact(rt.db, { household_id: h.household_id, source_episode_id: ep, subject: { type: 'household', name: 'The household' }, predicate: 'prefers_cuisine', object: { type: 'cuisine', name: 'Thai' }, confidence: 1, quotes: [{ quote: 'love Thai food', char_start: 3, char_end: 17 }] });
  assert.ok(r.status === 'written');
  // the nightly decay closes it a year on
  await rt.db.prepare(`UPDATE episodes SET received_at = ? WHERE episode_id = ?`).bind(daysFromNow(-400), ep).run();
  const closed = await decayFacts(rt.db, h.household_id, new Date());
  assert.ok(closed.closed >= 1);
  const s = { h, env: testEnv(rt).env, net: newNetwork() } as unknown as Scenario;
  const { payload } = await generate(s);
  assert.ok(payload.done.lines.some((l) => l.text === 'Stopped assuming you prefer Thai'), JSON.stringify(payload.done.lines));
  assert.equal(payload.done.counts.habits_decayed >= 1, true);
  const claim = payload.proof.find((p) => p.text === 'Stopped assuming you prefer Thai')!;
  assert.deepEqual(claim.facts[0]!.quotes.map((q) => q.quote), ['love Thai food'], 'and it is grounded in the sentence that made it');
});

test('LIFE RECEIPT G1/G7: a line whose evidence no longer verifies, or that characterizes tax treatment, is dropped before anything is stored or sent', async () => {
  const { h } = await fx.cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005', fee: { cents: 89_500, postsOn: '2027-01-15' } });
  const facts = await fx.all<{ fact_id: string; predicate: string }>(`SELECT fact_id, predicate FROM facts WHERE household_id = ?`, h.household_id);
  const good = facts.find((f) => f.predicate === 'annual_fee')!.fact_id;
  const damaged = facts.find((f) => f.predicate === 'card_last4')!.fact_id;
  await rt.db.prepare('UPDATE fact_quotes SET char_start = char_start + 1, char_end = char_end + 1 WHERE fact_id = ?').bind(damaged).run();
  const draft: ReceiptDraft = {
    week_start: '2026-11-23',
    as_of: new Date().toISOString(),
    found: [
      { text: 'Fee is $895', cents: 89_500, claim_id: 'c_good' },
      { text: 'Last four digits are known', cents: 5_000, claim_id: 'c_damaged' },
      { text: 'Pretend fact', cents: 7_000, claim_id: 'c_missing_fact' },
      { text: 'No supporting fact at all', cents: 3_000, claim_id: 'c_null' },
      { text: 'No facts listed', cents: 2_000, claim_id: 'c_empty' },
      { text: 'This fee is tax deductible', cents: 1_000, claim_id: 'c_tax_claim' },
      { text: 'You can deduct this fee', cents: 900, claim_id: 'c_tax_line' },
    ],
    done: [],
    waiting: [
      { run_id: 'run_1', title: 'Stage a call', why: 'Enter on line 5 of the return', state: 'R1_STAGED', claim_id: 'c_good' },
      { run_id: 'run_2', title: 'Forward 3 receipts', why: 'Forward a few recent receipts to your drop address', state: 'R1_STAGED', claim_id: null },
      { run_id: 'run_3', title: 'Ungrounded card', why: 'x', state: 'R1_STAGED', claim_id: 'c_damaged' },
    ],
    next_week: [],
    claims: [
      { claim_id: 'c_good', text: 'The card has an $895 annual fee', facts: [good] },
      { claim_id: 'c_damaged', text: 'The card ends 1005', facts: [damaged] },
      { claim_id: 'c_missing_fact', text: 'Something', facts: ['fact_that_does_not_exist'] },
      { claim_id: 'c_null', text: 'Something', facts: [null] },
      { claim_id: 'c_empty', text: 'Something', facts: [] },
      { claim_id: 'c_tax_claim', text: 'The fee is deductible', facts: [good] },
      { claim_id: 'c_tax_line', text: 'The card has an annual fee', facts: [good] },
    ],
    counts: { documents_filed: 0, facts_updated: 0, habits_decayed: 0, missions_completed: 0 },
  };
  const payload = await groundReceipt(rt.db, h.household_id, draft, 'rcpt_unit');
  assert.deepEqual(payload.found.lines.map((l) => l.claim_id), ['c_good'], 'only the grounded, tax-neutral line survives');
  assert.equal(payload.found.total_cents, 89_500, 'the total is recomputed from what is left');
  assert.deepEqual(payload.waiting.map((w) => w.run_id), ['run_2'], 'the claim-free "forward receipts" card is kept; the tax-worded and ungrounded cards are not');
  const dropped = payload.dropped.map((d) => `${d.gate}:${d.claim_id}`).sort();
  assert.deepEqual(dropped, ['G1:c_damaged', 'G1:c_empty', 'G1:c_missing_fact', 'G1:c_null', 'G7:c_good', 'G7:c_tax_claim', 'G7:c_tax_line'], 'each failure is recorded once, with its gate');
  assert.deepEqual(payload.proof.map((p) => p.claim_id), ['c_good'], 'only claims that survived are shown as proof');
  assert.ok(!JSON.stringify(payload).match(/deduct/i), 'no tax language anywhere in what would be stored or sent');
});

test('LIFE RECEIPT G1 in the pipeline: damaging a quote removes its line, its money, and its proof, and the drop is audited without the text', async () => {
  const s = await fx.buildScenario();
  const intact = await generate(s);
  const line = intact.payload.found.lines[0]!;
  const proof = intact.payload.proof.find((p) => p.claim_id === line.claim_id)!;
  for (const f of proof.facts) await rt.db.prepare('UPDATE fact_quotes SET char_start = char_start + 1, char_end = char_end + 1 WHERE fact_id = ?').bind(f.fact_id).run();
  const email = fakeEmail();
  const { payload } = await generate(s, { notify: true, email });
  assert.ok(!payload.found.lines.some((l) => l.text === line.text), 'the damaged line is gone');
  assert.equal(payload.found.total_cents, payload.found.lines.reduce((n, l) => n + (l.cents ?? 0), 0), 'the total is recomputed from the surviving lines');
  assert.ok(payload.found.total_cents < intact.payload.found.total_cents);
  assert.ok(payload.dropped.some((d) => d.gate === 'G1'));
  assert.ok(!payload.proof.some((p) => p.claim_id === line.claim_id));
  assert.ok(!email.sent.some((m) => m.text.includes(line.text) || m.html.includes(line.text)), 'what failed the gate is not e-mailed');
  const audits = await fx.all<{ detail_json: string }>(`SELECT detail_json FROM audit_log WHERE household_id = ? AND action = 'receipt_line_dropped'`, s.h.household_id);
  assert.ok(audits.length >= 1 && audits.every((a) => !a.detail_json.includes(line.text)), 'the audit row names the gate, not the household\'s words');
});

test('LIFE RECEIPT store: one receipt per household per week; regenerating replaces it and its claims, never duplicating them', async () => {
  const s = await fx.buildScenario();
  await fx.stage(s.h);
  const a = await generate(s);
  const rows = () => fx.all<{ receipt_id: string }>('SELECT receipt_id FROM life_receipts WHERE household_id = ?', s.h.household_id);
  assert.equal((await rows()).length, 1);
  const claimRows = async (id: string): Promise<number> => (await fx.one<{ n: number }>(`SELECT COUNT(*) AS n FROM claims WHERE run_id = ? AND household_id = ?`, id, s.h.household_id)).n;
  assert.equal(await claimRows(a.payload.receipt_id), a.payload.proof.length);
  const b = await generate(s);
  assert.equal((await rows()).length, 1, 'the unique (household, week) holds');
  assert.equal(b.payload.receipt_id, a.payload.receipt_id, 'the id is stable for the week');
  assert.equal(await claimRows(b.payload.receipt_id), b.payload.proof.length, 'old claims were replaced, not piled up');
  const support = await fx.one<{ n: number }>(`SELECT COUNT(*) AS n FROM claim_support s JOIN claims c ON c.claim_id = s.claim_id WHERE c.run_id = ?`, b.payload.receipt_id);
  assert.equal(support.n, b.payload.proof.reduce((n, p) => n + p.facts.length, 0));
  assert.deepEqual((await fx.all<{ status: string }>('SELECT DISTINCT status FROM claims WHERE run_id = ?', b.payload.receipt_id)).map((r) => r.status), ['grounded']);
  // another week is another receipt
  const later = await generate(s, { as_of: daysFromNow(9) });
  assert.notEqual(later.payload.receipt_id, a.payload.receipt_id);
  assert.equal((await rows()).length, 2);
});

test('LIFE RECEIPT notify: e-mails every member and sends a payload-less push, only when asked; a missing mailbox or a failed send never stops the receipt', async () => {
  const s = await fx.buildScenario();
  await fx.stage(s.h);
  await rt.db.prepare(`INSERT INTO members (member_id, household_id, email, role, age_attested_18) VALUES (?, ?, 'second@example.org', 'adult', 1)`).bind(`mem_second_${randomUUID().slice(0, 6)}`, s.h.household_id).run();
  await rt.db.prepare('UPDATE members SET push_endpoint = ? WHERE member_id = ?').bind(JSON.stringify({ endpoint: PUSH_ENDPOINT }), `mem_${s.h.household_id}`).run();

  const quiet = fakeEmail();
  const off = await generate(s, { notify: false, email: quiet });
  assert.deepEqual([off.log, quiet.sent.length, s.net.push.length], [['assemble', 'ground', 'store'], 0, 0], 'an on-demand receipt is not mailed or pushed');

  const email = fakeEmail();
  const on = await generate(s, { notify: true, email });
  assert.deepEqual(on.log, ['assemble', 'ground', 'store', 'email', 'push']);
  assert.deepEqual(on.outcome, { ...on.outcome, emailed: 2, pushed: 1 });
  assert.deepEqual(email.sent.map((m) => m.to).sort(), [s.h.member_email, 'second@example.org'].sort());
  const m = email.sent[0]!;
  assert.equal(m.from, 'receipts@inbound.test', 'the sender domain comes from INBOUND_DOMAIN');
  assert.equal(m.subject, 'Your TestApp week', 'the product name comes from APP_NAME');
  for (const heading of ['FOUND', 'DONE', 'WAITING ON ONE TAP', 'NEXT WEEK', 'PROOF']) assert.ok(m.text.includes(heading), heading);
  for (const heading of ['Found:', '<h2>Done</h2>', 'Waiting on one tap', 'Next week', '<h2>Proof</h2>']) assert.ok(m.html.includes(heading), heading);
  assert.equal(s.net.push.length, 1);
  assert.equal(s.net.push[0]!.bodyLength, 0, 'the push carries nothing');
  const row = await fx.one<{ emailed_at: string | null; pushed_at: string | null }>('SELECT emailed_at, pushed_at FROM life_receipts WHERE household_id = ?', s.h.household_id);
  assert.ok(row.emailed_at !== null && row.pushed_at !== null);

  // one mailbox failing: the other still gets it, the failure is audited, nothing throws
  const flaky = fakeEmail();
  flaky.failFor.add('second@example.org');
  const f = await generate(s, { notify: true, email: flaky });
  assert.equal((f.outcome as { emailed: number }).emailed, 1);
  assert.equal((await fx.all(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'receipt_email_failed'`, s.h.household_id)).length, 1);

  // no e-mail binding: stored and pushed, the skip audited
  const noMail = await generate(s, { notify: true });
  assert.equal((noMail.outcome as { emailed: number }).emailed, 0);
  assert.ok((await fx.all(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'receipt_email_skipped'`, s.h.household_id)).length >= 1);
});

test('LIFE RECEIPT e-mail: everything is HTML-escaped', () => {
  const evil = '<img src=x onerror=alert(1)> & "quoted" \'single\'';
  assert.equal(escapeHtml(evil), '&lt;img src=x onerror=alert(1)&gt; &amp; &quot;quoted&quot; &#39;single&#39;');
  const payload = {
    version: 1, receipt_id: 'r', household_id: 'h', week_start: '2026-11-23', as_of: '2026-11-29T13:00:00Z',
    found: { total_cents: 12_345, lines: [{ text: evil, cents: 12_345, claim_id: 'c' }] },
    done: { lines: [], counts: { documents_filed: 0, facts_updated: 0, habits_decayed: 0, missions_completed: 0 } },
    waiting: [{ run_id: 'run', title: evil, why: '', state: 'R1_STAGED', claim_id: null }], next_week: [],
    proof: [{ claim_id: 'c', text: evil, facts: [{ fact_id: 'f', predicate: 'p', quotes: [{ quote: evil, source: 'user_input', episode_id: 'e', char_start: 0, char_end: 1 }] }] }], dropped: [],
  } as ReceiptPayload;
  const html = renderReceiptHtml(payload, evil);
  assert.ok(!/<img|onerror=alert\(1\)>/.test(html), 'no raw tag survives');
  assert.ok(html.includes('&lt;img src=x onerror=alert(1)&gt;'));
  assert.ok(renderReceiptText(payload, 'TestApp').includes('$123.45'));
});

test('API receipt: latest generates the first receipt on demand, generate refreshes it, and each household sees only its own', async () => {
  const s = await fx.buildScenario();
  await fx.stage(s.h);
  const email = fakeEmail();
  s.env.RECEIPT_EMAIL = email.sender;
  assert.equal((await api(s.env, s.net, '/api/receipt/latest', { token: null })).status, 401);
  assert.equal((await api(s.env, s.net, '/api/receipt/generate', { token: null, method: 'POST' })).status, 401);
  assert.equal((await fx.all('SELECT 1 FROM life_receipts WHERE household_id = ?', s.h.household_id)).length, 0);
  const first = await api(s.env, s.net, '/api/receipt/latest', { h: s.h });
  assert.equal(first.status, 200);
  assert.equal(first.headers.get('cache-control'), 'no-store');
  const r1 = (await bodyOf(first))['receipt'] as ReceiptPayload;
  assert.equal(r1.household_id, s.h.household_id);
  assert.equal(r1.waiting.length, 3);
  assert.equal(((await bodyOf(await api(s.env, s.net, '/api/receipt/latest', { h: s.h })))['receipt'] as ReceiptPayload).as_of, r1.as_of, 'a second GET returns the stored receipt');
  await new Promise((r) => setTimeout(r, 5));
  const regenerated = await api(s.env, s.net, '/api/receipt/generate', { h: s.h, method: 'POST' });
  assert.equal(regenerated.status, 200);
  const r2 = (await bodyOf(regenerated))['receipt'] as ReceiptPayload;
  assert.deepEqual([r2.receipt_id, r2.as_of > r1.as_of], [r1.receipt_id, true]);
  assert.equal(email.sent.length, 0, 'an on-demand receipt is never mailed');
  const stranger = await makeHousehold(rt.db);
  const theirs = (await bodyOf(await api(s.env, s.net, '/api/receipt/latest', { h: stranger })))['receipt'] as ReceiptPayload;
  assert.equal(theirs.household_id, stranger.household_id);
  assert.equal(theirs.found.lines.length + theirs.done.lines.length + theirs.waiting.length, 0, 'an empty household gets an empty receipt');
});

test('SHARE CARD: $100 buckets rounded down, exact wording, product name from APP_NAME, escaped, private headers', async () => {
  assert.deepEqual([0, 9_999, 10_000, 39_999, 40_000, 99_999, 123_456, -5].map(bucketDollars), [0, 0, 100, 300, 400, 900, 1200, 0]);
  const svg = renderShareCard({ recovered_cents: 40_000, documents_filed: 41 }, 'Hayes');
  assert.deepEqual([...svg.matchAll(/<text[^>]*>([^<]*)<\/text>/g)].map((m) => m[1]), ['Hayes recovered $400+ in card credits', '41 receipts filed · 0 prompts typed']);
  assert.ok(renderShareCard({ recovered_cents: 0, documents_filed: 1 }, 'Hayes').includes('1 receipt filed · 0 prompts typed'), 'singular');
  const early = renderShareCard({ recovered_cents: 9_999, documents_filed: 3 }, 'Hayes');
  assert.ok(early.includes('Hayes is finding card credits for you') && !/recovered/.test(early), 'under $100 it claims no recovery');
  const nasty = renderShareCard({ recovered_cents: 10_000, documents_filed: 0 }, 'A&B <x>');
  assert.ok(nasty.includes('A&amp;B &lt;x&gt; recovered $100+') && !nasty.includes('<x>'));

  const s = await fx.buildScenario();
  assert.equal((await api(s.env, s.net, '/api/receipt/share-card.svg', { token: null })).status, 401);
  const res = await api(s.env, s.net, '/api/receipt/share-card.svg', { h: s.h });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('content-type'), 'image/svg+xml; charset=utf-8');
  assert.equal(res.headers.get('cache-control'), 'private, no-store');
  assert.equal(res.headers.get('x-content-type-options'), 'nosniff');
  assert.equal(res.headers.get('content-security-policy'), "default-src 'none'; style-src 'unsafe-inline'");
  assert.match((await res.text()).replace(/\n/g, ' '), /^<\?xml.*<svg xmlns="http:\/\/www\.w3\.org\/2000\/svg"/);
});

test('SHARE CARD privacy: only the two aggregate numbers reach the SVG; no merchant, card, venue, date, place or tax data does', async () => {
  const s = await fx.buildScenario();
  const runs = await fx.stage(s.h);
  // $400+ recovered: confirmed credit missions worth $412.34
  await rt.db.prepare(`UPDATE mission_runs SET state = 'CONFIRMED', params_json = json_patch(params_json, ?) WHERE run_id = ?`).bind(JSON.stringify({ recoverable_cents: 41_234, confidence: 1 }), runs['expiring_credit']!).run();
  // a tax-vault document and a venue, with identifying names
  await rt.db.batch([
    rt.db.prepare(`INSERT INTO entities (entity_id, household_id, type, canonical_name) VALUES ('ent_w2', ?, 'document', 'W-2 2025 Acme Payroll Inc')`).bind(s.h.household_id),
    rt.db.prepare(`INSERT INTO entities (entity_id, household_id, type, canonical_name) VALUES ('ent_venue', ?, 'venue', 'Carbone New York')`).bind(s.h.household_id),
  ]);
  const res = await api(s.env, s.net, '/api/receipt/share-card.svg', { h: s.h });
  const svg = await res.text();
  const texts = [...svg.matchAll(/<text[^>]*>([^<]*)<\/text>/g)].map((m) => m[1]!);
  const docs = (await fx.one<{ n: number }>(`SELECT COUNT(*) AS n FROM episodes e WHERE e.household_id = ? AND e.source IN ('email','pdf','csv','upload') AND e.redaction_status IN ('clean','redacted') AND EXISTS (SELECT 1 FROM facts f WHERE f.source_episode_id = e.episode_id)`, s.h.household_id)).n;
  assert.deepEqual(texts, ['TestApp recovered $400+ in card credits', `${docs} ${docs === 1 ? 'receipt' : 'receipts'} filed · 0 prompts typed`], 'rounded DOWN from $412.34, and nothing else');
  const names = (await fx.all<{ canonical_name: string }>('SELECT canonical_name FROM entities WHERE household_id = ?', s.h.household_id)).map((r) => r.canonical_name);
  const banned = [...names, 'Amex', 'Platinum', 'Resy', 'Carbone', 'Netflix', 'Uber', 'Lisbon', 'Anniversary', '1005', 'W-2', 'Acme', 'Payroll', 'deduct', 'tax', 'New York', 'Checking'];
  for (const word of banned) assert.ok(!svg.toLowerCase().includes(word.toLowerCase()), `the card must not mention "${word}"`);
  assert.ok(!/\d{4}-\d{2}-\d{2}/.test(svg), 'no date');
  assert.ok(!/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d/.test(svg), 'no month and day');
  assert.deepEqual([...svg.matchAll(/\$[\d,.]+/g)].map((m) => m[0]), ['$400', '$400'], 'the only amount is the bucketed one (in the text and its aria-label)');
  // another household's confirmed missions never leak in
  const other = await makeHousehold(rt.db);
  const otherSvg = await (await api(s.env, s.net, '/api/receipt/share-card.svg', { h: other })).text();
  assert.ok(otherSvg.includes('is finding card credits for you') && otherSvg.includes('0 receipts filed'));
});

test('SCHEDULE: the hourly cron starts a receipt only at Sunday 08:00 household-local time, once, for live households', async () => {
  const { env, workflows } = testEnv(rt);
  const mk = async (tz: string, o: { plan?: string; deleted?: boolean } = {}): Promise<Household> => {
    const h = await makeHousehold(rt.db);
    await rt.db.prepare('UPDATE households SET timezone = ?, plan = ?, deleted_at = ? WHERE household_id = ?').bind(tz, o.plan ?? 'monthly', o.deleted ? '2026-01-01T00:00:00Z' : null, h.household_id).run();
    return h;
  };
  const ny = await mk('America/New_York');
  const tokyo = await mk('Asia/Tokyo');
  const london = await mk('Europe/London');
  const canceled = await mk('America/New_York', { plan: 'canceled' });
  const deleted = await mk('America/New_York', { deleted: true });
  const bogus = await mk('Mars/Phobos');
  const started = (h: Household) => workflows.receipt.filter((w) => w.params.household_id === h.household_id);
  const sunday = new Date('2026-12-13T13:00:00Z'); // Sunday 08:00 in New York
  const first = await runScheduled(env, sunday);
  assert.equal(started(ny).length, 1);
  assert.deepEqual([started(ny)[0]!.id, started(ny)[0]!.params.notify], [`receipt_${ny.household_id}_2026-12-13`, true]);
  for (const h of [tokyo, london, canceled, deleted, bogus]) assert.equal(started(h).length, 0, h.household_id);
  assert.ok(first.receipts_started >= 1);
  const again = await runScheduled(env, sunday);
  assert.equal(started(ny).length, 1, 'a repeat of the same hour starts nothing');
  assert.ok(again.receipts_skipped >= 1 && again.receipts_started === 0);
  await runScheduled(env, new Date('2026-12-13T14:00:00Z')); // 09:00 in New York
  await runScheduled(env, new Date('2026-12-12T13:00:00Z')); // Saturday 08:00
  await runScheduled(env, new Date('2026-12-14T13:00:00Z')); // Monday 08:00
  assert.equal(started(ny).length, 1, 'not at other hours or on other days');
  await runScheduled(env, new Date('2026-12-12T23:00:00Z')); // Sunday 08:00 in Tokyo
  assert.deepEqual(started(tokyo).map((w) => w.id), [`receipt_${tokyo.household_id}_2026-12-13`]);
  await runScheduled(env, new Date('2026-12-13T08:00:00Z')); // Sunday 08:00 in London (winter)
  assert.equal(started(london).length, 1);
});

test('SCHEDULE: the same cron grants annual-plan households their monthly credits', async () => {
  const { env } = testEnv(rt);
  const h = await makeHousehold(rt.db);
  await rt.db.prepare(`UPDATE households SET plan = 'annual', plan_period_end = ? WHERE household_id = ?`).bind(daysFromNow(200), h.household_id).run();
  const res = await runScheduled(env, new Date());
  assert.ok(res.annual_grants >= 1);
  assert.equal((await creditBalances(rt.db, h.household_id)).subscription, 12);
  assert.equal((await runScheduled(env, new Date())).annual_grants, 0, 'once a month');
});

// ═══════════════ DropAlarmWF ═══════════════

/** A bare household with a pipeline-ready Env and network (no sweep, no CSV): enough for the concierge workflows. */
async function light(extra: Partial<Env> = {}): Promise<Scenario> {
  const h = await makeHousehold(rt.db);
  const keys = await vapidKeys();
  const { env, workflows } = testEnv(rt, { VAPID_PUBLIC_KEY: keys.publicKey, VAPID_PRIVATE_KEY: keys.privateKey, VAPID_SUBJECT: keys.subject, ...extra });
  return { h, env, net: newNetwork(), workflows, cardId: '', feeDate: '', anniversary: null } as Scenario;
}
const connectCalendar = async (s: Scenario): Promise<void> => {
  await exchangeCode(fx.calendarDeps(s.env, s.net), s.h.household_id, 'good-code', 'https://app.example/cb');
};
const subscribeMember = (s: Scenario) => rt.db.prepare('UPDATE members SET push_endpoint = ? WHERE household_id = ?').bind(JSON.stringify({ endpoint: PUSH_ENDPOINT }), s.h.household_id).run();

/** A venue whose tables for `reservationDate` open `daysUntilRelease` days from now (the rule is tuned to make it so), at 09:00 New York. */
async function alarmFixture(s: Scenario, o: { daysUntilRelease: number; platform?: 'resy' | 'opentable' | 'tock' | 'sevenrooms' | 'direct'; url?: string; party?: number }) {
  const releaseDate = new Date(Date.now() + o.daysUntilRelease * 86_400_000).toISOString().slice(0, 10);
  const daysAhead = 30;
  const reservation_date = addDaysUtil(releaseDate, daysAhead);
  const venue = await makeVenue(rt.db, s.h, { name: 'Carbone', platform: o.platform ?? 'resy', ...(o.url === undefined ? {} : { url: o.url }), days_ahead: daysAhead, time: '09:00', tz: 'America/New_York' });
  const rule = (await loadRule(rt.db, s.h.household_id, venue.venue_entity_id))!;
  const created = await createDropAlarm(rt.db, s.h.household_id, { venue_entity_id: venue.venue_entity_id, reservation_date, party_size: o.party ?? 2 }, new Date());
  assert.ok(created.ok, JSON.stringify(created));
  const release = releaseAtMs(rule, reservation_date)!;
  return { ...venue, rule, reservation_date, alarm_id: (created as { alarm_id: string }).alarm_id, release, party: o.party ?? 2 };
}
const addDaysUtil = (d: string, n: number): string => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86_400_000).toISOString().slice(0, 10);

type Alarm = Awaited<ReturnType<typeof alarmFixture>>;
async function runAlarm(s: Scenario, a: Alarm, o: { now: number; events?: Record<string, unknown>; onSleep?: () => Promise<void> }) {
  const clock = { now: new Date(o.now) };
  const base = directStep(undefined, o.events ?? {}, clock);
  const step = { ...base.step, sleep: async (name: string, d: number | string) => { await base.step.sleep(name, d); await o.onSleep?.(); } };
  const deps = missionDepsFromEnv(s.env, 'run_alarm', s.h.household_id, { network: s.net.fetch, now: () => clock.now });
  const outcome = await runDropAlarm(step, deps, { alarm_id: a.alarm_id, household_id: s.h.household_id });
  return { outcome, clock, log: base.log.map((l) => l.name), waits: base.waits, sleeps: base.sleeps };
}
const alarmStatus = async (a: Alarm): Promise<string> => (await fx.one<{ status: string }>('SELECT status FROM drop_alarms WHERE alarm_id = ?', a.alarm_id)).status;
const alarmRun = async (a: Alarm): Promise<string> => alarmRunId(a.alarm_id);

test('DROP ALARM release time: days ahead, local time and zone give the exact instant, across DST and zones; an unusable rule gives none', () => {
  const rule = (o: Partial<ReleaseRule> = {}): ReleaseRule => ({ venue_entity_id: 'v', venue_name: 'V', platform: 'resy', venue_url: 'https://resy.com/x', days_ahead: 30, release_local_time: '09:00', release_tz: 'America/New_York', verified_on: '2026-11-26', ...o });
  const iso = (r: ReleaseRule, d: string): string | null => { const ms = releaseAtMs(r, d); return ms === null ? null : new Date(ms).toISOString(); };
  assert.equal(iso(rule(), '2027-03-20'), '2027-02-18T14:00:00.000Z', '30 days before Mar 20 is Feb 18, 09:00 EST = 14:00Z');
  assert.equal(iso(rule(), '2027-04-20'), '2027-03-21T13:00:00.000Z', 'after DST began (Mar 14) the same 09:00 is 13:00Z');
  assert.equal(iso(rule(), '2027-04-13'), '2027-03-14T13:00:00.000Z', 'the day DST begins: 09:00 is already EDT');
  assert.equal(iso(rule({ release_tz: 'Asia/Tokyo', release_local_time: '10:00', days_ahead: 14 }), '2027-01-15'), '2027-01-01T01:00:00.000Z');
  assert.equal(iso(rule({ release_tz: 'Europe/London', release_local_time: '00:00', days_ahead: 0 }), '2027-01-15'), '2027-01-15T00:00:00.000Z');
  assert.equal(iso(rule({ days_ahead: 90 }), '2027-03-01'), '2026-12-01T14:00:00.000Z', 'crosses a year boundary');
  assert.equal(addDays('2027-03-01', -1), '2027-02-28');
  assert.equal(addDays('2028-03-01', -1), '2028-02-29', 'leap day');
  for (const bad of [rule({ release_local_time: '25:00' }), rule({ release_local_time: '9:00' }), rule({ release_local_time: '09:60' }), rule({ release_tz: 'Mars/Phobos' }), rule({ days_ahead: -1 }), rule({ days_ahead: 366 }), rule({ days_ahead: 1.5 })]) {
    assert.equal(releaseAtMs(bad, '2027-03-20'), null, JSON.stringify(bad));
  }
  assert.equal(releaseAtMs(rule(), '2027-02-30'), null);
  assert.equal(releaseAtMs(rule(), 'tomorrow'), null);
  assert.equal(RELEASE_LEAD_MS, 120_000);
});

test('DROP ALARM links: only an https booking page on the right platform host is ever offered; the date and party size are filled in per platform', () => {
  const rule = (platform: ReleaseRule['platform'], venue_url: string): ReleaseRule => ({ venue_entity_id: 'v', venue_name: 'V', platform, venue_url, days_ahead: 30, release_local_time: '09:00', release_tz: 'America/New_York', verified_on: '2026-11-26' });
  const target = (p: ReleaseRule['platform'], url: string) => venueLinkTarget(rule(p, url), '2027-03-20', 4);
  assert.deepEqual(target('resy', 'https://resy.com/cities/ny/carbone'), { merchant_domain: 'resy.com', path: '/cities/ny/carbone', query: { seats: '4', date: '2027-03-20' } });
  assert.deepEqual(target('opentable', 'https://www.opentable.com/r/carbone?ref=1'), { merchant_domain: 'www.opentable.com', path: '/r/carbone', query: { ref: '1', covers: '4', dateTime: '2027-03-20T19:00' } });
  assert.deepEqual(target('tock', 'https://www.exploretock.com/carbone'), { merchant_domain: 'www.exploretock.com', path: '/carbone', query: { size: '4', date: '2027-03-20' } });
  assert.deepEqual(target('sevenrooms', 'https://www.sevenrooms.com/reservations/carbone'), { merchant_domain: 'www.sevenrooms.com', path: '/reservations/carbone', query: { party_size: '4', date: '2027-03-20' } });
  assert.deepEqual(target('direct', 'https://carbone.example/reserve'), { merchant_domain: 'carbone.example', path: '/reserve', query: { party_size: '4', date: '2027-03-20' } });
  const refused: Array<[ReleaseRule['platform'], string]> = [
    ['resy', 'http://resy.com/x'], ['resy', 'https://resy.com.evil.test/x'], ['resy', 'https://evilresy.com/x'], ['resy', 'https://user:pw@resy.com/x'], ['resy', 'https://resy.com:8443/x'],
    ['resy', 'https://opentable.com/x'], ['opentable', 'https://resy.com/x'], ['resy', 'javascript:alert(1)'], ['resy', 'not a url'],
    ['direct', 'http://carbone.example/x'], ['direct', 'https://127.0.0.1/x'], ['direct', 'https://192.168.1.10/x'], ['direct', 'https://localhost/x'], ['direct', 'https://printer.local/x'], ['direct', 'https://intranet/x'],
  ];
  for (const [platform, url] of refused) assert.equal(target(platform, url), null, `${platform} ${url}`);
});

test('DROP ALARM creation: validated inputs, a grounding fact quoting the member\'s own request, and the exact wake-up time', async () => {
  const s = await light();
  const venue = await makeVenue(rt.db, s.h, { name: 'Carbone', days_ahead: 30, time: '09:00', tz: 'America/New_York' });
  const inDays = (n: number): string => new Date(Date.now() + n * 86_400_000).toISOString().slice(0, 10);
  const create = (o: Record<string, unknown>) => createDropAlarm(rt.db, s.h.household_id, { venue_entity_id: venue.venue_entity_id, reservation_date: inDays(60), party_size: 2, ...o }, new Date());
  const bads: Array<[Record<string, unknown>, string]> = [
    [{ venue_entity_id: 5 }, 'bad_input'], [{ reservation_date: 'soon' }, 'bad_input'], [{ reservation_date: '2027-02-30' }, 'bad_input'], [{ party_size: 0 }, 'bad_input'],
    [{ party_size: 13 }, 'bad_input'], [{ party_size: 2.5 }, 'bad_input'], [{ party_size: '2' }, 'bad_input'], [{ venue_entity_id: 'ent_nope' }, 'unknown_venue'],
    [{ reservation_date: inDays(10) }, 'release_passed'], [{ reservation_date: inDays(30) }, 'release_passed'], [{ reservation_date: inDays(30 + 400) }, 'too_far'],
  ];
  for (const [o, code] of bads) assert.equal((await create(o) as { code?: string }).code, code, JSON.stringify(o));
  const stranger = await light();
  assert.equal(((await createDropAlarm(rt.db, stranger.h.household_id, { venue_entity_id: venue.venue_entity_id, reservation_date: inDays(60), party_size: 2 }, new Date())) as { code?: string }).code, 'unknown_venue', 'another household\'s venue');
  const bare = await rt.db.prepare(`INSERT INTO entities (entity_id, household_id, type, canonical_name) VALUES ('ent_norule_' || ?, ?, 'venue', 'No Rule Bistro')`).bind(s.h.household_id, s.h.household_id).run();
  assert.ok(bare.success);
  assert.equal(((await createDropAlarm(rt.db, s.h.household_id, { venue_entity_id: `ent_norule_${s.h.household_id}`, reservation_date: inDays(60), party_size: 2 }, new Date())) as { code?: string }).code, 'no_release_rule');
  const unsafe = await makeVenue(rt.db, s.h, { url: 'http://resy.com/x' });
  assert.equal(((await createDropAlarm(rt.db, s.h.household_id, { venue_entity_id: unsafe.venue_entity_id, reservation_date: inDays(60), party_size: 2 }, new Date())) as { code?: string }).code, 'unsafe_venue_url');

  const reservation = inDays(60);
  const now = new Date();
  const ok = await createDropAlarm(rt.db, s.h.household_id, { venue_entity_id: venue.venue_entity_id, reservation_date: reservation, party_size: 2 }, now);
  assert.ok(ok.ok);
  if (!ok.ok) return;
  const rule = (await loadRule(rt.db, s.h.household_id, venue.venue_entity_id))!;
  const release = releaseAtMs(rule, reservation)!;
  assert.equal(ok.release_at, new Date(release).toISOString());
  assert.equal(ok.sleep_ms, release - 120_000 - now.getTime(), 'it wakes 120 seconds before the release');
  const row = await fx.one<{ status: string; grounding_fact_id: string; party_size: number }>('SELECT status, grounding_fact_id, party_size FROM drop_alarms WHERE alarm_id = ?', ok.alarm_id);
  assert.deepEqual([row.status, row.party_size], ['scheduled', 2]);
  const fact = await fx.one<{ predicate: string; value_text: string; quote: string; source: string; redacted_text: string }>(
    `SELECT f.predicate, f.value_text, q.quote, e.source, e.redacted_text FROM facts f JOIN fact_quotes q ON q.fact_id = f.fact_id JOIN episodes e ON e.episode_id = q.episode_id WHERE f.fact_id = ?`, row.grounding_fact_id);
  assert.deepEqual([fact.predicate, fact.value_text, fact.source], ['drop_alarm_target', `${reservation}|2`, 'user_input']);
  assert.equal(fact.quote, `Alert me when Carbone releases tables for ${reservation}, party of 2.`);
  assert.equal(fact.redacted_text, fact.quote, 'the evidence is the member\'s own request, verbatim');
  assert.equal(((await create({ reservation_date: reservation }) as { code?: string }).code), 'duplicate', 'one alarm per venue, date and party size');
});

test('DROP ALARM timeline: sleeps until release minus 120 s, THEN stages the card, pushes, and on approval issues the deep link; no venue or blocklisted host is ever contacted', async () => {
  const s = await light({ AFFILIATE_TAGS: JSON.stringify({ 'resy.com': { program: 'p', param: 'aff', value: 'x' } }) });
  await subscribeMember(s);
  await fx.grant(s.h, 2);
  const a = await alarmFixture(s, { daysUntilRelease: 3 });
  const run_id = await alarmRun(a);
  const start = a.release - 3 * 86_400_000 + 3_600_000; // an hour into the last stretch
  const wait = a.release - 120_000 - start;
  const { outcome, clock, log, waits, sleeps } = await runAlarm(s, a, {
    now: start,
    events: {
      [`approve:${run_id}`]: async () => {
        assert.equal(await alarmStatus(a), 'staged');
        assert.equal(s.net.push.length, 1, 'the push tickle went out before the member was asked to approve');
        return approveWith(run_id, s.h.household_id)();
      },
      [`confirmed:${run_id}`]: {},
    },
  });

  assert.deepEqual(sleeps, [{ name: 'sleep_until_release_minus_120s', duration: wait, ms: wait }], 'one sleep, to exactly T - 120 s');
  assert.equal(clock.now.getTime(), a.release - 120_000 + 0, 'the clock stood at T - 120 s when the card was staged');
  assert.deepEqual(log.slice(0, 4), ['load_rule', 'stage_card', 'compile', 'debit_credit']);
  assert.equal(outcome.status, 'confirmed');
  const run = (await getRun(rt.db, s.h.household_id, run_id))!;
  assert.equal(run.created_at, new Date(a.release - 120_000).toISOString(), 'the card did not exist before the wake-up');
  assert.equal(run.state, 'CONFIRMED');
  assert.deepEqual([run.params['workflow'], run.params['workflow_instance'], run.template_key, run.expires_at], ['drop_alarm', a.alarm_id, 'drop_alarm', new Date(a.release + 2 * 3_600_000).toISOString()]);
  const payload = run.params['payload'] as { copy: { title: string; why: string; r2_label: string }; action: { merchant_domain: string; path: string; query: Record<string, string> }; price_cents: number };
  assert.match(payload.copy.title, /^Carbone is releasing tables for [A-Z][a-z]{2} \d{1,2} at 09:00 America\/New_York$/);
  assert.equal(payload.copy.r2_label, 'Open the booking page');
  assert.deepEqual([payload.action.merchant_domain, payload.action.path, payload.action.query, payload.price_cents], ['resy.com', '/cities/ny/some-venue', { seats: '2', date: a.reservation_date }, 0]);
  assert.deepEqual(waits.map((w) => w.type), [`approve:${run_id}`, `confirmed:${run_id}`]);
  assert.equal(waits[0]!.timeout, 2 * 3_600_000 + 120_000, 'the approval window runs to two hours after the release');

  const link = await fx.one<{ merchant_domain: string; url: string; affiliate_program: string | null }>('SELECT merchant_domain, url, affiliate_program FROM deeplinks WHERE run_id = ?', run_id);
  assert.equal(link.merchant_domain, 'resy.com');
  assert.equal(new URL(link.url).searchParams.get('seats'), '2');
  assert.equal(new URL(link.url).searchParams.get('date'), a.reservation_date);
  assert.equal(link.affiliate_program, null, 'a Resy link is never tagged, even when a tag is configured');
  assert.equal(await alarmStatus(a), 'link_issued');
  assert.equal(await fx.balance(s.h), 1, 'one credit for the card');
  assert.equal(s.net.push.length, 2, 'card ready, link ready');
  assert.ok(!s.net.hosts.some((h) => /resy|opentable|exploretock|sevenrooms/.test(h)), `no venue or blocklisted host was contacted: ${s.net.hosts.join(',')}`);
  assert.ok(!s.net.hosts.some((h) => h.endsWith('googleapis.com') && h !== PUSH_HOST), 'a drop alarm reads no calendar');
  assert.equal((await fx.all(`SELECT 1 FROM audit_log WHERE household_id = ? AND action = 'egress_blocked'`, s.h.household_id)).length, 0);
});

test('DROP ALARM timing edges: inside the last 120 s there is no sleep; up to two hours after release the card is still staged; later it is missed', async () => {
  for (const [offset, staged] of [[-60_000, true], [30 * 60_000, true], [2 * 3_600_000 - 1000, true], [2 * 3_600_000, false]] as const) {
    const s = await light();
    await fx.grant(s.h, 1);
    const a = await alarmFixture(s, { daysUntilRelease: 1 });
    const { outcome, sleeps } = await runAlarm(s, a, { now: a.release + offset });
    assert.equal(sleeps.length, 0, `offset ${offset}: nothing to sleep for`);
    const runs = await fx.all('SELECT 1 FROM mission_runs WHERE household_id = ?', s.h.household_id);
    if (staged) {
      assert.equal(outcome.status, 'expired', `offset ${offset}: the card was staged and waited for an approval that never came`);
      assert.equal(runs.length, 1, `offset ${offset}`);
      assert.equal(await fx.balance(s.h), 0, 'the card cost its credit');
    } else {
      assert.deepEqual(outcome, { status: 'missed' });
      assert.equal(await alarmStatus(a), 'missed');
      assert.equal(runs.length, 0, 'no card for a release that is long gone');
      assert.equal(await fx.balance(s.h), 1, 'and no credit taken');
    }
  }
});

test('DROP ALARM: a changed rule, or an alarm canceled while it slept, stages nothing and takes no credit', async () => {
  const changed = await light();
  await fx.grant(changed.h, 1);
  const a = await alarmFixture(changed, { daysUntilRelease: 2 });
  await rt.db.prepare(`UPDATE venue_release_rules SET release_local_time = '10:00' WHERE venue_entity_id = ?`).bind(a.venue_entity_id).run();
  const r1 = await runAlarm(changed, a, { now: a.release - 86_400_000 });
  assert.deepEqual(r1.outcome, { status: 'ignored', reason: 'the release rule changed' });
  assert.equal(r1.sleeps.length, 0, 'it does not sleep towards a time that is no longer right');
  assert.equal(await alarmStatus(a), 'canceled');
  assert.equal(await fx.balance(changed.h), 1);

  const canceled = await light();
  await fx.grant(canceled.h, 1);
  const b = await alarmFixture(canceled, { daysUntilRelease: 2 });
  const r2 = await runAlarm(canceled, b, { now: b.release - 86_400_000, onSleep: async () => { await rt.db.prepare(`UPDATE drop_alarms SET status = 'canceled' WHERE alarm_id = ?`).bind(b.alarm_id).run(); } });
  assert.deepEqual(r2.outcome, { status: 'ignored', reason: 'the alarm is no longer valid' });
  assert.equal(r2.sleeps.length, 1);
  assert.deepEqual(r2.log, ['load_rule', 'stage_card']);
  assert.equal((await fx.all('SELECT 1 FROM mission_runs WHERE household_id = ?', canceled.h.household_id)).length, 0);
  assert.equal(await fx.balance(canceled.h), 1);
  assert.equal(canceled.net.push.length, 0);

  const gone = await light();
  const c = await alarmFixture(gone, { daysUntilRelease: 2 });
  await rt.db.prepare(`UPDATE drop_alarms SET status = 'canceled' WHERE alarm_id = ?`).bind(c.alarm_id).run();
  assert.deepEqual((await runAlarm(gone, c, { now: c.release - 86_400_000 })).outcome, { status: 'ignored', reason: 'alarm is canceled' });
});

test('DROP ALARM outcomes: decline and expiry keep the credit, a card that changed is refused and refunded, and no credit means a clean halt', async () => {
  const declined = await light();
  await subscribeMember(declined);
  await fx.grant(declined.h, 1);
  const a = await alarmFixture(declined, { daysUntilRelease: 1 });
  const ra = await alarmRun(a);
  const r1 = await runAlarm(declined, a, { now: a.release - 300_000, events: { [`approve:${ra}`]: { decision: 'decline', payload_hash: 'x' } } });
  assert.equal(r1.outcome.status, 'declined');
  assert.equal(await alarmStatus(a), 'canceled');
  assert.equal(await fx.balance(declined.h), 0, 'a decline keeps the debit');

  const expired = await light();
  await fx.grant(expired.h, 1);
  const b = await alarmFixture(expired, { daysUntilRelease: 1 });
  assert.equal((await runAlarm(expired, b, { now: b.release - 300_000 })).outcome.status, 'expired');
  assert.equal(await fx.balance(expired.h), 0);

  const stale = await light();
  await fx.grant(stale.h, 1);
  const c = await alarmFixture(stale, { daysUntilRelease: 1 });
  const rc = await alarmRun(c);
  const r3 = await runAlarm(stale, c, {
    now: c.release - 300_000,
    events: { [`approve:${rc}`]: async () => { const hash = (await approveWith(rc, stale.h.household_id)()).payload_hash; await rt.db.prepare(`UPDATE venue_release_rules SET release_local_time = '11:00' WHERE venue_entity_id = ?`).bind(c.venue_entity_id).run(); return { decision: 'approve', payload_hash: hash } satisfies ApprovalEvent; } },
  });
  assert.equal(r3.outcome.status, 'failed_gate', 'the rule changed after the member approved');
  assert.deepEqual((r3.outcome as { stage: string }).stage, 'r3');
  assert.equal(await fx.balance(stale.h), 1, 'refunded');
  assert.equal(await alarmStatus(c), 'canceled');
  assert.equal((await fx.all('SELECT 1 FROM deeplinks WHERE run_id = ?', await alarmRun(c))).length, 0);

  const broke = await light();
  await subscribeMember(broke);
  const d = await alarmFixture(broke, { daysUntilRelease: 1 });
  const r4 = await runAlarm(broke, d, { now: d.release - 300_000 });
  assert.deepEqual(r4.outcome, { status: 'insufficient_credits', auto_recharge: 'disabled' });
  assert.equal(await alarmStatus(d), 'canceled');
  assert.equal(broke.net.push.length, 0, 'nothing is pushed for a run that cannot start');
});

test('API drop alarms: creating one starts DropAlarmWF under the alarm id; events for its card go to THAT workflow; cancel is idempotent and private', async () => {
  const s = await light();
  await fx.grant(s.h, 1);
  const venue = await makeVenue(rt.db, s.h, { name: 'Carbone', days_ahead: 30, time: '09:00', tz: 'America/New_York' });
  const reservation = new Date(Date.now() + 60 * 86_400_000).toISOString().slice(0, 10);
  const post = (path: string, body: unknown, who: Household = s.h) => api(s.env, s.net, path, { h: who, body });
  assert.equal((await api(s.env, s.net, '/api/drop-alarms', { token: null, body: {} })).status, 401);
  const codes: Array<[unknown, number, string]> = [[{}, 400, 'bad_input'], [{ venue_entity_id: venue.venue_entity_id, reservation_date: new Date(Date.now() + 5 * 86_400_000).toISOString().slice(0, 10), party_size: 2 }, 409, 'release_passed'], [{ venue_entity_id: 'ent_nope', reservation_date: reservation, party_size: 2 }, 404, 'unknown_venue']];
  for (const [body, status, code] of codes) {
    const r = await post('/api/drop-alarms', body);
    assert.deepEqual([r.status, (await bodyOf(r))['error']], [status, code]);
  }
  assert.equal(s.workflows.alarm.length, 0);
  const ok = await post('/api/drop-alarms', { venue_entity_id: venue.venue_entity_id, reservation_date: reservation, party_size: 2 });
  assert.equal(ok.status, 201);
  const body = await bodyOf(ok);
  const alarm_id = String(body['alarm_id']);
  assert.match(alarm_id, /^alm_[0-9a-f]{32}$/);
  assert.deepEqual(s.workflows.alarm, [{ id: alarm_id, params: { alarm_id, household_id: s.h.household_id } }]);
  assert.equal((await post('/api/drop-alarms', { venue_entity_id: venue.venue_entity_id, reservation_date: reservation, party_size: 2 })).status, 409, 'a duplicate');

  // the card for this alarm lives in the DropAlarmWF instance: approve and confirm are routed there, not to MissionRunWF
  const a = { alarm_id, venue_entity_id: venue.venue_entity_id } as Alarm;
  const run_id = await alarmRun(a);
  const release = releaseAtMs((await loadRule(rt.db, s.h.household_id, venue.venue_entity_id))!, reservation)!;
  const { outcome } = await runAlarm(s, { ...a, release } as Alarm, {
    now: release - 300_000,
    events: {
      [`approve:${run_id}`]: async () => {
        const hash = (await getRun(rt.db, s.h.household_id, run_id))!.params['payload_hash'];
        assert.equal((await post(`/api/missions/${run_id}/start`, {})).status, 200, 'the card is already running inside its alarm workflow');
        assert.equal((await post(`/api/missions/${run_id}/approve`, { payload_hash: hash })).status, 202);
        return s.workflows.events.find((e) => e.type === `approve:${run_id}`)!.payload;
      },
      [`confirmed:${run_id}`]: async () => {
        assert.equal((await post(`/api/missions/${run_id}/confirm`, {})).status, 202);
        return s.workflows.events.find((e) => e.type === `confirmed:${run_id}`)!.payload;
      },
    },
  });
  assert.equal(outcome.status, 'confirmed');
  assert.deepEqual(s.workflows.events.map((e) => e.id), [alarm_id, alarm_id], 'both events went to the alarm\'s workflow instance');
  assert.equal(s.workflows.mission.length, 0, 'MissionRunWF never started');

  // cancel: owner only, idempotent
  const other = await light();
  const v2 = await makeVenue(rt.db, s.h, { days_ahead: 30, time: '09:00', tz: 'America/New_York' });
  const second = await post('/api/drop-alarms', { venue_entity_id: v2.venue_entity_id, reservation_date: reservation, party_size: 3 });
  const id2 = String((await bodyOf(second))['alarm_id']);
  assert.equal((await post(`/api/drop-alarms/${id2}/cancel`, {}, other.h)).status, 404, 'not someone else\'s');
  const cancel = await post(`/api/drop-alarms/${id2}/cancel`, {});
  assert.deepEqual([cancel.status, (await bodyOf(cancel))['status']], [200, 'canceled']);
  assert.equal((await bodyOf(await post(`/api/drop-alarms/${id2}/cancel`, {})))['status'], 'already_ended');
  assert.equal((await post(`/api/drop-alarms/alm_${'0'.repeat(32)}/cancel`, {})).status, 404);
  assert.equal((await post('/api/drop-alarms/not-an-id/cancel', {})).status, 404);
});

// ═══════════════ PriceWatchWF ═══════════════

const duffelEnv = { DUFFEL_API_KEY: 'duffel_test_0123456789' };
const dateIn = (n: number): string => new Date(Date.now() + n * 86_400_000).toISOString().slice(0, 10);
const offer = (dollars: string, o: { id?: string; currency?: string; carrier?: string } = {}): DuffelOffer => ({ id: o.id ?? `off_${randomUUID().slice(0, 8)}`, total_amount: dollars, total_currency: o.currency ?? 'USD', owner: { name: o.carrier ?? 'TAP Air Portugal' } });

async function watchFixture(s: Scenario, o: { ceiling?: number; depart?: number; ret?: number | null; envelope?: number | null; calendar?: boolean; booking?: string } = {}) {
  if (o.calendar !== false) await connectCalendar(s);
  if (o.envelope !== null) {
    await rt.db.prepare(`INSERT OR REPLACE INTO envelopes (household_id, envelope_key, monthly_target_cents) VALUES (?, 'travel', ?)`).bind(s.h.household_id, o.envelope ?? 300_000).run();
    // fresh spend data, so Gate G2 can trust the envelope's headroom (a household with a budget has imported some transactions)
    const episode = `ep_${randomUUID().slice(0, 12)}`;
    await rt.db.batch([
      rt.db.prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, redaction_status) VALUES (?, ?, 'csv', 'coffee', ?, 'clean')`).bind(episode, s.h.household_id, randomUUID()),
      rt.db.prepare(`INSERT INTO transactions (txn_id, household_id, episode_id, posted_on, amount_cents, descriptor_norm, envelope_key) VALUES (?, ?, ?, ?, -450, 'COFFEE SHOP', 'dining')`).bind(`txn_${randomUUID().slice(0, 12)}`, s.h.household_id, episode, new Date().toISOString().slice(0, 10)),
    ]);
  }
  const depart_date = dateIn(o.depart ?? 90);
  const return_date = o.ret === null ? null : dateIn((o.depart ?? 90) + (o.ret ?? 7));
  const res = await createPriceWatch(rt.db, s.h.household_id, { origin: 'jfk', destination: 'LIS', depart_date, return_date, ceiling_cents: o.ceiling ?? 60_000, ...(o.booking === undefined ? {} : { booking_domain: o.booking }) }, new Date());
  assert.ok(res.ok, JSON.stringify(res));
  return { watch_id: (res as { watch_id: string }).watch_id, depart_date, return_date };
}
type Watch = Awaited<ReturnType<typeof watchFixture>>;

function watchDeps(s: Scenario, clock: { now: Date }): PriceWatchDeps {
  const deps = missionDepsFromEnv(s.env, 'run_watch', s.h.household_id, { network: s.net.fetch, now: () => clock.now });
  return { ...deps, startMission: (o) => s.env.MISSION_RUN_WF.create(o) };
}
const watchRow = (w: Watch) => fx.one<{ active: number; run_id: string | null; last_checked_at: string | null; last_fare_cents: number | null }>('SELECT active, run_id, last_checked_at, last_fare_cents FROM price_watches WHERE watch_id = ?', w.watch_id);
const duffelSearches = (s: Scenario): number => s.net.duffel.calls.filter((c) => c.path === '/air/offer_requests?return_offers=true').length;

async function runWatch(s: Scenario, w: Watch, o: { clock?: { now: Date }; onSleep?: (n: number) => Promise<void> } = {}) {
  const clock = o.clock ?? { now: new Date() };
  const base = directStep(undefined, {}, clock);
  let slept = 0;
  const step = { ...base.step, sleep: async (name: string, d: number | string) => { await base.step.sleep(name, d); await o.onSleep?.(slept++); } };
  const outcome = await runPriceWatch(step, watchDeps(s, clock), { watch_id: w.watch_id, household_id: s.h.household_id });
  return { outcome, clock, log: base.log.map((l) => l.name), sleeps: base.sleeps };
}

test('DUFFEL adapter: search only, cheapest USD fare by decimal-string arithmetic, 4xx/5xx raised, nothing else ever called', async () => {
  assert.deepEqual(['540.00', '99.9', '1', '1234567.89', '0.05', ' 12.50 '].map(amountToCents), [54_000, 9_990, 100, 123_456_789, 5, 1_250]);
  for (const bad of ['1e3', '-5', '5.123', 'abc', '', '12,50', '$5', null, 12, undefined]) assert.equal(amountToCents(bad), null, String(bad));
  const s = await light();
  const guarded = createGuardedFetch({ db: rt.db, fetchImpl: s.net.fetch, ttlMs: 0 });
  const q = { origin: 'JFK', destination: 'LIS', depart_date: '2027-03-05', return_date: '2027-03-12', adults: 2 };
  s.net.duffel.offers = [offer('900.00', { currency: 'EUR' }), offer('610.50', { id: 'off_b' }), offer('abc'), offer('540.99', { id: 'off_a', carrier: 'TAP' }), offer('0.00'), { ...offer('1.00'), id: undefined as unknown as string }];
  assert.deepEqual(await searchFlights({ fetch: guarded, apiKey: 'k' }, q), { offer_id: 'off_a', total_cents: 54_099, carrier: 'TAP' }, 'the cheapest USD offer; EUR, malformed and zero fares are ignored');
  const call = s.net.duffel.calls[0]!;
  assert.deepEqual([call.method, call.path], ['POST', '/air/offer_requests?return_offers=true']);
  const data = call.body['data'] as { slices: Array<Record<string, string>>; passengers: unknown[]; cabin_class: string };
  assert.deepEqual(data.slices, [{ origin: 'JFK', destination: 'LIS', departure_date: '2027-03-05' }, { origin: 'LIS', destination: 'JFK', departure_date: '2027-03-12' }]);
  assert.deepEqual([data.passengers.length, data.cabin_class], [2, 'economy']);
  await searchFlights({ fetch: guarded, apiKey: 'k' }, { ...q, return_date: null, adults: 1 });
  assert.equal(((s.net.duffel.calls[1]!.body['data'] as { slices: unknown[] }).slices).length, 1, 'one-way is one slice');
  s.net.duffel.offers = [];
  assert.equal(await searchFlights({ fetch: guarded, apiKey: 'k' }, q), null);
  for (const status of [400, 401, 500, 503]) {
    s.net.duffel.status = status;
    await assert.rejects(searchFlights({ fetch: guarded, apiKey: 'k' }, q), (e: Error) => e.name === 'DuffelError' && (e as { status?: number }).status === status);
  }
  assert.ok(s.net.duffel.calls.every((c) => c.method === 'POST' && c.path === '/air/offer_requests?return_offers=true'), 'only the search endpoint is ever called');
  assert.ok(/^[A-Z]{3}$/.test('LIS') && !/^[A-Z]{3}$/.test('lis') && !/^[A-Z]{3}$/.test('LISB'));
});

test('API price watches: validated, grounded in the member\'s request, started as PriceWatchWF, at most two active, stoppable, private', async () => {
  const s = await light(duffelEnv);
  const post = (path: string, body: unknown, who: Household = s.h) => api(s.env, s.net, path, { h: who, body });
  const good = { origin: 'jfk', destination: 'lis', depart_date: dateIn(90), return_date: dateIn(97), ceiling_cents: 60_000 };
  assert.equal((await api(s.env, s.net, '/api/price-watches', { token: null, body: good })).status, 401);
  const bads: Array<[Record<string, unknown>, string]> = [
    [{ origin: 'JFKX' }, 'bad_input'], [{ origin: 'J1K' }, 'bad_input'], [{ destination: 'jfk' }, 'bad_input'], [{ depart_date: dateIn(-1) }, 'bad_input'], [{ depart_date: 'someday' }, 'bad_input'],
    [{ depart_date: dateIn(400), return_date: dateIn(405) }, 'bad_input'], [{ return_date: dateIn(80) }, 'bad_input'], [{ return_date: dateIn(200) }, 'bad_input'], [{ adults: 0 }, 'bad_input'],
    [{ adults: 5 }, 'bad_input'], [{ ceiling_cents: 4_999 }, 'bad_input'], [{ ceiling_cents: 1_000_001 }, 'bad_input'], [{ ceiling_cents: 500.5 }, 'bad_input'], [{ ceiling_cents: '600' }, 'bad_input'],
    [{ booking_domain: 'evil.example' }, 'bad_input'], [{ booking_domain: 'resy.com' }, 'bad_input'],
  ];
  for (const [patch, code] of bads) {
    const r = await post('/api/price-watches', { ...good, ...patch });
    assert.deepEqual([r.status, (await bodyOf(r))['error']], [400, code], JSON.stringify(patch));
  }
  assert.equal(s.workflows.watch.length, 0);
  const first = await post('/api/price-watches', good);
  assert.equal(first.status, 201);
  const id1 = String((await bodyOf(first))['watch_id']);
  assert.match(id1, /^pwt_[0-9a-f]{32}$/);
  assert.deepEqual(s.workflows.watch, [{ id: id1, params: { watch_id: id1, household_id: s.h.household_id } }]);
  const row = await fx.one<{ active: number; checks_per_day: number; ceiling_cents: number; params_json: string }>('SELECT active, checks_per_day, ceiling_cents, params_json FROM price_watches WHERE watch_id = ?', id1);
  const params = parseWatchParams(row.params_json)!;
  assert.deepEqual([row.active, row.checks_per_day, row.ceiling_cents, params.origin, params.destination, params.booking_domain, params.adults], [1, 1, 60_000, 'JFK', 'LIS', 'google.com', 1]);
  const fact = await fx.one<{ quote: string; source: string }>(`SELECT q.quote, e.source FROM facts f JOIN fact_quotes q ON q.fact_id = f.fact_id JOIN episodes e ON e.episode_id = q.episode_id WHERE f.fact_id = ?`, params.fact_id);
  assert.equal(fact.quote, `Watch flights from JFK to LIS departing ${good.depart_date} returning ${good.return_date} under $600.`);
  assert.equal(fact.source, 'user_input');

  assert.equal((await post('/api/price-watches', { ...good, destination: 'CDG' })).status, 201);
  const third = await post('/api/price-watches', { ...good, destination: 'FCO' });
  assert.deepEqual([third.status, (await bodyOf(third))['error']], [409, 'watch_limit'], 'a third active watch is refused');
  assert.equal(s.workflows.watch.length, 2, 'and starts nothing');
  assert.equal((await fx.all('SELECT 1 FROM price_watches WHERE household_id = ?', s.h.household_id)).length, 2);

  const other = await light();
  assert.equal((await post(`/api/price-watches/${id1}/stop`, {}, other.h)).status, 404, 'not someone else\'s');
  const stop = await post(`/api/price-watches/${id1}/stop`, {});
  assert.deepEqual([stop.status, (await bodyOf(stop))['status']], [200, 'stopped']);
  assert.equal((await bodyOf(await post(`/api/price-watches/${id1}/stop`, {})))['status'], 'already_ended');
  assert.equal((await post('/api/price-watches', { ...good, destination: 'FCO' })).status, 201, 'stopping freed a slot');
  assert.equal((await post(`/api/price-watches/pwt_${'0'.repeat(32)}/stop`, {})).status, 404);
  assert.equal((await post('/api/price-watches', good, other.h)).status, 201, 'the cap is per household');
});

test('PRICE WATCH daily check: a deal needs a fare at or under the ceiling that fits the travel envelope (G3) and a free calendar (G4); each miss waits a day', async () => {
  const clock = { now: new Date() };
  const decide = async (arrange: (s: Scenario, w: Watch) => Promise<void> | void, o: Parameters<typeof watchFixture>[1] = {}, extra: Partial<Env> = duffelEnv) => {
    const s = await light(extra);
    const w = await watchFixture(s, o);
    s.net.duffel.offers = [offer('540.00')];
    await arrange(s, w);
    const check = await dailyCheck(watchDeps(s, clock), { watch_id: w.watch_id, household_id: s.h.household_id });
    return { s, w, check };
  };
  const reason = (c: Check): string => (c.action === 'trigger' ? 'trigger' : `${c.action}:${c.reason}`);
  assert.equal(reason((await decide(() => undefined)).check), 'trigger');
  assert.equal(reason((await decide((s) => { s.net.duffel.offers = [offer('600.00')]; })).check), 'trigger', 'a fare equal to the ceiling is a deal');
  assert.equal(reason((await decide((s) => { s.net.duffel.offers = [offer('600.01')]; })).check), 'sleep:above_ceiling');
  const above = await decide((s) => { s.net.duffel.offers = [offer('700.00')]; });
  assert.equal((await watchRow(above.w)).last_fare_cents, 70_000, 'the fare seen is recorded');
  assert.equal(reason((await decide((s) => { s.net.duffel.offers = []; })).check), 'sleep:no_offers');
  assert.equal(reason((await decide((s) => { s.net.duffel.offers = [offer('540.00', { currency: 'EUR' })]; })).check), 'sleep:no_offers', 'only USD fares count');

  // G3: price + 10% within the travel envelope's headroom
  assert.equal(reason((await decide(() => undefined, { envelope: 59_399 })).check), 'sleep:no_headroom', '$540 + 10% = $594 does not fit $593.99');
  assert.equal(reason((await decide(() => undefined, { envelope: 59_400 })).check), 'trigger', 'and fits $594.00 exactly');
  assert.equal(reason((await decide(() => undefined, { envelope: null })).check), 'sleep:no_headroom', 'no travel envelope: no headroom is known');
  const stale = await decide(async (s) => { await rt.db.prepare(`UPDATE episodes SET received_at = ? WHERE household_id = ?`).bind(new Date(Date.now() - 36 * 86_400_000).toISOString(), s.h.household_id).run(); });
  assert.equal(reason(stale.check), 'sleep:stale_spend_data', 'Gate G2 would refuse a budget older than 35 days, so no card is built on it');
  assert.equal(reason((await decide(async (s) => { await rt.db.prepare('DELETE FROM transactions WHERE household_id = ?').bind(s.h.household_id).run(); })).check), 'sleep:stale_spend_data', 'and none at all is not fresh');
  // G4: the trip must be free on the calendar
  assert.equal(reason((await decide((s, w) => { s.net.google.busy = [{ start: `${w.depart_date}T10:00:00Z`, end: `${w.depart_date}T11:00:00Z` }]; })).check), 'sleep:calendar_busy');
  assert.equal(reason((await decide((s, w) => { s.net.google.busy = [{ start: `${w.return_date}T00:30:00Z`, end: `${w.return_date}T01:00:00Z` }]; })).check), 'sleep:calendar_busy', 'busy on the return day too');
  assert.equal(reason((await decide((s, w) => { s.net.google.busy = [{ start: `${dateIn(60)}T10:00:00Z`, end: `${dateIn(60)}T11:00:00Z` }]; void w; })).check), 'trigger', 'busy before the trip does not matter');
  assert.equal(reason((await decide(() => undefined, { calendar: false })).check), 'sleep:calendar_busy', 'an unreadable calendar is not a free one');
  // no key: no search at all
  const keyless = await decide(() => undefined, {}, {});
  assert.equal(reason(keyless.check), 'sleep:no_api_key');
  assert.equal(duffelSearches(keyless.s), 0);
});

test('PRICE WATCH search failures: a rejection is "no offers", an outage is retried without using up the day', async () => {
  const clock = { now: new Date() };
  const s = await light(duffelEnv);
  const w = await watchFixture(s);
  const deps = watchDeps(s, clock);
  const params = { watch_id: w.watch_id, household_id: s.h.household_id };
  s.net.duffel.status = 422;
  assert.deepEqual(await dailyCheck(deps, params), { action: 'sleep', reason: 'no_offers' });
  s.net.duffel.status = null;
  await rt.db.prepare('UPDATE price_watches SET last_checked_at = NULL WHERE watch_id = ?').bind(w.watch_id).run();
  s.net.duffel.status = 503;
  await assert.rejects(dailyCheck(deps, params), /Duffel answered 503/);
  assert.equal((await watchRow(w)).last_checked_at, null, 'the claim on today\'s check was handed back');
  s.net.duffel.status = null;
  s.net.duffel.offers = [offer('500.00')];
  assert.equal((await dailyCheck(deps, params)).action, 'trigger', 'so the retry can search again');
  // in the loop, an outage that outlasts the retries becomes "try tomorrow", not the end of the watch
  const s2 = await light(duffelEnv);
  const w2 = await watchFixture(s2);
  s2.net.duffel.status = 500;
  const loop = await runWatch(s2, w2, { onSleep: async () => { await rt.db.prepare('UPDATE price_watches SET active = 0 WHERE watch_id = ?').bind(w2.watch_id).run(); } });
  assert.deepEqual(loop.outcome, { status: 'stopped', reason: 'inactive', day: 1 });
  assert.deepEqual(loop.sleeps.map((x) => [x.name, x.ms]), [['sleep_0', 86_400_000]]);
});

test('PRICE WATCH is limited to one search a day per watch, even with two instances or a replay', async () => {
  const s = await light(duffelEnv);
  const w = await watchFixture(s);
  s.net.duffel.offers = [offer('700.00')];
  const clock = { now: new Date() };
  const deps = watchDeps(s, clock);
  const params = { watch_id: w.watch_id, household_id: s.h.household_id };
  const [a, b] = await Promise.all([dailyCheck(deps, params), dailyCheck(deps, params)]);
  assert.equal(duffelSearches(s), 1, 'two instances at once: one search');
  assert.deepEqual([a, b].map((c) => (c.action === 'sleep' ? c.reason : c.action)).sort(), ['above_ceiling', 'checked_recently']);
  assert.deepEqual(await dailyCheck(deps, params), { action: 'sleep', reason: 'checked_recently' });
  clock.now = new Date(clock.now.getTime() + 20 * 3_600_000 - 1);
  assert.equal((await dailyCheck(deps, params) as { reason: string }).reason, 'checked_recently', '19 h 59 min later is still the same day');
  clock.now = new Date(clock.now.getTime() + 1);
  assert.equal((await dailyCheck(deps, params) as { reason: string }).reason, 'above_ceiling', 'at 20 h it checks again');
  assert.equal(duffelSearches(s), 2);
  assert.equal(CHECKS_PER_DAY, 1);
  // a watch set to 0 checks a day is paused: no search
  await rt.db.prepare('UPDATE price_watches SET checks_per_day = 0, last_checked_at = NULL WHERE watch_id = ?').bind(w.watch_id).run();
  assert.deepEqual(await dailyCheck(deps, params), { action: 'sleep', reason: 'paused' });
  assert.equal(duffelSearches(s), 2);
});

test('PRICE WATCH ends when it should: stopped, after departure, or when the household is gone', async () => {
  const s = await light(duffelEnv);
  const clock = { now: new Date() };
  const deps = watchDeps(s, clock);
  const w = await watchFixture(s);
  const p = { watch_id: w.watch_id, household_id: s.h.household_id };
  await rt.db.prepare('UPDATE price_watches SET active = 0 WHERE watch_id = ?').bind(w.watch_id).run();
  assert.deepEqual(await dailyCheck(deps, p), { action: 'stop', reason: 'inactive' });
  assert.deepEqual(await dailyCheck(deps, { ...p, watch_id: 'pwt_missing' }), { action: 'stop', reason: 'inactive' });
  assert.deepEqual(await dailyCheck(deps, { ...p, household_id: 'hh_other' }), { action: 'stop', reason: 'inactive' }, 'a watch is only ever read for its own household');
  const s2 = await light(duffelEnv);
  const w2 = await watchFixture(s2, { depart: 10 });
  const deps2 = watchDeps(s2, { now: new Date(Date.now() + 11 * 86_400_000) });
  assert.deepEqual(await dailyCheck(deps2, { watch_id: w2.watch_id, household_id: s2.h.household_id }), { action: 'stop', reason: 'departed' });
  assert.equal((await watchRow(w2)).active, 0, 'and it is switched off');
  assert.equal(duffelSearches(s2), 0);
  const s3 = await light(duffelEnv);
  const w3 = await watchFixture(s3);
  await rt.db.prepare(`UPDATE households SET deleted_at = '2026-01-01T00:00:00Z' WHERE household_id = ?`).bind(s3.h.household_id).run();
  assert.deepEqual(await dailyCheck(watchDeps(s3, clock), { watch_id: w3.watch_id, household_id: s3.h.household_id }), { action: 'stop', reason: 'household gone' });
});

test('PRICE WATCH loop: one check a day with step.sleep("1 day") between, then a card and MissionRunWF the day the fare drops', async () => {
  const s = await light(duffelEnv);
  const w = await watchFixture(s, { ceiling: 60_000 });
  const fares = ['720.00', '650.00', '540.00'];
  s.net.duffel.offers = [offer(fares[0]!)];
  const startedAt = new Date();
  const clock = { now: new Date(startedAt) };
  const { outcome, log, sleeps } = await runWatch(s, w, { clock, onSleep: async (n) => { s.net.duffel.offers = [offer(fares[n + 1]!)]; } });

  assert.equal(outcome.status, 'triggered');
  const run_id = (outcome as { run_id: string }).run_id;
  assert.deepEqual(outcome, { status: 'triggered', run_id, day: 2 });
  assert.deepEqual(log, ['check_0', 'check_1', 'check_2', 'trigger_2']);
  assert.deepEqual(sleeps, [{ name: 'sleep_0', duration: '1 day', ms: 86_400_000 }, { name: 'sleep_1', duration: '1 day', ms: 86_400_000 }]);
  assert.equal(clock.now.getTime() - startedAt.getTime(), 2 * 86_400_000);
  assert.equal(duffelSearches(s), 3, 'exactly one search per day');
  assert.ok(s.net.duffel.calls.every((c) => c.method === 'POST' && c.path === '/air/offer_requests?return_offers=true'), 'search only: no orders, no payments');

  const row = await watchRow(w);
  assert.deepEqual([row.active, row.run_id, row.last_fare_cents], [0, run_id, 54_000], 'one deal, one card: the watch is switched off');
  const run = (await getRun(rt.db, s.h.household_id, run_id))!;
  assert.deepEqual([run.template_key, run.state, run.params['trigger_key'], run.params['dedupe_key']], ['flight_watch', 'R1_STAGED', 'T_PRICE_WATCH', w.watch_id]);
  assert.match(String(run.params['title']), /^JFK to LIS fares dropped to \$540 for /);
  assert.deepEqual(s.workflows.mission, [{ id: run_id, params: { run_id, household_id: s.h.household_id } }], 'MissionRunWF was started for the card');
  const claims = await fx.all<{ text: string; status: string }>('SELECT text, status FROM claims WHERE run_id = ?', run_id);
  assert.deepEqual(claims.map((c) => c.status), ['grounded']);
  assert.match(claims[0]!.text, /^You asked to watch JFK to LIS \(.*\) for fares under \$600$/);
  const run2 = await runPriceWatch(directStep().step, watchDeps(s, { now: new Date() }), { watch_id: w.watch_id, household_id: s.h.household_id });
  assert.deepEqual(run2, { status: 'stopped', reason: 'inactive', day: 0 }, 'a second instance finds nothing left to do');
  assert.equal(s.workflows.mission.length, 1);
});

test('PRICE WATCH deal in MissionRunWF: fresh price, envelope and calendar checks, the member\'s approval, then an affiliate-tagged booking link; nothing is booked', async () => {
  const s = await light({ ...duffelEnv, AFFILIATE_TAGS: JSON.stringify({ 'google.com': { program: 'flights', param: 'aff', value: 'hayes-9' } }) });
  await subscribeMember(s);
  await fx.grant(s.h, 1);
  const w = await watchFixture(s, { ceiling: 60_000 });
  s.net.duffel.offers = [offer('540.00')];
  const clock = { now: new Date() };
  const triggered = await runPriceWatch(directStep().step, watchDeps(s, clock), { watch_id: w.watch_id, household_id: s.h.household_id });
  assert.equal(triggered.status, 'triggered');
  const run_id = (triggered as { run_id: string }).run_id;
  s.net.duffel.calls.length = 0;
  s.net.google.log.length = 0;

  const { step, waits } = directStep(undefined, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id), [`confirmed:${run_id}`]: {} });
  const outcome = await runMission(step, missionDepsFromEnv(s.env, run_id, s.h.household_id, { network: s.net.fetch }), { run_id, household_id: s.h.household_id });
  assert.equal(outcome.status, 'confirmed');
  assert.deepEqual(waits.map((x) => x.type), [`approve:${run_id}`, `confirmed:${run_id}`]);
  assert.ok(duffelSearches(s) >= 2, 'the price was re-read at the first gates and again at R3');
  assert.ok(s.net.duffel.calls.every((c) => c.path === '/air/offer_requests?return_offers=true'));
  const busyCalls = s.net.google.log.filter((l) => l.path.endsWith('/freeBusy'));
  assert.ok(busyCalls.length >= 2, 'the calendar was re-read too');
  const asked = JSON.parse(busyCalls[0]!.body) as { timeMin: string; timeMax: string };
  assert.deepEqual(asked, { ...asked, timeMin: tripSpan({ depart_date: w.depart_date, return_date: w.return_date }, 'America/New_York').start, timeMax: tripSpan({ depart_date: w.depart_date, return_date: w.return_date }, 'America/New_York').end });

  const run = (await getRun(rt.db, s.h.household_id, run_id))!;
  const payload = run.params['payload'] as { price_cents: number; envelope_key: string; proposed_range: { start: string; end: string }; action: { merchant_domain: string; path: string } };
  assert.deepEqual([payload.price_cents, payload.envelope_key, payload.action.merchant_domain, payload.action.path], [54_000, 'travel', 'google.com', '/travel/flights']);
  assert.deepEqual(payload.proposed_range, tripSpan({ depart_date: w.depart_date, return_date: w.return_date }, 'America/New_York'));
  const link = await fx.one<{ url: string; affiliate_program: string | null }>('SELECT url, affiliate_program FROM deeplinks WHERE run_id = ?', run_id);
  const u = new URL(link.url);
  assert.deepEqual([u.hostname, u.pathname, u.searchParams.get('aff'), link.affiliate_program], ['google.com', '/travel/flights', 'hayes-9', 'flights'], 'an affiliate-tagged deep link');
  assert.equal(u.searchParams.get('q'), `Flights from JFK to LIS on ${w.depart_date} through ${w.return_date}`);
  assert.equal(await fx.balance(s.h), 0);
});

test('PRICE WATCH deal gates: a fare that rose, a calendar that filled, or an envelope that shrank after staging refuses the card and refunds', async () => {
  const stage = async (extra: (s: Scenario) => void = () => undefined) => {
    const s = await light(duffelEnv);
    await fx.grant(s.h, 1);
    const w = await watchFixture(s, { ceiling: 60_000 });
    s.net.duffel.offers = [offer('540.00')];
    const t = await runPriceWatch(directStep().step, watchDeps(s, { now: new Date() }), { watch_id: w.watch_id, household_id: s.h.household_id });
    extra(s);
    return { s, w, run_id: (t as { run_id: string }).run_id };
  };
  const run = async (s: Scenario, run_id: string) => {
    const { step } = directStep(undefined, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id) });
    return runMission(step, missionDepsFromEnv(s.env, run_id, s.h.household_id, { network: s.net.fetch }), { run_id, household_id: s.h.household_id });
  };
  const rose = await stage((s) => { s.net.duffel.offers = [offer('700.00')]; });
  const a = await run(rose.s, rose.run_id);
  assert.equal(a.status, 'failed_gate');
  assert.ok((a as { failed: string[] }).failed.includes('G2'), 'the conditions behind the card no longer hold');
  assert.equal(await fx.balance(rose.s.h), 1, 'refunded');
  assert.equal((await fx.all('SELECT 1 FROM deeplinks WHERE run_id = ?', rose.run_id)).length, 0);

  const busy = await stage((s) => { s.net.google.busy = [{ start: new Date(Date.now() - 86_400_000).toISOString(), end: new Date(Date.now() + 200 * 86_400_000).toISOString() }]; });
  assert.deepEqual(await run(busy.s, busy.run_id), { status: 'failed_gate', stage: 'r1', failed: ['G4'] });
  assert.equal(await fx.balance(busy.s.h), 1);

  const shrunk = await stage();
  await rt.db.prepare(`UPDATE envelopes SET monthly_target_cents = 50000 WHERE household_id = ?`).bind(shrunk.s.h.household_id).run();
  assert.deepEqual(await run(shrunk.s, shrunk.run_id), { status: 'failed_gate', stage: 'r1', failed: ['G3'] });
  assert.equal(await fx.balance(shrunk.s.h), 1);
});

test('PRICE WATCH: only the run PriceWatchWF created for a watch may act on it', async () => {
  const s = await light(duffelEnv);
  await fx.grant(s.h, 1);
  const w = await watchFixture(s);
  s.net.duffel.offers = [offer('500.00')];
  const params = parseWatchParams((await fx.one<{ params_json: string }>('SELECT params_json FROM price_watches WHERE watch_id = ?', w.watch_id)).params_json)!;
  const data = { watch_id: w.watch_id, origin: 'JFK', destination: 'LIS', depart_date: w.depart_date, return_date: w.return_date, fare_cents: 50_000, ceiling_cents: 60_000 };
  // a card for the watch that the workflow did not create: its run id is not the watch's
  const run_id = await insertRun(rt.db, s.h.household_id, { origin: 'user', template_key: 'flight_watch', trigger_key: 'T_PRICE_WATCH', dedupe_key: w.watch_id, copy: buildCopy('T_PRICE_WATCH', data), recoverable_cents: 0, urgency: 1, confidence: 1, score: 1, data, claims: [{ key: 'watch', facts: [params.fact_id] }], now: new Date() });
  const { step } = directStep(undefined, { [`approve:${run_id}`]: approveWith(run_id, s.h.household_id) });
  const outcome = await runMission(step, missionDepsFromEnv(s.env, run_id, s.h.household_id, { network: s.net.fetch }), { run_id, household_id: s.h.household_id });
  assert.equal(outcome.status, 'failed_gate');
  assert.ok((outcome as { failed: string[] }).failed.includes('G2'));
  assert.equal(duffelSearches(s), 0, 'no search was made for a run that does not own the watch');
  assert.equal(await fx.balance(s.h), 1);
});

// ═══════════════ structure ═══════════════

test('STRUCTURE (M5): the new Workflows are bound and exported as thin wrappers; secrets stay out of wrangler.toml; Stripe and Duffel live only in their own modules', () => {
  const root = resolve(process.env['PROJECT_ROOT'] ?? process.cwd());
  const read = (p: string): string => readFileSync(join(root, p), 'utf8');
  const toml = read('wrangler.toml');
  for (const [name, binding, cls] of [['life-receipt', 'LIFE_RECEIPT_WF', 'LifeReceiptWF'], ['drop-alarm', 'DROP_ALARM_WF', 'DropAlarmWF'], ['price-watch', 'PRICE_WATCH_WF', 'PriceWatchWF']]) {
    assert.match(toml, new RegExp(`\\[\\[workflows\\]\\]\\s*\\nname = "${name}"\\s*\\nbinding = "${binding}"\\s*\\nclass_name = "${cls}"`));
    assert.match(read('src/index.ts'), new RegExp(`export \\{ ${cls} \\} from '\\./workflows/`));
  }
  assert.match(toml, /\[\[send_email\]\]\s*\nname = "RECEIPT_EMAIL"/);
  const vars = /\[vars\]\n([\s\S]*?)(?=\n\[|\n#|$)/.exec(toml)![1]!;
  assert.deepEqual([...vars.matchAll(/^(\w+) = /gm)].map((m) => m[1]), ['APP_NAME', 'INBOUND_DOMAIN'], 'no secret or key is committed in [vars]');
  for (const secret of ['STRIPE_SECRET_KEY', 'STRIPE_WEBHOOK_SECRET', 'DUFFEL_API_KEY']) assert.ok(!new RegExp(`^${secret}\\s*=`, 'm').test(toml), `${secret} is a wrangler secret`);

  for (const [file, calls] of [['src/workflows/lifeReceiptWF.ts', 'runLifeReceipt('], ['src/workflows/dropAlarmWF.ts', 'runDropAlarm('], ['src/workflows/priceWatchWF.ts', 'runPriceWatch(']] as const) {
    const wrapper = read(file);
    assert.ok(wrapper.split('\n').length < 30, `${file}: the logic lives where the tests can reach it`);
    assert.ok(wrapper.includes(calls), file);
    assert.ok(!/runGates|debitCredit|grantPack|insertRun|transitionRun/.test(wrapper), file);
  }

  const walk = (dir: string): string[] => readdirSync(join(root, dir)).flatMap((n) => (statSync(join(root, dir, n)).isDirectory() ? walk(`${dir}/${n}`) : [`${dir}/${n}`]));
  const sources = walk('src').filter((f) => f.endsWith('.ts'));
  const code = (f: string): string => read(f).replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
  for (const f of sources) {
    const c = code(f);
    if (f !== 'src/billing/stripe.ts') assert.ok(!/api\.stripe\.com/.test(c), `${f}: only src/billing/stripe.ts talks to Stripe`);
    if (f !== 'src/travel/duffel.ts') assert.ok(!/api\.duffel\.com/.test(c), `${f}: only src/travel/duffel.ts talks to Duffel`);
    // (src/redact.ts names account numbers because it REMOVES them; everything that moves money or books travel must never touch them)
    if (f !== 'src/redact.ts') assert.ok(!/air\/orders|\/v1\/charges|\/v1\/payment_methods|\/v1\/tokens|card_number|\bcvv\b|routing_number|account_number/i.test(c), `${f}: no booking, no card data, no bank data`);
    assert.ok(!/Hayes|askhayes/i.test(c), `${f}: the product name and domain come from APP_NAME and INBOUND_DOMAIN`);
  }
  const hook = code('src/billing/stripeWebhook.ts');
  assert.ok(hook.includes('request.text()') && !/request\.json\(|\.formData\(/.test(hook), 'the signature is checked against the raw body');
  assert.ok(!/Math\.random/.test(sources.filter((f) => f.startsWith('src/billing/')).map(code).join('\n')), 'billing ids come from crypto, never Math.random');
});

// ── END ──
