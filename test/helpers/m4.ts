/**
 * Milestone 4 fixtures: the Milestone 3 flagship household (card + calendar + Monarch CSV) bound to a runtime, credits, a router that
 * fakes the three networks MissionRunWF talks to (Google, the model vendors, Web Push) underneath the REAL egress guard, and VAPID keys.
 * D1, R2 and KV are the real local ones; only the network and the Workflows engine are faked.
 */

import { randomUUID } from 'node:crypto';
import { runCalendarSync } from '../../src/connectors/calendarPipeline';
import { exchangeCode, type CalendarDeps, type GoogleEvent } from '../../src/connectors/googleCalendar';
import { putEncrypted, sha256Hex } from '../../src/crypto/vault';
import { writeFact } from '../../src/graph/writeFact';
import { runIngestFile } from '../../src/ingest/filePipeline';
import { recomputeCreditPeriods } from '../../src/ingest/spend';
import { createGateway } from '../../src/llm/gateway';
import { runNightlySweep } from '../../src/maintenance/sweepPipeline';
import { missionDepsFromEnv, type MissionDeps } from '../../src/missions/runPipeline';
import { createGuardedFetch } from '../../src/net/egress';
import type { Env } from '../../src/types';
import type { TestRuntime } from './index';
import { directStep, fakeProvider, makeHousehold, openaiReply, testEnv, type Household, type ProviderCall } from './m2';
import { day, fakeGoogle, newGoogle, type FakeGoogleState } from './m3';
import assert from 'node:assert/strict';

export const q = (s: string): string => `"${s.replace(/"/g, '""')}"`;
export const MONARCH_HEADER = 'Date,Merchant,Category,Account,Original Statement,Notes,Amount,Tags,Owner';
export const mrow = (date: string, merchant: string, category: string, account: string, orig: string, amount: string): string =>
  `${date},${q(merchant)},${q(category)},${q(account)},${q(orig)},,${amount},,`;
export const monthDay = (monthOffset: number, d: number): string => {
  const now = new Date();
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + monthOffset, d)).toISOString().slice(0, 10);
};
export const AMEX = 'Platinum Card® (...1005)';
export const allDayEvent = (id: string, summary: string, start: string, end: string): GoogleEvent => ({ id, summary, start: { date: start }, end: { date: end } });

export const PUSH_HOST = 'fcm.googleapis.com';
export const PUSH_ENDPOINT = `https://${PUSH_HOST}/fcm/send/abc123`;

/** A fresh VAPID key pair in the format the app expects (base64url 65-byte public point, 32-byte private scalar). */
export async function vapidKeys(): Promise<{ publicKey: string; privateKey: string; subject: string }> {
  const pair = (await crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify'])) as CryptoKeyPair;
  const jwk = (await crypto.subtle.exportKey('jwk', pair.privateKey)) as { d: string; x: string; y: string };
  const b64u = (b64: string): Uint8Array => Uint8Array.from(Buffer.from(b64.replace(/-/g, '+').replace(/_/g, '/'), 'base64'));
  const point = new Uint8Array(65);
  point[0] = 4;
  point.set(b64u(jwk.x), 1);
  point.set(b64u(jwk.y), 33);
  return { publicKey: Buffer.from(point).toString('base64url'), privateKey: jwk.d, subject: 'mailto:ops@example.test' };
}

export interface PushCall {
  url: string;
  authorization: string | null;
  ttl: string | null;
  bodyLength: number;
}

export interface Network {
  google: FakeGoogleState;
  llm: ProviderCall[];
  push: PushCall[];
  /** Status the push service answers with. */
  pushStatus: number;
  /** What the worker model answers (JSON text of the structured output). Default: a polite script. */
  workerReply: () => Response;
  /** Every host the router saw. */
  hosts: string[];
  fetch: typeof fetch;
}

export const SAFE_SCRIPT = 'Hello, I am calling about my annual fee. I have used the card credits this year and would like to know what retention offers you have before I decide.';

export function newNetwork(): Network {
  const google = newGoogle();
  const googleFetch = fakeGoogle(google);
  const llm: ProviderCall[] = [];
  const net: Network = {
    google,
    llm,
    push: [],
    pushStatus: 201,
    workerReply: () => openaiReply({ script: SAFE_SCRIPT }),
    hosts: [],
    fetch: undefined as unknown as typeof fetch,
  };
  const provider = fakeProvider((call) => {
    llm.push(call);
    return net.workerReply();
  });
  net.fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url);
    net.hosts.push(url.hostname);
    if (url.hostname === 'api.openai.com' || url.hostname === 'api.anthropic.com') return provider.fetch(input, init);
    if (url.hostname === PUSH_HOST) {
      const h = new Headers(init?.headers);
      net.push.push({ url: url.href, authorization: h.get('authorization'), ttl: h.get('ttl'), bodyLength: typeof init?.body === 'string' ? init.body.length : init?.body ? 1 : 0 });
      return new Response(null, { status: net.pushStatus });
    }
    if (url.hostname.endsWith('googleapis.com')) return googleFetch(input, init);
    return new Response('unexpected host', { status: 500 });
  }) as typeof fetch;
  return net;
}

export interface Scenario {
  h: Household;
  env: Env;
  net: Network;
  cardId: string;
  feeDate: string;
  anniversary: string | null;
  workflows: ReturnType<typeof testEnv>['workflows'];
}

interface Fact {
  subject: { type: 'card_account' | 'household' | 'person'; name: string };
  predicate: string;
  value_text?: string;
  value_cents?: number;
  quote: string;
}

export function fixtures(rt: TestRuntime) {
  const db = rt.db;
  const all = async <T>(sql: string, ...b: unknown[]): Promise<T[]> => (await db.prepare(sql).bind(...b).all<T>()).results;
  const one = async <T>(sql: string, ...b: unknown[]): Promise<T> => (await db.prepare(sql).bind(...b).first<T>())!;

  async function userEpisode(h: Household, text: string): Promise<string> {
    const id = `ep_${randomUUID().slice(0, 12)}`;
    await db
      .prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, occurred_at, redaction_status) VALUES (?, ?, 'user_input', ?, ?, NULL, 'clean')`)
      .bind(id, h.household_id, text, randomUUID())
      .run();
    return id;
  }

  async function writeFacts(h: Household, episodeId: string, text: string, facts: Fact[]): Promise<void> {
    for (const f of facts) {
      const at = text.indexOf(f.quote);
      assert.ok(at >= 0, `quote not in text: ${f.quote}`);
      const r = await writeFact(db, {
        household_id: h.household_id,
        source_episode_id: episodeId,
        subject: f.subject,
        predicate: f.predicate,
        confidence: 1,
        ...(f.value_text !== undefined ? { value_text: f.value_text } : {}),
        ...(f.value_cents !== undefined ? { value_cents: f.value_cents } : {}),
        quotes: [{ quote: f.quote, char_start: at, char_end: at + f.quote.length }],
      });
      assert.ok(r.status === 'written' || r.status === 'reaffirmed', JSON.stringify(r));
    }
  }

  async function cardHousehold(o: { name: string; product: string; last4: string; fee?: { cents: number; postsOn: string } }) {
    const h = await makeHousehold(db);
    const text = `${o.name} ending ${o.last4}${o.fee ? `, annual fee $${o.fee.cents / 100} posts ${o.fee.postsOn}` : ''}.`;
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
    await writeFacts(h, episode, text, facts);
    const card = await one<{ entity_id: string }>(`SELECT entity_id FROM entities WHERE household_id = ? AND type = 'card_account'`, h.household_id);
    return { h, cardId: card.entity_id, episode };
  }

  /** +n credits (admin_adjust). Households start with none: grants come from Stripe events in a later milestone. */
  async function grant(h: Household, n: number): Promise<void> {
    await db
      .prepare(`INSERT INTO credit_ledger (entry_id, household_id, delta, reason, run_id, stripe_event_id) VALUES (?, ?, ?, 'admin_adjust', NULL, NULL)`)
      .bind(`led_grant_${randomUUID()}`, h.household_id, n)
      .run();
  }

  const balance = async (h: Household): Promise<number> => (await one<{ b: number }>(`SELECT COALESCE(SUM(delta), 0) AS b FROM credit_ledger WHERE household_id = ?`, h.household_id)).b;

  function calendarDeps(env: Env, net: Network): CalendarDeps {
    return { env, fetch: createGuardedFetch({ db, fetchImpl: net.fetch, ttlMs: 0 }), now: () => new Date() };
  }

  async function ingestCsv(env: Env, h: Household, text: string): Promise<void> {
    const bytes = new TextEncoder().encode(text);
    const sha256 = await sha256Hex(bytes);
    const r2_key = `raw/${h.household_id}/${sha256}.csv.enc`;
    await putEncrypted(env, h.household_id, r2_key, bytes);
    const gateway = createGateway({ db, apiKeys: {}, fetch: (async () => { throw new Error('the LLM must not be called while ingesting'); }) as unknown as typeof fetch });
    await runIngestFile({ env, gateway }, { household_id: h.household_id, r2_key, file_name: 'export.csv', kind: 'csv', sha256 }, directStep().step);
  }

  /**
   * The Milestone 3 flagship household: an Amex Platinum with a $895 fee, Resy/dining credits, an anniversary in `occasionInDays`, free
   * Friday/Saturday evenings, and three months of Monarch spend. One sweep of it stages anniv_dining, annual_fee_roi and expiring_credit.
   */
  async function buildScenario(o: { occasionInDays?: number; extraDiningCents?: number; envExtra?: Partial<Env> } = {}): Promise<Scenario> {
    const feeDate = day(12);
    const { h, cardId } = await cardHousehold({ name: 'Amex Platinum', product: 'amex_platinum', last4: '1005', fee: { cents: 89500, postsOn: feeDate } });
    const net = newNetwork();
    const { env, workflows } = testEnv(rt, o.envExtra);
    const days = o.occasionInDays ?? 10;
    const anniversary = day(days);
    net.google.events.push(allDayEvent('occ', 'Our Anniversary', anniversary, day(days + 1)));
    net.google.events.push({ id: 'm1', summary: 'Team standup', start: { dateTime: `${day(1)}T14:00:00Z` }, end: { dateTime: `${day(1)}T14:30:00Z` } });
    const cal = calendarDeps(env, net);
    await exchangeCode(cal, h.household_id, 'good-code', 'https://app.example/cb');
    const { step } = directStep();
    await runCalendarSync({ cal, startSweep: async () => {} }, { household_id: h.household_id, then_sweep: false }, step);
    const rows = [
      mrow(monthDay(-3, 10), 'Carbone', 'Restaurants & Bar', 'Checking (...9999)', 'CARBONE NEW YORK', '-400.00'),
      mrow(monthDay(-2, 10), 'Carbone', 'Restaurants & Bar', 'Checking (...9999)', 'CARBONE NEW YORK', '-500.00'),
      mrow(monthDay(-1, 10), 'Carbone', 'Restaurants & Bar', 'Checking (...9999)', 'CARBONE NEW YORK', '-600.00'),
      mrow(monthDay(-1, 12), 'Paycheck', 'Paychecks', 'Checking (...9999)', 'ACME PAYROLL', '3000.00'),
    ];
    for (const m of [-3, -2, -1, 0]) rows.push(mrow(monthDay(m, 1), 'Netflix', 'Subscriptions', AMEX, 'NETFLIX.COM', '-15.99'));
    rows.push(mrow(monthDay(0, 1), 'Uber', 'Taxi & Ride Shares', AMEX, 'UBER *TRIP', '-15.00'));
    if ((o.extraDiningCents ?? 0) > 0) rows.push(mrow(monthDay(0, 1), 'Wine Bar', 'Restaurants & Bar', 'Checking (...9999)', 'WINE BAR', `-${((o.extraDiningCents ?? 0) / 100).toFixed(2)}`));
    await ingestCsv(env, h, [MONARCH_HEADER, ...rows].join('\n') + '\n');
    await recomputeCreditPeriods(db, h.household_id, { from: day(0), to: day(0) }, new Date());
    return { h, env, net, cardId, feeDate, anniversary, workflows };
  }

  /** One nightly sweep, then the staged runs by template. */
  async function stage(h: Household): Promise<Record<string, string>> {
    const { step } = directStep();
    await runNightlySweep({ db, now: () => new Date() }, { household_id: h.household_id }, step);
    const rows = await all<{ run_id: string; template_key: string }>(`SELECT run_id, template_key FROM mission_runs WHERE household_id = ? AND state = 'R1_STAGED'`, h.household_id);
    return Object.fromEntries(rows.map((r) => [r.template_key, r.run_id]));
  }

  function deps(s: Scenario, run_id: string, o: { now?: () => Date } = {}): MissionDeps {
    return missionDepsFromEnv(s.env, run_id, s.h.household_id, { network: s.net.fetch, ...(o.now ? { now: o.now } : {}) });
  }

  return { db, all, one, userEpisode, writeFacts, cardHousehold, grant, balance, calendarDeps, ingestCsv, buildScenario, stage, deps };
}
