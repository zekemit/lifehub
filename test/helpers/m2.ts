/**
 * Milestone 2 test fixtures: fake Workflow step, fake LLM providers, households, Env, raw email.
 * Only the network (LLM providers) and the Workflows engine are faked; D1 and R2 are the real local ones.
 */

import { randomUUID } from 'node:crypto';
import type { InboundMessage } from '../../src/workers/emailRouter';
import type { StepConfigLike, StepEvent, StepLike } from '../../src/ingest/steps';
import type { CalendarSyncParams, DropAlarmParams, Env, IngestEmailParams, IngestFileParams, LifeReceiptParams, MissionRunParams, NightlySweepParams, PriceWatchParams } from '../../src/types';
import type { QueryCounter } from './index';

// ── Workflow step (direct execution + accounting) ──

export interface StepLogEntry {
  name: string;
  queries: number;
}

/** What `waitForEvent` yields for an event type: a payload (or a function producing one, so it can read the DB), or TIMEOUT. */
export const TIMEOUT = Symbol('waitForEvent timeout');
export type EventScript = unknown | (() => unknown | Promise<unknown>);

export interface WaitLogEntry {
  name: string;
  type: string;
  timeout: number | string | undefined;
}

export interface SleepLogEntry {
  name: string;
  duration: number | string;
  ms: number;
}

const UNIT_MS: Record<string, number> = { second: 1000, minute: 60_000, hour: 3_600_000, day: 86_400_000, week: 604_800_000 };

/** Milliseconds for a workflow sleep duration: a number is ms, text is '<n> <unit>' (seconds, minutes, hours, days, weeks). */
export function durationMs(d: number | string): number {
  if (typeof d === 'number') return d;
  const m = /^(\d+(?:\.\d+)?)\s*(second|minute|hour|day|week)s?$/.exec(d.trim());
  if (m === null) throw new Error(`unsupported sleep duration: ${d}`);
  return Number(m[1]) * UNIT_MS[m[2]!]!;
}

/** A clock a test shares with the code under test; sleeping advances it, so a `sleep` is a jump forward in time, not a wait. */
export interface TestClock {
  now: Date;
}

export function directStep(
  counter?: QueryCounter,
  events: Record<string, EventScript> = {},
  clock?: TestClock,
): { step: StepLike; log: StepLogEntry[]; waits: WaitLogEntry[]; sleeps: SleepLogEntry[] } {
  const log: StepLogEntry[] = [];
  const waits: WaitLogEntry[] = [];
  const sleeps: SleepLogEntry[] = [];
  const step: StepLike = {
    async do<T>(name: string, a: StepConfigLike | (() => Promise<T>), b?: () => Promise<T>): Promise<T> {
      const callback = (typeof a === 'function' ? a : b) as () => Promise<T>;
      const before = counter?.count ?? 0;
      const result = await callback();
      log.push({ name, queries: (counter?.count ?? 0) - before });
      return result;
    },
    async sleep(name: string, duration: number | string): Promise<void> {
      const ms = durationMs(duration);
      sleeps.push({ name, duration, ms });
      if (clock !== undefined) clock.now = new Date(clock.now.getTime() + ms);
    },
    async waitForEvent<T>(name: string, options: { type: string; timeout?: number | string }): Promise<StepEvent<T>> {
      waits.push({ name, type: options.type, timeout: options.timeout });
      const script = events[options.type];
      if (script === undefined || script === TIMEOUT) throw new Error(`waitForEvent '${name}' timed out`);
      const payload = typeof script === 'function' ? await (script as () => unknown)() : script;
      if (payload === TIMEOUT) throw new Error(`waitForEvent '${name}' timed out`);
      return { type: options.type, payload: payload as T };
    },
  };
  return { step, log, waits, sleeps };
}

// ── fake LLM providers (network only) ──

export interface ProviderCall {
  url: string;
  host: string;
  body: Record<string, unknown>;
  /** All text sent to the model (system + user), for leak assertions. */
  sentText: string;
}

export type ProviderHandler = (call: ProviderCall) => Response | Promise<Response>;

export function openaiReply(content: unknown, usage = { prompt_tokens: 1000, completion_tokens: 200, cached: 0 }): Response {
  return new Response(
    JSON.stringify({
      choices: [{ finish_reason: 'stop', message: { content: typeof content === 'string' ? content : JSON.stringify(content) } }],
      usage: { prompt_tokens: usage.prompt_tokens, completion_tokens: usage.completion_tokens, prompt_tokens_details: { cached_tokens: usage.cached } },
    }),
    { status: 200, headers: { 'content-type': 'application/json' } },
  );
}

export function anthropicReply(toolInput: unknown, usage = { input_tokens: 1000, output_tokens: 200, cache_read_input_tokens: 0 }): Response {
  return new Response(
    JSON.stringify({ stop_reason: 'tool_use', content: [{ type: 'tool_use', name: 'x', input: toolInput }], usage }),
    { status: 200, headers: { 'content-type': 'application/json' } },
  );
}

export function fakeProvider(handler: ProviderHandler): { fetch: typeof fetch; calls: ProviderCall[] } {
  const calls: ProviderCall[] = [];
  const impl = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>;
    const messages = (body['messages'] as Array<{ content: string }> | undefined) ?? [];
    const sentText = [String(body['system'] ?? ''), ...messages.map((m) => m.content)].join('\n');
    const call: ProviderCall = { url, host: new URL(url).hostname, body, sentText };
    calls.push(call);
    return handler(call);
  };
  return { fetch: impl as typeof fetch, calls };
}

// ── household / env fixtures ──

const BASE32 = 'abcdefghijklmnopqrstuvwxyz234567';
export function randomAlias(): string {
  let s = 'h_';
  for (let i = 0; i < 10; i++) s += BASE32[Math.floor(Math.random() * 32)];
  return s;
}

export interface Household {
  household_id: string;
  alias_local: string;
  member_email: string;
}

export async function makeHousehold(db: D1Database): Promise<Household> {
  const household_id = `hh_${randomUUID().slice(0, 8)}`;
  const alias_local = randomAlias();
  const member_email = `member-${household_id}@example.org`;
  await db.batch([
    db.prepare(`INSERT INTO households (household_id, shard, alias_local, plan) VALUES (?, 0, ?, 'trial')`).bind(household_id, alias_local),
    db
      .prepare(`INSERT INTO members (member_id, household_id, email, role, age_attested_18) VALUES (?, ?, ?, 'owner', 1)`)
      .bind(`mem_${household_id}`, household_id, member_email),
  ]);
  return { household_id, alias_local, member_email };
}

export interface FakeWorkflows {
  email: Array<{ id: string; params: IngestEmailParams }>;
  file: Array<{ id: string; params: IngestFileParams }>;
  calendar: Array<{ id: string; params: CalendarSyncParams }>;
  sweep: Array<{ id: string; params: NightlySweepParams }>;
  mission: Array<{ id: string; params: MissionRunParams }>;
  receipt: Array<{ id: string; params: LifeReceiptParams }>;
  alarm: Array<{ id: string; params: DropAlarmParams }>;
  watch: Array<{ id: string; params: PriceWatchParams }>;
  /** Events sent to MissionRunWF instances: sendEvent({type, payload}). */
  events: Array<{ id: string; type: string; payload: unknown }>;
  /** Events sent to DropAlarmWF instances. Kept apart so a test can tell WHICH Workflow an event was routed to. */
  alarmEvents: Array<{ id: string; type: string; payload: unknown }>;
}

/** 32 random bytes, base64. */
export function randomKek(): string {
  return Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString('base64');
}

/** One key-encryption key per test process, like production's single KEK secret. */
const PROCESS_KEK = randomKek();

/**
 * An Env for tests. APP_NAME and INBOUND_DOMAIN deliberately differ from wrangler.toml, so any code that hardcoded
 * the real brand or domain would visibly misbehave.
 */
export function testEnv(rt: { db: D1Database; bucket: R2Bucket; kv?: KVNamespace }, extra: Partial<Env> = {}): { env: Env; workflows: FakeWorkflows } {
  const workflows: FakeWorkflows = { email: [], file: [], calendar: [], sweep: [], mission: [], receipt: [], alarm: [], watch: [], events: [], alarmEvents: [] };
  const recorder = <P>(list: Array<{ id: string; params: P }>) => ({
    create: async (o: { id: string; params: P }) => {
      if (list.some((w) => w.id === o.id)) throw new Error('instance already exists');
      list.push(o);
      return { id: o.id };
    },
  });
  const eventTarget = (into: Array<{ id: string; type: string; payload: unknown }>) => async (id: string) => ({
    id,
    sendEvent: async (e: { type: string; payload: unknown }) => {
      into.push({ id, type: e.type, payload: e.payload });
    },
  });
  const env = {
    DB: rt.db,
    DOCS_BUCKET: rt.bucket,
    APP_NAME: 'TestApp',
    INBOUND_DOMAIN: 'inbound.test',
    KEK_B64: PROCESS_KEK,
    OPENAI_API_KEY: 'sk-test',
    ANTHROPIC_API_KEY: 'ak-test',
    INGEST_EMAIL_WF: recorder(workflows.email),
    INGEST_FILE_WF: recorder(workflows.file),
    CALENDAR_SYNC_WF: recorder(workflows.calendar),
    NIGHTLY_SWEEP_WF: recorder(workflows.sweep),
    MISSION_RUN_WF: { ...recorder(workflows.mission), get: eventTarget(workflows.events) },
    LIFE_RECEIPT_WF: recorder(workflows.receipt),
    DROP_ALARM_WF: { ...recorder(workflows.alarm), get: eventTarget(workflows.alarmEvents) },
    PRICE_WATCH_WF: recorder(workflows.watch),
    KV: rt.kv,
    GOOGLE_CLIENT_ID: 'client-id.apps.test',
    GOOGLE_CLIENT_SECRET: 'client-secret',
    SESSION_SECRET: 'test-session-secret-0123456789',
    ...extra,
  } as unknown as Env;
  return { env, workflows };
}

// ── email fixtures ──

export function rfc822(o: {
  from?: string;
  to: string;
  subject: string;
  body: string;
  date?: string;
  authResults?: string | null;
  extraTopHeaders?: string[];
}): Uint8Array {
  const lines = [
    ...(o.extraTopHeaders ?? []),
    ...(o.authResults === null || o.authResults === undefined ? [] : [`Authentication-Results: ${o.authResults}`]),
    `From: Sender <${o.from ?? 'sender@example.com'}>`,
    `To: ${o.to}`,
    `Subject: ${o.subject}`,
    `Date: ${o.date ?? 'Tue, 03 Nov 2026 10:00:00 +0000'}`,
    `Message-ID: <${randomUUID()}@example.com>`,
    'MIME-Version: 1.0',
    'Content-Type: text/plain; charset=utf-8',
    '',
    o.body,
  ];
  return new TextEncoder().encode(lines.join('\r\n'));
}

export interface FakeMessage extends InboundMessage {
  rejected: string | null;
}

export function inboundMessage(raw: Uint8Array, o: { from: string; to: string; rawSize?: number }): FakeMessage {
  const msg = {
    from: o.from,
    to: o.to,
    rawSize: o.rawSize ?? raw.length,
    rejected: null as string | null,
    raw: new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(raw);
        controller.close();
      },
    }),
    setReject(reason: string) {
      msg.rejected = reason;
    },
  };
  return msg;
}

/** Wraps a raw message so it can be stored the way the router would: encrypted at `r2_key`. */
export const PASSING_AUTH = 'mx.example.net; dkim=pass header.d=example.com; spf=pass smtp.mailfrom=example.com';
