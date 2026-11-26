/**
 * src/llm/gateway.ts — the metered model router. Every LLM call in the app goes through callLlm().
 *
 * Routing (CLAUDE.md §0):  chief  -> gpt-6-sol,  failover claude-opus-5-5
 *                          worker -> gpt-6-luna, failover claude-haiku-5-5
 * Failover happens on ANY failure of the primary (network error, timeout, non-2xx, refusal, truncated or empty output).
 *
 * Guardrails, all deterministic and all enforced BEFORE any network call:
 *   1. Redaction guard: system and user text must pass redact() as 'clean'. Raw SSNs / card numbers / credentials
 *      can never leave the Worker through this module (UnredactedInputError).
 *   2. Token cliff: a worker call whose estimated input exceeds 90,000 tokens throws TokenCliffError (nothing is sent,
 *      nothing is billed, an audit row is written). Callers split with splitToTokenBudget(). The estimate is a
 *      deliberately pessimistic upper bound; the provider-reported counts are what get metered.
 *   3. Egress: all HTTP goes through the guarded fetch (src/net/egress.ts), so policy_blocklist is enforced.
 *
 * Metering: every successful call writes one llm_usage row (model, input_tokens incl. cached, cached_tokens,
 * output_tokens, cost_usd from src/llm/pricing.ts). Failed attempts write an audit_log row instead.
 */

import { redact } from '../redact';
import { createGuardedFetch } from '../net/egress';
import type { Env } from '../types';
import { costUsd, type LlmModel } from './pricing';

export const WORKER_INPUT_TOKEN_CEILING = 90_000;
/** Luna / Haiku price cliff: prompts above this are billed at a multiple of the normal rate. */
export const PRICE_CLIFF_TOKENS = 100_000;

export type LlmRole = 'chief' | 'worker';

export const MODEL_ROUTES: Readonly<Record<LlmRole, { primary: LlmModel; failover: LlmModel }>> = {
  chief: { primary: 'gpt-6-sol', failover: 'claude-opus-5-5' },
  worker: { primary: 'gpt-6-luna', failover: 'claude-haiku-5-5' },
};

const OPENAI_URL = 'https://api.openai.com/v1/chat/completions';
const ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages';
const ANTHROPIC_VERSION = '2023-06-01';
const DEFAULT_MAX_OUTPUT_TOKENS = 4096;
const DEFAULT_TIMEOUT_MS = 90_000;
const PROMPT_OVERHEAD_TOKENS = 64;

// ─── errors ─────────────────────────────────────────────────────────────────

export class TokenCliffError extends Error {
  override readonly name = 'TokenCliffError';
  constructor(
    readonly estimatedTokens: number,
    readonly ceiling: number,
    readonly purpose: string,
  ) {
    super(`worker call '${purpose}' needs ~${estimatedTokens} input tokens; the ceiling is ${ceiling}. Split the input.`);
  }
}

export class UnredactedInputError extends Error {
  override readonly name = 'UnredactedInputError';
  constructor(
    readonly part: 'system' | 'user',
    readonly detail: string,
  ) {
    super(`refusing to send ${part} text to a model: it is not redacted (${detail})`);
  }
}

export interface FailedAttempt {
  model: LlmModel;
  error: string;
}

export class LlmUnavailableError extends Error {
  override readonly name = 'LlmUnavailableError';
  constructor(readonly attempts: readonly FailedAttempt[]) {
    super(`all models failed: ${attempts.map((a) => `${a.model}: ${a.error}`).join(' | ')}`);
  }
}

class ProviderError extends Error {
  override readonly name = 'ProviderError';
}

// ─── public types ───────────────────────────────────────────────────────────

export interface LlmRequest {
  household_id?: string | null;
  run_id?: string | null;
  /** Short label for audit rows, e.g. 'email.extract'. Must not contain user data. */
  purpose: string;
  system: string;
  user: string;
  /** When set, the model must return JSON matching this schema (returned as `text`). */
  json_schema?: { name: string; schema: Record<string, unknown> };
  max_output_tokens?: number;
}

export interface LlmResult {
  text: string;
  model: LlmModel;
  input_tokens: number;
  cached_tokens: number;
  output_tokens: number;
  cost_usd: number;
  failed_over: boolean;
}

export interface GatewayDeps {
  db: D1Database;
  apiKeys: { openai?: string | undefined; anthropic?: string | undefined };
  /** MUST be a guarded fetch from createGuardedFetch(). */
  fetch: typeof fetch;
  now?: () => Date;
  newId?: () => string;
  /** Token estimator for the pre-flight ceiling. Default: estimateTokens. */
  countTokens?: (text: string) => number;
  timeoutMs?: number;
}

export interface Gateway {
  chief(req: LlmRequest): Promise<LlmResult>;
  worker(req: LlmRequest): Promise<LlmResult>;
}

// ─── token estimation / splitting ───────────────────────────────────────────

/**
 * Pessimistic input-token estimate: 1 token per 2.5 ASCII characters and 2 tokens per non-ASCII code point.
 * Real English text is ~4 characters per token, so this over-counts by roughly 1.6x on prose and stays above the true
 * count on digit-and-punctuation-heavy CSV / JSON text, which is the point: it guards a hard price cliff.
 */
export function estimateTokens(text: string): number {
  let ascii = 0;
  let other = 0;
  for (const ch of text) {
    if ((ch.codePointAt(0) ?? 0) < 128) ascii += 1;
    else other += 1;
  }
  return Math.ceil(ascii / 2.5) + other * 2;
}

export interface TextWindow {
  start: number;
  end: number;
}

/**
 * Splits `text` into contiguous windows of at most `maxTokens` (by `count`), preferring to break after a newline.
 * Windows are index ranges, so every window is an exact substring of `text` (quotes found in a window stay valid
 * against the whole text). Never returns an empty list for non-empty text.
 */
export function splitToTokenBudget(
  text: string,
  maxTokens: number,
  count: (s: string) => number = estimateTokens,
): TextWindow[] {
  if (text.length === 0) return [];
  const windows: TextWindow[] = [];
  let start = 0;
  while (start < text.length) {
    let end = text.length;
    if (count(text.slice(start, end)) > maxTokens) {
      let lo = start + 1;
      let hi = text.length;
      while (lo < hi) {
        const mid = Math.ceil((lo + hi) / 2);
        if (count(text.slice(start, mid)) <= maxTokens) lo = mid;
        else hi = mid - 1;
      }
      end = lo;
      const newline = text.lastIndexOf('\n', end - 1);
      if (newline >= start && newline + 1 > start + (end - start) / 2) end = newline + 1;
    }
    windows.push({ start, end });
    start = end;
  }
  return windows;
}

// ─── providers ──────────────────────────────────────────────────────────────

interface ProviderOutput {
  text: string;
  input_tokens: number;
  cached_tokens: number;
  output_tokens: number;
}

function describeHttpFailure(status: number, body: string): string {
  return `HTTP ${status}: ${body.slice(0, 300).replace(/\s+/g, ' ')}`;
}

async function callOpenAi(
  deps: GatewayDeps,
  model: LlmModel,
  req: LlmRequest,
  maxOut: number,
): Promise<ProviderOutput> {
  if (!deps.apiKeys.openai) throw new ProviderError('OPENAI_API_KEY is not configured');
  const body: Record<string, unknown> = {
    model,
    messages: [
      { role: 'system', content: req.system },
      { role: 'user', content: req.user },
    ],
    max_completion_tokens: maxOut,
  };
  if (req.json_schema !== undefined) {
    body['response_format'] = {
      type: 'json_schema',
      json_schema: { name: req.json_schema.name, schema: req.json_schema.schema, strict: true },
    };
  }
  const res = await deps.fetch(OPENAI_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${deps.apiKeys.openai}` },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(deps.timeoutMs ?? DEFAULT_TIMEOUT_MS),
  });
  const raw = await res.text();
  if (!res.ok) throw new ProviderError(describeHttpFailure(res.status, raw));
  const json = JSON.parse(raw) as {
    choices?: Array<{ finish_reason?: string; message?: { content?: string | null; refusal?: string | null } }>;
    usage?: { prompt_tokens?: number; completion_tokens?: number; prompt_tokens_details?: { cached_tokens?: number } };
  };
  const choice = json.choices?.[0];
  if (choice?.message?.refusal) throw new ProviderError('model refused the request');
  if (choice?.finish_reason === 'length') throw new ProviderError('output truncated (max_completion_tokens)');
  const text = choice?.message?.content;
  if (typeof text !== 'string' || text.length === 0) throw new ProviderError('empty completion');
  return {
    text,
    input_tokens: json.usage?.prompt_tokens ?? 0,
    cached_tokens: json.usage?.prompt_tokens_details?.cached_tokens ?? 0,
    output_tokens: json.usage?.completion_tokens ?? 0,
  };
}

async function callAnthropic(
  deps: GatewayDeps,
  model: LlmModel,
  req: LlmRequest,
  maxOut: number,
): Promise<ProviderOutput> {
  if (!deps.apiKeys.anthropic) throw new ProviderError('ANTHROPIC_API_KEY is not configured');
  const body: Record<string, unknown> = {
    model,
    max_tokens: maxOut,
    system: req.system,
    messages: [{ role: 'user', content: req.user }],
  };
  if (req.json_schema !== undefined) {
    body['tools'] = [
      { name: req.json_schema.name, description: 'Return the result.', input_schema: req.json_schema.schema },
    ];
    body['tool_choice'] = { type: 'tool', name: req.json_schema.name };
  }
  const res = await deps.fetch(ANTHROPIC_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': deps.apiKeys.anthropic,
      'anthropic-version': ANTHROPIC_VERSION,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(deps.timeoutMs ?? DEFAULT_TIMEOUT_MS),
  });
  const raw = await res.text();
  if (!res.ok) throw new ProviderError(describeHttpFailure(res.status, raw));
  const json = JSON.parse(raw) as {
    stop_reason?: string;
    content?: Array<{ type: string; text?: string; input?: unknown }>;
    usage?: {
      input_tokens?: number;
      output_tokens?: number;
      cache_read_input_tokens?: number;
      cache_creation_input_tokens?: number;
    };
  };
  if (json.stop_reason === 'max_tokens') throw new ProviderError('output truncated (max_tokens)');
  let text: string | undefined;
  if (req.json_schema !== undefined) {
    const block = json.content?.find((b) => b.type === 'tool_use');
    if (block?.input !== undefined) text = JSON.stringify(block.input);
  } else {
    text = json.content
      ?.filter((b) => b.type === 'text' && typeof b.text === 'string')
      .map((b) => b.text)
      .join('');
  }
  if (typeof text !== 'string' || text.length === 0) throw new ProviderError('empty completion');
  const cacheRead = json.usage?.cache_read_input_tokens ?? 0;
  return {
    text,
    // Anthropic reports uncached input separately; the gateway meters total prompt tokens.
    input_tokens: (json.usage?.input_tokens ?? 0) + cacheRead + (json.usage?.cache_creation_input_tokens ?? 0),
    cached_tokens: cacheRead,
    output_tokens: json.usage?.output_tokens ?? 0,
  };
}

function callProvider(deps: GatewayDeps, model: LlmModel, req: LlmRequest, maxOut: number): Promise<ProviderOutput> {
  return model.startsWith('claude-') ? callAnthropic(deps, model, req, maxOut) : callOpenAi(deps, model, req, maxOut);
}

// ─── the gateway ────────────────────────────────────────────────────────────

function defaultId(): string {
  return `llm_${crypto.randomUUID().replace(/-/g, '')}`;
}

async function audit(
  deps: GatewayDeps,
  household_id: string | null | undefined,
  action: string,
  detail: Record<string, unknown>,
): Promise<void> {
  const now = (deps.now ?? (() => new Date()))().toISOString();
  await deps.db
    .prepare(
      `INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'llm-gateway', ?, ?, ?)`,
    )
    .bind((deps.newId ?? defaultId)(), household_id ?? null, action, JSON.stringify(detail), now)
    .run();
}

function assertRedacted(part: 'system' | 'user', text: string): void {
  const r = redact(text);
  if (r.redaction_status === 'clean') return;
  throw new UnredactedInputError(part, r.redaction_status === 'quarantined' ? r.quarantine_reason : 'contains PII patterns');
}

/** Runs one metered, guarded LLM call. See the file header for the guarantees. */
export async function callLlm(deps: GatewayDeps, role: LlmRole, req: LlmRequest): Promise<LlmResult> {
  assertRedacted('system', req.system);
  assertRedacted('user', req.user);

  const count = deps.countTokens ?? estimateTokens;
  const schemaTokens = req.json_schema === undefined ? 0 : count(JSON.stringify(req.json_schema));
  const estimated = count(req.system) + count(req.user) + schemaTokens + PROMPT_OVERHEAD_TOKENS;
  if (role === 'worker' && estimated > WORKER_INPUT_TOKEN_CEILING) {
    await audit(deps, req.household_id, 'llm_token_cliff', {
      role,
      purpose: req.purpose,
      estimated_tokens: estimated,
      ceiling: WORKER_INPUT_TOKEN_CEILING,
    });
    throw new TokenCliffError(estimated, WORKER_INPUT_TOKEN_CEILING, req.purpose);
  }

  const route = MODEL_ROUTES[role];
  const maxOut = req.max_output_tokens ?? DEFAULT_MAX_OUTPUT_TOKENS;
  const attempts: FailedAttempt[] = [];

  for (const model of [route.primary, route.failover]) {
    let out: ProviderOutput;
    try {
      out = await callProvider(deps, model, req, maxOut);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      attempts.push({ model, error: message.slice(0, 400) });
      // Egress policy violations are programming errors, not outages: never fail over around them.
      if (err instanceof Error && err.name === 'PolicyBlockedError') throw err;
      await audit(deps, req.household_id, 'llm_attempt_failed', { role, model, purpose: req.purpose, error: message.slice(0, 400) });
      continue;
    }
    const cost = costUsd(model, out);
    const now = (deps.now ?? (() => new Date()))().toISOString();
    await deps.db
      .prepare(
        `INSERT INTO llm_usage (id, household_id, run_id, model, input_tokens, cached_tokens, output_tokens, cost_usd, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .bind(
        (deps.newId ?? defaultId)(),
        req.household_id ?? null,
        req.run_id ?? null,
        model,
        out.input_tokens,
        out.cached_tokens,
        out.output_tokens,
        cost,
        now,
      )
      .run();
    if (out.input_tokens > PRICE_CLIFF_TOKENS) {
      await audit(deps, req.household_id, 'llm_price_cliff_crossed', {
        model,
        purpose: req.purpose,
        input_tokens: out.input_tokens,
        estimated_tokens: estimated,
      });
    }
    return {
      text: out.text,
      model,
      input_tokens: out.input_tokens,
      cached_tokens: out.cached_tokens,
      output_tokens: out.output_tokens,
      cost_usd: cost,
      failed_over: model !== route.primary,
    };
  }
  throw new LlmUnavailableError(attempts);
}

export function createGateway(deps: GatewayDeps): Gateway {
  return {
    chief: (req) => callLlm(deps, 'chief', req),
    worker: (req) => callLlm(deps, 'worker', req),
  };
}

/** Production wiring: keys from secrets, network through the policy-guarded fetch. */
export function gatewayFromEnv(env: Pick<Env, 'DB' | 'OPENAI_API_KEY' | 'ANTHROPIC_API_KEY'>): Gateway {
  return createGateway({
    db: env.DB,
    apiKeys: { openai: env.OPENAI_API_KEY, anthropic: env.ANTHROPIC_API_KEY },
    fetch: createGuardedFetch({ db: env.DB }),
  });
}
