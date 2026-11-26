/**
 * src/ingest/steps.ts — shared plumbing for the ingestion pipelines.
 *
 * Pipelines are written against `StepLike`, the small part of Cloudflare's WorkflowStep they use, so the exact same
 * code runs inside a Workflow (durable, replayed) and inside the tests (direct execution with step accounting).
 */

import { writeFact, type WriteFactOptions } from '../graph/writeFact';
import type { FactProposal } from '../types';

export interface StepConfigLike {
  retries?: { limit: number; delay: string | number; backoff?: 'constant' | 'linear' | 'exponential' };
  timeout?: string | number;
}

export interface StepEvent<T> {
  type: string;
  payload: T;
}

export interface StepLike {
  do<T>(name: string, callback: () => Promise<T>): Promise<T>;
  do<T>(name: string, config: StepConfigLike, callback: () => Promise<T>): Promise<T>;
  /** Durably sleeps. `duration` is milliseconds, or text such as '1 day'. */
  sleep(name: string, duration: number | string): Promise<void>;
  /** Waits for an event sent to this workflow instance; rejects when `timeout` (ms) elapses first. */
  waitForEvent<T>(name: string, options: { type: string; timeout?: number | string }): Promise<StepEvent<T>>;
}

/**
 * D1 allows 1,000 queries per invocation and a fact write costs 10-18 of them (measured in the test suite), so a
 * Workflow step writes at most this many facts: 45 x 18 = 810 queries, leaving headroom for the step's own reads.
 */
export const MAX_FACTS_PER_STEP = 45;

/** Hard cap on facts accepted from one episode, so a runaway model cannot flood the graph. */
export const MAX_FACTS_PER_EPISODE = 200;

export const DEFAULT_STEP_CONFIG: StepConfigLike = {
  retries: { limit: 3, delay: '10 seconds', backoff: 'exponential' },
  timeout: '5 minutes',
};

export function chunk<T>(items: readonly T[], size: number): T[][] {
  if (!Number.isInteger(size) || size < 1) throw new RangeError('chunk size must be a positive integer');
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

export interface WriteSummary {
  written: number;
  reaffirmed: number;
  rejected: Record<string, number>;
}

/** Writes proposals one by one through writeFact(); returns counts only (step results are capped at 1 MiB). */
export async function writeFactsChunk(
  db: D1Database,
  proposals: readonly FactProposal[],
  options: WriteFactOptions = {},
): Promise<WriteSummary> {
  if (proposals.length > MAX_FACTS_PER_STEP) {
    throw new RangeError(`a step may write at most ${MAX_FACTS_PER_STEP} facts, got ${proposals.length}`);
  }
  const summary: WriteSummary = { written: 0, reaffirmed: 0, rejected: {} };
  for (const proposal of proposals) {
    const result = await writeFact(db, proposal, options);
    if (result.status === 'written') summary.written += 1;
    else if (result.status === 'reaffirmed') summary.reaffirmed += 1;
    else summary.rejected[result.reason] = (summary.rejected[result.reason] ?? 0) + 1;
  }
  return summary;
}

export function mergeSummaries(parts: readonly WriteSummary[]): WriteSummary {
  const total: WriteSummary = { written: 0, reaffirmed: 0, rejected: {} };
  for (const p of parts) {
    total.written += p.written;
    total.reaffirmed += p.reaffirmed;
    for (const [k, v] of Object.entries(p.rejected)) total.rejected[k] = (total.rejected[k] ?? 0) + v;
  }
  return total;
}

/**
 * A StepLike that simply runs each callback now, with no durability. For work done inside a request (an on-demand Life Receipt):
 * it cannot sleep or wait for events, and says so loudly rather than pretending.
 */
export const inlineStep: StepLike = {
  async do<T>(_name: string, a: StepConfigLike | (() => Promise<T>), b?: () => Promise<T>): Promise<T> {
    const callback = (typeof a === 'function' ? a : b) as () => Promise<T>;
    return callback();
  },
  async sleep(): Promise<void> {
    throw new Error('an inline step cannot sleep');
  },
  async waitForEvent<T>(): Promise<StepEvent<T>> {
    throw new Error('an inline step cannot wait for events');
  },
};
