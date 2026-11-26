/**
 * src/maintenance/decay.ts — habit decay (CLAUDE.md §4 step 5): close facts whose newest supporting episode is older
 * than their predicate's decay_days. Predicates with decay_days NULL never decay. Every closure is logged to audit_log as
 * 'decay_invalidate'. Closing sets valid_to only; the fact and its quotes stay as history.
 */

import { closeFacts } from '../graph/closeFacts';

interface DecayRow {
  fact_id: string;
  predicate: string;
  decay_days: number;
  last_support: string;
}

const MAX_PER_SWEEP = 1000;

export async function decayFacts(db: D1Database, household_id: string, asOf: Date): Promise<{ closed: number; examined: number }> {
  const { results } = await db
    .prepare(
      `SELECT f.fact_id, f.predicate, r.decay_days, MAX(COALESCE(e.occurred_at, e.received_at)) AS last_support
         FROM facts_current f
         JOIN predicate_registry r ON r.predicate = f.predicate AND r.decay_days IS NOT NULL
         JOIN fact_quotes q ON q.fact_id = f.fact_id
         JOIN episodes e ON e.episode_id = q.episode_id
        WHERE f.household_id = ?1
        GROUP BY f.fact_id, f.predicate, r.decay_days
       HAVING last_support < strftime('%Y-%m-%dT%H:%M:%fZ', ?2, '-' || r.decay_days || ' days')
        LIMIT ${MAX_PER_SWEEP}`,
    )
    .bind(household_id, asOf.toISOString())
    .all<DecayRow>();
  const closed = await closeFacts(
    db,
    household_id,
    results.map((r) => ({ fact_id: r.fact_id, detail: { predicate: r.predicate, decay_days: r.decay_days, last_support: r.last_support } })),
    asOf.toISOString(),
    'decay_invalidate',
    'nightly-sweep',
  );
  return { closed, examined: results.length };
}
