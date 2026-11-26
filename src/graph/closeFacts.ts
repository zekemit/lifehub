/**
 * src/graph/closeFacts.ts — closing facts that are no longer true or no longer supported (decay, cancelled calendar
 * events, free windows that became busy). The graph's only other writer besides writeFact(); it never inserts or edits
 * a value, it only sets `valid_to` on a currently-believed, still-open fact and records why in audit_log.
 *
 * valid_to is set to max(`at`, valid_from) so the schema's CHECK (valid_to >= valid_from) always holds.
 */

import type { IsoTimestamp } from '../types';

export interface CloseTarget {
  fact_id: string;
  /** Extra audit detail (predicate, last support, ...). Never raw episode text. */
  detail?: Record<string, unknown>;
}

const AUDIT_CHUNK = 100;

/** Closes the facts that are still open; returns how many were actually closed. Each closure writes one audit row. */
export async function closeFacts(
  db: D1Database,
  household_id: string,
  targets: readonly CloseTarget[],
  at: IsoTimestamp,
  action: string,
  actor: string,
): Promise<number> {
  let closed = 0;
  for (let i = 0; i < targets.length; i += AUDIT_CHUNK) {
    const group = targets.slice(i, i + AUDIT_CHUNK);
    const statements = group.flatMap((t) => [
      // Audit first, only if the fact is still open; the UPDATE below then closes exactly the facts that were logged.
      db
        .prepare(
          `INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at)
           SELECT ?, ?, ?, ?, ?, ?
            WHERE EXISTS (SELECT 1 FROM facts WHERE fact_id = ? AND household_id = ? AND valid_to IS NULL AND expired_at IS NULL)`,
        )
        .bind(
          `aud_${crypto.randomUUID().replace(/-/g, '')}`,
          household_id,
          actor,
          action,
          JSON.stringify({ fact_id: t.fact_id, ...t.detail }),
          at,
          t.fact_id,
          household_id,
        ),
      db
        .prepare(
          `UPDATE facts SET valid_to = CASE WHEN valid_from > ?1 THEN valid_from ELSE ?1 END
            WHERE fact_id = ?2 AND household_id = ?3 AND valid_to IS NULL AND expired_at IS NULL`,
        )
        .bind(at, t.fact_id, household_id),
    ]);
    const results = await db.batch(statements);
    closed += results.filter((_, n) => n % 2 === 1).reduce((sum, r) => sum + (r.meta.changes ?? 0), 0);
  }
  return closed;
}
