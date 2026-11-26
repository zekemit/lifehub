/**
 * src/missions/ledger.ts — credits (CLAUDE.md §5.3). credit_ledger is append-only; the balance is the SUM of deltas.
 *
 *   debit   -cost at compile, ONLY if the balance covers it. One conditional INSERT, so two missions racing for the last
 *           credit cannot both win and the balance can never go negative. Deterministic entry id => a retried step is a no-op.
 *   refund  +cost on FAILED_GATE or a system error; deterministic id and only against an existing debit => at most once.
 *           Nothing is refunded on DECLINED or EXPIRED.
 */

import type { IsoTimestamp } from '../types';

export async function creditBalance(db: D1Database, household_id: string): Promise<number> {
  const row = await db.prepare('SELECT COALESCE(SUM(delta), 0) AS balance FROM credit_ledger WHERE household_id = ?').bind(household_id).first<{ balance: number }>();
  return row?.balance ?? 0;
}

export type DebitResult = { ok: true; debited: boolean; balance: number } | { ok: false; balance: number };

export async function debitCredit(db: D1Database, household_id: string, run_id: string, cost: number, at: IsoTimestamp): Promise<DebitResult> {
  if (cost <= 0) return { ok: true, debited: false, balance: await creditBalance(db, household_id) };
  const entry = `led_debit_${run_id}`;
  const res = await db
    .prepare(
      `INSERT OR IGNORE INTO credit_ledger (entry_id, household_id, delta, reason, run_id, stripe_event_id, created_at)
       SELECT ?1, ?2, -?3, 'mission_debit', ?4, NULL, ?5
        WHERE (SELECT COALESCE(SUM(delta), 0) FROM credit_ledger WHERE household_id = ?2) >= ?3`,
    )
    .bind(entry, household_id, cost, run_id, at)
    .run();
  const balance = await creditBalance(db, household_id);
  if ((res.meta.changes ?? 0) > 0) return { ok: true, debited: true, balance };
  const existing = await db.prepare('SELECT 1 AS hit FROM credit_ledger WHERE entry_id = ?').bind(entry).first<{ hit: number }>();
  return existing === null ? { ok: false, balance } : { ok: true, debited: false, balance };
}

/** Gives back the credit debited for `run_id`, once. Returns true when a refund row was written by this call. */
export async function refundCredit(db: D1Database, household_id: string, run_id: string, at: IsoTimestamp): Promise<boolean> {
  const res = await db
    .prepare(
      `INSERT OR IGNORE INTO credit_ledger (entry_id, household_id, delta, reason, run_id, stripe_event_id, created_at)
       SELECT 'led_refund_' || ?1, household_id, -delta, 'mission_refund', run_id, NULL, ?2
         FROM credit_ledger WHERE entry_id = 'led_debit_' || ?1 AND household_id = ?3`,
    )
    .bind(run_id, at, household_id)
    .run();
  return (res.meta.changes ?? 0) > 0;
}
