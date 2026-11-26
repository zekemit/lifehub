/**
 * src/billing/credits.ts — the credit ledger (CLAUDE.md §5.3, Milestone 5 weighting).
 *
 * credit_ledger is append-only and every row moves two buckets (delta = sub_delta + pur_delta):
 *   subscription credits  trial_grant + monthly_grant. At each renewal what is unused above ROLLOVER_CAP expires.
 *   purchased credits     pack_purchase. Never expire; an 'expiry' row cannot touch them (table CHECK).
 *
 *   debit    cost credits at compile, SUBSCRIPTION FIRST then purchased, as one conditional INSERT that decides the split and checks
 *            the balance in the same statement. D1 runs statements serially, so racing missions cannot overdraw either bucket.
 *            The entry id is deterministic per run: a retried step is a no-op.
 *   refund   a mirror of the run's debit (same buckets, opposite sign), at most once.
 *   grants   idempotent on stripe_event_id (UNIQUE). A renewal expires the excess subscription credits first, then grants, in one batch.
 */

import type { IsoTimestamp } from '../types';
import { PACKS, ROLLOVER_CAP, TRIAL_CREDITS, type PackSku } from './packs';

export interface Balances {
  total: number;
  subscription: number;
  purchased: number;
}

export async function creditBalances(db: D1Database, household_id: string): Promise<Balances> {
  const row = await db
    .prepare('SELECT COALESCE(SUM(sub_delta), 0) AS sub, COALESCE(SUM(pur_delta), 0) AS pur FROM credit_ledger WHERE household_id = ?')
    .bind(household_id)
    .first<{ sub: number; pur: number }>();
  const subscription = row?.sub ?? 0;
  const purchased = row?.pur ?? 0;
  return { total: subscription + purchased, subscription, purchased };
}

export async function creditBalance(db: D1Database, household_id: string): Promise<number> {
  return (await creditBalances(db, household_id)).total;
}

export type DebitResult =
  | { ok: true; debited: boolean; balance: number; from_subscription: number; from_purchased: number }
  | { ok: false; balance: number };

const debitEntry = (run_id: string): string => `led_debit_${run_id}`;

/** Takes `cost` credits for `run_id`, subscription credits first. `ok: false` when the two buckets together cannot cover it. */
export async function debitCredit(db: D1Database, household_id: string, run_id: string, cost: number, at: IsoTimestamp): Promise<DebitResult> {
  if (!Number.isInteger(cost) || cost < 0) throw new RangeError('credit cost must be a non-negative integer');
  if (cost === 0) return { ok: true, debited: false, balance: await creditBalance(db, household_id), from_subscription: 0, from_purchased: 0 };
  const res = await db
    .prepare(
      `INSERT OR IGNORE INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id, stripe_event_id, created_at)
       SELECT ?1, ?2, -?3, -MIN(?3, MAX(b.sub, 0)), -(?3 - MIN(?3, MAX(b.sub, 0))), 'mission_debit', ?4, NULL, ?5
         FROM (SELECT COALESCE(SUM(sub_delta), 0) AS sub, COALESCE(SUM(pur_delta), 0) AS pur FROM credit_ledger WHERE household_id = ?2) b
        WHERE MAX(b.sub, 0) + b.pur >= ?3 AND ?3 - MIN(?3, MAX(b.sub, 0)) <= b.pur`,
    )
    .bind(debitEntry(run_id), household_id, cost, run_id, at)
    .run();
  const balances = await creditBalances(db, household_id);
  const row = await db.prepare('SELECT -sub_delta AS s, -pur_delta AS p FROM credit_ledger WHERE entry_id = ? AND household_id = ?').bind(debitEntry(run_id), household_id).first<{ s: number; p: number }>();
  if (row === null) return { ok: false, balance: balances.total };
  return { ok: true, debited: (res.meta.changes ?? 0) > 0, balance: balances.total, from_subscription: row.s, from_purchased: row.p };
}

/** Gives back what `run_id`'s debit took, bucket for bucket, once. True when this call wrote the refund. */
export async function refundCredit(db: D1Database, household_id: string, run_id: string, at: IsoTimestamp): Promise<boolean> {
  const res = await db
    .prepare(
      `INSERT OR IGNORE INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id, stripe_event_id, created_at)
       SELECT 'led_refund_' || ?1, household_id, -delta, -sub_delta, -pur_delta, 'mission_refund', run_id, NULL, ?2
         FROM credit_ledger WHERE entry_id = 'led_debit_' || ?1 AND household_id = ?3`,
    )
    .bind(run_id, at, household_id)
    .run();
  return (res.meta.changes ?? 0) > 0;
}

/**
 * The one-time Day-1 grant. Idempotent: its stripe_event_id is 'trial:<household_id>'. With `onlyIfMember`, the row is written only
 * while that member exists, so it can ride in the same batch as household creation and a lost creation race grants nothing.
 */
export function trialGrantStatement(db: D1Database, household_id: string, at: IsoTimestamp, onlyIfMember?: string): D1PreparedStatement {
  return db
    .prepare(
      `INSERT OR IGNORE INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id, stripe_event_id, created_at)
       SELECT ?1, ?2, ?3, ?3, 0, 'trial_grant', NULL, ?4, ?5 WHERE (?6 IS NULL OR EXISTS (SELECT 1 FROM members WHERE member_id = ?6 AND household_id = ?2))`,
    )
    .bind(`led_trial_${household_id}`, household_id, TRIAL_CREDITS, `trial:${household_id}`, at, onlyIfMember ?? null);
}

export async function grantTrial(db: D1Database, household_id: string, at: IsoTimestamp): Promise<boolean> {
  return ((await trialGrantStatement(db, household_id, at).run()).meta.changes ?? 0) > 0;
}

/**
 * A renewal: unused subscription credits above ROLLOVER_CAP expire, then `credits` are granted. One atomic batch, idempotent on `key`
 * (the Stripe event id, or 'annual_drip:<household>:<YYYY-MM>'): a replay changes nothing, in particular it never expires credits twice.
 */
export async function grantMonthly(
  db: D1Database,
  household_id: string,
  o: { key: string; credits: number },
  at: IsoTimestamp,
): Promise<{ granted: boolean; expired: number }> {
  const [expiry, grant] = await db.batch([
    db
      .prepare(
        `INSERT OR IGNORE INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id, stripe_event_id, created_at)
         SELECT 'led_expiry_' || ?1, ?2, -(b.sub - ?3), -(b.sub - ?3), 0, 'expiry', NULL, 'expiry:' || ?1, ?4
           FROM (SELECT COALESCE(SUM(sub_delta), 0) AS sub FROM credit_ledger WHERE household_id = ?2) b
          WHERE b.sub > ?3 AND NOT EXISTS (SELECT 1 FROM credit_ledger WHERE stripe_event_id = ?1)`,
      )
      .bind(o.key, household_id, ROLLOVER_CAP, at),
    db
      .prepare(
        `INSERT OR IGNORE INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id, stripe_event_id, created_at)
         VALUES ('led_grant_' || ?1, ?2, ?3, ?3, 0, 'monthly_grant', NULL, ?1, ?4)`,
      )
      .bind(o.key, household_id, o.credits, at),
  ]);
  let expired = 0;
  if ((expiry?.meta.changes ?? 0) > 0) {
    const row = await db.prepare(`SELECT -sub_delta AS n FROM credit_ledger WHERE entry_id = 'led_expiry_' || ?`).bind(o.key).first<{ n: number }>();
    expired = row?.n ?? 0;
  }
  return { granted: (grant?.meta.changes ?? 0) > 0, expired };
}

/** A top-up pack. Purchased credits: never expire. `key` makes it idempotent ('pack:<payment intent id>'). True when this call granted. */
export async function grantPack(db: D1Database, household_id: string, o: { key: string; sku: PackSku }, at: IsoTimestamp): Promise<boolean> {
  const res = await db
    .prepare(
      `INSERT OR IGNORE INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id, stripe_event_id, created_at)
       VALUES ('led_' || ?1, ?2, ?3, 0, ?3, 'pack_purchase', NULL, ?1, ?4)`,
    )
    .bind(o.key, household_id, PACKS[o.sku].credits, at)
    .run();
  return (res.meta.changes ?? 0) > 0;
}

function monthStartIso(at: Date): string {
  return new Date(Date.UTC(at.getUTCFullYear(), at.getUTCMonth(), 1)).toISOString();
}

/**
 * Annual plans pay once and are granted every month. Gives each annual household whose paid period is still running, and which has had
 * no monthly grant yet this (UTC) month, its monthly credits. Idempotent per household per month. Returns how many were granted.
 */
export async function grantAnnualDrips(db: D1Database, at: Date, credits: number): Promise<number> {
  const month = at.toISOString().slice(0, 7);
  const monthStart = monthStartIso(at);
  let granted = 0;
  for (;;) {
    const { results } = await db
      .prepare(
        `SELECT h.household_id FROM households h
          WHERE h.plan = 'annual' AND h.deleted_at IS NULL AND h.plan_period_end IS NOT NULL AND h.plan_period_end > ?1
            AND NOT EXISTS (SELECT 1 FROM credit_ledger l WHERE l.household_id = h.household_id AND l.reason = 'monthly_grant' AND l.created_at >= ?2)
          ORDER BY h.household_id LIMIT 200`,
      )
      .bind(at.toISOString(), monthStart)
      .all<{ household_id: string }>();
    if (results.length === 0) return granted;
    let progressed = 0;
    for (const r of results) {
      const res = await grantMonthly(db, r.household_id, { key: `annual_drip:${r.household_id}:${month}`, credits }, at.toISOString());
      if (res.granted) progressed += 1;
    }
    granted += progressed;
    if (progressed === 0) return granted; // every candidate already has this month's key: nothing left to do
  }
}
