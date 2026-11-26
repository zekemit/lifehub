/**
 * src/workers/scheduled.ts — the hourly cron. For every household whose LOCAL time is 03:00 it starts the nightly chain
 * (CLAUDE.md §5: "NightlySweepWF (03:00 local)"): CalendarSyncWF (which ends by starting NightlySweepWF) when the household has a
 * connected calendar, otherwise NightlySweepWF directly. Instance ids carry the household's local date, so a repeated
 * invocation in the same local hour cannot start the chain twice.
 */

import { grantAnnualDrips } from '../billing/credits';
import { ANNUAL_MONTHLY_GRANT_CREDITS } from '../billing/packs';
import type { Env } from '../types';

export const SWEEP_LOCAL_HOUR = 3;
/** The Life Receipt goes out Sunday at 08:00 household-local time. */
export const RECEIPT_LOCAL_HOUR = 8;

export function localHour(at: Date, timeZone: string): number {
  const hour = new Intl.DateTimeFormat('en-US', { timeZone, hour: '2-digit', hourCycle: 'h23' }).formatToParts(at).find((p) => p.type === 'hour')?.value;
  return Number(hour ?? -1);
}

export function localDate(at: Date, timeZone: string): string {
  const p = new Intl.DateTimeFormat('en-US', { timeZone, year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(at);
  const get = (t: string): string => p.find((x) => x.type === t)?.value ?? '';
  return `${get('year')}-${get('month')}-${get('day')}`;
}

export function localWeekday(at: Date, timeZone: string): string {
  return new Intl.DateTimeFormat('en-US', { timeZone, weekday: 'short' }).format(at);
}

function safeLocalHour(at: Date, timeZone: string): number {
  try {
    return localHour(at, timeZone);
  } catch {
    return -1; // unknown time zone: skip rather than guess
  }
}

export interface ScheduledResult {
  due: number;
  started: number;
  skipped: number;
  /** Life Receipt workflows started for households whose local time is Sunday 08:00, and those already started. */
  receipts_started: number;
  receipts_skipped: number;
  /** Annual-plan households given this month's credits. */
  annual_grants: number;
}

export async function runScheduled(env: Pick<Env, 'DB' | 'CALENDAR_SYNC_WF' | 'NIGHTLY_SWEEP_WF' | 'LIFE_RECEIPT_WF'>, at: Date): Promise<ScheduledResult> {
  const connected = new Set<string>();
  for (let offset = 0; ; offset += 500) {
    const { results } = await env.DB.prepare(
      `SELECT household_id FROM connectors WHERE kind = 'google_calendar' AND status = 'active' ORDER BY household_id LIMIT 500 OFFSET ?`,
    )
      .bind(offset)
      .all<{ household_id: string }>();
    for (const r of results) connected.add(r.household_id);
    if (results.length < 500) break;
  }

  const out: ScheduledResult = { due: 0, started: 0, skipped: 0, receipts_started: 0, receipts_skipped: 0, annual_grants: 0 };
  for (let offset = 0; ; offset += 500) {
    const { results } = await env.DB.prepare(
      `SELECT household_id, timezone FROM households WHERE deleted_at IS NULL AND plan <> 'canceled' ORDER BY household_id LIMIT 500 OFFSET ?`,
    )
      .bind(offset)
      .all<{ household_id: string; timezone: string }>();
    for (const h of results) {
      const hour = safeLocalHour(at, h.timezone);
      if (hour === RECEIPT_LOCAL_HOUR && localWeekday(at, h.timezone) === 'Sun') {
        try {
          await env.LIFE_RECEIPT_WF.create({ id: `receipt_${h.household_id}_${localDate(at, h.timezone)}`, params: { household_id: h.household_id, notify: true } });
          out.receipts_started += 1;
        } catch (err) {
          if (!/already|exist/i.test(err instanceof Error ? err.message : String(err))) throw err;
          out.receipts_skipped += 1;
        }
      }
      if (hour !== SWEEP_LOCAL_HOUR) continue;
      out.due += 1;
      const day = localDate(at, h.timezone);
      try {
        if (connected.has(h.household_id)) {
          await env.CALENDAR_SYNC_WF.create({ id: `cal_${h.household_id}_${day}`, params: { household_id: h.household_id, then_sweep: true } });
        } else {
          await env.NIGHTLY_SWEEP_WF.create({ id: `sweep_${h.household_id}_${day}`, params: { household_id: h.household_id } });
        }
        out.started += 1;
      } catch (err) {
        if (!/already|exist/i.test(err instanceof Error ? err.message : String(err))) throw err;
        out.skipped += 1;
      }
    }
    if (results.length < 500) break;
  }
  out.annual_grants = await grantAnnualDrips(env.DB, at, ANNUAL_MONTHLY_GRANT_CREDITS);
  return out;
}
