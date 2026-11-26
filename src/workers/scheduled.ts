/**
 * src/workers/scheduled.ts — the hourly cron. For every household whose LOCAL time is 03:00 it starts the nightly chain
 * (CLAUDE.md §5: "NightlySweepWF (03:00 local)"): CalendarSyncWF (which ends by starting NightlySweepWF) when the household has a
 * connected calendar, otherwise NightlySweepWF directly. Instance ids carry the household's local date, so a repeated
 * invocation in the same local hour cannot start the chain twice.
 */

import type { Env } from '../types';

export const SWEEP_LOCAL_HOUR = 3;

export function localHour(at: Date, timeZone: string): number {
  const hour = new Intl.DateTimeFormat('en-US', { timeZone, hour: '2-digit', hourCycle: 'h23' }).formatToParts(at).find((p) => p.type === 'hour')?.value;
  return Number(hour ?? -1);
}

export function localDate(at: Date, timeZone: string): string {
  const p = new Intl.DateTimeFormat('en-US', { timeZone, year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(at);
  const get = (t: string): string => p.find((x) => x.type === t)?.value ?? '';
  return `${get('year')}-${get('month')}-${get('day')}`;
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
}

export async function runScheduled(env: Pick<Env, 'DB' | 'CALENDAR_SYNC_WF' | 'NIGHTLY_SWEEP_WF'>, at: Date): Promise<ScheduledResult> {
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

  const out: ScheduledResult = { due: 0, started: 0, skipped: 0 };
  for (let offset = 0; ; offset += 500) {
    const { results } = await env.DB.prepare(
      `SELECT household_id, timezone FROM households WHERE deleted_at IS NULL AND plan <> 'canceled' ORDER BY household_id LIMIT 500 OFFSET ?`,
    )
      .bind(offset)
      .all<{ household_id: string; timezone: string }>();
    for (const h of results) {
      if (safeLocalHour(at, h.timezone) !== SWEEP_LOCAL_HOUR) continue;
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
  return out;
}
