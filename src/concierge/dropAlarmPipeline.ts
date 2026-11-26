/**
 * src/concierge/dropAlarmPipeline.ts — DropAlarmWF (Engine B). Written against StepLike like every pipeline, so the tests run the real code.
 *
 *   load_rule   read the stored release rule and work out the exact release instant in the venue's time zone
 *   sleep       step.sleep until release - 120 seconds
 *   stage_card  stage the dining card (R2) with the venue's booking page as a pre-filled deep link
 *   ...         then MissionRunWF's own steps: debit, gates G1-G9, notify (a payload-less Web Push), wait for the member's approval,
 *               re-gate, issue the R3 deep link, wait for confirmation
 *   finish      record how the alarm ended
 *
 * Nothing here ever contacts a venue or a booking platform: the rule is stored, the clock is the workflow's, and the link is only
 * handed to the member to tap.
 */

import { sha256Hex } from '../crypto/vault';
import type { StepLike } from '../ingest/steps';
import { buildCopy } from '../missions/copy';
import { insertRun } from '../missions/engine';
import { runMission, type MissionDeps, type MissionOutcome } from '../missions/runPipeline';
import type { DropAlarmParams } from '../types';
import { ALARM_CARD_WINDOW_MS, readDropAlarm } from './readers';
import { MAX_SLEEP_MS, RELEASE_LEAD_MS, loadRule, releaseAtMs } from './venues';

export type DropAlarmOutcome = MissionOutcome | { status: 'missed' } | { status: 'ignored'; reason: string };

/** One run per alarm, with an id derived from the alarm so a retried step finds its own row. */
export const alarmRunId = async (alarm_id: string): Promise<string> => `run_${(await sha256Hex(`alarm:${alarm_id}`)).slice(0, 32)}`;

interface Plan {
  go: boolean;
  reason: string;
  sleep_ms: number;
  run_id: string;
}

export async function runDropAlarm(step: StepLike, deps: MissionDeps, params: DropAlarmParams): Promise<DropAlarmOutcome> {
  const db = deps.env.DB;
  const { alarm_id, household_id } = params;
  const setStatus = (status: string, run_id: string | null = null) =>
    db.prepare('UPDATE drop_alarms SET status = ?, run_id = COALESCE(?, run_id) WHERE alarm_id = ? AND household_id = ?').bind(status, run_id, alarm_id, household_id).run();

  const plan = await step.do('load_rule', async (): Promise<Plan> => {
    const run_id = await alarmRunId(alarm_id);
    const stop = async (reason: string, status: string | null): Promise<Plan> => {
      if (status !== null) await setStatus(status);
      return { go: false, reason, sleep_ms: 0, run_id };
    };
    const alarm = await db
      .prepare('SELECT venue_entity_id, reservation_date, release_at, status FROM drop_alarms WHERE alarm_id = ? AND household_id = ?')
      .bind(alarm_id, household_id)
      .first<{ venue_entity_id: string; reservation_date: string; release_at: string; status: string }>();
    if (alarm === null) return stop('no such alarm', null);
    if (alarm.status !== 'scheduled') return stop(`alarm is ${alarm.status}`, null);
    const rule = await loadRule(db, household_id, alarm.venue_entity_id);
    const release = rule === null ? null : releaseAtMs(rule, alarm.reservation_date);
    if (release === null) return stop('the release rule is unusable', 'canceled');
    // the rule must still say what the alarm was created with; if the venue's rule changed, the member is not woken at a wrong time
    if (new Date(release).toISOString() !== alarm.release_at) return stop('the release rule changed', 'canceled');
    const nowMs = deps.now().getTime();
    if (nowMs >= release + ALARM_CARD_WINDOW_MS) return stop('missed', 'missed');
    const sleep_ms = Math.max(0, release - RELEASE_LEAD_MS - nowMs);
    if (sleep_ms > MAX_SLEEP_MS) return stop('too far ahead', 'canceled');
    return { go: true, reason: 'ok', sleep_ms, run_id };
  });
  if (!plan.go) return plan.reason === 'missed' ? { status: 'missed' } : { status: 'ignored', reason: plan.reason };

  if (plan.sleep_ms > 0) await step.sleep('sleep_until_release_minus_120s', plan.sleep_ms);

  const staged = await step.do('stage_card', async () => {
    const existing = await db.prepare('SELECT 1 AS ok FROM mission_runs WHERE run_id = ? AND household_id = ?').bind(plan.run_id, household_id).first<{ ok: number }>();
    if (existing !== null) return true; // a retry of this step
    const read = await readDropAlarm(deps, household_id, alarm_id);
    if (!read.candidate_found) {
      await setStatus('canceled');
      return false;
    }
    await insertRun(db, household_id, {
      run_id: plan.run_id,
      origin: 'user',
      // events for this run go to THIS workflow instance, not to MissionRunWF
      extra_params: { workflow: 'drop_alarm', workflow_instance: alarm_id },
      template_key: 'drop_alarm',
      trigger_key: 'T_DROP_ALARM',
      dedupe_key: alarm_id,
      copy: buildCopy('T_DROP_ALARM', read.data),
      recoverable_cents: 0,
      urgency: 1,
      confidence: 1,
      score: 1,
      data: read.data,
      claims: read.claims.map((c) => ({ key: c.key, facts: c.facts.filter((f): f is string => typeof f === 'string') })),
      now: deps.now(),
    });
    await setStatus('staged', plan.run_id);
    return true;
  });
  if (!staged) return { status: 'ignored', reason: 'the alarm is no longer valid' };

  const outcome = await runMission(step, deps, { run_id: plan.run_id, household_id });
  await step.do('finish', async () => {
    await setStatus(outcome.status === 'confirmed' || outcome.status === 'link_issued' ? 'link_issued' : 'canceled');
  });
  return outcome;
}
