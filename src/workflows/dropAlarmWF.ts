/**
 * src/workflows/dropAlarmWF.ts — Cloudflare Workflow wrapper for DropAlarmWF. The logic lives in src/concierge/dropAlarmPipeline.ts.
 * The instance id is the alarm id; the member's taps arrive as the same `approve:<run_id>` / `confirmed:<run_id>` events as MissionRunWF.
 */

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import { alarmRunId, runDropAlarm } from '../concierge/dropAlarmPipeline';
import { missionDepsFromEnv } from '../missions/runPipeline';
import type { StepLike } from '../ingest/steps';
import type { DropAlarmParams, Env } from '../types';

export class DropAlarmWF extends WorkflowEntrypoint<Env, DropAlarmParams> {
  override async run(event: WorkflowEvent<DropAlarmParams>, step: WorkflowStep): Promise<void> {
    const { alarm_id, household_id } = event.payload;
    await runDropAlarm(step as unknown as StepLike, missionDepsFromEnv(this.env, await alarmRunId(alarm_id), household_id), event.payload);
  }
}
