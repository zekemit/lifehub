/**
 * src/workflows/missionRunWF.ts — Cloudflare Workflow wrapper for MissionRunWF (CLAUDE.md §5.1). The state machine lives in
 * src/missions/runPipeline.ts; this class only supplies the durable `step` and production wiring.
 *
 * The instance id is the run_id. The member's taps arrive as events: `approve:<run_id>` ({decision, payload_hash, member_id}) and
 * `confirmed:<run_id>`.
 */

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import { missionDepsFromEnv, runMission } from '../missions/runPipeline';
import type { StepLike } from '../ingest/steps';
import type { Env, MissionRunParams } from '../types';

export class MissionRunWF extends WorkflowEntrypoint<Env, MissionRunParams> {
  override async run(event: WorkflowEvent<MissionRunParams>, step: WorkflowStep): Promise<void> {
    const { run_id, household_id } = event.payload;
    await runMission(step as unknown as StepLike, missionDepsFromEnv(this.env, run_id, household_id), event.payload);
  }
}
