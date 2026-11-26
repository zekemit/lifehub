/**
 * src/workflows/nightlySweepWF.ts — Cloudflare Workflow wrapper. All logic lives in src/maintenance/sweepPipeline.ts.
 */

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import { runNightlySweep } from '../maintenance/sweepPipeline';
import type { StepLike } from '../ingest/steps';
import type { Env, NightlySweepParams } from '../types';

export class NightlySweepWF extends WorkflowEntrypoint<Env, NightlySweepParams> {
  override async run(event: WorkflowEvent<NightlySweepParams>, step: WorkflowStep): Promise<void> {
    const at = event.payload.as_of !== undefined ? new Date(event.payload.as_of) : new Date(event.timestamp);
    await runNightlySweep({ db: this.env.DB, now: () => at }, event.payload, step as unknown as StepLike);
  }
}
