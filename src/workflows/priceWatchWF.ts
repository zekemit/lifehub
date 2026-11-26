/**
 * src/workflows/priceWatchWF.ts — Cloudflare Workflow wrapper for PriceWatchWF. The logic lives in src/concierge/priceWatchPipeline.ts.
 */

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import { runPriceWatch, watchRunId } from '../concierge/priceWatchPipeline';
import { missionDepsFromEnv } from '../missions/runPipeline';
import type { StepLike } from '../ingest/steps';
import type { Env, PriceWatchParams } from '../types';

export class PriceWatchWF extends WorkflowEntrypoint<Env, PriceWatchParams> {
  override async run(event: WorkflowEvent<PriceWatchParams>, step: WorkflowStep): Promise<void> {
    const { watch_id, household_id } = event.payload;
    const deps = missionDepsFromEnv(this.env, await watchRunId(watch_id), household_id);
    await runPriceWatch(step as unknown as StepLike, { ...deps, startMission: (o) => this.env.MISSION_RUN_WF.create(o) }, event.payload);
  }
}
