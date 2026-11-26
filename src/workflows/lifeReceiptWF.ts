/**
 * src/workflows/lifeReceiptWF.ts — Cloudflare Workflow wrapper for LifeReceiptWF (Sunday 08:00 household-local, started by the hourly
 * cron). The logic lives in src/receipt/pipeline.ts.
 */

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import { createGuardedFetch } from '../net/egress';
import { runLifeReceipt } from '../receipt/pipeline';
import type { StepLike } from '../ingest/steps';
import type { Env, LifeReceiptParams } from '../types';

export class LifeReceiptWF extends WorkflowEntrypoint<Env, LifeReceiptParams> {
  override async run(event: WorkflowEvent<LifeReceiptParams>, step: WorkflowStep): Promise<void> {
    const at = event.payload.as_of !== undefined ? new Date(event.payload.as_of) : new Date(event.timestamp);
    await runLifeReceipt(step as unknown as StepLike, { db: this.env.DB, fetch: createGuardedFetch({ db: this.env.DB }), env: this.env, now: () => at }, event.payload);
  }
}
