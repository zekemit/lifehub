/**
 * src/workflows/ingestEmailWF.ts — Cloudflare Workflow wrapper. All logic lives in src/ingest/emailPipeline.ts.
 */

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import { gatewayFromEnv } from '../llm/gateway';
import { runIngestEmail } from '../ingest/emailPipeline';
import type { StepLike } from '../ingest/steps';
import type { Env, IngestEmailParams } from '../types';

export class IngestEmailWF extends WorkflowEntrypoint<Env, IngestEmailParams> {
  override async run(event: WorkflowEvent<IngestEmailParams>, step: WorkflowStep): Promise<void> {
    await runIngestEmail({ env: this.env, gateway: gatewayFromEnv(this.env) }, event.payload, step as unknown as StepLike);
  }
}
