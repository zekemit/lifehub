/**
 * src/workflows/ingestFileWF.ts — Cloudflare Workflow wrapper. All logic lives in src/ingest/filePipeline.ts.
 */

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import { gatewayFromEnv } from '../llm/gateway';
import { runIngestFile } from '../ingest/filePipeline';
import type { StepLike } from '../ingest/steps';
import type { Env, IngestFileParams } from '../types';

export class IngestFileWF extends WorkflowEntrypoint<Env, IngestFileParams> {
  override async run(event: WorkflowEvent<IngestFileParams>, step: WorkflowStep): Promise<void> {
    await runIngestFile({ env: this.env, gateway: gatewayFromEnv(this.env) }, event.payload, step as unknown as StepLike);
  }
}
