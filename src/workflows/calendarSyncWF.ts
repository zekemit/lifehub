/**
 * src/workflows/calendarSyncWF.ts — Cloudflare Workflow wrapper. All logic lives in src/connectors/calendarPipeline.ts.
 */

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import { runCalendarSync } from '../connectors/calendarPipeline';
import { createGuardedFetch } from '../net/egress';
import type { StepLike } from '../ingest/steps';
import type { CalendarSyncParams, Env } from '../types';

export class CalendarSyncWF extends WorkflowEntrypoint<Env, CalendarSyncParams> {
  override async run(event: WorkflowEvent<CalendarSyncParams>, step: WorkflowStep): Promise<void> {
    const at = new Date(event.timestamp);
    await runCalendarSync(
      {
        cal: { env: this.env, fetch: createGuardedFetch({ db: this.env.DB }), now: () => at },
        startSweep: async (household_id) => {
          try {
            await this.env.NIGHTLY_SWEEP_WF.create({
              id: `sweep_${household_id}_${at.toISOString().slice(0, 13).replace(/\D/g, '')}`,
              params: { household_id },
            });
          } catch (err) {
            if (!/already|exist/i.test(err instanceof Error ? err.message : String(err))) throw err;
          }
        },
      },
      event.payload,
      step as unknown as StepLike,
    );
  }
}
