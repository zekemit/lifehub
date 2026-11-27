/**
 * src/index.ts — Worker entrypoint: HTTP API, Email Routing handler, hourly cron, and the Workflow class exports.
 */

import { handleApi } from './workers/api';
import { handleEmail, type InboundMessage } from './workers/emailRouter';
import { runScheduled } from './workers/scheduled';
import type { Env } from './types';

export { CalendarSyncWF } from './workflows/calendarSyncWF';
export { DropAlarmWF } from './workflows/dropAlarmWF';
export { GoalExecutionWF } from './workflows/goalExecutionWF';
export { IngestEmailWF } from './workflows/ingestEmailWF';
export { IngestFileWF } from './workflows/ingestFileWF';
export { LifeReceiptWF } from './workflows/lifeReceiptWF';
export { MissionRunWF } from './workflows/missionRunWF';
export { NightlySweepWF } from './workflows/nightlySweepWF';
export { PriceWatchWF } from './workflows/priceWatchWF';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return handleApi(request, env);
  },
  async email(message: InboundMessage, env: Env): Promise<void> {
    await handleEmail(message, env);
  },
  async scheduled(controller: { scheduledTime: number }, env: Env): Promise<void> {
    await runScheduled(env, new Date(controller.scheduledTime));
  },
};
