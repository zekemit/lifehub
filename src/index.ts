/**
 * src/index.ts — Worker entrypoint: Email Routing handler + Workflow class exports.
 */

import { handleEmail, type InboundMessage } from './workers/emailRouter';
import type { Env } from './types';

export { IngestEmailWF } from './workflows/ingestEmailWF';
export { IngestFileWF } from './workflows/ingestFileWF';

export default {
  async email(message: InboundMessage, env: Env): Promise<void> {
    await handleEmail(message, env);
  },
};
