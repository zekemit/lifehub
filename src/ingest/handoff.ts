/**
 * src/ingest/handoff.ts — what happens to a message the funnel's Layers 0-2 let through, whichever channel delivered it:
 * encrypt the raw MIME into R2 and start IngestEmailWF. Shared by the Email Worker (auto-forward) and the Gmail connector, so both channels
 * reach the same vault and the same pipeline.
 *
 * A failure after the funnel claimed the message gives the claim back (releaseClaim), so a redelivery or the next sync can try again instead of
 * being refused as a duplicate of something that never happened.
 */

import { objectExists, putEncrypted } from '../crypto/vault';
import type { Env, IngestEmailParams } from '../types';
import { releaseClaim, type Channel } from './inboxTriage';

export type HandoffOutcome = { status: 'accepted'; episode_id: string } | { status: 'duplicate'; episode_id: string };

export async function handOff(
  env: Pick<Env, 'DB' | 'DOCS_BUCKET' | 'KEK_B64' | 'INGEST_EMAIL_WF'>,
  o: {
    household_id: string;
    raw: Uint8Array;
    sha256: string;
    episode_id: string;
    envelope_from: string;
    /** The funnel's claim (ingested_messages.message_id_norm), or null when the message skipped the funnel. */
    claimed: string | null;
    channel: Channel;
  },
): Promise<HandoffOutcome> {
  const { household_id, episode_id } = o;
  try {
    const existing = await env.DB.prepare('SELECT episode_id FROM episodes WHERE household_id = ? AND sha256 = ?')
      .bind(household_id, o.sha256)
      .first<{ episode_id: string }>();
    if (existing !== null) return { status: 'duplicate', episode_id: existing.episode_id };

    const r2_key = `raw/${household_id}/${o.sha256}.eml.enc`;
    if (!(await objectExists(env, r2_key))) await putEncrypted(env, household_id, r2_key, o.raw);

    const params: IngestEmailParams = {
      household_id,
      r2_key,
      sha256: o.sha256,
      size_bytes: o.raw.length,
      envelope_from: o.envelope_from,
      ...(o.claimed === null ? {} : { message_id_norm: o.claimed, channel: o.channel }),
    };
    try {
      await env.INGEST_EMAIL_WF.create({ id: episode_id, params });
    } catch (err) {
      if (/already|exist/i.test(err instanceof Error ? err.message : String(err))) return { status: 'duplicate', episode_id };
      throw err;
    }
    return { status: 'accepted', episode_id };
  } catch (err) {
    if (o.claimed !== null) await releaseClaim(env.DB, household_id, o.claimed);
    throw err;
  }
}
