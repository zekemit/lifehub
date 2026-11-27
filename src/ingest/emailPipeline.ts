/**
 * src/ingest/emailPipeline.ts — IngestEmailWF's logic (CLAUDE.md §5): MIME (R2, encrypted) -> redact -> extract -> writeFact.
 *
 *   step 'ingest'      decrypt + parse MIME, redact, store the episode. A QUARANTINED episode is stored with
 *                      redacted_text NULL and the pipeline stops here: no model is ever called.
 *   step 'extract-<i>' one worker-model call per text window (<= 60K estimated tokens each).
 *   step 'write-<k>'   writeFact() for at most 45 proposals, so one step stays far below D1's 1,000-queries limit.
 *
 * Milestone 6 adds two steps around extraction (src/ingest/inboxTriage.ts and extractors.ts):
 *   step 'triage'      Layer 3 of the inbox funnel: a micro-classifier over `from`, `subject` and the first 600 redacted characters
 *                      (<= 2,000 tokens). IGNORE stops here, before any fact extraction. Otherwise the bucket picks the extraction profile.
 *   step 'triggers'    after facts are written, the three life-admin triggers are evaluated at once and any new card is staged.
 *
 * Written against StepLike so the same code runs inside a Workflow and directly in tests.
 */

import { getDecrypted, sha256Hex } from '../crypto/vault';
import type { WriteFactOptions } from '../graph/writeFact';
import { estimateTokens, splitToTokenBudget, type Gateway, type TextWindow } from '../llm/gateway';
import { stageAfterIngest, type StagedCard } from '../missions/triggers';
import { redact } from '../redact';
import type { IngestEmailParams, RedactionStatus } from '../types';
import {
  EXTRACTION_WINDOW_TOKENS,
  MAX_EXTRACTION_WINDOWS,
  extractFacts,
  loadSafeEpisodeText,
  proposalKey,
} from './extract';
import { sanitizeProposals, extractionProfile } from './extractors';
import { classifyEmail, headerLinesOf, recordBucket, type TriageBucket } from './inboxTriage';
import { parseEmail } from './mime';
import {
  DEFAULT_STEP_CONFIG,
  MAX_FACTS_PER_EPISODE,
  MAX_FACTS_PER_STEP,
  chunk,
  mergeSummaries,
  writeFactsChunk,
  type StepLike,
  type WriteSummary,
} from './steps';
import type { Env, FactProposal } from '../types';

export interface EmailPipelineDeps {
  env: Pick<Env, 'DB' | 'DOCS_BUCKET' | 'KEK_B64' | 'APP_NAME'>;
  gateway: Gateway;
  writeOptions?: WriteFactOptions;
  /**
   * Run Layer 3 (the micro-classifier) and bucket-aware extraction. Defaults to true; production never sets it. `false` extracts with every
   * predicate and no classifier call (the extraction-only behavior of Milestones 1-5, used by their tests).
   */
  triage?: boolean;
  /** Stage the life-admin cards right after the facts are written. Defaults to true. */
  triggers?: boolean;
  /** Clock for staging. Default: the real one. */
  now?: () => Date;
}

export type EmailPipelineResult =
  | { status: 'quarantined'; episode_id: string; reason: string }
  | { status: 'missing_object'; episode_id: string }
  /** Layer 3 sorted it IGNORE: stored as an episode, nothing extracted. */
  | { status: 'ignored'; episode_id: string; bucket: 'IGNORE' }
  | {
      status: 'done';
      episode_id: string;
      redaction_status: Extract<RedactionStatus, 'clean' | 'redacted'>;
      windows: number;
      proposed: number;
      dropped: { malformed: number; quote_not_found: number };
      writes: WriteSummary;
      steps: number;
      /** The Layer 3 bucket, or null when triage is off or the model's answer was not a bucket. */
      bucket: TriageBucket | null;
      /** Cards staged by the post-ingest trigger step. */
      staged: StagedCard[];
    };

interface IngestStepResult {
  episode_id: string;
  redaction_status: RedactionStatus;
  quarantine_reason: string | null;
  missing: boolean;
  windows: TextWindow[];
}

export async function runIngestEmail(
  deps: EmailPipelineDeps,
  params: IngestEmailParams,
  step: StepLike,
): Promise<EmailPipelineResult> {
  const { env } = deps;
  const ingested = await step.do('ingest', DEFAULT_STEP_CONFIG, async (): Promise<IngestStepResult> => {
    const episode_id = `ep_${(await sha256Hex(`${params.household_id}:${params.sha256}`)).slice(0, 32)}`;
    const raw = await getDecrypted(env, params.household_id, params.r2_key);
    if (raw === null) {
      return { episode_id, redaction_status: 'pending', quarantine_reason: null, missing: true, windows: [] };
    }
    const mail = await parseEmail(raw);
    const redacted = redact(mail.text);
    await env.DB.prepare(
      `INSERT OR IGNORE INTO episodes
         (episode_id, household_id, source, source_ref, r2_key, redacted_text, sha256, occurred_at, redaction_status)
       VALUES (?, ?, 'email', ?, ?, ?, ?, ?, ?)`,
    )
      .bind(
        episode_id,
        params.household_id,
        params.envelope_from,
        params.r2_key,
        redacted.redacted_text,
        params.sha256,
        mail.occurred_at,
        redacted.redaction_status,
      )
      .run();
    if (params.message_id_norm !== undefined) {
      await recordBucket(env.DB, params.household_id, params.message_id_norm, redacted.redaction_status === 'quarantined' ? 'QUARANTINED' : null, episode_id);
    }
    if (redacted.redaction_status === 'quarantined') {
      await env.DB.prepare(
        `INSERT INTO audit_log (id, household_id, actor, action, detail_json) VALUES (?, ?, 'ingest-email', 'episode_quarantined', ?)`,
      )
        .bind(`aud_${crypto.randomUUID().replace(/-/g, '')}`, params.household_id, JSON.stringify({ episode_id, reason: redacted.quarantine_reason }))
        .run();
      return {
        episode_id,
        redaction_status: 'quarantined',
        quarantine_reason: redacted.quarantine_reason,
        missing: false,
        windows: [],
      };
    }
    const text = redacted.redacted_text;
    const windows =
      estimateTokens(text) <= EXTRACTION_WINDOW_TOKENS
        ? text.length > 0
          ? [{ start: 0, end: text.length }]
          : []
        : splitToTokenBudget(text, EXTRACTION_WINDOW_TOKENS).slice(0, MAX_EXTRACTION_WINDOWS);
    return { episode_id, redaction_status: redacted.redaction_status, quarantine_reason: null, missing: false, windows };
  });

  if (ingested.missing) return { status: 'missing_object', episode_id: ingested.episode_id };
  if (ingested.redaction_status === 'quarantined') {
    return { status: 'quarantined', episode_id: ingested.episode_id, reason: ingested.quarantine_reason ?? 'unknown' };
  }

  let steps = 1;
  let bucket: TriageBucket | null = null;
  if (deps.triage !== false) {
    // Layer 3: only redacted text, only from / subject / the first 600 characters, never more than 2,000 tokens.
    bucket = await step.do('triage', DEFAULT_STEP_CONFIG, async () => {
      const text = await loadSafeEpisodeText(env.DB, params.household_id, ingested.episode_id);
      const { from, subject } = headerLinesOf(text);
      const found = await classifyEmail({ gateway: deps.gateway, appName: env.APP_NAME }, { household_id: params.household_id, from, subject, redacted_text: text });
      if (params.message_id_norm !== undefined && found !== null) await recordBucket(env.DB, params.household_id, params.message_id_norm, found, ingested.episode_id);
      return found;
    });
    steps += 1;
    if (bucket === 'IGNORE') return { status: 'ignored', episode_id: ingested.episode_id, bucket: 'IGNORE' };
  }
  const profile = extractionProfile(bucket);

  const dropped = { malformed: 0, quote_not_found: 0 };
  const seen = new Set<string>();
  const proposals: FactProposal[] = [];
  for (const [i, window] of ingested.windows.entries()) {
    const outcome = await step.do(`extract-${i}`, DEFAULT_STEP_CONFIG, () =>
      extractFacts(
        { db: env.DB, gateway: deps.gateway, appName: env.APP_NAME },
        { household_id: params.household_id, episode_id: ingested.episode_id, window },
        profile,
      ),
    );
    steps += 1;
    dropped.malformed += outcome.dropped.malformed;
    dropped.quote_not_found += outcome.dropped.quote_not_found;
    for (const p of outcome.proposals) {
      const key = proposalKey(p);
      if (!seen.has(key) && proposals.length < MAX_FACTS_PER_EPISODE) {
        seen.add(key);
        proposals.push(p);
      }
    }
  }

  const sanitized = sanitizeProposals(proposals);
  dropped.malformed += sanitized.dropped;

  const parts: WriteSummary[] = [];
  for (const [k, group] of chunk(sanitized.kept, MAX_FACTS_PER_STEP).entries()) {
    parts.push(await step.do(`write-${k}`, DEFAULT_STEP_CONFIG, () => writeFactsChunk(env.DB, group, deps.writeOptions)));
    steps += 1;
  }
  const writes = mergeSummaries(parts);

  // New facts may complete a card (a price drop for an item bought last week, a delay for a booked trip): evaluate the life-admin triggers now.
  let staged: StagedCard[] = [];
  if (deps.triggers !== false && writes.written > 0) {
    staged = (await step.do('triggers', DEFAULT_STEP_CONFIG, () => stageAfterIngest(env.DB, params.household_id, (deps.now ?? (() => new Date()))()))).staged;
    steps += 1;
  }

  return {
    status: 'done',
    episode_id: ingested.episode_id,
    redaction_status: ingested.redaction_status as 'clean' | 'redacted',
    windows: ingested.windows.length,
    proposed: proposals.length,
    dropped,
    writes,
    steps,
    bucket,
    staged,
  };
}
