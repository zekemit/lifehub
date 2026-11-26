/**
 * src/ingest/filePipeline.ts — IngestFileWF's logic (CLAUDE.md §5): CSV / PDF-text -> dedupe -> categorize -> credits.
 *
 *   prepare              decrypt, REDACT, store episode(s) (<= 300K chars each). Quarantined => episode stored with NULL
 *                        text and nothing else runs. Parsing always works on the redacted text.
 *   categorize-k / insert-k   500 rows per pair of steps: label unknown merchants (model), then INSERT OR IGNORE
 *   envelopes            trailing-3-month median targets
 *   credits              credit_periods recomputed from transactions
 *   recurring-detect / recurring-write-k   recurring charges -> writeFact(), 45 facts per step
 *
 * Re-running the same file is a no-op: episode ids and txn ids are deterministic, inserts are OR IGNORE, and
 * envelopes / credit periods are recomputed from stored data rather than incremented.
 */

import { getDecrypted, sha256Hex } from '../crypto/vault';
import type { WriteFactOptions } from '../graph/writeFact';
import type { Gateway } from '../llm/gateway';
import { redact } from '../redact';
import type { Env, FactProposal, IngestFileParams } from '../types';
import { parseSpendText, type ParsedSpend, type ParsedTxn } from './csv';
import {
  DEFAULT_STEP_CONFIG,
  MAX_FACTS_PER_STEP,
  chunk,
  mergeSummaries,
  writeFactsChunk,
  type StepLike,
  type WriteSummary,
} from './steps';
import {
  ROWS_PER_STEP,
  categorizeBatch,
  detectRecurring,
  entityNames,
  insertBatch,
  loadOutflowHistory,
  recomputeCreditPeriods,
  recomputeEnvelopes,
  seriesToProposals,
  txnId,
} from './spend';

/** Characters of redacted text per episode; keeps every episode far below D1's 2 MB row limit. */
export const EPISODE_CHUNK_CHARS = 300_000;

export interface FilePipelineDeps {
  env: Pick<Env, 'DB' | 'DOCS_BUCKET' | 'KEK_B64' | 'APP_NAME'>;
  gateway: Gateway;
  now?: () => Date;
  writeOptions?: WriteFactOptions;
}

interface EpisodeSlice {
  episode_id: string;
  /** Rows [row_from, row_to) belong to this episode. */
  row_from: number;
  row_to: number;
  /** Slice of the redacted text stored as this episode. */
  start: number;
  end: number;
}

type PrepareResult =
  | { status: 'missing_object' }
  | { status: 'quarantined'; episode_id: string; reason: string }
  | { status: 'empty'; skipped: number; format: string }
  | {
      status: 'ready';
      format: string;
      convention: string;
      skipped: number;
      rows: number;
      min_date: string;
      max_date: string;
      episodes: EpisodeSlice[];
    };

export type FilePipelineResult =
  | { status: 'missing_object' }
  | { status: 'quarantined'; episode_id: string; reason: string }
  | { status: 'empty'; skipped: number }
  | {
      status: 'done';
      format: string;
      convention: string;
      rows: number;
      skipped: number;
      inserted: number;
      duplicates: number;
      merchants_labeled: number;
      envelopes_updated: number;
      credit_periods_updated: number;
      recurring_series: number;
      recurring_writes: WriteSummary;
      steps: number;
    };

async function episodeIdFor(household_id: string, sha: string, chunkIndex: number): Promise<string> {
  return `ep_${(await sha256Hex(`${household_id}:${sha}:${chunkIndex}`)).slice(0, 32)}`;
}

interface Loaded {
  text: string;
  parsed: ParsedSpend;
}

export async function runIngestFile(
  deps: FilePipelineDeps,
  params: IngestFileParams,
  step: StepLike,
): Promise<FilePipelineResult> {
  const { env } = deps;
  const clock = deps.now ?? (() => new Date());
  const asOfIso = params.as_of ?? (await step.do('clock', async () => clock().toISOString()));
  const asOf = new Date(asOfIso);
  const spendDeps = { db: env.DB, gateway: deps.gateway, appName: env.APP_NAME, now: () => asOf };
  let steps = 1;

  /** Decrypt + redact + parse. Deterministic, so every step can recompute it instead of passing megabytes around. */
  async function load(): Promise<
    | { status: 'ok'; value: Loaded }
    | { status: 'missing_object' }
    | { status: 'quarantined'; reason: string }
  > {
    const raw = await getDecrypted(env, params.household_id, params.r2_key);
    if (raw === null) return { status: 'missing_object' };
    const redacted = redact(new TextDecoder().decode(raw));
    if (redacted.redaction_status === 'quarantined') return { status: 'quarantined', reason: redacted.quarantine_reason };
    const parsed = parseSpendText(params.kind, redacted.redacted_text, {
      sign_convention: params.sign_convention ?? 'auto',
      statement_year: params.statement_year ?? null,
      asOf,
    });
    return { status: 'ok', value: { text: redacted.redacted_text, parsed } };
  }

  async function loadOrThrow(): Promise<Loaded> {
    const loaded = await load();
    if (loaded.status !== 'ok') throw new Error(`file is no longer readable (${loaded.status})`);
    return loaded.value;
  }

  // ── prepare ──
  const prepared = await step.do('prepare', DEFAULT_STEP_CONFIG, async (): Promise<PrepareResult> => {
    const raw = await getDecrypted(env, params.household_id, params.r2_key);
    if (raw === null) return { status: 'missing_object' };
    const redacted = redact(new TextDecoder().decode(raw));
    const source = params.kind === 'csv' ? 'csv' : 'pdf';
    if (redacted.redaction_status === 'quarantined') {
      const episode_id = await episodeIdFor(params.household_id, params.sha256, 0);
      await env.DB.batch([
        env.DB.prepare(
          `INSERT OR IGNORE INTO episodes (episode_id, household_id, source, source_ref, r2_key, redacted_text, sha256, redaction_status)
           VALUES (?, ?, ?, ?, ?, NULL, ?, 'quarantined')`,
        ).bind(episode_id, params.household_id, source, params.file_name, params.r2_key, params.sha256),
        env.DB.prepare(
          `INSERT INTO audit_log (id, household_id, actor, action, detail_json) VALUES (?, ?, 'ingest-file', 'episode_quarantined', ?)`,
        ).bind(`aud_${crypto.randomUUID().replace(/-/g, '')}`, params.household_id, JSON.stringify({ episode_id, reason: redacted.quarantine_reason })),
      ]);
      return { status: 'quarantined', episode_id, reason: redacted.quarantine_reason };
    }
    const text = redacted.redacted_text;
    const parsed = parseSpendText(params.kind, text, {
      sign_convention: params.sign_convention ?? 'auto',
      statement_year: params.statement_year ?? null,
      asOf,
    });
    if (parsed.rows.length === 0) return { status: 'empty', skipped: parsed.skipped, format: parsed.format };

    // Split into episodes at row boundaries.
    const slices: EpisodeSlice[] = [];
    let from = 0;
    for (let i = 1; i <= parsed.rows.length; i++) {
      const rowStart = parsed.rows[from]!.start;
      const nextStart = i < parsed.rows.length ? parsed.rows[i]!.start : text.length;
      if (nextStart - (slices.length === 0 ? 0 : rowStart) > EPISODE_CHUNK_CHARS || i === parsed.rows.length) {
        slices.push({
          episode_id: await episodeIdFor(params.household_id, params.sha256, slices.length),
          row_from: from,
          row_to: i,
          start: slices.length === 0 ? 0 : rowStart,
          end: nextStart,
        });
        from = i;
      }
    }
    const dates = parsed.rows.map((r) => r.posted_on).sort();
    const statements = await Promise.all(
      slices.map(async (s, n) => {
        const chunkRows = parsed.rows.slice(s.row_from, s.row_to);
        const lastDate = chunkRows.map((r) => r.posted_on).sort().at(-1)!;
        const sha = slices.length === 1 ? params.sha256 : await sha256Hex(`${params.sha256}:${n}`);
        return env.DB.prepare(
          `INSERT OR IGNORE INTO episodes
             (episode_id, household_id, source, source_ref, r2_key, redacted_text, sha256, occurred_at, redaction_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        ).bind(
          s.episode_id,
          params.household_id,
          source,
          params.file_name,
          params.r2_key,
          text.slice(s.start, s.end),
          sha,
          `${lastDate}T00:00:00.000Z`,
          redacted.redaction_status,
        );
      }),
    );
    await env.DB.batch(statements);
    return {
      status: 'ready',
      format: parsed.format,
      convention: parsed.convention,
      skipped: parsed.skipped,
      rows: parsed.rows.length,
      min_date: dates[0]!,
      max_date: dates.at(-1)!,
      episodes: slices,
    };
  });

  if (prepared.status === 'missing_object') return { status: 'missing_object' };
  if (prepared.status === 'quarantined') return { status: 'quarantined', episode_id: prepared.episode_id, reason: prepared.reason };
  if (prepared.status === 'empty') return { status: 'empty', skipped: prepared.skipped };

  const episodeForRow = (row: ParsedTxn): string => {
    const slice = prepared.episodes.find((e) => row.index >= e.row_from && row.index < e.row_to);
    if (slice === undefined) throw new Error(`row ${row.index} has no episode`);
    return slice.episode_id;
  };

  // ── categorize + insert, 500 rows per pair of steps ──
  let inserted = 0;
  let duplicates = 0;
  let labeled = 0;
  for (let k = 0; k * ROWS_PER_STEP < prepared.rows; k++) {
    const labelResult = await step.do(`categorize-${k}`, DEFAULT_STEP_CONFIG, async () => {
      const { parsed } = await loadOrThrow();
      return categorizeBatch(spendDeps, params.household_id, parsed.rows.slice(k * ROWS_PER_STEP, (k + 1) * ROWS_PER_STEP));
    });
    labeled += labelResult.unknown;
    const insertResult = await step.do(`insert-${k}`, DEFAULT_STEP_CONFIG, async () => {
      const { parsed } = await loadOrThrow();
      return insertBatch(
        spendDeps,
        { household_id: params.household_id, defaultCardId: params.default_card_entity_id ?? null, episodeFor: episodeForRow },
        parsed.rows.slice(k * ROWS_PER_STEP, (k + 1) * ROWS_PER_STEP),
      );
    });
    steps += 2;
    inserted += insertResult.inserted;
    duplicates += insertResult.duplicates;
  }

  // ── derived tables ──
  const envelopes = await step.do('envelopes', DEFAULT_STEP_CONFIG, () => recomputeEnvelopes(env.DB, params.household_id, asOf));
  const credits = await step.do('credits', DEFAULT_STEP_CONFIG, () =>
    recomputeCreditPeriods(env.DB, params.household_id, { from: prepared.min_date, to: prepared.max_date }, asOf),
  );
  steps += 2;

  // ── recurring charges ──
  const proposals = await step.do('recurring-detect', DEFAULT_STEP_CONFIG, async (): Promise<FactProposal[]> => {
    const { text, parsed } = await loadOrThrow();
    const rowByTxn = new Map<string, ParsedTxn>();
    for (const row of parsed.rows) rowByTxn.set(await txnId(params.household_id, row), row);
    const since = new Date(asOf.getTime() - 400 * 86_400_000).toISOString().slice(0, 10);
    const history = await loadOutflowHistory(env.DB, params.household_id, since);
    const series = detectRecurring(history).filter((s) => rowByTxn.has(s.latest.txn_id));
    const names = await entityNames(
      env.DB,
      params.household_id,
      series.flatMap((s) => (s.merchant_entity_id === null ? [] : [s.merchant_entity_id])),
    );
    const out: FactProposal[] = [];
    const sliceOf = (row: ParsedTxn): EpisodeSlice | undefined => prepared.episodes.find((e) => row.index >= e.row_from && row.index < e.row_to);
    for (const s of series) {
      out.push(
        ...seriesToProposals(s, {
          household_id: params.household_id,
          merchantName: s.merchant_entity_id === null ? null : (names.get(s.merchant_entity_id) ?? null),
          episode_id: (txn) => {
            const row = rowByTxn.get(txn.txn_id);
            return row === undefined ? null : (sliceOf(row)?.episode_id ?? null);
          },
          quoteFor: (txn) => {
            const row = rowByTxn.get(txn.txn_id);
            const slice = row === undefined ? undefined : sliceOf(row);
            if (row === undefined || slice === undefined) return null;
            const start = row.start - slice.start;
            const end = Math.min(row.end - slice.start, start + 1000);
            return { quote: text.slice(slice.start + start, slice.start + end), char_start: start, char_end: end };
          },
        }),
      );
    }
    return out;
  });
  steps += 1;

  const writes: WriteSummary[] = [];
  for (const [k, group] of chunk(proposals, MAX_FACTS_PER_STEP).entries()) {
    writes.push(await step.do(`recurring-write-${k}`, DEFAULT_STEP_CONFIG, () => writeFactsChunk(env.DB, group, deps.writeOptions)));
    steps += 1;
  }

  return {
    status: 'done',
    format: prepared.format,
    convention: prepared.convention,
    rows: prepared.rows,
    skipped: prepared.skipped,
    inserted,
    duplicates,
    merchants_labeled: labeled,
    envelopes_updated: envelopes.updated,
    credit_periods_updated: credits.updated,
    recurring_series: new Set(proposals.map((p) => JSON.stringify(p.subject))).size,
    recurring_writes: mergeSummaries(writes),
    steps,
  };
}
