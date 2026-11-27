/**
 * src/ingest/extract.ts — LLM fact extraction. The model only PROPOSES; code validates and writeFact() writes.
 *
 * Hard rules enforced here:
 *   - The model sees `episodes.redacted_text` and nothing else. loadSafeEpisodeText() is the only way text gets into a
 *     prompt, and it refuses episodes that are not 'clean' / 'redacted' (a quarantined episode has NULL text).
 *   - Every proposal must carry a quote that appears VERBATIM in the episode; offsets are located by code
 *     (locateQuote), never trusted from the model. writeFact() re-verifies them against the stored text.
 *   - Output is shape-checked field by field; anything malformed is dropped and counted.
 */

import { MAX_QUOTE_CHARS, locateQuote } from '../graph/writeFact';
import type { Gateway, TextWindow } from '../llm/gateway';
import { ENTITY_TYPES, type EntityRef, type EntityType, type FactProposal } from '../types';
import type { ExtractionProfile } from './extractors';

/** One extraction call covers at most this many (estimated) tokens, far below the 90,000 worker ceiling. */
export const EXTRACTION_WINDOW_TOKENS = 60_000;
/** Windows extracted per episode; the remainder of an enormous document is ignored. */
export const MAX_EXTRACTION_WINDOWS = 6;
const MAX_FACTS_PER_WINDOW = 100;

export class EpisodeNotSafeError extends Error {
  override readonly name = 'EpisodeNotSafeError';
}

/** Returns the episode's redacted text, or throws if the episode may not be shown to a model. */
export async function loadSafeEpisodeText(db: D1Database, household_id: string, episode_id: string): Promise<string> {
  const row = await db
    .prepare('SELECT redaction_status, redacted_text FROM episodes WHERE episode_id = ? AND household_id = ?')
    .bind(episode_id, household_id)
    .first<{ redaction_status: string; redacted_text: string | null }>();
  if (row === null) throw new EpisodeNotSafeError(`episode ${episode_id} not found`);
  if ((row.redaction_status !== 'clean' && row.redaction_status !== 'redacted') || row.redacted_text === null) {
    throw new EpisodeNotSafeError(`episode ${episode_id} is ${row.redaction_status}; it must not reach a model`);
  }
  return row.redacted_text;
}

export interface ExtractionOutcome {
  proposals: FactProposal[];
  dropped: { malformed: number; quote_not_found: number };
}

interface PredicateInfo {
  predicate: string;
  cardinality: string;
  value_type: string;
}

function extractionSchema(predicates: readonly string[]): Record<string, unknown> {
  const nullableString = { type: ['string', 'null'] };
  const nullableNumber = { type: ['number', 'null'] };
  return {
    type: 'object',
    additionalProperties: false,
    required: ['facts'],
    properties: {
      facts: {
        type: 'array',
        items: {
          type: 'object',
          additionalProperties: false,
          required: [
            'subject_type',
            'subject_name',
            'predicate',
            'object_type',
            'object_name',
            'value_text',
            'value_num',
            'value_cents',
            'currency',
            'valid_from',
            'valid_to',
            'confidence',
            'quote',
          ],
          properties: {
            subject_type: { type: 'string', enum: [...ENTITY_TYPES] },
            subject_name: { type: 'string' },
            predicate: { type: 'string', enum: [...predicates] },
            object_type: nullableString,
            object_name: nullableString,
            value_text: nullableString,
            value_num: nullableNumber,
            value_cents: nullableNumber,
            currency: nullableString,
            valid_from: nullableString,
            valid_to: nullableString,
            confidence: { type: 'number' },
            quote: { type: 'string' },
          },
        },
      },
    },
  };
}

function systemPrompt(appName: string, predicates: readonly PredicateInfo[], focus: readonly string[] = []): string {
  const list = predicates.map((p) => `- ${p.predicate} (${p.cardinality}, ${p.value_type})`).join('\n');
  return [
    `You extract structured facts for ${appName}, a household life-admin assistant.`,
    'Read the document and output ONLY facts that it states explicitly. Never guess or infer.',
    'Every fact needs "quote": an exact, contiguous, verbatim substring of the document that states the fact.',
    'Copy the quote character for character. Do not paraphrase, shorten with ellipses, or merge separate passages.',
    'Use only the predicates listed below and give each the value kind its type requires:',
    '  entity -> object_type and object_name;  text -> value_text;  num -> value_num;',
    '  date -> value_text as YYYY-MM-DD;  money -> value_cents as integer cents;',
    '  interval -> value_text as <start>/<end> with ISO dates.',
    'Set unused fields to null. valid_from is the date the fact took effect, when the document says so, as YYYY-MM-DD.',
    'The document contains masked numbers such as [ACCT ****1234]; keep them masked and never reconstruct hidden digits.',
    'If nothing qualifies, return {"facts": []}.',
    ...(focus.length === 0 ? [] : ['', ...focus]),
    '',
    'Predicates:',
    list,
  ].join('\n');
}

function asString(v: unknown): string | null {
  return typeof v === 'string' && v.trim().length > 0 ? v : null;
}

function asEntityType(v: unknown): EntityType | null {
  return typeof v === 'string' && (ENTITY_TYPES as readonly string[]).includes(v) ? (v as EntityType) : null;
}

/**
 * Calls the worker model on one window of one episode and converts its output into grounded proposals. A `profile` (src/ingest/extractors.ts)
 * narrows the predicates the model may use and adds a focus note; without one every predicate in the registry is offered.
 */
export async function extractFacts(
  deps: { db: D1Database; gateway: Gateway; appName: string },
  input: { household_id: string; episode_id: string; window: TextWindow },
  profile: ExtractionProfile | null = null,
): Promise<ExtractionOutcome> {
  const text = await loadSafeEpisodeText(deps.db, input.household_id, input.episode_id);
  const windowText = text.slice(input.window.start, input.window.end);
  const { results: all } = await deps.db
    .prepare('SELECT predicate, cardinality, value_type FROM predicate_registry ORDER BY predicate')
    .all<PredicateInfo>();
  const allowed = profile?.predicates ?? null;
  const predicates = allowed === null ? all : all.filter((p) => allowed.includes(p.predicate));

  const result = await deps.gateway.worker({
    household_id: input.household_id,
    purpose: 'extract.facts',
    system: systemPrompt(deps.appName, predicates, profile?.focus ?? []),
    user: `Document:\n<<<\n${windowText}\n>>>`,
    json_schema: { name: 'extracted_facts', schema: extractionSchema(predicates.map((p) => p.predicate)) },
    max_output_tokens: 8192,
  });

  const outcome: ExtractionOutcome = { proposals: [], dropped: { malformed: 0, quote_not_found: 0 } };
  let parsed: unknown;
  try {
    parsed = JSON.parse(result.text);
  } catch {
    outcome.dropped.malformed += 1;
    return outcome;
  }
  const facts = (parsed as { facts?: unknown }).facts;
  if (!Array.isArray(facts)) {
    outcome.dropped.malformed += 1;
    return outcome;
  }

  for (const item of facts.slice(0, MAX_FACTS_PER_WINDOW)) {
    if (typeof item !== 'object' || item === null) {
      outcome.dropped.malformed += 1;
      continue;
    }
    const f = item as Record<string, unknown>;
    const subjectType = asEntityType(f['subject_type']);
    const subjectName = asString(f['subject_name']);
    const predicate = asString(f['predicate']);
    const quote = asString(f['quote']);
    if (subjectType === null || subjectName === null || predicate === null || quote === null) {
      outcome.dropped.malformed += 1;
      continue;
    }
    const located = locateQuote(text, quote) ?? locateQuote(text, quote.trim());
    if (located === null) {
      outcome.dropped.quote_not_found += 1;
      continue;
    }
    if (located.char_end - located.char_start > MAX_QUOTE_CHARS) {
      outcome.dropped.malformed += 1;
      continue;
    }
    let object: EntityRef | null = null;
    const objectType = asEntityType(f['object_type']);
    const objectName = asString(f['object_name']);
    if (objectType !== null && objectName !== null) object = { type: objectType, name: objectName };

    const confidence = typeof f['confidence'] === 'number' && Number.isFinite(f['confidence']) ? f['confidence'] : 0.5;
    outcome.proposals.push({
      household_id: input.household_id,
      source_episode_id: input.episode_id,
      subject: { type: subjectType, name: subjectName },
      predicate,
      object,
      value_text: asString(f['value_text']),
      value_num: typeof f['value_num'] === 'number' ? f['value_num'] : null,
      value_cents: typeof f['value_cents'] === 'number' ? f['value_cents'] : null,
      currency: asString(f['currency']),
      valid_from: asString(f['valid_from']),
      valid_to: asString(f['valid_to']),
      confidence: Math.min(1, Math.max(0, confidence)),
      quotes: [{ quote: text.slice(located.char_start, located.char_end), ...located }],
    });
  }
  return outcome;
}

/** Key identifying "the same proposed fact", used to drop duplicates across windows. */
export function proposalKey(p: FactProposal): string {
  return JSON.stringify([p.subject, p.predicate, p.object ?? null, p.value_text ?? null, p.value_num ?? null, p.value_cents ?? null, p.valid_from ?? null]);
}
