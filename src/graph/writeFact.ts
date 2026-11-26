/**
 * src/graph/writeFact.ts — the deterministic bi-temporal graph writer (CLAUDE.md §4).
 *
 * An LLM only ever PROPOSES a fact (with verbatim quotes). This module validates the proposal and is the only
 * code that writes `facts` / `fact_quotes` / `entities` / `entity_aliases`.
 *
 *   1. Verify quotes   episodes.redacted_text.slice(char_start, char_end) === quote for every quote, else reject.
 *                      The episode must be redacted (clean | redacted) and belong to the household.
 *   2. Resolve entities  alias -> (optional injected adjudicator, top-3, similarity >= 0.85) -> new entity.
 *   3. cardinality='one' conflicts (a believed fact with a different value overlapping in valid time):
 *        new.valid_from >  old.valid_from  => WORLD CHANGE  old.valid_to   = new.valid_from
 *        new.valid_from <= old.valid_from  => CORRECTION    old.expired_at = now, old.superseded_by = new
 *        Exception, so out-of-order ingestion cannot corrupt history: when the new fact would CORRECT an
 *        incumbent that came from strictly NEWER evidence (episode occurred_at ?? received_at), the new fact is
 *        a historical back-fill: it is stored closed at the incumbent's valid_from and the incumbent is untouched.
 *   4. One atomic db.batch(): [stale-state guard] + new entities/aliases + fact + closures + quotes + audit rows.
 *   5. (Nightly decay lives in NightlySweepWF, not here.)
 *
 * Also handled:
 *   - Re-assertion of an equal believed fact (same value, overlapping valid time) writes NO new fact: the new
 *     quotes are attached to it as extra support ('reaffirmed'), which is what keeps decay_days honest and makes
 *     Workflow step retries idempotent.
 *   - Concurrency: the batch starts with a guard that aborts the whole transaction if the facts this write
 *     read (same household/subject/predicate [+ value for 'many']) changed since they were read; a lost alias
 *     race (PRIMARY KEY on entity_aliases) aborts it too. Both cases re-plan from fresh reads, up to maxAttempts.
 *   - Rejections are values ({status:'rejected', reason, detail}), never exceptions. Exceptions are real faults.
 *
 * Every table access is scoped by household_id. The caller passes the D1Database of the household's shard.
 */

import {
  ENTITY_TYPES,
  type EntityRef,
  type EntityType,
  type EpisodeRow,
  type FactProposal,
  type FactRejectionReason,
  type FactSupersession,
  type IsoTimestamp,
  type PredicateRegistryRow,
  type QuoteProposal,
  type ValueType,
  type WriteFactResult,
} from '../types';

// ─── public tuning constants ────────────────────────────────────────────────

/** Minimum quote length, measured after trimming whitespace. */
export const MIN_QUOTE_CHARS = 3;
/** Quotes longer than this are rejected (a whole-document quote grounds anything). */
export const MAX_QUOTE_CHARS = 1000;
/** Minimum string similarity for an existing entity to be offered to the adjudicator. */
export const ADJUDICATION_MIN_SIMILARITY = 0.85;
/** Maximum number of candidates offered to the adjudicator. */
export const ADJUDICATION_MAX_CANDIDATES = 3;

const MAX_TEXT_CHARS = 2000;
const CANDIDATE_PAGE_SIZE = 500;
const CANDIDATE_MAX_PAGES = 10;
const ACTOR = 'writeFact';

// ─── public types ───────────────────────────────────────────────────────────

export interface EntityCandidate {
  entity_id: string;
  canonical_name: string;
  similarity: number;
}

export interface AdjudicationRequest {
  household_id: string;
  type: EntityType;
  name: string;
  /** At most ADJUDICATION_MAX_CANDIDATES, best first, each with similarity >= ADJUDICATION_MIN_SIMILARITY. */
  candidates: readonly EntityCandidate[];
}

export interface AdjudicationDecision {
  /** Must be one of the offered candidates; anything else is ignored. */
  entity_id: string;
  confidence: number;
}

/** Injected LLM step (via src/llm/gateway.ts). Return null for "none of these". Never trusted blindly. */
export type EntityAdjudicator = (req: AdjudicationRequest) => Promise<AdjudicationDecision | null>;

export interface WriteFactOptions {
  /** Clock. Default: () => new Date(). */
  now?: () => Date;
  /** Id generator. Default: prefix + '_' + 32 hex chars of crypto.randomUUID(). */
  newId?: (prefix: 'fact' | 'ent' | 'aud') => string;
  /** Entity adjudicator. When omitted, an alias miss creates a new entity. */
  adjudicate?: EntityAdjudicator;
  /** Plan/commit attempts under concurrent writers. Default 4. */
  maxAttempts?: number;
}

// ─── small result helpers ───────────────────────────────────────────────────

interface Ok<T> {
  ok: true;
  value: T;
}
interface Fail {
  ok: false;
  reason: FactRejectionReason;
  detail: string;
}
type Result<T> = Ok<T> | Fail;

function ok<T>(value: T): Ok<T> {
  return { ok: true, value };
}
function fail(reason: FactRejectionReason, detail: string): Fail {
  return { ok: false, reason, detail };
}
function toRejection(f: Fail): WriteFactResult {
  return { status: 'rejected', reason: f.reason, detail: f.detail };
}

// ─── text / alias helpers ───────────────────────────────────────────────────

function collapse(s: string): string {
  return s.normalize('NFC').replace(/\s+/g, ' ').trim();
}

function normalizeName(name: string): string {
  return name
    .normalize('NFKD')
    .replace(/\p{M}+/gu, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/['’`]/g, '')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .trim();
}

/**
 * Key used in entity_aliases.alias_norm: '<type>:<normalized name>'. The table's primary key has no type column,
 * so the type is part of the key. Returns null when the name normalizes to nothing.
 */
export function aliasKey(type: EntityType, name: string): string | null {
  const norm = normalizeName(name);
  return norm.length === 0 ? null : `${type}:${norm}`;
}

function bigramCounts(s: string): Map<string, number> {
  const counts = new Map<string, number>();
  for (let i = 0; i < s.length - 1; i++) {
    const gram = s.slice(i, i + 2);
    counts.set(gram, (counts.get(gram) ?? 0) + 1);
  }
  return counts;
}

/** Sørensen–Dice coefficient on character bigrams, 0..1. */
function similarity(a: string, b: string): number {
  if (a === b) return 1;
  if (a.length < 2 || b.length < 2) return 0;
  const ca = bigramCounts(a);
  const cb = bigramCounts(b);
  let overlap = 0;
  for (const [gram, n] of ca) overlap += Math.min(n, cb.get(gram) ?? 0);
  return (2 * overlap) / (a.length - 1 + (b.length - 1));
}

/**
 * Finds `quote` in the episode text and returns offsets suitable for a QuoteProposal (first occurrence).
 * LLMs are unreliable at counting characters, so extraction code locates quotes with this and writeFact
 * then re-verifies them against the stored text.
 */
export function locateQuote(
  redactedText: string,
  quote: string,
): { char_start: number; char_end: number } | null {
  if (quote.length === 0) return null;
  const at = redactedText.indexOf(quote);
  return at < 0 ? null : { char_start: at, char_end: at + quote.length };
}

// ─── time helpers ───────────────────────────────────────────────────────────

const DATE_ONLY = /^(\d{4})-(\d{2})-(\d{2})$/;
const DATE_TIME =
  /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?\s*(Z|[+-]\d{2}(?::?\d{2})?)?$/i;
const FOREVER = '￿';

function isRealDate(y: number, m: number, d: number): boolean {
  const t = new Date(Date.UTC(y, m - 1, d));
  return t.getUTCFullYear() === y && t.getUTCMonth() === m - 1 && t.getUTCDate() === d;
}

/**
 * Normalizes 'YYYY-MM-DD' or an ISO-8601 date-time (zone-less values are read as UTC) to the canonical
 * 'YYYY-MM-DDTHH:MM:SS.sssZ' form used by every timestamp column. Returns null for anything else,
 * including impossible calendar dates.
 */
export function toIsoTimestamp(input: string): IsoTimestamp | null {
  const s = input.trim();
  const d = DATE_ONLY.exec(s);
  if (d) {
    return isRealDate(Number(d[1]), Number(d[2]), Number(d[3])) ? `${s}T00:00:00.000Z` : null;
  }
  const m = DATE_TIME.exec(s);
  if (!m) return null;
  const [y, mo, da, h, mi] = [m[1], m[2], m[3], m[4], m[5]].map(Number) as [number, number, number, number, number];
  const sec = m[6] === undefined ? 0 : Number(m[6]);
  const ms = m[7] === undefined ? 0 : Number(m[7].slice(0, 3).padEnd(3, '0'));
  if (!isRealDate(y, mo, da) || h > 23 || mi > 59 || sec > 59) return null;
  let offsetMinutes = 0;
  const zone = m[8];
  if (zone !== undefined && zone.toUpperCase() !== 'Z') {
    const digits = zone.slice(1).replace(':', '');
    const oh = Number(digits.slice(0, 2));
    const om = digits.length > 2 ? Number(digits.slice(2, 4)) : 0;
    if (oh > 23 || om > 59) return null;
    offsetMinutes = (zone.startsWith('-') ? -1 : 1) * (oh * 60 + om);
  }
  const epoch = Date.UTC(y, mo - 1, da, h, mi, sec, ms) - offsetMinutes * 60_000;
  return Number.isFinite(epoch) ? new Date(epoch).toISOString() : null;
}

function overlaps(aFrom: string, aTo: string | null, bFrom: string, bTo: string | null): boolean {
  return aFrom < (bTo ?? FOREVER) && bFrom < (aTo ?? FOREVER);
}

// ─── proposal validation (no I/O) ───────────────────────────────────────────

interface ValueDraft {
  object: EntityRef | null;
  value_text: string | null;
  value_num: number | null;
  value_cents: number | null;
  currency: string | null;
}

interface CheckedProposal {
  household_id: string;
  source_episode_id: string;
  predicate: string;
  subject: EntityRef;
  object: EntityRef | null;
  value_text: string | null;
  value_num: number | null;
  value_cents: number | null;
  currency: string | null;
  valid_from: IsoTimestamp | null;
  valid_to: IsoTimestamp | null;
  confidence: number;
  quotes: QuoteProposal[];
}

function isNonEmptyString(x: unknown): x is string {
  return typeof x === 'string' && x.length > 0;
}

function validateRef(raw: unknown, label: string): Result<EntityRef> {
  if (typeof raw !== 'object' || raw === null) {
    return fail('invalid_input', `${label} must be an entity reference`);
  }
  const r = raw as Record<string, unknown>;
  if (r['entity_id'] !== undefined) {
    return isNonEmptyString(r['entity_id'])
      ? ok({ entity_id: r['entity_id'] })
      : fail('invalid_input', `${label}.entity_id must be a non-empty string`);
  }
  const type = r['type'];
  const name = r['name'];
  if (typeof type !== 'string' || !(ENTITY_TYPES as readonly string[]).includes(type) || typeof name !== 'string') {
    return fail('invalid_input', `${label} must be {entity_id} or {type, name} with a known entity type`);
  }
  if (aliasKey(type as EntityType, name) === null) {
    return fail('invalid_input', `${label}.name has no letters or digits`);
  }
  const attrs = r['attrs'];
  if (attrs !== undefined && (typeof attrs !== 'object' || attrs === null || Array.isArray(attrs))) {
    return fail('invalid_input', `${label}.attrs must be a plain object`);
  }
  return ok({
    type: type as EntityType,
    name: collapse(name),
    ...(attrs === undefined ? {} : { attrs: attrs as Readonly<Record<string, unknown>> }),
  });
}

function validateProposal(p: FactProposal): Result<CheckedProposal> {
  if (typeof p !== 'object' || p === null) return fail('invalid_input', 'proposal must be an object');
  if (!isNonEmptyString(p.household_id)) return fail('invalid_input', 'household_id is required');
  if (!isNonEmptyString(p.source_episode_id)) return fail('invalid_input', 'source_episode_id is required');
  if (!isNonEmptyString(p.predicate)) return fail('invalid_input', 'predicate is required');
  if (typeof p.confidence !== 'number' || !Number.isFinite(p.confidence) || p.confidence < 0 || p.confidence > 1) {
    return fail('invalid_input', 'confidence must be a number between 0 and 1');
  }

  const subject = validateRef(p.subject, 'subject');
  if (!subject.ok) return subject;
  let object: EntityRef | null = null;
  if (p.object !== undefined && p.object !== null) {
    const o = validateRef(p.object, 'object');
    if (!o.ok) return o;
    object = o.value;
  }

  if (!Array.isArray(p.quotes) || p.quotes.length === 0) {
    return fail('no_quotes', 'at least one verbatim quote is required');
  }
  const quotes: QuoteProposal[] = [];
  const seenStarts = new Set<number>();
  for (const q of p.quotes as readonly QuoteProposal[]) {
    if (
      typeof q !== 'object' ||
      q === null ||
      typeof q.quote !== 'string' ||
      !Number.isInteger(q.char_start) ||
      !Number.isInteger(q.char_end) ||
      q.char_start < 0 ||
      q.char_end <= q.char_start
    ) {
      return fail('invalid_input', 'each quote needs a string and integer 0 <= char_start < char_end');
    }
    if (q.quote.trim().length < MIN_QUOTE_CHARS || q.quote.length > MAX_QUOTE_CHARS) {
      return fail('invalid_input', `quote length must be ${MIN_QUOTE_CHARS}..${MAX_QUOTE_CHARS} characters`);
    }
    if (!seenStarts.has(q.char_start)) {
      seenStarts.add(q.char_start);
      quotes.push({ quote: q.quote, char_start: q.char_start, char_end: q.char_end });
    }
  }

  const optionalText = (v: unknown, name: string): Result<string | null> =>
    v === undefined || v === null
      ? ok(null)
      : typeof v === 'string'
        ? ok(v)
        : fail('invalid_input', `${name} must be a string`);
  const optionalNumber = (v: unknown, name: string): Result<number | null> =>
    v === undefined || v === null
      ? ok(null)
      : typeof v === 'number' && Number.isFinite(v)
        ? ok(v)
        : fail('invalid_input', `${name} must be a finite number`);

  const text = optionalText(p.value_text, 'value_text');
  if (!text.ok) return text;
  const num = optionalNumber(p.value_num, 'value_num');
  if (!num.ok) return num;
  const cents = optionalNumber(p.value_cents, 'value_cents');
  if (!cents.ok) return cents;
  const currency = optionalText(p.currency, 'currency');
  if (!currency.ok) return currency;

  const validFrom = optionalText(p.valid_from, 'valid_from');
  if (!validFrom.ok) return validFrom;
  const validTo = optionalText(p.valid_to, 'valid_to');
  if (!validTo.ok) return validTo;
  let from: IsoTimestamp | null = null;
  let to: IsoTimestamp | null = null;
  if (validFrom.value !== null && validFrom.value !== '') {
    from = toIsoTimestamp(validFrom.value);
    if (from === null) return fail('invalid_input', 'valid_from is not a valid ISO-8601 date or time');
  }
  if (validTo.value !== null && validTo.value !== '') {
    to = toIsoTimestamp(validTo.value);
    if (to === null) return fail('invalid_input', 'valid_to is not a valid ISO-8601 date or time');
  }
  if (from !== null && to !== null && to <= from) {
    return fail('invalid_input', 'valid_to must be later than valid_from');
  }

  return ok({
    household_id: p.household_id,
    source_episode_id: p.source_episode_id,
    predicate: p.predicate,
    subject: subject.value,
    object,
    value_text: text.value,
    value_num: num.value,
    value_cents: cents.value,
    currency: currency.value,
    valid_from: from,
    valid_to: to,
    confidence: p.confidence,
    quotes,
  });
}

function present(v: string | number | null | undefined): boolean {
  return v !== null && v !== undefined && v !== '';
}

function normalizeDate(s: string): string | null {
  const m = DATE_ONLY.exec(s);
  return m !== null && isRealDate(Number(m[1]), Number(m[2]), Number(m[3])) ? s : null;
}

function normalizeIntervalPart(s: string): string | null {
  return normalizeDate(s) ?? toIsoTimestamp(s);
}

/** Checks the value fields against the predicate's value_type and returns the canonical form. */
function normalizeValue(valueType: ValueType, p: CheckedProposal): Result<ValueDraft> {
  const blank: ValueDraft = { object: null, value_text: null, value_num: null, value_cents: null, currency: null };
  const stray = (...fields: Array<[string, string | number | null]>): string | null =>
    fields.find(([, v]) => present(v))?.[0] ?? null;
  const text = p.value_text === null ? null : collapse(p.value_text);

  switch (valueType) {
    case 'entity': {
      if (p.object === null) return fail('invalid_value', 'this predicate needs an object entity');
      const extra = stray(['value_text', text], ['value_num', p.value_num], ['value_cents', p.value_cents]);
      return extra === null ? ok({ ...blank, object: p.object }) : fail('invalid_value', `${extra} is not allowed for an entity predicate`);
    }
    case 'text': {
      if (p.object !== null) return fail('invalid_value', 'object is not allowed for a text predicate');
      const extra = stray(['value_num', p.value_num], ['value_cents', p.value_cents]);
      if (extra !== null) return fail('invalid_value', `${extra} is not allowed for a text predicate`);
      if (text === null || text === '') return fail('invalid_value', 'value_text is required');
      if (text.length > MAX_TEXT_CHARS) return fail('invalid_value', `value_text exceeds ${MAX_TEXT_CHARS} characters`);
      return ok({ ...blank, value_text: text });
    }
    case 'num': {
      if (p.object !== null) return fail('invalid_value', 'object is not allowed for a num predicate');
      const extra = stray(['value_text', text], ['value_cents', p.value_cents]);
      if (extra !== null) return fail('invalid_value', `${extra} is not allowed for a num predicate`);
      if (p.value_num === null) return fail('invalid_value', 'value_num is required');
      return ok({ ...blank, value_num: p.value_num });
    }
    case 'date': {
      if (p.object !== null) return fail('invalid_value', 'object is not allowed for a date predicate');
      const extra = stray(['value_num', p.value_num], ['value_cents', p.value_cents]);
      if (extra !== null) return fail('invalid_value', `${extra} is not allowed for a date predicate`);
      const date = text === null ? null : normalizeDate(text);
      return date === null ? fail('invalid_value', "value_text must be a real calendar date 'YYYY-MM-DD'") : ok({ ...blank, value_text: date });
    }
    case 'money': {
      if (p.object !== null) return fail('invalid_value', 'object is not allowed for a money predicate');
      const extra = stray(['value_text', text], ['value_num', p.value_num]);
      if (extra !== null) return fail('invalid_value', `${extra} is not allowed for a money predicate`);
      if (p.value_cents === null || !Number.isSafeInteger(p.value_cents)) {
        return fail('invalid_value', 'value_cents must be an integer number of cents');
      }
      const currency = (p.currency === null || p.currency === '' ? 'USD' : p.currency).toUpperCase();
      if (!/^[A-Z]{3}$/.test(currency)) return fail('invalid_value', 'currency must be a 3-letter ISO 4217 code');
      return ok({ ...blank, value_cents: p.value_cents, currency });
    }
    case 'interval': {
      if (p.object !== null) return fail('invalid_value', 'object is not allowed for an interval predicate');
      const extra = stray(['value_num', p.value_num], ['value_cents', p.value_cents]);
      if (extra !== null) return fail('invalid_value', `${extra} is not allowed for an interval predicate`);
      const parts = (text ?? '').split('/');
      const start = parts.length === 2 ? normalizeIntervalPart(parts[0] ?? '') : null;
      const end = parts.length === 2 ? normalizeIntervalPart(parts[1] ?? '') : null;
      if (start === null || end === null) {
        return fail('invalid_value', "value_text must be '<start>/<end>' with ISO dates or date-times");
      }
      if ((toIsoTimestamp(start) ?? '') > (toIsoTimestamp(end) ?? '')) {
        return fail('invalid_value', 'interval start must not be after its end');
      }
      return ok({ ...blank, value_text: `${start}/${end}` });
    }
  }
}

// ─── persistence helpers ────────────────────────────────────────────────────

type EpisodeForWrite = Pick<
  EpisodeRow,
  'episode_id' | 'household_id' | 'redaction_status' | 'redacted_text' | 'occurred_at' | 'received_at'
>;

interface PendingEntity {
  entity_id: string;
  type: EntityType;
  canonical_name: string;
  attrs_json: string;
}

interface PendingAlias {
  alias_norm: string;
  entity_id: string;
  method: 'deterministic' | 'llm_adjudicated';
  confidence: number;
}

interface Staging {
  memo: Map<string, string>;
  entities: PendingEntity[];
  aliases: PendingAlias[];
}

interface Incumbent {
  fact_id: string;
  object_id: string | null;
  value_text: string | null;
  value_num: number | null;
  value_cents: number | null;
  currency: string | null;
  valid_from: IsoTimestamp;
  valid_to: IsoTimestamp | null;
  learned_at: IsoTimestamp;
  evidence_at: string;
}

interface Resolved {
  household_id: string;
  predicate: PredicateRegistryRow;
  episode: EpisodeForWrite;
  value: ValueDraft;
  confidence: number;
  quotes: readonly QuoteProposal[];
  validFrom: IsoTimestamp;
  validTo: IsoTimestamp | null;
  evidenceAt: IsoTimestamp;
}

interface Attempt {
  statements: D1PreparedStatement[];
  result: WriteFactResult;
}

function defaultNewId(prefix: 'fact' | 'ent' | 'aud'): string {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '')}`;
}

function errorText(err: unknown): string {
  if (!(err instanceof Error)) return String(err);
  const cause = (err as { cause?: unknown }).cause;
  return cause === undefined ? err.message : `${err.message} | ${errorText(cause)}`;
}

/** Lost a race: our guard tripped, or another writer created the same alias first. */
function isConcurrencyAbort(err: unknown): boolean {
  const text = errorText(err);
  return (
    /NOT NULL constraint failed: audit_log\.actor/.test(text) ||
    /UNIQUE constraint failed: entity_aliases\./.test(text)
  );
}

/** Follows entities.merged_into (depth-limited) to the canonical entity inside the household. */
async function canonicalEntity(
  db: D1Database,
  household_id: string,
  entity_id: string,
): Promise<{ entity_id: string; type: EntityType } | null> {
  return db
    .prepare(
      `WITH RECURSIVE chain(entity_id, type, merged_into, depth) AS (
         SELECT entity_id, type, merged_into, 0 FROM entities WHERE household_id = ?1 AND entity_id = ?2
         UNION ALL
         SELECT e.entity_id, e.type, e.merged_into, c.depth + 1
           FROM entities e JOIN chain c ON e.entity_id = c.merged_into
          WHERE e.household_id = ?1 AND c.depth < 8
       )
       SELECT entity_id, type FROM chain WHERE merged_into IS NULL ORDER BY depth DESC LIMIT 1`,
    )
    .bind(household_id, entity_id)
    .first<{ entity_id: string; type: EntityType }>();
}

async function findCandidates(
  db: D1Database,
  household_id: string,
  type: EntityType,
  name: string,
): Promise<EntityCandidate[]> {
  const wanted = normalizeName(name);
  const found: EntityCandidate[] = [];
  for (let page = 0; page < CANDIDATE_MAX_PAGES; page++) {
    const { results } = await db
      .prepare(
        `SELECT entity_id, canonical_name FROM entities
          WHERE household_id = ? AND type = ? AND merged_into IS NULL
          ORDER BY entity_id LIMIT ? OFFSET ?`,
      )
      .bind(household_id, type, CANDIDATE_PAGE_SIZE, page * CANDIDATE_PAGE_SIZE)
      .all<{ entity_id: string; canonical_name: string }>();
    for (const row of results) {
      const score = similarity(wanted, normalizeName(row.canonical_name));
      if (score >= ADJUDICATION_MIN_SIMILARITY) {
        found.push({ entity_id: row.entity_id, canonical_name: row.canonical_name, similarity: score });
      }
    }
    if (results.length < CANDIDATE_PAGE_SIZE) break;
  }
  found.sort((a, b) => b.similarity - a.similarity || a.entity_id.localeCompare(b.entity_id));
  return found.slice(0, ADJUDICATION_MAX_CANDIDATES);
}

/** alias -> adjudication -> new entity. Writes nothing; stages creations for the batch. */
async function resolveRef(
  db: D1Database,
  household_id: string,
  ref: EntityRef,
  stage: Staging,
  options: { newId: NonNullable<WriteFactOptions['newId']>; adjudicate: EntityAdjudicator | undefined },
): Promise<Result<string>> {
  if ('entity_id' in ref) {
    const row = await canonicalEntity(db, household_id, ref.entity_id);
    return row === null
      ? fail('entity_not_found', `entity ${ref.entity_id} does not exist in this household`)
      : ok(row.entity_id);
  }

  const key = aliasKey(ref.type, ref.name);
  if (key === null) return fail('invalid_input', 'entity name has no letters or digits');
  const memoized = stage.memo.get(key);
  if (memoized !== undefined) return ok(memoized);

  const alias = await db
    .prepare('SELECT entity_id FROM entity_aliases WHERE household_id = ? AND alias_norm = ?')
    .bind(household_id, key)
    .first<{ entity_id: string }>();
  if (alias !== null) {
    const row = await canonicalEntity(db, household_id, alias.entity_id);
    if (row === null) return fail('entity_not_found', `alias ${key} points at a missing entity`);
    if (row.type !== ref.type) {
      return fail('entity_type_mismatch', `alias ${key} resolves to a ${row.type}, expected ${ref.type}`);
    }
    stage.memo.set(key, row.entity_id);
    return ok(row.entity_id);
  }

  if (options.adjudicate !== undefined) {
    const candidates = await findCandidates(db, household_id, ref.type, ref.name);
    if (candidates.length > 0) {
      const decision = await options.adjudicate({ household_id, type: ref.type, name: ref.name, candidates });
      if (decision !== null && candidates.some((c) => c.entity_id === decision.entity_id)) {
        const row = await canonicalEntity(db, household_id, decision.entity_id);
        if (row !== null && row.type === ref.type) {
          stage.aliases.push({
            alias_norm: key,
            entity_id: row.entity_id,
            method: 'llm_adjudicated',
            confidence: Math.min(1, Math.max(0, Number.isFinite(decision.confidence) ? decision.confidence : 0)),
          });
          stage.memo.set(key, row.entity_id);
          return ok(row.entity_id);
        }
      }
    }
  }

  const entity_id = options.newId('ent');
  stage.entities.push({
    entity_id,
    type: ref.type,
    canonical_name: ref.name,
    attrs_json: JSON.stringify(ref.attrs ?? {}),
  });
  stage.aliases.push({ alias_norm: key, entity_id, method: 'deterministic', confidence: 1 });
  stage.memo.set(key, entity_id);
  return ok(entity_id);
}

interface ValueDraftIds {
  object_id: string | null;
  value_text: string | null;
  value_num: number | null;
  value_cents: number | null;
  currency: string | null;
}

/** Value equality for facts: entity ids, case-insensitive text, numbers, and cents (+ currency). */
function sameValue(a: ValueDraftIds, b: ValueDraftIds): boolean {
  return (
    a.object_id === b.object_id &&
    (a.value_text ?? '').toLowerCase() === (b.value_text ?? '').toLowerCase() &&
    a.value_num === b.value_num &&
    a.value_cents === b.value_cents &&
    (a.value_cents === null || (a.currency ?? 'USD') === (b.currency ?? 'USD'))
  );
}

function evidenceTimestamp(raw: string): string {
  return toIsoTimestamp(raw) ?? raw;
}

function fingerprint(rows: readonly Incumbent[]): string {
  let newest = '';
  let open = 0;
  for (const r of rows) {
    if (r.learned_at > newest) newest = r.learned_at;
    if (r.valid_to === null) open += 1;
  }
  return `${rows.length}:${newest}:${open}`;
}

/**
 * One plan: resolve entities, read the believed facts for this key, decide reaffirm / insert / close, and
 * assemble the statements of the single atomic batch. Nothing is written here.
 */
async function plan(
  db: D1Database,
  r: Resolved,
  proposal: CheckedProposal,
  now: IsoTimestamp,
  options: { newId: NonNullable<WriteFactOptions['newId']>; adjudicate: EntityAdjudicator | undefined },
): Promise<Result<Attempt>> {
  const stage: Staging = { memo: new Map(), entities: [], aliases: [] };

  const subject = await resolveRef(db, r.household_id, proposal.subject, stage, options);
  if (!subject.ok) return subject;
  let objectId: string | null = null;
  if (r.value.object !== null) {
    const object = await resolveRef(db, r.household_id, r.value.object, stage, options);
    if (!object.ok) return object;
    objectId = object.value;
  }

  const draft: ValueDraftIds = {
    object_id: objectId,
    value_text: r.value.value_text,
    value_num: r.value.value_num,
    value_cents: r.value.value_cents,
    currency: r.value.currency,
  };

  // The set of believed facts this decision depends on (also the scope of the stale-state guard).
  const manyScope = r.predicate.cardinality === 'many';
  const where =
    `f.household_id = ? AND f.subject_id = ? AND f.predicate = ? AND f.expired_at IS NULL` +
    (manyScope
      ? ` AND f.object_id IS ? AND lower(f.value_text) IS lower(?) AND f.value_num IS ? AND f.value_cents IS ?`
      : '');
  const whereParams: Array<string | number | null> = [r.household_id, subject.value, r.predicate.predicate];
  if (manyScope) whereParams.push(draft.object_id, draft.value_text, draft.value_num, draft.value_cents);

  const { results } = await db
    .prepare(
      `SELECT f.fact_id, f.object_id, f.value_text, f.value_num, f.value_cents, f.currency, f.valid_from, f.valid_to,
              f.learned_at, COALESCE(e.occurred_at, e.received_at) AS evidence_at
         FROM facts f JOIN episodes e ON e.episode_id = f.source_episode_id
        WHERE ${where}
        ORDER BY f.valid_from, f.fact_id`,
    )
    .bind(...whereParams)
    .all<Incumbent>();
  const believed = results;

  const statements: D1PreparedStatement[] = [];
  const guard = db
    .prepare(
      // Inserts a NULL into a NOT NULL column (=> aborts the whole transaction) only when the facts read
      // above are no longer in the state we read them in; otherwise it selects zero rows.
      `INSERT INTO audit_log (id, household_id, actor, action)
       SELECT ?, ?, NULL, 'writeFact.guard'
        WHERE (SELECT COUNT(*) || ':' || COALESCE(MAX(f.learned_at), '') || ':' || COALESCE(SUM(f.valid_to IS NULL), 0)
                 FROM facts f WHERE ${where}) <> ?`,
    )
    .bind(options.newId('aud'), r.household_id, ...whereParams, fingerprint(believed));
  statements.push(guard);

  for (const e of stage.entities) {
    statements.push(
      db
        .prepare(
          `INSERT INTO entities (entity_id, household_id, type, canonical_name, attrs_json, created_at)
           VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .bind(e.entity_id, r.household_id, e.type, e.canonical_name, e.attrs_json, now),
    );
  }
  for (const a of stage.aliases) {
    statements.push(
      db
        .prepare(
          `INSERT INTO entity_aliases (household_id, alias_norm, entity_id, method, confidence)
           VALUES (?, ?, ?, ?, ?)`,
        )
        .bind(r.household_id, a.alias_norm, a.entity_id, a.method, a.confidence),
    );
  }

  const insertQuotes = (factId: string, orIgnore: boolean): void => {
    for (const q of r.quotes) {
      statements.push(
        db
          .prepare(
            `INSERT ${orIgnore ? 'OR IGNORE ' : ''}INTO fact_quotes (fact_id, episode_id, quote, char_start, char_end)
             VALUES (?, ?, ?, ?, ?)`,
          )
          .bind(factId, r.episode.episode_id, q.quote, q.char_start, q.char_end),
      );
    }
  };

  // ── decide ──
  let from = r.validFrom;
  let to = r.validTo;
  const differing = (): Incumbent[] =>
    believed.filter((b) => !sameValue(draft, b) && overlaps(from, to, b.valid_from, b.valid_to));

  if (r.predicate.cardinality === 'one') {
    // Older evidence must never override newer evidence: clamp such a fact to end where the newer incumbent begins.
    for (;;) {
      const stale = differing().filter(
        (b) => from <= b.valid_from && r.evidenceAt < evidenceTimestamp(b.evidence_at),
      );
      if (stale.length === 0) break;
      const clampTo = stale.reduce((min, b) => (b.valid_from < min ? b.valid_from : min), FOREVER);
      if (clampTo <= from) {
        return fail('stale_evidence', 'a fact from newer evidence already covers this period with a different value');
      }
      to = clampTo;
    }
  }

  const conflicts = r.predicate.cardinality === 'one' ? differing() : [];
  const equal = believed.find((b) => sameValue(draft, b) && overlaps(from, to, b.valid_from, b.valid_to));

  if (equal !== undefined && conflicts.length === 0) {
    insertQuotes(equal.fact_id, true);
    return ok({ statements, result: { status: 'reaffirmed', fact_id: equal.fact_id } });
  }

  const factId = options.newId('fact');
  const superseded: FactSupersession[] = [];
  const closures: D1PreparedStatement[] = [];
  const audits: D1PreparedStatement[] = [];
  for (const old of conflicts) {
    const covers = to === null || (old.valid_to !== null && to >= old.valid_to);
    if (!covers) {
      return fail(
        'partial_overlap',
        `the new fact ends before fact ${old.fact_id} does; splitting a fact's validity is not supported`,
      );
    }
    const kind = from > old.valid_from ? 'world_change' : 'correction';
    superseded.push({ fact_id: old.fact_id, kind });
    closures.push(
      kind === 'world_change'
        ? db
            .prepare(
              `UPDATE facts SET valid_to = ? WHERE fact_id = ? AND household_id = ? AND expired_at IS NULL`,
            )
            .bind(from, old.fact_id, r.household_id)
        : db
            .prepare(
              `UPDATE facts SET expired_at = ?, superseded_by = ?
                WHERE fact_id = ? AND household_id = ? AND expired_at IS NULL`,
            )
            .bind(now, factId, old.fact_id, r.household_id),
    );
    audits.push(
      db
        .prepare(
          `INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .bind(
          options.newId('aud'),
          r.household_id,
          ACTOR,
          kind === 'world_change' ? 'fact_world_change' : 'fact_correction',
          JSON.stringify({
            predicate: r.predicate.predicate,
            subject_id: subject.value,
            old_fact_id: old.fact_id,
            new_fact_id: factId,
          }),
          now,
        ),
    );
  }

  statements.push(
    db
      .prepare(
        `INSERT INTO facts (fact_id, household_id, subject_id, predicate, object_id, value_text, value_num,
                            value_cents, currency, valid_from, valid_to, learned_at, expired_at, superseded_by,
                            confidence, source_episode_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)`,
      )
      .bind(
        factId,
        r.household_id,
        subject.value,
        r.predicate.predicate,
        draft.object_id,
        draft.value_text,
        draft.value_num,
        draft.value_cents,
        draft.currency,
        from,
        to,
        now,
        r.confidence,
        r.episode.episode_id,
      ),
  );
  statements.push(...closures); // after the INSERT: superseded_by references the new fact
  insertQuotes(factId, false);
  statements.push(...audits);

  return ok({
    statements,
    result: {
      status: 'written',
      fact_id: factId,
      learned_at: now,
      valid_from: from,
      valid_to: to,
      superseded,
      created_entity_ids: stage.entities.map((e) => e.entity_id),
    },
  });
}

// ─── public API ─────────────────────────────────────────────────────────────

/**
 * Validates one proposed fact and, if it grounds, writes it atomically. See the file header for the rules.
 *
 * @param db the D1 database of the household's shard
 */
export async function writeFact(
  db: D1Database,
  proposal: FactProposal,
  options: WriteFactOptions = {},
): Promise<WriteFactResult> {
  const now = (options.now ?? (() => new Date()))().toISOString();
  const planOptions = { newId: options.newId ?? defaultNewId, adjudicate: options.adjudicate };
  const maxAttempts = Math.max(1, options.maxAttempts ?? 4);

  // 1. Shape.
  const checked = validateProposal(proposal);
  if (!checked.ok) return toRejection(checked);
  const p = checked.value;

  // 2. Episode: exists in this household, and is safe (redacted) text.
  const episode = await db
    .prepare(
      `SELECT episode_id, household_id, redaction_status, redacted_text, occurred_at, received_at
         FROM episodes WHERE episode_id = ? AND household_id = ?`,
    )
    .bind(p.source_episode_id, p.household_id)
    .first<EpisodeForWrite>();
  if (episode === null) {
    return toRejection(fail('episode_not_found', `episode ${p.source_episode_id} not found in this household`));
  }
  const text = episode.redacted_text;
  if ((episode.redaction_status !== 'clean' && episode.redaction_status !== 'redacted') || text === null) {
    return toRejection(
      fail('episode_not_redacted', `episode is ${episode.redaction_status}; only clean or redacted text may ground facts`),
    );
  }

  // 3. Verbatim quote check: the stored text, not the caller's, is the authority.
  for (const q of p.quotes) {
    if (text.slice(q.char_start, q.char_end) !== q.quote) {
      return toRejection(
        fail('quote_mismatch', `quote at [${q.char_start}, ${q.char_end}) is not a verbatim substring of the episode`),
      );
    }
  }

  // 4. Predicate + value shape.
  const predicate = await db
    .prepare('SELECT predicate, cardinality, value_type, decay_days, domain FROM predicate_registry WHERE predicate = ?')
    .bind(p.predicate)
    .first<PredicateRegistryRow>();
  if (predicate === null) return toRejection(fail('unknown_predicate', `predicate '${p.predicate}' is not registered`));
  const value = normalizeValue(predicate.value_type, p);
  if (!value.ok) return toRejection(value);

  const occurredAt = episode.occurred_at === null ? null : toIsoTimestamp(episode.occurred_at);
  const receivedAt = toIsoTimestamp(episode.received_at) ?? now;
  const evidenceAt = occurredAt ?? receivedAt;
  const resolved: Resolved = {
    household_id: p.household_id,
    predicate,
    episode,
    value: value.value,
    confidence: p.confidence,
    quotes: p.quotes,
    validFrom: p.valid_from ?? evidenceAt,
    validTo: p.valid_to,
    evidenceAt,
  };
  if (resolved.validTo !== null && resolved.validTo <= resolved.validFrom) {
    return toRejection(fail('invalid_input', 'valid_to must be later than valid_from (default valid_from is the episode time)'));
  }

  // 5. Plan + atomic commit, re-planning if a concurrent writer got there first.
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const planned = await plan(db, resolved, p, now, planOptions);
    if (!planned.ok) return toRejection(planned);
    try {
      await db.batch(planned.value.statements);
      return planned.value.result;
    } catch (err) {
      if (!isConcurrencyAbort(err)) throw err;
    }
  }
  return toRejection(
    fail('conflict_retry_exhausted', `facts for this subject/predicate kept changing; gave up after ${maxAttempts} attempts`),
  );
}
