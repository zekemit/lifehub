/**
 * src/ingest/spend.ts — the spend engine behind IngestFileWF. Everything here is deterministic except
 * categorizeUnknown(), the single place a model is consulted (worker model, unknown merchants only).
 *
 *   resolve merchants   alias lookup (exact, then longest descriptor prefix) -> Monarch's Merchant column ->
 *                       model batch categorization -> merchant entity + alias (never a model write to the graph:
 *                       names and envelope keys are validated here first)
 *   insert              INSERT OR IGNORE keyed on (household_id, posted_on, amount_cents, descriptor_norm); txn_id is a
 *                       hash of that key, so re-importing a file changes nothing
 *   envelopes           monthly_target_cents = median of the 3 preceding calendar months' spend per envelope
 *   credit periods      used_cents recomputed from the transactions table (never incremented), so it is idempotent
 *   recurring           >= 2 charges, each within +/-10% of the previous, spaced at a known cadence +/- tolerance
 *
 * D1 budget: all bulk writes are multi-row statements (<= 90 bound parameters), so a 500-row step costs ~150 queries.
 */

import { sha256Hex } from '../crypto/vault';
import { aliasKey } from '../graph/writeFact';
import type { Gateway } from '../llm/gateway';
import type { BenefitCatalogRow, FactProposal } from '../types';
import { categoryToEnvelope, lastFourOf, type ParsedTxn } from './csv';

export const ROWS_PER_STEP = 500;
const MAX_BOUND_PARAMS = 90;
const LLM_DESCRIPTORS_PER_CALL = 150;

export const ENVELOPE_KEYS = [
  'dining',
  'groceries',
  'travel',
  'transportation',
  'shopping',
  'entertainment',
  'subscriptions',
  'utilities',
  'health',
  'home',
  'personal_care',
  'education',
  'gifts',
  'fees',
  'uncategorized',
] as const;

// ─── SQL helpers ────────────────────────────────────────────────────────────

/** Builds multi-row INSERT statements, each within the bound-parameter budget. `rowTemplate` e.g. "(?, ?, 'x', ?)". */
export function multiRowStatements(
  db: D1Database,
  head: string,
  rowTemplate: string,
  rows: ReadonlyArray<ReadonlyArray<string | number | null>>,
  tail = '',
): D1PreparedStatement[] {
  const perRow = (rowTemplate.match(/\?/g) ?? []).length;
  const perStatement = Math.max(1, Math.floor(MAX_BOUND_PARAMS / perRow));
  const out: D1PreparedStatement[] = [];
  for (let i = 0; i < rows.length; i += perStatement) {
    const group = rows.slice(i, i + perStatement);
    out.push(
      db
        .prepare(`${head} VALUES ${group.map(() => rowTemplate).join(', ')} ${tail}`.trim())
        .bind(...group.flat()),
    );
  }
  return out;
}

async function inChunks<T, R>(items: readonly T[], size: number, fn: (group: T[]) => Promise<R[]>): Promise<R[]> {
  const out: R[] = [];
  for (let i = 0; i < items.length; i += size) out.push(...(await fn(items.slice(i, i + size))));
  return out;
}

const placeholders = (n: number): string => Array.from({ length: n }, () => '?').join(', ');

function daysBetween(a: string, b: string): number {
  return Math.round((Date.parse(`${b}T00:00:00Z`) - Date.parse(`${a}T00:00:00Z`)) / 86_400_000);
}

const ymd = (d: Date): string => d.toISOString().slice(0, 10);

// ─── merchants ──────────────────────────────────────────────────────────────

/** alias_norm -> entity_id for every merchant alias in the household. */
export type MerchantIndex = Map<string, string>;

export async function loadMerchantAliases(db: D1Database, household_id: string): Promise<MerchantIndex> {
  const index: MerchantIndex = new Map();
  for (let offset = 0; ; offset += 500) {
    const { results } = await db
      .prepare(
        `SELECT alias_norm, entity_id FROM entity_aliases
          WHERE household_id = ? AND alias_norm LIKE 'merchant:%' ORDER BY alias_norm LIMIT 500 OFFSET ?`,
      )
      .bind(household_id, offset)
      .all<{ alias_norm: string; entity_id: string }>();
    for (const r of results) index.set(r.alias_norm, r.entity_id);
    if (results.length < 500) return index;
  }
}

/** Exact alias on Monarch's merchant name, then the longest descriptor prefix (up to 4 words) that is a known alias. */
export function resolveMerchant(index: MerchantIndex, row: ParsedTxn): string | null {
  if (row.merchant_hint !== null) {
    const key = aliasKey('merchant', row.merchant_hint);
    const hit = key === null ? undefined : index.get(key);
    if (hit !== undefined) return hit;
  }
  const words = row.descriptor_base.split(' ');
  for (let n = Math.min(words.length, 4); n >= 1; n--) {
    const key = aliasKey('merchant', words.slice(0, n).join(' '));
    const hit = key === null ? undefined : index.get(key);
    if (hit !== undefined) return hit;
  }
  return null;
}

interface NewMerchant {
  name: string;
  envelope_key: string | null;
  extraAliasKeys: string[];
  method: 'deterministic' | 'llm_adjudicated';
  confidence: number;
}

/** Creates merchant entities + aliases idempotently (deterministic ids, INSERT OR IGNORE) and refreshes `index`. */
async function createMerchants(
  db: D1Database,
  household_id: string,
  merchants: readonly NewMerchant[],
  index: MerchantIndex,
  now: string,
): Promise<void> {
  const entityRows: Array<Array<string | number | null>> = [];
  const aliasRows: Array<Array<string | number | null>> = [];
  const wanted = new Set<string>();
  for (const m of merchants) {
    const key = aliasKey('merchant', m.name);
    if (key === null) continue;
    const id = `ent_${(await sha256Hex(`${household_id}|${key}`)).slice(0, 32)}`;
    entityRows.push([id, household_id, m.name.trim().slice(0, 80), JSON.stringify(m.envelope_key === null ? {} : { envelope_key: m.envelope_key }), now]);
    for (const alias of new Set([key, ...m.extraAliasKeys])) {
      aliasRows.push([household_id, alias, id, m.method, m.confidence]);
      wanted.add(alias);
    }
  }
  const statements = [
    ...multiRowStatements(
      db,
      `INSERT OR IGNORE INTO entities (entity_id, household_id, type, canonical_name, attrs_json, created_at)`,
      `(?, ?, 'merchant', ?, ?, ?)`,
      entityRows,
    ),
    ...multiRowStatements(
      db,
      `INSERT OR IGNORE INTO entity_aliases (household_id, alias_norm, entity_id, method, confidence)`,
      `(?, ?, ?, ?, ?)`,
      aliasRows,
    ),
  ];
  if (statements.length > 0) await db.batch(statements);
  const rows = await inChunks([...wanted], MAX_BOUND_PARAMS - 1, async (group) => {
    const { results } = await db
      .prepare(`SELECT alias_norm, entity_id FROM entity_aliases WHERE household_id = ? AND alias_norm IN (${placeholders(group.length)})`)
      .bind(household_id, ...group)
      .all<{ alias_norm: string; entity_id: string }>();
    return results;
  });
  for (const r of rows) index.set(r.alias_norm, r.entity_id);
}

export interface Categorization {
  merchant_name: string;
  envelope_key: string;
}

/** The one model call in the spend engine. Returns validated results keyed by the descriptor that was sent. */
export async function categorizeUnknown(
  deps: { gateway: Gateway; appName: string },
  household_id: string,
  descriptors: readonly string[],
): Promise<Map<string, Categorization>> {
  const out = new Map<string, Categorization>();
  for (let i = 0; i < descriptors.length; i += LLM_DESCRIPTORS_PER_CALL) {
    const group = descriptors.slice(i, i + LLM_DESCRIPTORS_PER_CALL);
    const result = await deps.gateway.worker({
      household_id,
      purpose: 'spend.categorize',
      system: [
        `You label bank and card transaction descriptors for ${deps.appName}, a household budgeting assistant.`,
        'For each descriptor return the merchant\'s everyday name and exactly one spending category.',
        `Allowed categories: ${ENVELOPE_KEYS.join(', ')}. When unsure use "uncategorized".`,
        'Return one item per descriptor, repeating the descriptor exactly as given.',
      ].join('\n'),
      user: group.map((d, n) => `${n + 1}. ${d}`).join('\n'),
      json_schema: {
        name: 'merchant_labels',
        schema: {
          type: 'object',
          additionalProperties: false,
          required: ['items'],
          properties: {
            items: {
              type: 'array',
              items: {
                type: 'object',
                additionalProperties: false,
                required: ['descriptor', 'merchant_name', 'envelope_key'],
                properties: {
                  descriptor: { type: 'string' },
                  merchant_name: { type: 'string' },
                  envelope_key: { type: 'string', enum: [...ENVELOPE_KEYS] },
                },
              },
            },
          },
        },
      },
      max_output_tokens: 8192,
    });
    let items: unknown;
    try {
      items = (JSON.parse(result.text) as { items?: unknown }).items;
    } catch {
      continue;
    }
    if (!Array.isArray(items)) continue;
    const sent = new Set(group);
    for (const item of items) {
      const r = item as Record<string, unknown>;
      const descriptor = typeof r['descriptor'] === 'string' ? r['descriptor'] : '';
      const name = typeof r['merchant_name'] === 'string' ? r['merchant_name'].trim().slice(0, 80) : '';
      if (!sent.has(descriptor) || aliasKey('merchant', name) === null) continue;
      const envelope = (ENVELOPE_KEYS as readonly string[]).includes(String(r['envelope_key'])) ? String(r['envelope_key']) : 'uncategorized';
      out.set(descriptor, { merchant_name: name, envelope_key: envelope });
    }
  }
  return out;
}

export interface SpendDeps {
  db: D1Database;
  gateway: Gateway;
  appName: string;
  now: () => Date;
}

/** Model step for one batch: label descriptors no alias knows, then create their merchants. */
export async function categorizeBatch(
  deps: SpendDeps,
  household_id: string,
  rows: readonly ParsedTxn[],
): Promise<{ unknown: number; created: number }> {
  const index = await loadMerchantAliases(deps.db, household_id);
  const unknown = [
    ...new Set(rows.filter((r) => r.merchant_hint === null && resolveMerchant(index, r) === null).map((r) => r.descriptor_base)),
  ];
  if (unknown.length === 0) return { unknown: 0, created: 0 };
  const labels = await categorizeUnknown(deps, household_id, unknown);
  const byMerchant = new Map<string, NewMerchant>();
  for (const [descriptor, label] of labels) {
    const key = aliasKey('merchant', label.merchant_name);
    if (key === null) continue;
    const existing = byMerchant.get(key);
    const descriptorKey = aliasKey('merchant', descriptor);
    if (existing === undefined) {
      byMerchant.set(key, {
        name: label.merchant_name,
        envelope_key: label.envelope_key,
        extraAliasKeys: descriptorKey === null ? [] : [descriptorKey],
        method: 'llm_adjudicated',
        confidence: 0.8,
      });
    } else if (descriptorKey !== null) existing.extraAliasKeys.push(descriptorKey);
  }
  await createMerchants(deps.db, household_id, [...byMerchant.values()], index, deps.now().toISOString());
  return { unknown: unknown.length, created: byMerchant.size };
}

// ─── cards ──────────────────────────────────────────────────────────────────

export interface CardIndex {
  byLast4: Map<string, string[]>;
  byAlias: Map<string, string>;
}

export async function loadCardIndex(db: D1Database, household_id: string): Promise<CardIndex> {
  const byLast4 = new Map<string, string[]>();
  const { results: facts } = await db
    .prepare(`SELECT subject_id, value_text FROM facts_current WHERE household_id = ? AND predicate = 'card_last4'`)
    .bind(household_id)
    .all<{ subject_id: string; value_text: string | null }>();
  for (const f of facts) {
    if (f.value_text === null) continue;
    byLast4.set(f.value_text, [...(byLast4.get(f.value_text) ?? []), f.subject_id]);
  }
  const byAlias = new Map<string, string>();
  const { results: aliases } = await db
    .prepare(`SELECT alias_norm, entity_id FROM entity_aliases WHERE household_id = ? AND alias_norm LIKE 'card_account:%'`)
    .bind(household_id)
    .all<{ alias_norm: string; entity_id: string }>();
  for (const a of aliases) byAlias.set(a.alias_norm, a.entity_id);
  return { byLast4, byAlias };
}

/**
 * A row that names its account is matched by that account only (last4, then alias); a row with no account column uses
 * the file's default card. A multi-account export must never have another account's rows assigned to one card.
 */
export function resolveCard(index: CardIndex, accountHint: string | null, defaultCard: string | null): string | null {
  if (accountHint === null) return defaultCard;
  const last4 = lastFourOf(accountHint);
  const candidates = last4 === null ? undefined : index.byLast4.get(last4);
  if (candidates !== undefined && candidates.length === 1) return candidates[0] ?? null;
  const key = aliasKey('card_account', accountHint);
  return key === null ? null : (index.byAlias.get(key) ?? null);
}

// ─── insert ─────────────────────────────────────────────────────────────────

export function txnId(household_id: string, row: Pick<ParsedTxn, 'posted_on' | 'amount_cents' | 'descriptor_norm'>): Promise<string> {
  return sha256Hex(`${household_id}|${row.posted_on}|${row.amount_cents}|${row.descriptor_norm}`).then((h) => `txn_${h.slice(0, 32)}`);
}

export interface InsertContext {
  household_id: string;
  defaultCardId: string | null;
  episodeFor: (row: ParsedTxn) => string;
}

export async function insertBatch(
  deps: SpendDeps,
  ctx: InsertContext,
  rows: readonly ParsedTxn[],
): Promise<{ inserted: number; duplicates: number }> {
  const { db } = deps;
  const index = await loadMerchantAliases(db, ctx.household_id);

  // Monarch names the merchant: create any we have not seen (deterministic path, no model).
  const fresh = new Map<string, NewMerchant>();
  for (const row of rows) {
    if (row.merchant_hint === null) continue;
    const key = aliasKey('merchant', row.merchant_hint);
    if (key === null || index.has(key) || fresh.has(key)) continue;
    fresh.set(key, {
      name: row.merchant_hint,
      envelope_key: categoryToEnvelope(row.category_hint),
      extraAliasKeys: [],
      method: 'deterministic',
      confidence: 1,
    });
  }
  if (fresh.size > 0) await createMerchants(db, ctx.household_id, [...fresh.values()], index, deps.now().toISOString());

  const merchantOf = rows.map((r) => resolveMerchant(index, r));
  const entityIds = [...new Set(merchantOf.filter((m): m is string => m !== null))];
  const envelopeOfEntity = new Map<string, string | null>();
  await inChunks(entityIds, MAX_BOUND_PARAMS - 1, async (group) => {
    const { results } = await db
      .prepare(`SELECT entity_id, attrs_json FROM entities WHERE household_id = ? AND entity_id IN (${placeholders(group.length)})`)
      .bind(ctx.household_id, ...group)
      .all<{ entity_id: string; attrs_json: string }>();
    for (const e of results) {
      let key: string | null = null;
      try {
        const v = (JSON.parse(e.attrs_json) as { envelope_key?: unknown }).envelope_key;
        key = typeof v === 'string' ? v : null;
      } catch {
        key = null;
      }
      envelopeOfEntity.set(e.entity_id, key);
    }
    return [];
  });

  const cards = await loadCardIndex(db, ctx.household_id);
  const values: Array<Array<string | number | null>> = [];
  for (const [i, row] of rows.entries()) {
    const merchant = merchantOf[i] ?? null;
    let envelope: string | null;
    if (row.category_hint !== null) envelope = categoryToEnvelope(row.category_hint);
    else {
      const learned = merchant === null ? null : (envelopeOfEntity.get(merchant) ?? null);
      envelope = row.amount_cents < 0 ? (learned ?? 'uncategorized') : learned !== 'uncategorized' ? learned : null;
    }
    values.push([
      await txnId(ctx.household_id, row),
      ctx.household_id,
      ctx.episodeFor(row),
      row.posted_on,
      row.amount_cents,
      row.descriptor_norm,
      merchant,
      resolveCard(cards, row.account_hint, ctx.defaultCardId),
      envelope,
    ]);
  }
  const statements = multiRowStatements(
    db,
    `INSERT OR IGNORE INTO transactions
       (txn_id, household_id, episode_id, posted_on, amount_cents, descriptor_norm, merchant_entity_id, card_entity_id, envelope_key)`,
    `(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    values,
  );
  let inserted = 0;
  if (statements.length > 0) {
    const results = await db.batch(statements);
    inserted = results.reduce((sum, r) => sum + (r.meta.changes ?? 0), 0);
  }
  return { inserted, duplicates: rows.length - inserted };
}

// ─── envelopes ──────────────────────────────────────────────────────────────

export function median(values: readonly number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  if (sorted.length === 0) return 0;
  const mid = sorted.length >> 1;
  return sorted.length % 2 === 1 ? (sorted[mid] ?? 0) : Math.round(((sorted[mid - 1] ?? 0) + (sorted[mid] ?? 0)) / 2);
}

/**
 * monthly_target_cents := median of net spend in the (up to) 3 complete calendar months before `asOf`'s month, per
 * envelope. Months before the household's first transaction are not counted; months inside its history with no spend
 * in an envelope count as 0. Envelopes whose median is not positive are left untouched.
 */
export async function recomputeEnvelopes(
  db: D1Database,
  household_id: string,
  asOf: Date,
): Promise<{ updated: number }> {
  const y = asOf.getUTCFullYear();
  const m = asOf.getUTCMonth();
  const monthStart = (offset: number): string => ymd(new Date(Date.UTC(y, m + offset, 1)));
  const windowStart = monthStart(-3);
  const windowEnd = monthStart(0);

  const first = await db
    .prepare('SELECT MIN(posted_on) AS first_on FROM transactions WHERE household_id = ?')
    .bind(household_id)
    .first<{ first_on: string | null }>();
  if (first === null || first.first_on === null) return { updated: 0 };
  const months = [-3, -2, -1].map((o) => monthStart(o).slice(0, 7)).filter((ym) => ym >= first.first_on!.slice(0, 7));
  if (months.length === 0) return { updated: 0 };

  const { results } = await db
    .prepare(
      `SELECT envelope_key, strftime('%Y-%m', posted_on) AS ym, -SUM(amount_cents) AS spend
         FROM transactions
        WHERE household_id = ? AND envelope_key IS NOT NULL AND posted_on >= ? AND posted_on < ?
        GROUP BY envelope_key, ym`,
    )
    .bind(household_id, windowStart, windowEnd)
    .all<{ envelope_key: string; ym: string; spend: number }>();
  const perEnvelope = new Map<string, Map<string, number>>();
  for (const r of results) {
    const monthsMap = perEnvelope.get(r.envelope_key) ?? new Map<string, number>();
    monthsMap.set(r.ym, Math.max(0, r.spend));
    perEnvelope.set(r.envelope_key, monthsMap);
  }
  const rows: Array<Array<string | number | null>> = [];
  for (const [key, byMonth] of perEnvelope) {
    const target = median(months.map((ym) => byMonth.get(ym) ?? 0));
    if (target > 0) rows.push([household_id, key, target]);
  }
  const statements = multiRowStatements(
    db,
    `INSERT INTO envelopes (household_id, envelope_key, monthly_target_cents)`,
    `(?, ?, ?)`,
    rows,
    `ON CONFLICT (household_id, envelope_key) DO UPDATE SET monthly_target_cents = excluded.monthly_target_cents`,
  );
  if (statements.length > 0) await db.batch(statements);
  return { updated: rows.length };
}

// ─── credit periods ─────────────────────────────────────────────────────────

export interface Period {
  start: string;
  end: string;
}

/** Periods of `cadence` overlapping [from, to] (inclusive 'YYYY-MM-DD'). cardmember_year needs the card's anniversary. */
export function periodsFor(
  cadence: BenefitCatalogRow['cadence'],
  from: string,
  to: string,
  anniversary: { month: number; day: number } | null,
): Period[] {
  const out: Period[] = [];
  const fromDate = new Date(`${from}T00:00:00Z`);
  const toDate = new Date(`${to}T00:00:00Z`);
  const lastDay = (d: Date): string => ymd(new Date(d.getTime() - 86_400_000));
  const push = (start: Date, endExclusive: Date): void => {
    if (ymd(endExclusive) > from && ymd(start) <= to) out.push({ start: ymd(start), end: lastDay(endExclusive) });
  };
  const years = [fromDate.getUTCFullYear() - 1, toDate.getUTCFullYear() + 1];
  for (let y = years[0]!; y <= years[1]!; y++) {
    if (cadence === 'annual') push(new Date(Date.UTC(y, 0, 1)), new Date(Date.UTC(y + 1, 0, 1)));
    else if (cadence === 'semiannual') {
      for (const m of [0, 6]) push(new Date(Date.UTC(y, m, 1)), new Date(Date.UTC(y, m + 6, 1)));
    } else if (cadence === 'quarterly') {
      for (const m of [0, 3, 6, 9]) push(new Date(Date.UTC(y, m, 1)), new Date(Date.UTC(y, m + 3, 1)));
    } else if (cadence === 'monthly') {
      for (let m = 0; m < 12; m++) push(new Date(Date.UTC(y, m, 1)), new Date(Date.UTC(y, m + 1, 1)));
    } else if (anniversary !== null) {
      push(new Date(Date.UTC(y, anniversary.month - 1, anniversary.day)), new Date(Date.UTC(y + 1, anniversary.month - 1, anniversary.day)));
    }
  }
  return out;
}

/** Descriptor tokens of a catalog row; 'category:' tokens cannot be matched against descriptors and are dropped. */
export function descriptorTokens(merchantMatch: string): string[] {
  return merchantMatch
    .split('|')
    .map((t) => t.trim().toUpperCase())
    .filter((t) => t !== '' && !t.startsWith('CATEGORY:'));
}

/**
 * Recomputes credit_periods for every card whose product is in benefit_catalog, over the periods that overlap
 * [rangeFrom, rangeTo] and the current period. used_cents = min(benefit amount, matching outflows in the period), taken
 * from the transactions table, so running it twice (or re-importing a file) yields identical rows.
 *
 * A period is only reported unused ('open' while running, 'expired_unused' once over) when the card's transactions
 * actually cover it; otherwise no row is written, because "unused" would be a claim the data cannot support.
 * Benefits that can only be matched by issuer category have no measurable spend and are skipped.
 */
export async function recomputeCreditPeriods(
  db: D1Database,
  household_id: string,
  range: { from: string; to: string },
  asOf: Date,
): Promise<{ updated: number }> {
  const today = ymd(asOf);
  const { results: cards } = await db
    .prepare(`SELECT subject_id, value_text FROM facts_current WHERE household_id = ? AND predicate = 'card_product'`)
    .bind(household_id)
    .all<{ subject_id: string; value_text: string | null }>();
  if (cards.length === 0) return { updated: 0 };
  const { results: catalog } = await db.prepare('SELECT * FROM benefit_catalog').all<BenefitCatalogRow>();
  const { results: openedOn } = await db
    .prepare(`SELECT subject_id, value_text FROM facts_current WHERE household_id = ? AND predicate = 'card_opened_on'`)
    .bind(household_id)
    .all<{ subject_id: string; value_text: string | null }>();
  const anniversaryOf = new Map<string, { month: number; day: number }>();
  for (const o of openedOn) {
    const m = o.value_text === null ? null : /^\d{4}-(\d{2})-(\d{2})$/.exec(o.value_text);
    if (m) anniversaryOf.set(o.subject_id, { month: Number(m[1]), day: Number(m[2]) });
  }

  const from = range.from < today ? range.from : today;
  const to = range.to > today ? range.to : today;
  const upserts: Array<Array<string | number | null>> = [];

  for (const card of cards) {
    const benefits = catalog.filter((b) => b.card_product === card.value_text && descriptorTokens(b.merchant_match).length > 0);
    if (benefits.length === 0) continue;
    const coverage = await db
      .prepare('SELECT MIN(posted_on) AS lo, MAX(posted_on) AS hi FROM transactions WHERE household_id = ? AND card_entity_id = ?')
      .bind(household_id, card.subject_id)
      .first<{ lo: string | null; hi: string | null }>();
    if (coverage === null || coverage.lo === null || coverage.hi === null) continue;

    const planned = benefits.flatMap((b) =>
      periodsFor(b.cadence, from, to, anniversaryOf.get(card.subject_id) ?? null)
        .filter((p) => p.start <= today)
        .map((p) => ({ benefit: b, period: p })),
    );
    if (planned.length === 0) continue;
    const loadFrom = planned.reduce((lo, p) => (p.period.start < lo ? p.period.start : lo), planned[0]!.period.start);
    const loadTo = planned.reduce((hi, p) => (p.period.end > hi ? p.period.end : hi), planned[0]!.period.end);

    const spend: Array<{ posted_on: string; cents: number; descriptor: string }> = [];
    for (let offset = 0; ; offset += 500) {
      const { results } = await db
        .prepare(
          `SELECT posted_on, amount_cents, descriptor_norm FROM transactions
            WHERE household_id = ? AND card_entity_id = ? AND amount_cents < 0 AND posted_on BETWEEN ? AND ?
            ORDER BY posted_on, txn_id LIMIT 500 OFFSET ?`,
        )
        .bind(household_id, card.subject_id, loadFrom, loadTo, offset)
        .all<{ posted_on: string; amount_cents: number; descriptor_norm: string }>();
      spend.push(...results.map((r) => ({ posted_on: r.posted_on, cents: -r.amount_cents, descriptor: r.descriptor_norm })));
      if (results.length < 500) break;
    }

    for (const { benefit, period } of planned) {
      const tokens = descriptorTokens(benefit.merchant_match);
      const matched = spend
        .filter((s) => s.posted_on >= period.start && s.posted_on <= period.end && tokens.some((t) => s.descriptor.includes(t)))
        .reduce((sum, s) => sum + s.cents, 0);
      const used = Math.min(matched, benefit.amount_cents);
      let status: string;
      if (used >= benefit.amount_cents) status = 'used';
      else if (used > 0) status = 'partially_used';
      else if (period.end >= today) status = 'open';
      else if (coverage.lo <= period.start && daysBetween(coverage.hi, period.end) <= 7) status = 'expired_unused';
      else continue;
      upserts.push([household_id, card.subject_id, benefit.benefit_key, period.start, period.end, used, status]);
    }
  }
  const statements = multiRowStatements(
    db,
    `INSERT INTO credit_periods (household_id, card_entity_id, benefit_key, period_start, period_end, used_cents, status)`,
    `(?, ?, ?, ?, ?, ?, ?)`,
    upserts,
    `ON CONFLICT (household_id, card_entity_id, benefit_key, period_start) DO UPDATE SET
       period_end = excluded.period_end, used_cents = excluded.used_cents, status = excluded.status
     WHERE credit_periods.status <> 'unconfirmed'`,
  );
  if (statements.length > 0) await db.batch(statements);
  return { updated: upserts.length };
}

// ─── recurring charges ──────────────────────────────────────────────────────

export const CADENCES = [
  { name: 'weekly', days: 7 },
  { name: 'biweekly', days: 14 },
  { name: 'monthly', days: 30 },
  { name: 'quarterly', days: 91 },
  { name: 'annual', days: 365 },
] as const;
export type CadenceName = (typeof CADENCES)[number]['name'];

/** +/-5 days, tightened for short cadences so 'weekly' cannot swallow everything: min(5, floor(20% of cadence)). */
export function cadenceTolerance(days: number): number {
  return Math.min(5, Math.floor(days * 0.2));
}

export interface HistoryTxn {
  txn_id: string;
  posted_on: string;
  amount_cents: number;
  merchant_entity_id: string | null;
  descriptor_norm: string;
  card_entity_id: string | null;
  episode_id: string;
}

export interface RecurringSeries {
  key: string;
  merchant_entity_id: string | null;
  descriptor_base: string;
  cadence: CadenceName;
  /** Positive cents of the latest charge. */
  amount_cents: number;
  /** Date of the first charge in the trailing run that has exactly this amount. */
  valid_from: string;
  occurrences: number;
  latest: HistoryTxn;
}

function baseOf(descriptorNorm: string): string {
  return descriptorNorm.replace(/ ~\d+$/, '');
}

/** Pure detector: groups outflows by merchant, links consecutive charges, keeps each merchant's latest run. */
export function detectRecurring(history: readonly HistoryTxn[]): RecurringSeries[] {
  const groups = new Map<string, HistoryTxn[]>();
  for (const t of history) {
    if (t.amount_cents >= 0) continue;
    const key = t.merchant_entity_id ?? `d:${baseOf(t.descriptor_norm)}`;
    groups.set(key, [...(groups.get(key) ?? []), t]);
  }
  const series: RecurringSeries[] = [];
  for (const [key, list] of groups) {
    const sorted = [...list].sort((a, b) => a.posted_on.localeCompare(b.posted_on) || a.txn_id.localeCompare(b.txn_id));
    let runStart = -1;
    let runCadence: CadenceName | null = null;
    let best: { start: number; end: number; cadence: CadenceName } | null = null;
    for (let i = 1; i < sorted.length; i++) {
      const prev = sorted[i - 1]!;
      const cur = sorted[i]!;
      const gap = daysBetween(prev.posted_on, cur.posted_on);
      const cadence = CADENCES.find((c) => Math.abs(gap - c.days) <= cadenceTolerance(c.days));
      const a = Math.abs(prev.amount_cents);
      const b = Math.abs(cur.amount_cents);
      const linked = cadence !== undefined && Math.abs(b - a) <= 0.1 * a;
      if (linked && cadence !== undefined) {
        if (runCadence !== cadence.name || runStart < 0) {
          runStart = i - 1;
          runCadence = cadence.name;
        }
        best = { start: runStart, end: i, cadence: cadence.name };
      } else {
        runStart = -1;
        runCadence = null;
      }
    }
    if (best === null) continue;
    const latest = sorted[best.end]!;
    let first = best.end;
    while (first > best.start && sorted[first - 1]!.amount_cents === latest.amount_cents) first -= 1;
    series.push({
      key,
      merchant_entity_id: latest.merchant_entity_id,
      descriptor_base: baseOf(latest.descriptor_norm),
      cadence: best.cadence,
      amount_cents: Math.abs(latest.amount_cents),
      valid_from: sorted[first]!.posted_on,
      occurrences: best.end - best.start + 1,
      latest,
    });
  }
  return series.sort((a, b) => a.key.localeCompare(b.key));
}

export async function loadOutflowHistory(db: D1Database, household_id: string, since: string): Promise<HistoryTxn[]> {
  const out: HistoryTxn[] = [];
  for (let offset = 0; offset < 20_000; offset += 500) {
    const { results } = await db
      .prepare(
        `SELECT txn_id, posted_on, amount_cents, merchant_entity_id, descriptor_norm, card_entity_id, episode_id
           FROM transactions WHERE household_id = ? AND amount_cents < 0 AND posted_on >= ?
          ORDER BY posted_on, txn_id LIMIT 500 OFFSET ?`,
      )
      .bind(household_id, since, offset)
      .all<HistoryTxn>();
    out.push(...results);
    if (results.length < 500) break;
  }
  return out;
}

export async function entityNames(db: D1Database, household_id: string, ids: readonly string[]): Promise<Map<string, string>> {
  const names = new Map<string, string>();
  await inChunks([...new Set(ids)], MAX_BOUND_PARAMS - 1, async (group) => {
    const { results } = await db
      .prepare(`SELECT entity_id, canonical_name FROM entities WHERE household_id = ? AND entity_id IN (${placeholders(group.length)})`)
      .bind(household_id, ...group)
      .all<{ entity_id: string; canonical_name: string }>();
    for (const r of results) names.set(r.entity_id, r.canonical_name);
    return [];
  });
  return names;
}

function titleCase(s: string): string {
  return s.toLowerCase().replace(/\b[a-z]/g, (c) => c.toUpperCase());
}

/** Turns a detected series into graph proposals (amount, cadence, merchant, card), grounded on the latest charge's line. */
export function seriesToProposals(
  series: RecurringSeries,
  input: {
    household_id: string;
    episode_id: string;
    merchantName: string | null;
    quote: { quote: string; char_start: number; char_end: number };
  },
): FactProposal[] {
  const name = input.merchantName ?? titleCase(series.descriptor_base);
  const subject = { type: 'recurring_charge', name } as const;
  const common = {
    household_id: input.household_id,
    source_episode_id: input.episode_id,
    subject,
    valid_from: series.valid_from,
    confidence: 0.9,
    quotes: [input.quote],
  };
  const proposals: FactProposal[] = [
    { ...common, predicate: 'recurring_amount', value_cents: series.amount_cents, currency: 'USD' },
    { ...common, predicate: 'recurring_cadence', value_text: series.cadence },
    {
      ...common,
      predicate: 'recurring_merchant',
      object: series.merchant_entity_id === null ? { type: 'merchant', name } : { entity_id: series.merchant_entity_id },
    },
  ];
  if (series.latest.card_entity_id !== null) {
    proposals.push({ ...common, predicate: 'charged_to_card', object: { entity_id: series.latest.card_entity_id } });
  }
  return proposals;
}
