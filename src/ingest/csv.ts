/**
 * src/ingest/csv.ts — pure parsers for spend files. No I/O.
 *
 * Inputs (always the REDACTED text, so no account number ever reaches a transaction row):
 *   - Monarch exports        Date, Merchant, Category, Account, Original Statement, Notes, Amount, Tags, ...
 *   - Bank / card CSVs       Date | Posted Date | Transaction Date, Description, Amount  (or Debit + Credit columns)
 *   - PDF statement text     lines such as  "11/03 RESY *CARBONE NEW YORK NY 112.40"
 *
 * Output amounts are integer cents, NEGATIVE for money leaving the household, matching transactions.amount_cents.
 * Every parsed row also carries its [start, end) offsets in the parsed text, so callers can quote the exact source line.
 */

import type { SignConvention } from '../types';

export class CsvFormatError extends Error {
  override readonly name = 'CsvFormatError';
}

export type SpendFormat = 'monarch' | 'amex' | 'debit_credit' | 'generic' | 'statement_text';

export interface ParsedTxn {
  /** Position among accepted rows, in file order. */
  index: number;
  /** [start, end) of the source line(s) in the parsed text. */
  start: number;
  end: number;
  posted_on: string;
  amount_cents: number;
  descriptor_raw: string;
  /** Normalized descriptor without the repeat suffix. */
  descriptor_base: string;
  /** descriptor_base, plus ' ~2', ' ~3'... for the 2nd, 3rd... identical (date, amount, descriptor) row in the file. */
  descriptor_norm: string;
  merchant_hint: string | null;
  category_hint: string | null;
  account_hint: string | null;
}

export interface ParsedSpend {
  format: SpendFormat;
  convention: 'negative_is_outflow' | 'positive_is_outflow';
  rows: ParsedTxn[];
  /** Records that had a date/amount that could not be read. */
  skipped: number;
}

// ─── CSV tokenizer (RFC 4180) ───────────────────────────────────────────────

export interface CsvRecord {
  cells: string[];
  start: number;
  end: number;
}

export function parseCsv(text: string): CsvRecord[] {
  const records: CsvRecord[] = [];
  let cells: string[] = [];
  let cell = '';
  let inQuotes = false;
  let start = text.charCodeAt(0) === 0xfeff ? 1 : 0;
  let i = start;
  const finish = (end: number): void => {
    cells.push(cell);
    if (!(cells.length === 1 && cells[0]!.trim() === '')) records.push({ cells, start, end });
    cells = [];
    cell = '';
  };
  while (i < text.length) {
    const ch = text[i]!;
    if (inQuotes) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          cell += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
      } else cell += ch;
      i += 1;
      continue;
    }
    if (ch === '"' && cell === '') inQuotes = true;
    else if (ch === ',') {
      cells.push(cell);
      cell = '';
    } else if (ch === '\n' || ch === '\r') {
      finish(i);
      if (ch === '\r' && text[i + 1] === '\n') i += 1;
      start = i + 1;
    } else cell += ch;
    i += 1;
  }
  if (cell !== '' || cells.length > 0) finish(text.length);
  return records;
}

// ─── scalar parsers ─────────────────────────────────────────────────────────

/** Parses "$1,234.56", "-12.30", "(12.30)", "12.30-", "12.30 CR" into signed integer cents. null if unreadable. */
export function toCents(input: string): number | null {
  let s = input.trim().replace(/−/g, '-');
  if (s === '') return null;
  let negative = false;
  if (/^\(.*\)$/.test(s)) {
    negative = true;
    s = s.slice(1, -1);
  }
  if (/CR$/i.test(s)) {
    negative = !negative;
    s = s.replace(/\s*CR$/i, '');
  }
  s = s.replace(/\s*DR$/i, '');
  if (s.endsWith('-')) {
    negative = !negative;
    s = s.slice(0, -1);
  }
  if (s.startsWith('-')) {
    negative = !negative;
    s = s.slice(1);
  } else if (s.startsWith('+')) s = s.slice(1);
  s = s.replace(/[$,\s]/g, '');
  const m = /^(\d+)(?:\.(\d{1,2}))?$/.exec(s);
  if (m === null) return null;
  const cents = Number(m[1]) * 100 + Number((m[2] ?? '').padEnd(2, '0') || '0');
  if (!Number.isSafeInteger(cents)) return null;
  return negative ? -cents : cents;
}

function realDate(y: number, m: number, d: number): string | null {
  const t = new Date(Date.UTC(y, m - 1, d));
  if (t.getUTCFullYear() !== y || t.getUTCMonth() !== m - 1 || t.getUTCDate() !== d) return null;
  return `${String(y).padStart(4, '0')}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
}

export interface DateContext {
  /** Year for 'MM/DD' dates. */
  year?: number | null;
  /** Reference "now"; an inferred-year date later than this is moved to the previous year. */
  asOf: Date;
}

/** Parses ISO, Y/M/D, M/D/YYYY, M/D/YY and (with a year context) M/D into 'YYYY-MM-DD'. */
export function parseDate(input: string, ctx: DateContext): string | null {
  const s = input.trim();
  let m = /^(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[T ].*)?$/.exec(s);
  if (m) return realDate(Number(m[1]), Number(m[2]), Number(m[3]));
  m = /^(\d{1,2})[/-](\d{1,2})[/-](\d{4}|\d{2})(?:\s.*)?$/.exec(s);
  if (m) {
    const y = m[3]!.length === 2 ? 2000 + Number(m[3]) : Number(m[3]);
    return realDate(y, Number(m[1]), Number(m[2]));
  }
  m = /^(\d{1,2})\/(\d{1,2})$/.exec(s);
  if (m) {
    const explicit = ctx.year ?? null;
    const year = explicit ?? ctx.asOf.getUTCFullYear();
    const date = realDate(year, Number(m[1]), Number(m[2]));
    if (date === null) return null;
    if (explicit === null && date > new Date(ctx.asOf.getTime() + 86_400_000).toISOString().slice(0, 10)) {
      return realDate(year - 1, Number(m[1]), Number(m[2]));
    }
    return date;
  }
  return null;
}

/**
 * Descriptor normal form used for dedupe, merchant matching and benefit matching: upper case, a leading payment-processor
 * tag ('SQ *', 'TST*', 'SP *', 'PP*', 'PAYPAL *') removed, remaining punctuation ('*', '#') turned into spaces, only
 * A-Z 0-9 & + kept, and bare numbers of 3+ digits (store ids, zip codes) dropped.
 */
export function normalizeDescriptor(raw: string): string {
  const tokens = raw
    .normalize('NFKC')
    .toUpperCase()
    .replace(/^\s*(?:SQ|TST|SP|PP|PAYPAL)\s*\*\s*/, '')
    .replace(/[*#]/g, ' ')
    .replace(/[^A-Z0-9&+ ]+/g, ' ')
    .split(/\s+/)
    .filter((t) => t !== '' && !/^\d{3,}$/.test(t));
  const out = tokens.join(' ').slice(0, 80).trim();
  return out === '' ? 'UNKNOWN' : out;
}

const NON_BUDGET_CATEGORY = /(transfer|credit card payment|paycheck|income|dividend|interest|payment)/i;

/** Envelope key for a bank/Monarch category, or null for money that is not budgeted (transfers, payments, income). */
export function categoryToEnvelope(category: string | null): string | null {
  if (category === null || category.trim() === '') return null;
  if (NON_BUDGET_CATEGORY.test(category)) return null;
  const slug = category
    .normalize('NFKC')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 40);
  return slug === '' ? null : slug;
}

/** Last four digits of an account label such as "Platinum Card (...1005)", or null. */
export function lastFourOf(label: string | null): string | null {
  if (label === null) return null;
  const m = /(\d{4})\D*$/.exec(label.trim());
  return m ? (m[1] ?? null) : null;
}

// ─── file parsers ───────────────────────────────────────────────────────────

const DATE_HEADERS = ['posted date', 'post date', 'posting date', 'date', 'transaction date', 'trans date', 'trans. date'];
const DESC_HEADERS = ['original statement', 'description', 'details', 'payee', 'name', 'merchant', 'memo'];
const ACCOUNT_HEADERS = ['account', 'account name', 'card no.', 'card no', 'account #'];

function findHeader(headers: readonly string[], candidates: readonly string[]): number {
  for (const c of candidates) {
    const at = headers.indexOf(c);
    if (at >= 0) return at;
  }
  return -1;
}

interface RawRow {
  start: number;
  end: number;
  posted_on: string;
  amount: number;
  descriptor_raw: string;
  merchant_hint: string | null;
  category_hint: string | null;
  account_hint: string | null;
}

function finish(
  raws: RawRow[],
  format: SpendFormat,
  convention: 'negative_is_outflow' | 'positive_is_outflow',
  skipped: number,
  fixedSign: boolean,
): ParsedSpend {
  const seen = new Map<string, number>();
  const rows: ParsedTxn[] = raws.map((r, index) => {
    const amount_cents = fixedSign ? r.amount : convention === 'positive_is_outflow' ? -r.amount : r.amount;
    const base = normalizeDescriptor(r.descriptor_raw);
    const key = `${r.posted_on}|${amount_cents}|${base}`;
    const n = (seen.get(key) ?? 0) + 1;
    seen.set(key, n);
    return {
      index,
      start: r.start,
      end: r.end,
      posted_on: r.posted_on,
      amount_cents,
      descriptor_raw: r.descriptor_raw,
      descriptor_base: base,
      descriptor_norm: n > 1 ? `${base} ~${n}` : base,
      merchant_hint: r.merchant_hint,
      category_hint: r.category_hint,
      account_hint: r.account_hint,
    };
  });
  return { format, convention, rows, skipped };
}

function detectConvention(
  requested: SignConvention,
  format: SpendFormat,
  amounts: readonly number[],
): 'negative_is_outflow' | 'positive_is_outflow' {
  if (requested !== 'auto') return requested;
  if (format === 'amex') return 'positive_is_outflow';
  if (format === 'monarch') return 'negative_is_outflow';
  const positive = amounts.filter((a) => a > 0).length;
  const negative = amounts.filter((a) => a < 0).length;
  return positive + negative > 0 && positive / (positive + negative) > 0.7 ? 'positive_is_outflow' : 'negative_is_outflow';
}

export interface ParseOptions {
  sign_convention?: SignConvention;
  statement_year?: number | null;
  asOf: Date;
}

/** Parses a Monarch export or a bank / card CSV. Throws CsvFormatError when no date/amount columns can be found. */
export function parseSpendCsv(text: string, options: ParseOptions): ParsedSpend {
  const records = parseCsv(text);
  const headerAt = records.findIndex((r) => findHeader(r.cells.map((c) => c.trim().toLowerCase()), DATE_HEADERS) >= 0);
  if (headerAt < 0) throw new CsvFormatError('no date column found');
  const header = records[headerAt]!.cells.map((c) => c.trim().toLowerCase());
  const dateCol = findHeader(header, DATE_HEADERS);
  const descCol = findHeader(header, DESC_HEADERS);
  const amountCol = header.indexOf('amount');
  const debitCol = header.indexOf('debit');
  const creditCol = header.indexOf('credit');
  if (descCol < 0) throw new CsvFormatError('no description column found');
  if (amountCol < 0 && debitCol < 0 && creditCol < 0) throw new CsvFormatError('no amount (or debit/credit) column found');
  const merchantCol = header.indexOf('merchant');
  const categoryCol = header.indexOf('category');
  const accountCol = findHeader(header, ACCOUNT_HEADERS);

  const format: SpendFormat =
    header.includes('original statement') && merchantCol >= 0 && amountCol >= 0
      ? 'monarch'
      : header.includes('card member')
        ? 'amex'
        : amountCol < 0
          ? 'debit_credit'
          : 'generic';

  const ctx: DateContext = { year: options.statement_year ?? null, asOf: options.asOf };
  const raws: RawRow[] = [];
  let skipped = 0;
  const cell = (r: CsvRecord, col: number): string => (col >= 0 ? (r.cells[col] ?? '').trim() : '');
  for (const r of records.slice(headerAt + 1)) {
    const posted_on = parseDate(cell(r, dateCol), ctx);
    let amount: number | null;
    if (amountCol >= 0) amount = toCents(cell(r, amountCol));
    else {
      const debit = toCents(cell(r, debitCol));
      const credit = toCents(cell(r, creditCol));
      amount = debit !== null && debit !== 0 ? -Math.abs(debit) : credit !== null && credit !== 0 ? Math.abs(credit) : null;
    }
    const descriptor = cell(r, descCol) || cell(r, merchantCol);
    if (posted_on === null || amount === null || descriptor === '') {
      skipped += 1;
      continue;
    }
    const merchant = format === 'monarch' ? cell(r, merchantCol) : '';
    const category = cell(r, categoryCol);
    const account = cell(r, accountCol);
    raws.push({
      start: r.start,
      end: r.end,
      posted_on,
      amount,
      descriptor_raw: descriptor,
      merchant_hint: merchant === '' ? null : merchant,
      category_hint: category === '' ? null : category,
      account_hint: account === '' ? null : account,
    });
  }
  const fixedSign = amountCol < 0;
  const convention = fixedSign
    ? 'negative_is_outflow'
    : detectConvention(options.sign_convention ?? 'auto', format, raws.map((r) => r.amount));
  return finish(raws, format, convention, skipped, fixedSign);
}

const STATEMENT_LINE =
  /^\s*(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)(?:\s+\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)?\s+(\S.*?)\s+(-?\$?\(?[\d,]+\.\d{2}\)?(?:\s?(?:CR|DR))?-?)\s*$/i;

/** Parses text extracted from a card statement PDF. Charges are positive on statements unless told otherwise. */
export function parseStatementText(text: string, options: ParseOptions): ParsedSpend {
  const ctx: DateContext = { year: options.statement_year ?? null, asOf: options.asOf };
  const raws: RawRow[] = [];
  let skipped = 0;
  let offset = 0;
  for (const line of text.split('\n')) {
    const start = offset;
    offset += line.length + 1;
    const m = STATEMENT_LINE.exec(line.replace(/\r$/, ''));
    if (m === null) continue;
    const posted_on = parseDate(m[1]!, ctx);
    const amount = toCents(m[3]!);
    if (posted_on === null || amount === null) {
      skipped += 1;
      continue;
    }
    raws.push({
      start,
      end: start + line.replace(/\r$/, '').length,
      posted_on,
      amount,
      descriptor_raw: m[2]!,
      merchant_hint: null,
      category_hint: null,
      account_hint: null,
    });
  }
  const requested = options.sign_convention ?? 'auto';
  const convention = requested === 'negative_is_outflow' ? 'negative_is_outflow' : 'positive_is_outflow';
  return finish(raws, 'statement_text', convention, skipped, false);
}

export function parseSpendText(kind: 'csv' | 'pdf_text', text: string, options: ParseOptions): ParsedSpend {
  return kind === 'csv' ? parseSpendCsv(text, options) : parseStatementText(text, options);
}
