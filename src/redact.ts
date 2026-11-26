/**
 * src/redact.ts — deterministic PII scrubbing. Runs on every episode BEFORE any LLM call
 * (CLAUDE.md §0: "Redact SSN/ITIN/full account numbers BEFORE any LLM call; keep last4").
 *
 * Pure and synchronous: same input => same output, no I/O, no clock, no randomness.
 *
 * What it does, in order:
 *   0. Canonicalize: Unicode NFKC (folds full-width digits/spaces to ASCII) and drop zero-width characters
 *      and soft hyphens, so digits cannot be hidden from the patterns. `redacted_text` is therefore the
 *      NFKC form of the input; all quote offsets are computed against it.
 *   1. Credentials ("password: Xk39aZ", "PIN is 4821", "verification code: 482913") => QUARANTINE.
 *      A secret of arbitrary shape cannot be delimited reliably, so the whole episode is withheld.
 *   2. SSN / ITIN, written 123-45-6789 (also space, dot or Unicode-dash separated)  => [SSN] / [ITIN]
 *      (ITIN = leading 9 and group 50-65, 70-88, 90-92 or 94-99). Bare 9-digit numbers after a label
 *      (SSN, social security, tax id, TIN, ITIN, "SS #") are redacted too. Nothing is kept: no last4.
 *   3. Card / account numbers: any run of 13+ digits, plus the separated card layouts 4-4-4-(1..7),
 *      4-4-4-4-(1..3), 4-6-5 and 4-6-4  => [ACCT ****1234]  (last4 kept so facts can reference the card).
 *      Digit-only date chains such as "2026-11-26 2026-11-27" are not card-shaped and are left alone.
 *   4. Labeled account / routing numbers with 8+ digits ("Account number: 12345678", "routing 021000021")
 *      => [ACCT ****5678]  (CLAUDE.md §0 says "full account numbers", which are often shorter than 13 digits).
 *   5. Card security codes after a label (CVV, CVC, CID, security code)  => [CVV].
 *   6. Defense in depth: the output is scanned again with the same rules; any hit => QUARANTINE.
 *
 * The result never contains matched values, only counts. Quarantined results carry `redacted_text: null`;
 * store that as NULL so no downstream code can pass it to a model. Redaction is idempotent:
 * redact(redact(x).redacted_text).redaction_status === 'clean'.
 */

import type { RedactionStatus } from './types';

export type RedactionKind = 'ssn' | 'itin' | 'long_number' | 'labeled_account_number' | 'cvv';

export interface RedactionFinding {
  kind: RedactionKind;
  count: number;
}

export type QuarantineReason = 'credential_detected' | 'residual_sensitive_pattern';

export interface RedactionApplied {
  redaction_status: Extract<RedactionStatus, 'clean' | 'redacted'>;
  redacted_text: string;
  findings: readonly RedactionFinding[];
}

export interface RedactionQuarantined {
  redaction_status: Extract<RedactionStatus, 'quarantined'>;
  redacted_text: null;
  quarantine_reason: QuarantineReason;
  findings: readonly RedactionFinding[];
}

export type RedactionResult = RedactionApplied | RedactionQuarantined;

export const SSN_TOKEN = '[SSN]';
export const ITIN_TOKEN = '[ITIN]';
export const CVV_TOKEN = '[CVV]';

/** Placeholder for a card/account number; keeps only the last 4 digits. */
export function acctToken(last4: string): string {
  return `[ACCT ****${last4}]`;
}

/** Matches an acctToken() and captures its last4. Fresh instance per call (stateful /g regex). */
export function acctTokenPattern(): RegExp {
  return /\[ACCT \*{4}(\d{4})\]/g;
}

// ─── patterns ───────────────────────────────────────────────────────────────

const ZERO_WIDTH = /[­​-‍⁠﻿]/g;

/** label + (":" | "=" | "is") + a 4+ char token containing a digit. */
const CREDENTIAL =
  /\b(?:password|passcode|passphrase|pin|otp|one[- ]time\s+(?:code|password|passcode)|verification\s+code|security\s+answer)\b\s*(?:[:=]|\bis\b)\s*(?=\S*\d)\S{4,}/i;

/** 123-45-6789 with space / dot / hyphen / Unicode-dash separators. */
const SSN_SEPARATED = /(?<!\d)(\d{3})[ .\-‐-―−](\d{2})[ .\-‐-―−]\d{4}(?!\d)/g;

/** A TIN label, a short gap with no digits, then exactly nine digits. */
const TIN_LABELED =
  /((?:\bssn\b|\bitin\b|\btin\b|\bsocial\s+security(?:\s+(?:number|num|no\.?|#))?|\bss\s*(?:#|no\.?)|\b(?:taxpayer\s+(?:id|identification)|tax\s+id)(?:\s+(?:number|num|no\.?|#))?))([^\d\n]{0,24}?)(?<!\d)(\d{9})(?!\d)/gi;

const LONG_RUN = /(?<!\d)\d{13,}(?!\d)/g;

const CARD_5_GROUPS = /(?<!\d)\d{4}([ \-‐-―−])\d{4}\1\d{4}\1\d{4}\1\d{1,3}(?!\d)/g;
const CARD_AMEX_DINERS = /(?<!\d)\d{4}([ \-‐-―−])\d{6}\1\d{4,5}(?!\d)/g;
const CARD_4_4_4_X = /(?<!\d)\d{4}([ \-‐-―−])\d{4}\1\d{4}\1\d{1,7}(?!\d)/g;

const ACCOUNT_LABELED =
  /(\b(?:acct|account|a\/c|routing|aba|rtn)\b(?:\s*(?:number|num|no\.?|#))?)(\s*[:#\-]?\s*(?:is\s+)?)(\d(?:[ \-]?\d){7,})(?!\d)/gi;

const CVV_LABELED =
  /(\b(?:cvv2?|cvc2?|cid|security\s+code|card\s+verification\s+(?:code|value|number))(?:\s*(?:#|no\.?))?)(\s*[:#=\-]?\s*(?:is\s+)?)(\d{3,4})(?!\d)/gi;

const DATE_LIKE = /^\d{4}-\d{1,2}-\d{1,2}$/;

// ─── helpers ────────────────────────────────────────────────────────────────

function isItinGroup(group: string): boolean {
  const n = Number(group);
  return (n >= 50 && n <= 65) || (n >= 70 && n <= 88) || (n >= 90 && n <= 92) || (n >= 94 && n <= 99);
}

function classifyTin(area: string, group: string): 'ssn' | 'itin' {
  return area.startsWith('9') && isItinGroup(group) ? 'itin' : 'ssn';
}

function digitsOf(s: string): string {
  return s.replace(/\D/g, '');
}

type Counter = Record<RedactionKind, number>;

function emptyCounter(): Counter {
  return { ssn: 0, itin: 0, long_number: 0, labeled_account_number: 0, cvv: 0 };
}

const FINDING_ORDER: readonly RedactionKind[] = ['ssn', 'itin', 'long_number', 'labeled_account_number', 'cvv'];

function toFindings(counts: Counter): RedactionFinding[] {
  return FINDING_ORDER.filter((kind) => counts[kind] > 0).map((kind) => ({ kind, count: counts[kind] }));
}

/** Applies rules 2–5. Returns the rewritten text and how many replacements each rule made. */
function applyRules(input: string): { text: string; counts: Counter } {
  const counts = emptyCounter();
  let text = input;

  text = text.replace(SSN_SEPARATED, (_m, area: string, group: string) => {
    const kind = classifyTin(area, group);
    counts[kind] += 1;
    return kind === 'itin' ? ITIN_TOKEN : SSN_TOKEN;
  });

  text = text.replace(TIN_LABELED, (_m, label: string, gap: string, digits: string) => {
    const kind = classifyTin(digits.slice(0, 3), digits.slice(3, 5));
    counts[kind] += 1;
    return `${label}${gap}${kind === 'itin' ? ITIN_TOKEN : SSN_TOKEN}`;
  });

  const maskDigits = (m: string): string => {
    counts.long_number += 1;
    return acctToken(digitsOf(m).slice(-4));
  };
  text = text.replace(LONG_RUN, maskDigits);
  text = text.replace(CARD_5_GROUPS, maskDigits);
  text = text.replace(CARD_AMEX_DINERS, maskDigits);
  text = text.replace(CARD_4_4_4_X, maskDigits);

  text = text.replace(ACCOUNT_LABELED, (m, label: string, gap: string, number: string) => {
    if (digitsOf(number).length < 8 || DATE_LIKE.test(number)) return m;
    counts.labeled_account_number += 1;
    return `${label}${gap}${acctToken(digitsOf(number).slice(-4))}`;
  });

  text = text.replace(CVV_LABELED, (_m, label: string, gap: string) => {
    counts.cvv += 1;
    return `${label}${gap}${CVV_TOKEN}`;
  });

  return { text, counts };
}

function total(counts: Counter): number {
  return counts.ssn + counts.itin + counts.long_number + counts.labeled_account_number + counts.cvv;
}

// ─── public API ─────────────────────────────────────────────────────────────

/**
 * Scrubs one episode's text. Call this before inserting the episode and before any model sees it:
 *
 *   const r = redact(raw);
 *   // r.redaction_status === 'quarantined'  =>  store redacted_text NULL, never send to a model
 *   // otherwise                             =>  r.redacted_text is the only text a model may read
 */
export function redact(text: string): RedactionResult {
  const canonical = text.normalize('NFKC').replace(ZERO_WIDTH, '');

  if (CREDENTIAL.test(canonical)) {
    return {
      redaction_status: 'quarantined',
      redacted_text: null,
      quarantine_reason: 'credential_detected',
      findings: [],
    };
  }

  const { text: scrubbed, counts } = applyRules(canonical);
  const findings = toFindings(counts);

  if (total(applyRules(scrubbed).counts) > 0) {
    return {
      redaction_status: 'quarantined',
      redacted_text: null,
      quarantine_reason: 'residual_sensitive_pattern',
      findings,
    };
  }

  return {
    redaction_status: total(counts) > 0 ? 'redacted' : 'clean',
    redacted_text: scrubbed,
    findings,
  };
}
