/**
 * src/ingest/extractors.ts — what to extract from an email, decided by the bucket the triage funnel put it in (src/ingest/inboxTriage.ts).
 *
 * The model still only PROPOSES (CLAUDE.md §0): extractFacts() locates every quote itself and writeFact() re-verifies it. This module adds two
 * deterministic things around that:
 *   - a PROFILE per bucket: the predicates the model may use and a short focus note, so a receipt is read for its price, return window,
 *     warranty and serial number and not for everything the graph knows about. A bucket with no profile, or no bucket, uses every predicate.
 *   - sanitizeProposals(): the subject of a life-admin fact must be the right kind of entity (a price belongs to a PRODUCT, a delay to a TRIP, a
 *     registration to an EVENT) and its number must be believable. The standing triggers in migrations/0004 join on exactly those subjects, so a
 *     fact on the wrong subject could never ground a card; it is dropped here instead of sitting in the graph looking like evidence.
 *
 * The three standing triggers these facts feed (T_CLOSING_RETURN_OR_WARRANTY, T_PRICE_DROP_OR_DELAY_CLAIM, T_FAMILY_EVENT_AUTOPILOT) are in
 * src/missions/triggers.ts.
 */

import type { EntityType, FactProposal } from '../types';
import type { TriageBucket } from './inboxTriage';

export interface ExtractionProfile {
  bucket: TriageBucket;
  /** The predicates the model may use; null means every predicate in the registry. */
  predicates: readonly string[] | null;
  /** Extra instructions appended to the extraction prompt. */
  focus: readonly string[];
}

const PRODUCT_NOTE =
  "Use subject_type 'product' and the product's name exactly as the document writes it (for example 'Dyson V15 Detect') as the subject of every product fact. " +
  'Name the product the same way every time so that later messages about it resolve to the same product.';

const PROFILES: Readonly<Partial<Record<TriageBucket, ExtractionProfile>>> = {
  RECEIPT_ORDER: {
    bucket: 'RECEIPT_ORDER',
    predicates: ['purchase_price_cents', 'return_window_closes_on', 'warranty_expires_on', 'serial_number', 'price_drop_detected_cents'],
    focus: [
      'This is a purchase receipt, an order confirmation, or a price notice about something already bought.',
      PRODUCT_NOTE,
      'purchase_price_cents: the price of that one item as stated, in cents; not the order total when both are shown.',
      "return_window_closes_on: the date the document says returns or replacements are accepted until. Only when a date is stated; never add days to the purchase date yourself.",
      'warranty_expires_on: only when the document states the date the warranty ends.',
      'serial_number: only when the document prints one.',
      'price_drop_detected_cents: only when the document says the price of that item fell, and then the size of the drop in cents.',
    ],
  },
  WARRANTY_PRODUCT: {
    bucket: 'WARRANTY_PRODUCT',
    predicates: ['warranty_expires_on', 'serial_number', 'purchase_price_cents', 'return_window_closes_on'],
    focus: [
      'This is a warranty, product registration or product notice.',
      PRODUCT_NOTE,
      'warranty_expires_on: the date the warranty ends, only when stated. serial_number: only when printed.',
    ],
  },
  TRAVEL_ITINERARY: {
    bucket: 'TRAVEL_ITINERARY',
    predicates: ['trip_window', 'trip_destination', 'flight_delay_minutes', 'prefers_airline'],
    focus: [
      'This is a travel booking or a change to one.',
      "Use subject_type 'trip' and name the trip as '<origin> to <destination> on <departure date YYYY-MM-DD>' for every trip fact, the same way in every message about that flight.",
      'flight_delay_minutes: only when the document states how long the flight is delayed, in minutes (convert hours to minutes). Leave it out for a cancellation that gives no delay.',
    ],
  },
  FAMILY_SCHOOL_EVENT: {
    bucket: 'FAMILY_SCHOOL_EVENT',
    predicates: ['event_date', 'event_kind', 'event_window', 'registration_opens_at', 'required_checklist_item', 'attends'],
    focus: [
      'This is a school, camp, sports or activity message.',
      "Use subject_type 'event' and the event's name as written as the subject of every event fact.",
      'event_window: the first and last day as <start>/<end>. event_date: a single date. registration_opens_at: the date registration opens, only when stated.',
      'required_checklist_item: one fact per item the family must bring or complete (a form, a fee, a vaccination record), quoted as written.',
    ],
  },
  BILL_SUBSCRIPTION: {
    bucket: 'BILL_SUBSCRIPTION',
    predicates: null,
    focus: ['This is a bill, statement or subscription notice. Prefer the recurring-charge, amount and next-charge predicates.'],
  },
  TAX_LEGAL_DOC: {
    bucket: 'TAX_LEGAL_DOC',
    predicates: null,
    focus: ['This is a tax, insurance or legal document. Prefer the document predicates. Describe what the document says; never characterize tax treatment.'],
  },
  PERSONAL_FOLLOWUP_NEEDED: {
    bucket: 'PERSONAL_FOLLOWUP_NEEDED',
    predicates: null,
    focus: [],
  },
};

/** The profile for a bucket; null (every predicate, no focus note) for an unclassified message. IGNORE never reaches extraction. */
export function extractionProfile(bucket: TriageBucket | null): ExtractionProfile | null {
  return bucket === null ? null : (PROFILES[bucket] ?? null);
}

/** The kind of entity each life-admin predicate must be about. */
export const SUBJECT_OF: Readonly<Record<string, EntityType>> = {
  purchase_price_cents: 'product',
  return_window_closes_on: 'product',
  warranty_expires_on: 'product',
  serial_number: 'product',
  price_drop_detected_cents: 'product',
  flight_delay_minutes: 'trip',
  registration_opens_at: 'event',
  required_checklist_item: 'event',
  event_window: 'event',
};

/** A delay longer than a week is a misreading (a date read as minutes), not a delay. */
export const MAX_DELAY_MINUTES = 7 * 24 * 60;

export interface Sanitized {
  kept: FactProposal[];
  /** Proposals removed because the subject was the wrong kind of entity or the value was not believable. */
  dropped: number;
}

function believable(p: FactProposal): boolean {
  switch (p.predicate) {
    case 'purchase_price_cents':
    case 'price_drop_detected_cents':
      return typeof p.value_cents === 'number' && Number.isSafeInteger(p.value_cents) && p.value_cents > 0;
    case 'flight_delay_minutes':
      return typeof p.value_num === 'number' && Number.isFinite(p.value_num) && p.value_num > 0 && p.value_num <= MAX_DELAY_MINUTES;
    case 'serial_number': {
      const v = (p.value_text ?? '').trim();
      return v.length >= 4 && v.length <= 64 && /[0-9]/.test(v) && !/\s{2,}/.test(v);
    }
    case 'required_checklist_item': {
      const v = (p.value_text ?? '').trim();
      return v.length >= 3 && v.length <= 200;
    }
    default:
      return true;
  }
}

/** Drops life-admin facts about the wrong kind of subject, or with a value that cannot be right. Every other proposal passes through untouched. */
export function sanitizeProposals(proposals: readonly FactProposal[]): Sanitized {
  const kept: FactProposal[] = [];
  let dropped = 0;
  for (const p of proposals) {
    const subject = SUBJECT_OF[p.predicate];
    if (subject !== undefined && (!('type' in p.subject) || p.subject.type !== subject || !believable(p))) {
      dropped += 1;
      continue;
    }
    kept.push(p);
  }
  return { kept, dropped };
}
