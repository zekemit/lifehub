/**
 * src/missions/home.ts — the Home screen's data (CLAUDE.md §7): the week's header metrics and the top 3 staged cards,
 * each with the verbatim quotes that ground it ("evidence chips").
 *
 * Evidence is re-verified on the way out: a quote is returned only if it is still a verbatim substring of its episode's
 * redacted text, so a chip can never show words the source does not contain.
 */

import { ACTIVE_STATES, HOME_CAPACITY, loadVerifiedEvidence, type Evidence } from './engine';
import type { MissionState } from '../types';

/** States a card can be shown in: staged, in flight in MissionRunWF, awaiting the member, or holding an issued link. */
const HOME_STATES = ['COMPILED', 'R0_READ', 'GATED', 'R1_STAGED', 'R2_AWAITING', 'R2_APPROVED', 'R3_LINK_ISSUED'] as const;

export interface EvidenceChip {
  claim_id: string;
  claim: string;
  fact_id: string;
  predicate: string;
  quote: string;
  source: string;
  episode_id: string;
  char_start: number;
  char_end: number;
}

export interface HomeLink {
  url: string;
  merchant_domain: string;
  affiliate_program: string | null;
  issued_at: string;
}

export interface HomeCard {
  run_id: string;
  state: MissionState;
  /** Hash of the exact card the member approves; send it back with the approval. Null until MissionRunWF has staged the card. */
  payload_hash: string | null;
  /** Model-written call script (retention / cancel), shown on the card; null for cards without one. */
  script: string | null;
  /** The issued deep link, once MissionRunWF has reached R3. */
  link: HomeLink | null;
  trigger_key: string;
  title: string;
  why: string;
  r2_label: string;
  recoverable_cents: number;
  urgency: number;
  confidence: number;
  score: number;
  /** True when the card rests on a credit period or fee history the data cannot confirm yet. */
  unconfirmed: boolean;
  expires_at: string | null;
  evidence: EvidenceChip[];
}

export interface HomeFeed {
  header: {
    /** Recoverable money surfaced this week, weighted by confidence so unconfirmed cards never overstate it. */
    found_cents: number;
    /** Runs confirmed done this week. */
    filed_count: number;
    /** Staged cards waiting for the member's tap. */
    waiting_count: number;
  };
  cards: HomeCard[];
}

const WEEK_MS = 7 * 86_400_000;

interface RunRow {
  run_id: string;
  state: MissionState;
  params_json: string;
  expires_at: string | null;
}

export async function getHome(db: D1Database, household_id: string, now: Date): Promise<HomeFeed> {
  const iso = now.toISOString();
  const weekAgo = new Date(now.getTime() - WEEK_MS).toISOString();

  const found = await db
    .prepare(
      `SELECT COALESCE(SUM(CAST(json_extract(params_json, '$.recoverable_cents') * COALESCE(json_extract(params_json, '$.confidence'), 1.0) AS INTEGER)), 0) AS cents
         FROM mission_runs
        WHERE household_id = ? AND created_at >= ? AND state IN (${[...ACTIVE_STATES, 'CONFIRMED'].map((s) => `'${s}'`).join(', ')})`,
    )
    .bind(household_id, weekAgo)
    .first<{ cents: number }>();
  const filed = await db
    .prepare(`SELECT COUNT(*) AS n FROM mission_runs WHERE household_id = ? AND state = 'CONFIRMED' AND updated_at >= ?`)
    .bind(household_id, weekAgo)
    .first<{ n: number }>();
  const waiting = await db
    .prepare(
      `SELECT COUNT(*) AS n FROM mission_runs
        WHERE household_id = ? AND state IN ('R1_STAGED', 'R2_AWAITING') AND (expires_at IS NULL OR expires_at >= ?)`,
    )
    .bind(household_id, iso)
    .first<{ n: number }>();

  const { results: runs } = await db
    .prepare(
      `SELECT run_id, state, params_json, expires_at FROM mission_runs
        WHERE household_id = ? AND state IN (${HOME_STATES.map((s) => `'${s}'`).join(', ')}) AND (expires_at IS NULL OR expires_at >= ?)
        ORDER BY CASE state WHEN 'R3_LINK_ISSUED' THEN 0 WHEN 'R2_APPROVED' THEN 1 ELSE 2 END,
                 json_extract(params_json, '$.score') DESC, created_at DESC, run_id
        LIMIT ?`,
    )
    .bind(household_id, iso, HOME_CAPACITY)
    .all<RunRow>();

  const cards: HomeCard[] = [];
  for (const run of runs) {
    const p = JSON.parse(run.params_json) as Record<string, unknown>;
    const { results: claims } = await db
      .prepare('SELECT claim_id, text FROM claims WHERE run_id = ? AND household_id = ? ORDER BY claim_id')
      .bind(run.run_id, household_id)
      .all<{ claim_id: string; text: string }>();
    const { results: support } = await db
      .prepare(
        `SELECT s.claim_id, s.fact_id FROM claim_support s JOIN claims c ON c.claim_id = s.claim_id WHERE c.run_id = ? AND c.household_id = ?`,
      )
      .bind(run.run_id, household_id)
      .all<{ claim_id: string; fact_id: string }>();
    const evidence = await loadVerifiedEvidence(db, household_id, [...new Set(support.map((s) => s.fact_id))]);
    const chips: EvidenceChip[] = [];
    for (const claim of claims) {
      const seen = new Set<string>();
      for (const s of support.filter((x) => x.claim_id === claim.claim_id)) {
        const first: Evidence | undefined = (evidence.get(s.fact_id) ?? [])[0];
        if (first === undefined || seen.has(first.fact_id)) continue;
        seen.add(first.fact_id);
        chips.push({
          claim_id: claim.claim_id,
          claim: claim.text,
          fact_id: first.fact_id,
          predicate: first.predicate,
          quote: first.quote,
          source: first.source,
          episode_id: first.episode_id,
          char_start: first.char_start,
          char_end: first.char_end,
        });
      }
    }
    const link = p['link'] as Partial<HomeLink> | null | undefined;
    const payload = p['payload'] as { script?: unknown } | null | undefined;
    cards.push({
      run_id: run.run_id,
      state: run.state,
      payload_hash: typeof p['payload_hash'] === 'string' ? p['payload_hash'] : null,
      script: typeof payload?.script === 'string' ? payload.script : null,
      link:
        link !== null && link !== undefined && typeof link.url === 'string'
          ? { url: link.url, merchant_domain: String(link.merchant_domain ?? ''), affiliate_program: typeof link.affiliate_program === 'string' ? link.affiliate_program : null, issued_at: String(link.issued_at ?? '') }
          : null,
      trigger_key: String(p['trigger_key'] ?? ''),
      title: String(p['title'] ?? ''),
      why: String(p['why'] ?? ''),
      r2_label: String(p['r2_label'] ?? ''),
      recoverable_cents: Number(p['recoverable_cents'] ?? 0),
      urgency: Number(p['urgency'] ?? 0),
      confidence: Number(p['confidence'] ?? 1),
      score: Number(p['score'] ?? 0),
      unconfirmed: p['unconfirmed'] === true,
      expires_at: run.expires_at,
      evidence: chips,
    });
  }

  return { header: { found_cents: found?.cents ?? 0, filed_count: filed?.n ?? 0, waiting_count: waiting?.n ?? 0 }, cards };
}
