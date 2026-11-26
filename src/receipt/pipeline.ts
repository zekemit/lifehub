/**
 * src/receipt/pipeline.ts — LifeReceiptWF (CLAUDE.md §5: "Sun 08:00 local: assemble -> ground every claim -> email + push").
 *
 *   assemble   build the five sections from the household's own record (src/receipt/assemble.ts)
 *   ground     Gate G1 on every claim (each fact it rests on must still have a verbatim quote) and Gate G7 on every line (no tax
 *              characterization). A line that fails is DROPPED, never softened, and the Found total is recomputed from what is left.
 *   store      claims + claim_support + the receipt, replacing any earlier receipt for the same week
 *   email      to each member, only when `notify` is set (the Sunday cron; an on-demand receipt is not mailed)
 *   push       a payload-less Web Push tickle, same condition
 * Nothing is e-mailed or pushed that did not pass the gates, because everything the gates rejected is already gone by then.
 */

import { sha256Hex } from '../crypto/vault';
import { loadVerifiedEvidence } from '../missions/engine';
import { assertNoTaxLanguage } from '../missions/copy';
import type { StepLike } from '../ingest/steps';
import { notifyHousehold } from '../notify/webPush';
import type { EmailSender, Env, LifeReceiptParams } from '../types';
import { assembleReceipt, type DraftLine, type ReceiptDraft, type WaitingCard } from './assemble';
import { renderReceiptHtml, renderReceiptText } from './render';

export interface ReceiptDeps {
  db: D1Database;
  /** Guarded fetch (Web Push). */
  fetch: typeof fetch;
  env: Pick<Env, 'APP_NAME' | 'INBOUND_DOMAIN' | 'VAPID_PUBLIC_KEY' | 'VAPID_PRIVATE_KEY' | 'VAPID_SUBJECT'> & { RECEIPT_EMAIL?: EmailSender | undefined };
  now: () => Date;
}

export interface ProofQuote {
  quote: string;
  source: string;
  episode_id: string;
  char_start: number;
  char_end: number;
}

export interface ProofEntry {
  claim_id: string;
  text: string;
  facts: Array<{ fact_id: string; predicate: string; quotes: ProofQuote[] }>;
}

export interface ReceiptPayload {
  version: 1;
  receipt_id: string;
  household_id: string;
  week_start: string;
  as_of: string;
  found: { total_cents: number; lines: Array<Omit<DraftLine, 'claim_id'> & { claim_id: string }> };
  done: { lines: Array<Omit<DraftLine, 'claim_id'> & { claim_id: string }>; counts: ReceiptDraft['counts'] };
  waiting: WaitingCard[];
  next_week: Array<Omit<DraftLine, 'claim_id'> & { claim_id: string }>;
  proof: ProofEntry[];
  /** Lines the gates removed (their reasons, not their text). */
  dropped: Array<{ gate: 'G1' | 'G7'; claim_id: string | null }>;
}

export type ReceiptOutcome = { status: 'stored'; receipt_id: string; found_cents: number; dropped: number; emailed: number; pushed: number };

export const receiptIdFor = async (household_id: string, week_start: string): Promise<string> => `rcpt_${(await sha256Hex(`receipt:${household_id}:${week_start}`)).slice(0, 24)}`;

const QUOTES_PER_FACT = 3;
/** D1 allows 1,000 queries per invocation; a receipt this big would not be a receipt anyone reads. */
const MAX_CLAIM_ROWS = 400;

/** Gates G1 and G7 over a draft. Returns the payload with only the lines that passed. */
export async function groundReceipt(db: D1Database, household_id: string, draft: ReceiptDraft, receipt_id: string): Promise<ReceiptPayload> {
  const factIds = [...new Set(draft.claims.flatMap((c) => c.facts).filter((f): f is string => typeof f === 'string' && f !== ''))];
  const evidence = await loadVerifiedEvidence(db, household_id, factIds);
  const dropped: ReceiptPayload['dropped'] = [];
  const kept = new Map<string, ProofEntry>();
  const claimOk = new Map<string, boolean>();
  for (const c of draft.claims) {
    // G1: at least one fact, every fact present and holding at least one quote that is still a verbatim substring of its episode
    const g1 = c.facts.length > 0 && c.facts.every((f) => typeof f === 'string' && (evidence.get(f) ?? []).length > 0);
    let ok = g1;
    let gate: 'G1' | 'G7' = 'G1';
    if (g1) {
      try {
        assertNoTaxLanguage(c.text); // G7
      } catch {
        ok = false;
        gate = 'G7';
      }
    }
    claimOk.set(c.claim_id, ok);
    if (!ok) {
      dropped.push({ gate, claim_id: c.claim_id });
      continue;
    }
    kept.set(c.claim_id, {
      claim_id: c.claim_id,
      text: c.text,
      facts: (c.facts as string[]).map((fact_id) => {
        const quotes = evidence.get(fact_id) ?? [];
        return {
          fact_id,
          predicate: quotes[0]?.predicate ?? '',
          quotes: quotes.slice(0, QUOTES_PER_FACT).map((q) => ({ quote: q.quote, source: q.source, episode_id: q.episode_id, char_start: q.char_start, char_end: q.char_end })),
        };
      }),
    });
  }
  // a line's own words are checked by G7 too (a line can say more than its claim does)
  const lineOk = (l: DraftLine): l is DraftLine & { claim_id: string } => {
    if (l.claim_id === null || claimOk.get(l.claim_id) !== true) return false;
    try {
      assertNoTaxLanguage(l.text);
      return true;
    } catch {
      dropped.push({ gate: 'G7', claim_id: l.claim_id });
      return false;
    }
  };
  const found = draft.found.filter(lineOk);
  const doneLines = draft.done.filter(lineOk);
  const nextWeek = draft.next_week.filter(lineOk);
  const waiting = draft.waiting.filter((w) => {
    if (w.claim_id === null) {
      try {
        assertNoTaxLanguage(w.title, w.why);
        return true;
      } catch {
        dropped.push({ gate: 'G7', claim_id: null });
        return false;
      }
    }
    if (claimOk.get(w.claim_id) !== true) return false;
    try {
      assertNoTaxLanguage(w.title, w.why);
      return true;
    } catch {
      dropped.push({ gate: 'G7', claim_id: w.claim_id });
      return false;
    }
  });
  const used = new Set([...found, ...doneLines, ...nextWeek].map((l) => l.claim_id).concat(waiting.flatMap((w) => (w.claim_id === null ? [] : [w.claim_id]))));
  return {
    version: 1,
    receipt_id,
    household_id,
    week_start: draft.week_start,
    as_of: draft.as_of,
    found: { total_cents: found.reduce((sum, l) => sum + (l.cents ?? 0), 0), lines: found },
    done: { lines: doneLines, counts: draft.counts },
    waiting,
    next_week: nextWeek,
    proof: [...kept.values()].filter((p) => used.has(p.claim_id)),
    dropped,
  };
}

async function store(db: D1Database, payload: ReceiptPayload, at: string): Promise<void> {
  const { receipt_id, household_id } = payload;
  const statements: D1PreparedStatement[] = [
    db.prepare('DELETE FROM claim_support WHERE claim_id IN (SELECT claim_id FROM claims WHERE run_id = ? AND household_id = ?)').bind(receipt_id, household_id),
    db.prepare('DELETE FROM claims WHERE run_id = ? AND household_id = ?').bind(receipt_id, household_id),
  ];
  let rows = 0;
  for (const p of payload.proof) {
    if (rows >= MAX_CLAIM_ROWS) break;
    statements.push(db.prepare(`INSERT INTO claims (claim_id, household_id, run_id, text, status, created_at) VALUES (?, ?, ?, ?, 'grounded', ?)`).bind(p.claim_id, household_id, receipt_id, p.text, at));
    for (const f of p.facts) {
      statements.push(db.prepare('INSERT OR IGNORE INTO claim_support (claim_id, fact_id) VALUES (?, ?)').bind(p.claim_id, f.fact_id));
      rows += 1;
    }
  }
  statements.push(
    db
      .prepare(
        `INSERT INTO life_receipts (receipt_id, household_id, week_start, payload_json, found_cents, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (household_id, week_start) DO UPDATE SET payload_json = excluded.payload_json, found_cents = excluded.found_cents,
                                                              emailed_at = NULL, pushed_at = NULL, updated_at = excluded.updated_at`,
      )
      .bind(receipt_id, household_id, payload.week_start, JSON.stringify(payload), payload.found.total_cents, at, at),
  );
  for (let i = 0; i < statements.length; i += 100) await db.batch(statements.slice(i, i + 100));
}

async function audit(db: D1Database, household_id: string, action: string, detail: Record<string, unknown>, at: string): Promise<void> {
  await db
    .prepare(`INSERT INTO audit_log (id, household_id, actor, action, detail_json, created_at) VALUES (?, ?, 'life-receipt', ?, ?, ?)`)
    .bind(`aud_${crypto.randomUUID().replace(/-/g, '')}`, household_id, action, JSON.stringify(detail), at)
    .run();
}

export async function runLifeReceipt(step: StepLike, deps: ReceiptDeps, params: LifeReceiptParams): Promise<ReceiptOutcome> {
  const { db } = deps;
  const { household_id } = params;
  const clock = (): Date => (params.as_of !== undefined ? new Date(params.as_of) : deps.now());

  const draft = await step.do('assemble', async () => {
    const hh = await db.prepare('SELECT timezone FROM households WHERE household_id = ? AND deleted_at IS NULL').bind(household_id).first<{ timezone: string }>();
    if (hh === null) throw new Error(`household ${household_id} not found`);
    return assembleReceipt(db, household_id, hh.timezone, clock());
  });

  const payload = await step.do('ground', async () => {
    const receipt_id = await receiptIdFor(household_id, draft.week_start);
    const grounded = await groundReceipt(db, household_id, draft, receipt_id);
    for (const d of grounded.dropped) await audit(db, household_id, 'receipt_line_dropped', d, clock().toISOString());
    return grounded;
  });

  await step.do('store', async () => store(db, payload, clock().toISOString()));

  let emailed = 0;
  let pushed = 0;
  if (params.notify) {
    emailed = await step.do('email', async () => {
      const sender = deps.env.RECEIPT_EMAIL;
      if (sender === undefined) {
        await audit(db, household_id, 'receipt_email_skipped', { reason: 'no email binding' }, clock().toISOString());
        return 0;
      }
      const { results } = await db.prepare('SELECT email FROM members WHERE household_id = ? ORDER BY created_at, member_id').bind(household_id).all<{ email: string }>();
      let sent = 0;
      for (const m of results) {
        try {
          await sender.send({
            to: m.email,
            from: `receipts@${deps.env.INBOUND_DOMAIN}`,
            subject: `Your ${deps.env.APP_NAME} week`,
            text: renderReceiptText(payload, deps.env.APP_NAME),
            html: renderReceiptHtml(payload, deps.env.APP_NAME),
          });
          sent += 1;
        } catch (err) {
          await audit(db, household_id, 'receipt_email_failed', { error: err instanceof Error ? err.message.slice(0, 200) : 'error' }, clock().toISOString());
        }
      }
      if (sent > 0) await db.prepare('UPDATE life_receipts SET emailed_at = ? WHERE receipt_id = ?').bind(clock().toISOString(), payload.receipt_id).run();
      return sent;
    });
    pushed = await step.do('push', async () => {
      const res = await notifyHousehold({ db, fetch: deps.fetch, env: deps.env, now: clock }, household_id, 'receipt_ready');
      if (res.sent > 0) await db.prepare('UPDATE life_receipts SET pushed_at = ? WHERE receipt_id = ?').bind(clock().toISOString(), payload.receipt_id).run();
      return res.sent;
    });
  }
  return { status: 'stored', receipt_id: payload.receipt_id, found_cents: payload.found.total_cents, dropped: payload.dropped.length, emailed, pushed };
}
