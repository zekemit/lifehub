/**
 * Milestone 1 behavioural suite: redaction engine + bi-temporal fact writer.
 * Runs against a REAL local D1 (migrations/0001_init.sql applied by scripts/test.mjs).
 */

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { acctTokenPattern, redact } from '../src/redact';
import { aliasKey, locateQuote, toIsoTimestamp, writeFact, type EntityAdjudicator } from '../src/graph/writeFact';
import type { FactProposal, WriteFactResult } from '../src/types';
import { openTestRuntime, type TestRuntime } from './helpers';

let rt: TestRuntime;
let db: D1Database;
before(async () => {
  rt = await openTestRuntime();
  db = rt.db;
});
after(async () => {
  await rt.dispose();
});

// ═══════════════════════════ redaction ═══════════════════════════

const ok = (s: string) => { const r = redact(s); assert.notEqual(r.redaction_status, 'quarantined', s); return r as Extract<ReturnType<typeof redact>, {redacted_text: string}>; };

test('SSN formats', () => {
  assert.equal(ok('SSN 123-45-6789 ok').redacted_text, 'SSN [SSN] ok');
  assert.equal(ok('123 45 6789').redacted_text, '[SSN]');
  assert.equal(ok('123.45.6789').redacted_text, '[SSN]');
  assert.equal(ok('123–45–6789').redacted_text, '[SSN]');
  assert.equal(ok('x 078-05-1120.').redacted_text, 'x [SSN].');
});
test('ITIN vs SSN', () => {
  assert.equal(ok('ITIN 912-70-1234').redacted_text, 'ITIN [ITIN]');
  assert.equal(ok('912-69-1234').redacted_text, '[SSN]');        // 69 is not an ITIN group
  assert.equal(ok('812-70-1234').redacted_text, '[SSN]');        // does not start with 9
  assert.equal(ok('950-93-1234').redacted_text, '[SSN]');        // 93 excluded
  assert.equal(ok('994-99-0000').redacted_text, '[ITIN]');
});
test('labeled bare 9-digit', () => {
  assert.equal(ok('SSN: 123456789').redacted_text, 'SSN: [SSN]');
  assert.equal(ok('Social Security Number is 123456789.').redacted_text, 'Social Security Number is [SSN].');
  assert.equal(ok('SS # 123456789').redacted_text, 'SS # [SSN]');
  assert.equal(ok('Taxpayer ID number: 912701234').redacted_text, 'Taxpayer ID number: [ITIN]');
  assert.equal(ok('order 123456789 shipped').redacted_text, 'order 123456789 shipped'); // unlabeled 9 digits untouched
});
test('cards keep last4', () => {
  assert.equal(ok('Visa 4111111111111111 exp').redacted_text, 'Visa [ACCT ****1111] exp');
  assert.equal(ok('4111 1111 1111 1111').redacted_text, '[ACCT ****1111]');
  assert.equal(ok('4111-1111-1111-1111').redacted_text, '[ACCT ****1111]');
  assert.equal(ok('3782 822463 10005').redacted_text, '[ACCT ****0005]');          // Amex 4-6-5
  assert.equal(ok('3056 930902 5904').redacted_text, '[ACCT ****5904]');            // Diners 4-6-4
  assert.equal(ok('378282246310005').redacted_text, '[ACCT ****0005]');             // 15 contiguous
  assert.equal(ok('6011 1111 1111 1117 123').redacted_text, '[ACCT ****7123]');     // 19 digits, 5 groups
  assert.equal(ok('4111111111111').redacted_text, '[ACCT ****1111]');               // 13 digits
  assert.equal(ok('1234567890123456789').redacted_text, '[ACCT ****6789]');         // 19 digits
  assert.equal(ok('12345678901234567890123').redacted_text, '[ACCT ****0123]');     // >19 still redacted
  assert.equal(ok('４１１１１１１１１１１１１１１１').redacted_text, '[ACCT ****1111]'); // full-width digits
  assert.equal(ok('4111​1111​1111​1111').redacted_text, '[ACCT ****1111]'); // zero-width evasion
});
test('does not eat dates, money, short numbers, phones', () => {
  for (const s of ['2026-11-26 2026-11-27', 'Check-in 2026-12-01 2026-12-05 two nights', '$1,234,567,890,123.45', 'call 1-800-555-1234',
    '+44 7911 123456', '12 Mar 2026', 'Total: 123456789012 cents? no that is 12 digits', 'ref 1234 5678 9012', 'ssn ending in 6789', 'card ending 1234', 'Account: 2026-11-26',
    'account ending in 12345678']) {
    assert.equal(ok(s).redacted_text, s, s);
    assert.equal(ok(s).redaction_status, 'clean', s);
  }
});
test('labeled account / routing / cvv', () => {
  assert.equal(ok('Account number: 123456789012').redacted_text, 'Account number: [ACCT ****9012]');
  assert.equal(ok('routing 021000021').redacted_text, 'routing [ACCT ****0021]');
  assert.equal(ok('Acct #: 1234 5678').redacted_text, 'Acct #: [ACCT ****5678]');
  assert.equal(ok('account 1234567').redacted_text, 'account 1234567'); // 7 digits: below threshold
  assert.equal(ok('CVV: 123').redacted_text, 'CVV: [CVV]');
  assert.equal(ok('Security code 4821').redacted_text, 'Security code [CVV]');
});
test('credentials quarantine', () => {
  for (const s of ['Your temporary password: Xk39!aZq', 'PIN is 4821.', 'verification code: 482913', 'passcode=ab12cd', 'one-time code: 123456']) {
    const r = redact(s); assert.equal(r.redaction_status, 'quarantined', s); assert.equal(r.redacted_text, null);
  }
  for (const s of ['Password: ********', 'A password is required', 'pin is loose', 'Reset your password here']) {
    assert.notEqual(redact(s).redaction_status, 'quarantined', s);
  }
});
test('idempotent + findings never contain values + status', () => {
  const raw = 'SSN 123-45-6789, card 4111 1111 1111 1111, acct number 123456789012, CVV 123';
  const r = ok(raw);
  assert.equal(r.redaction_status, 'redacted');
  assert.deepEqual(r.findings, [{ kind: 'ssn', count: 1 }, { kind: 'long_number', count: 1 }, { kind: 'labeled_account_number', count: 1 }, { kind: 'cvv', count: 1 }]);
  const again = ok(r.redacted_text);
  assert.equal(again.redaction_status, 'clean');
  assert.equal(again.redacted_text, r.redacted_text);
  assert.ok(!JSON.stringify(r.findings).match(/\d{4}/));
  assert.equal([...r.redacted_text.matchAll(acctTokenPattern())].map(m => m[1]).join(','), '1111,9012');
  assert.equal(ok('').redaction_status, 'clean');
});
test('multiple and adjacent', () => {
  assert.equal(ok('4111111111111111 and 5500000000000004').redacted_text, '[ACCT ****1111] and [ACCT ****0004]');
  assert.equal(ok('123-45-6789 4111 1111 1111 1111').redacted_text, '[SSN] [ACCT ****1111]');
  assert.equal(ok('2026-11-26 4111 1111 1111 1111').redacted_text, '2026-11-26 [ACCT ****1111]');
});

// ═══════════════════════════ writeFact ═══════════════════════════

// ── helpers ──
const hhId = () => `hh_${randomUUID().slice(0, 8)}`;
async function episode(hh: string, text: string | null, o: { occurred_at?: string | null; status?: string } = {}) {
  const id = `ep_${randomUUID().slice(0, 8)}`;
  await db.prepare(`INSERT INTO episodes (episode_id, household_id, source, redacted_text, sha256, occurred_at, redaction_status) VALUES (?,?,?,?,?,?,?)`)
    .bind(id, hh, 'email', text, randomUUID(), o.occurred_at ?? null, o.status ?? 'redacted').run();
  return id;
}
const q = (text: string, quote: string) => ({ quote, ...locateQuote(text, quote)! });
const all = async <T>(sql: string, ...b: unknown[]) => (await db.prepare(sql).bind(...b).all<T>()).results;
const one = async <T>(sql: string, ...b: unknown[]) => (await db.prepare(sql).bind(...b).first<T>())!;
const written = (r: WriteFactResult) => { assert.equal(r.status, 'written', JSON.stringify(r)); return r as Extract<WriteFactResult, { status: 'written' }>; };
const rejected = (r: WriteFactResult, reason: string) => { assert.equal(r.status, 'rejected', JSON.stringify(r)); assert.equal((r as any).reason, reason, JSON.stringify(r)); };

function fee(hh: string, ep: string, text: string, dollars: number, o: Partial<FactProposal> = {}): FactProposal {
  return { household_id: hh, source_episode_id: ep, subject: { type: 'card_account', name: 'Amex Platinum' }, predicate: 'annual_fee', value_cents: dollars * 100,
    confidence: 0.9, quotes: [q(text, `$${dollars}`)], ...o };
}

// ── tests ──
test('happy path writes fact, quotes, entity, alias atomically', async () => {
  const hh = hhId(); const text = 'Your Platinum Card annual fee is $895 per year.';
  const ep = await episode(hh, text, { occurred_at: '2025-09-01T10:00:00Z' });
  const r = written(await writeFact(db, fee(hh, ep, text, 895)));
  const f = await one<any>('SELECT * FROM facts WHERE fact_id=?', r.fact_id);
  assert.equal(f.value_cents, 89500); assert.equal(f.currency, 'USD'); assert.equal(f.valid_from, '2025-09-01T10:00:00.000Z');
  assert.equal(f.valid_to, null); assert.equal(f.expired_at, null); assert.equal(f.learned_at, r.learned_at); assert.equal(f.source_episode_id, ep);
  const quotes = await all<any>('SELECT * FROM fact_quotes WHERE fact_id=?', r.fact_id);
  assert.equal(quotes.length, 1); assert.equal(text.slice(quotes[0].char_start, quotes[0].char_end), quotes[0].quote);
  assert.equal(r.created_entity_ids.length, 1);
  const alias = await one<any>('SELECT * FROM entity_aliases WHERE household_id=?', hh);
  assert.equal(alias.alias_norm, 'card_account:amex platinum'); assert.equal(alias.method, 'deterministic');
  assert.equal((await all('SELECT 1 FROM facts_current WHERE household_id=?', hh)).length, 1);
});

test('quote mismatch rejects and leaves NOTHING behind (no orphan entities)', async () => {
  const hh = hhId(); const text = 'annual fee is $895';
  const ep = await episode(hh, text);
  const bad = fee(hh, ep, text, 895, { quotes: [{ quote: '$995', char_start: 14, char_end: 18 }] });
  rejected(await writeFact(db, bad), 'quote_mismatch');
  assert.equal((await all('SELECT 1 FROM entities WHERE household_id=?', hh)).length, 0);
  assert.equal((await all('SELECT 1 FROM facts WHERE household_id=?', hh)).length, 0);
  // right text, wrong offsets
  rejected(await writeFact(db, fee(hh, ep, text, 895, { quotes: [{ quote: '$895', char_start: 0, char_end: 4 }] })), 'quote_mismatch');
  // quote from caller that is not in the episode at all
  rejected(await writeFact(db, fee(hh, ep, text, 895, { quotes: [{ quote: 'totally invented', char_start: 0, char_end: 16 }] })), 'quote_mismatch');
});

test('input validation', async () => {
  const hh = hhId(); const text = 'fee $895 and birthday 1985-03-12';
  const ep = await episode(hh, text);
  rejected(await writeFact(db, fee(hh, ep, text, 895, { quotes: [] })), 'no_quotes');
  rejected(await writeFact(db, fee(hh, ep, text, 895, { confidence: 1.2 })), 'invalid_input');
  rejected(await writeFact(db, fee(hh, ep, text, 895, { quotes: [{ quote: 'a', char_start: 0, char_end: 1 }] })), 'invalid_input'); // too short
  rejected(await writeFact(db, fee(hh, ep, text, 895, { predicate: 'nope' })), 'unknown_predicate');
  rejected(await writeFact(db, fee(hh, ep, text, 895, { valid_from: 'last tuesday' })), 'invalid_input');
  rejected(await writeFact(db, fee(hh, ep, text, 895, { valid_from: '2026-02-30' })), 'invalid_input');
  rejected(await writeFact(db, fee(hh, ep, text, 895, { valid_from: '2026-03-01', valid_to: '2026-02-01' })), 'invalid_input');
  rejected(await writeFact(db, fee(hh, ep, text, 895, { subject: { type: 'bogus' as any, name: 'x' } })), 'invalid_input');
  rejected(await writeFact(db, fee(hh, ep, text, 895, { subject: { type: 'person', name: '!!!' } })), 'invalid_input');
  rejected(await writeFact(db, { ...fee(hh, ep, text, 895), value_cents: 8.95 }), 'invalid_value');
  rejected(await writeFact(db, { ...fee(hh, ep, text, 895), value_cents: null, value_text: '$895' }), 'invalid_value');
  rejected(await writeFact(db, { ...fee(hh, ep, text, 895), currency: 'dollars' }), 'invalid_value');
  rejected(await writeFact(db, { ...fee(hh, ep, text, 895), value_text: 'x' }), 'invalid_value'); // stray field
  // date predicate
  const b = (v: string) => ({ household_id: hh, source_episode_id: ep, subject: { type: 'person', name: 'Ava' } as const, predicate: 'birthday', value_text: v, confidence: 0.9, quotes: [q(text, '1985-03-12')] });
  rejected(await writeFact(db, b('1985-02-30')), 'invalid_value');
  rejected(await writeFact(db, b('March 12 1985')), 'invalid_value');
  written(await writeFact(db, b('1985-03-12')));
  // entity predicate without object
  rejected(await writeFact(db, { household_id: hh, source_episode_id: ep, subject: { type: 'person', name: 'Ava' }, predicate: 'spouse_of', confidence: 0.9, quotes: [q(text, 'fee $895')] }), 'invalid_value');
  // interval predicate
  const iv = (v: string) => ({ household_id: hh, source_episode_id: ep, subject: { type: 'trip', name: 'Paris' } as const, predicate: 'trip_window', value_text: v, confidence: 0.9, quotes: [q(text, 'fee $895')] });
  rejected(await writeFact(db, iv('2026-12-05/2026-12-01')), 'invalid_value');
  rejected(await writeFact(db, iv('2026-12-01')), 'invalid_value');
  const okIv = written(await writeFact(db, iv('2026-12-01/2026-12-05T10:00:00+01:00')));
  assert.equal((await one<any>('SELECT value_text FROM facts WHERE fact_id=?', okIv.fact_id)).value_text, '2026-12-01/2026-12-05T09:00:00.000Z');
  // num via 'many' predicate? use text predicate with whitespace collapse
  const t = written(await writeFact(db, { household_id: hh, source_episode_id: ep, subject: { type: 'person', name: 'Ava' }, predicate: 'home_city', value_text: '  New   York ', confidence: 0.8, quotes: [q(text, 'fee $895')] }));
  assert.equal((await one<any>('SELECT value_text FROM facts WHERE fact_id=?', t.fact_id)).value_text, 'New York');
});

test('episode guards: missing, other household, quarantined, pending', async () => {
  const hh = hhId(); const other = hhId(); const text = 'annual fee is $895';
  const ep = await episode(hh, text);
  rejected(await writeFact(db, fee(hh, 'ep_missing', text, 895)), 'episode_not_found');
  rejected(await writeFact(db, fee(other, ep, text, 895)), 'episode_not_found');
  const quarantined = await episode(hh, null, { status: 'quarantined' });
  rejected(await writeFact(db, fee(hh, quarantined, text, 895)), 'episode_not_redacted');
  const pending = await episode(hh, text, { status: 'pending' });
  rejected(await writeFact(db, fee(hh, pending, text, 895)), 'episode_not_redacted');
  const clean = await episode(hh, text, { status: 'clean' });
  written(await writeFact(db, fee(hh, clean, text, 895)));
});

test('alias resolution: surface variants share one entity; type is part of the key', async () => {
  const hh = hhId(); const text = 'Amex Platinum fee $895; AMEX  platinum!! also';
  const ep = await episode(hh, text, { occurred_at: '2025-09-01' });
  const a = written(await writeFact(db, fee(hh, ep, text, 895)));
  const b = written(await writeFact(db, fee(hh, ep, text, 895, { subject: { type: 'card_account', name: 'AMEX  platinum!!' }, predicate: 'card_last4', value_cents: undefined, value_text: 'fee $895'.slice(0, 3) === 'fee' ? 'x1y2' : '' , quotes: [q(text, 'AMEX  platinum!!')] })));
  assert.equal(a.created_entity_ids.length, 1); assert.equal(b.created_entity_ids.length, 0);
  const fa = await one<any>('SELECT subject_id FROM facts WHERE fact_id=?', a.fact_id); const fb = await one<any>('SELECT subject_id FROM facts WHERE fact_id=?', b.fact_id);
  assert.equal(fa.subject_id, fb.subject_id);
  // same name, different type => different entity
  const c = written(await writeFact(db, { household_id: hh, source_episode_id: ep, subject: { type: 'merchant', name: 'Amex Platinum' }, predicate: 'event_kind', value_text: 'noise', confidence: 0.5, quotes: [q(text, 'Amex Platinum')] }));
  assert.equal(c.created_entity_ids.length, 1);
  assert.equal(aliasKey('merchant', 'Café  Rouge'), 'merchant:cafe rouge');
});

test('WORLD CHANGE: later valid_from closes the old fact, nothing is expired', async () => {
  const hh = hhId();
  const t1 = 'fee is $695'; const e1 = await episode(hh, t1, { occurred_at: '2025-01-01' });
  const t2 = 'new fee is $895'; const e2 = await episode(hh, t2, { occurred_at: '2025-09-01' });
  const old = written(await writeFact(db, fee(hh, e1, t1, 695)));
  const neu = written(await writeFact(db, fee(hh, e2, t2, 895)));
  assert.deepEqual(neu.superseded, [{ fact_id: old.fact_id, kind: 'world_change' }]);
  const o = await one<any>('SELECT * FROM facts WHERE fact_id=?', old.fact_id);
  assert.equal(o.valid_to, '2025-09-01T00:00:00.000Z'); assert.equal(o.expired_at, null); assert.equal(o.superseded_by, null);
  const cur = await all<any>('SELECT value_cents FROM facts_current WHERE household_id=?', hh);
  assert.deepEqual(cur.map(x => x.value_cents), [89500]);
  const audit = await all<any>(`SELECT action FROM audit_log WHERE household_id=? AND actor='writeFact'`, hh);
  assert.deepEqual(audit.map(a => a.action), ['fact_world_change']);
});

test('CORRECTION: same/earlier valid_from from non-older evidence expires + supersedes', async () => {
  const hh = hhId();
  const t1 = 'fee is $795'; const e1 = await episode(hh, t1, { occurred_at: '2025-09-01' });
  const t2 = 'fee is actually $895'; const e2 = await episode(hh, t2, { occurred_at: '2025-10-15' });
  const old = written(await writeFact(db, fee(hh, e1, t1, 795)));
  const neu = written(await writeFact(db, fee(hh, e2, t2, 895, { valid_from: '2025-09-01' })));
  assert.deepEqual(neu.superseded, [{ fact_id: old.fact_id, kind: 'correction' }]);
  const o = await one<any>('SELECT * FROM facts WHERE fact_id=?', old.fact_id);
  assert.equal(o.expired_at, neu.learned_at); assert.equal(o.superseded_by, neu.fact_id); assert.equal(o.valid_to, null);
  assert.deepEqual((await all<any>('SELECT value_cents FROM facts WHERE household_id=? AND expired_at IS NULL', hh)).map(x => x.value_cents), [89500]);
  assert.equal((await all('SELECT 1 FROM facts WHERE household_id=?', hh)).length, 2); // old row is kept (bi-temporal history)
  // earlier valid_from also corrects
  const t3 = 'fee is $850'; const e3 = await episode(hh, t3, { occurred_at: '2025-11-01' });
  const r3 = written(await writeFact(db, fee(hh, e3, t3, 850, { valid_from: '2025-06-01' })));
  assert.equal(r3.superseded[0]!.kind, 'correction');
});

test('OUT-OF-ORDER ingestion: older evidence is back-filled, never overrides newer', async () => {
  const run = async (order: 'newest-first' | 'oldest-first') => {
    const hh = hhId();
    const tJun = 'price now $17.99'; const eJun = await episode(hh, tJun, { occurred_at: '2026-06-01' });
    const tJan = 'price is $15.99'; const eJan = await episode(hh, tJan, { occurred_at: '2026-01-01' });
    const sub = { type: 'recurring_charge', name: 'Netflix' } as const;
    const mk = (ep: string, t: string, cents: number, quote: string): FactProposal => ({ household_id: hh, source_episode_id: ep, subject: sub, predicate: 'recurring_amount', value_cents: cents, confidence: 0.9, quotes: [q(t, quote)] });
    const jun = () => writeFact(db, mk(eJun, tJun, 1799, '$17.99'));
    const jan = () => writeFact(db, mk(eJan, tJan, 1599, '$15.99'));
    const [first, second] = order === 'newest-first' ? [jun, jan] : [jan, jun];
    written(await first()); written(await second());
    const rows = await all<any>('SELECT value_cents, valid_from, valid_to, expired_at FROM facts WHERE household_id=? ORDER BY valid_from', hh);
    assert.deepEqual(rows, [
      { value_cents: 1599, valid_from: '2026-01-01T00:00:00.000Z', valid_to: '2026-06-01T00:00:00.000Z', expired_at: null },
      { value_cents: 1799, valid_from: '2026-06-01T00:00:00.000Z', valid_to: null, expired_at: null },
    ], order);
    assert.deepEqual((await all<any>('SELECT value_cents FROM facts_current WHERE household_id=?', hh)).map(r => r.value_cents), [1799]);
  };
  await run('newest-first'); await run('oldest-first');
});

test('stale evidence with identical valid_from is rejected without changes', async () => {
  const hh = hhId();
  const tNew = 'fee $895'; const eNew = await episode(hh, tNew, { occurred_at: '2025-10-01' });
  const tOld = 'fee $795'; const eOld = await episode(hh, tOld, { occurred_at: '2025-09-01' });
  written(await writeFact(db, fee(hh, eNew, tNew, 895, { valid_from: '2025-09-01' })));
  rejected(await writeFact(db, fee(hh, eOld, tOld, 795, { valid_from: '2025-09-01' })), 'stale_evidence');
  assert.equal((await all('SELECT 1 FROM facts WHERE household_id=?', hh)).length, 1);
});

test('REAFFIRM: equal fact adds support only; retries are idempotent', async () => {
  const hh = hhId(); const text = 'fee is $895';
  const e1 = await episode(hh, text, { occurred_at: '2025-09-01' }); const e2 = await episode(hh, text, { occurred_at: '2025-12-01' });
  const a = written(await writeFact(db, fee(hh, e1, text, 895)));
  const b = await writeFact(db, fee(hh, e2, text, 895));
  assert.deepEqual(b, { status: 'reaffirmed', fact_id: a.fact_id });
  const again = await writeFact(db, fee(hh, e2, text, 895)); assert.deepEqual(again, { status: 'reaffirmed', fact_id: a.fact_id });
  assert.equal((await all('SELECT 1 FROM facts WHERE household_id=?', hh)).length, 1);
  assert.equal((await all('SELECT 1 FROM fact_quotes WHERE fact_id=?', a.fact_id)).length, 2); // one per episode, no dup on retry
  assert.equal((await all(`SELECT 1 FROM audit_log WHERE household_id=?`, hh)).length, 0);
  // case/whitespace-insensitive text equality
  const t = 'she lives in New York';
  const e3 = await episode(hh, t);
  const mk = (v: string) => ({ household_id: hh, source_episode_id: e3, subject: { type: 'person', name: 'Ava' } as const, predicate: 'home_city', value_text: v, confidence: 0.7, quotes: [q(t, 'New York')] });
  const c = written(await writeFact(db, mk('New York'))); assert.deepEqual(await writeFact(db, mk(' new   york ')), { status: 'reaffirmed', fact_id: c.fact_id });
});

test("cardinality 'many': distinct values coexist, equal ones reaffirm", async () => {
  const hh = hhId(); const text = 'dined at Carbone and Via Carota; Carbone again';
  const ep = await episode(hh, text);
  const mk = (venue: string, quoted = venue) => ({ household_id: hh, source_episode_id: ep, subject: { type: 'household', name: 'Smith' } as const, predicate: 'dined_at', object: { type: 'venue', name: venue } as const, confidence: 0.8, quotes: [q(text, quoted)] });
  const a = written(await writeFact(db, mk('Carbone'))); const b = written(await writeFact(db, mk('Via Carota')));
  assert.notEqual(a.fact_id, b.fact_id); assert.deepEqual(a.superseded, []);
  assert.deepEqual(await writeFact(db, mk('carbone', 'Carbone')), { status: 'reaffirmed', fact_id: a.fact_id });
  assert.equal((await all('SELECT 1 FROM facts_current WHERE household_id=? AND predicate=?', hh, 'dined_at')).length, 2);
});

test('partial overlap is rejected (no silent loss of an open fact)', async () => {
  const hh = hhId(); const text = 'fee $795 then $895 for one month';
  const e = await episode(hh, text, { occurred_at: '2025-01-01' });
  written(await writeFact(db, fee(hh, e, text, 795)));
  const e2 = await episode(hh, text, { occurred_at: '2025-04-01' });
  rejected(await writeFact(db, fee(hh, e2, text, 895, { valid_from: '2025-03-01', valid_to: '2025-04-01' })), 'partial_overlap');
  assert.deepEqual((await all<any>('SELECT value_cents, valid_to, expired_at FROM facts WHERE household_id=?', hh)), [{ value_cents: 79500, valid_to: null, expired_at: null }]);
});

test('historical bounded fact that does not overlap writes without touching the current one', async () => {
  const hh = hhId(); const text = 'fee $595 until 2024-12-31; fee $795 now';
  const eNow = await episode(hh, text, { occurred_at: '2025-01-01' });
  written(await writeFact(db, fee(hh, eNow, text, 795)));
  const r = written(await writeFact(db, fee(hh, eNow, text, 595, { valid_from: '2023-01-01', valid_to: '2025-01-01' })));
  assert.deepEqual(r.superseded, []);
  assert.equal((await all('SELECT 1 FROM facts_current WHERE household_id=?', hh)).length, 1);
});

test('entity adjudication: only offered candidates are accepted; bogus/null => new entity', async () => {
  const hh = hhId(); const text = 'Chase Sapphire Reserv card, annual fee $795';
  const ep = await episode(hh, text, { occurred_at: '2025-06-01' });
  const base = written(await writeFact(db, fee(hh, ep, text, 795, { subject: { type: 'card_account', name: 'Chase Sapphire Reserve' }, quotes: [q(text, '$795')] })));
  const baseSubject = (await one<any>('SELECT subject_id FROM facts WHERE fact_id=?', base.fact_id)).subject_id;
  let seen: any = null;
  const yes: EntityAdjudicator = async (req) => { seen = req; return { entity_id: req.candidates[0]!.entity_id, confidence: 0.93 }; };
  const typo = { subject: { type: 'card_account', name: 'Chase Sapphire Reserv' } as const, predicate: 'card_last4', value_text: 'ab12', quotes: [q(text, 'Chase Sapphire Reserv')] };
  const r1 = written(await writeFact(db, fee(hh, ep, text, 0, { ...typo, value_cents: undefined }), { adjudicate: yes }));
  assert.equal(r1.created_entity_ids.length, 0);
  assert.equal((await one<any>('SELECT subject_id FROM facts WHERE fact_id=?', r1.fact_id)).subject_id, baseSubject);
  assert.equal(seen.candidates.length, 1); assert.ok(seen.candidates[0].similarity >= 0.85);
  const al = await one<any>(`SELECT method, confidence FROM entity_aliases WHERE household_id=? AND alias_norm='card_account:chase sapphire reserv'`, hh);
  assert.deepEqual(al, { method: 'llm_adjudicated', confidence: 0.93 });
  // bogus id => ignored => new entity
  const bogus: EntityAdjudicator = async () => ({ entity_id: 'ent_not_offered', confidence: 1 });
  const typo2 = { ...typo, subject: { type: 'card_account', name: 'Chase Sapphire Reserve!' } as const };
  // 'Chase Sapphire Reserve!' normalizes to the existing alias => direct hit, adjudicator never consulted
  const hit = written(await writeFact(db, fee(hh, ep, text, 0, { ...typo2, predicate: 'card_opened_on', value_text: '2024-01-02', value_cents: undefined }), { adjudicate: bogus }));
  assert.equal(hit.created_entity_ids.length, 0);
  const typo3 = { ...typo, subject: { type: 'card_account', name: 'Chase Sapphire Reservee' } as const };
  const r3 = written(await writeFact(db, fee(hh, ep, text, 0, { ...typo3, value_cents: undefined }), { adjudicate: bogus }));
  assert.equal(r3.created_entity_ids.length, 1);
  const none: EntityAdjudicator = async () => null;
  const r4 = written(await writeFact(db, fee(hh, ep, text, 0, { ...typo, subject: { type: 'card_account', name: 'Chase Sapphire Resrve' }, value_cents: undefined }), { adjudicate: none }));
  assert.equal(r4.created_entity_ids.length, 1);
  // different type is never offered
  let offered = -1;
  await writeFact(db, { household_id: hh, source_episode_id: ep, subject: { type: 'merchant', name: 'Chase Sapphire Reserve' }, predicate: 'event_kind', value_text: 'zzz', confidence: 0.5, quotes: [q(text, '$795')] }, { adjudicate: async (r) => { offered = r.candidates.length; return null; } });
  assert.equal(offered, -1);
});

test('merged entities resolve to the canonical entity; cross-household ids are rejected', async () => {
  const hh = hhId(); const other = hhId(); const text = 'birthday 1985-03-12';
  const ep = await episode(hh, text);
  const mkEnt = async (h: string, id: string, merged: string | null = null) =>
    db.prepare(`INSERT INTO entities (entity_id, household_id, type, canonical_name, merged_into) VALUES (?,?,?,?,?)`).bind(id, h, 'person', id, merged).run();
  const A = `ent_A_${hh}`, B = `ent_B_${hh}`, C = `ent_C_${other}`;
  await mkEnt(hh, B); await mkEnt(hh, A, B); await mkEnt(other, C);
  const r = written(await writeFact(db, { household_id: hh, source_episode_id: ep, subject: { entity_id: A }, predicate: 'birthday', value_text: '1985-03-12', confidence: 0.9, quotes: [q(text, '1985-03-12')] }));
  assert.equal((await one<any>('SELECT subject_id FROM facts WHERE fact_id=?', r.fact_id)).subject_id, B);
  rejected(await writeFact(db, { household_id: hh, source_episode_id: ep, subject: { entity_id: C }, predicate: 'birthday', value_text: '1985-03-12', confidence: 0.9, quotes: [q(text, '1985-03-12')] }), 'entity_not_found');
});

test('quote offsets are UTF-16 slice offsets (emoji, accents) and multiple quotes are stored', async () => {
  const hh = hhId(); const text = '🎉 Café Rouge: Reservation for the Smiths — anniversary 2026-05-04 🥂';
  const ep = await episode(hh, text);
  const r = written(await writeFact(db, { household_id: hh, source_episode_id: ep, subject: { type: 'household', name: 'Smith' }, predicate: 'anniversary_date', value_text: '2026-05-04', confidence: 0.9,
    quotes: [q(text, 'anniversary 2026-05-04'), q(text, 'Reservation for the Smiths'), q(text, 'anniversary 2026-05-04')] }));
  const rows = await all<any>('SELECT * FROM fact_quotes WHERE fact_id=? ORDER BY char_start', r.fact_id);
  assert.equal(rows.length, 2);
  for (const row of rows) assert.equal(text.slice(row.char_start, row.char_end), row.quote);
});

test('redact() output is accepted as grounding text; quarantined episodes are refused end to end', async () => {
  const hh = hhId();
  const raw = 'Card 4111 1111 1111 1111 (Amex Platinum) annual fee $895. SSN 123-45-6789.';
  const red = redact(raw); assert.equal(red.redaction_status, 'redacted');
  const ep = await episode(hh, red.redacted_text, { status: 'redacted' });
  const text = red.redacted_text!;
  const r = written(await writeFact(db, { household_id: hh, source_episode_id: ep, subject: { type: 'card_account', name: 'Amex Platinum' }, predicate: 'card_last4', value_text: '1111', confidence: 0.95, quotes: [q(text, '[ACCT ****1111]')] }));
  assert.ok(r.fact_id);
  // a quote that tries to carry the raw PAN cannot verify against redacted text
  rejected(await writeFact(db, { household_id: hh, source_episode_id: ep, subject: { type: 'card_account', name: 'Amex Platinum' }, predicate: 'card_last4', value_text: '1111', confidence: 0.95, quotes: [{ quote: '4111 1111 1111 1111', char_start: 5, char_end: 24 }] }), 'quote_mismatch');
  const bad = redact('password: Xk39!aZq'); assert.equal(bad.redaction_status, 'quarantined');
  const qe = await episode(hh, bad.redacted_text, { status: bad.redaction_status });
  rejected(await writeFact(db, { household_id: hh, source_episode_id: qe, subject: { type: 'person', name: 'Ava' }, predicate: 'home_city', value_text: 'x y z', confidence: 0.5, quotes: [{ quote: 'password', char_start: 0, char_end: 8 }] }), 'episode_not_redacted');
});

test('helpers: toIsoTimestamp / locateQuote', () => {
  assert.equal(toIsoTimestamp('2026-11-26'), '2026-11-26T00:00:00.000Z');
  assert.equal(toIsoTimestamp('2026-11-26T08:30'), '2026-11-26T08:30:00.000Z');
  assert.equal(toIsoTimestamp('2026-11-26 08:30:15.5-05:00'), '2026-11-26T13:30:15.500Z');
  assert.equal(toIsoTimestamp('2026-11-26T08:30:00+0530'), '2026-11-26T03:00:00.000Z');
  for (const bad of ['2026-02-30', '2026-13-01', '2026-11-26T25:00:00Z', '11/26/2026', '', 'tomorrow', '2026-11-26T08:30:00+25:00']) assert.equal(toIsoTimestamp(bad), null, bad);
  assert.deepEqual(locateQuote('abcabc', 'bc'), { char_start: 1, char_end: 3 }); assert.equal(locateQuote('abc', 'zz'), null); assert.equal(locateQuote('abc', ''), null);
});

// ── concurrency ──
test('CONCURRENCY: simultaneous identical writes yield exactly one fact (others reaffirm)', async () => {
  const hh = hhId(); const text = 'fee is $895';
  const eps = await Promise.all([1, 2, 3, 4, 5, 6].map(() => episode(hh, text, { occurred_at: '2025-09-01' })));
  const results = await Promise.all(eps.map(ep => writeFact(db, fee(hh, ep, text, 895))));
  assert.equal(results.filter(r => r.status === 'written').length, 1, JSON.stringify(results.map(r => r.status)));
  assert.equal(results.filter(r => r.status === 'reaffirmed').length, 5);
  assert.equal((await all('SELECT 1 FROM facts WHERE household_id=?', hh)).length, 1);
  assert.equal((await all('SELECT 1 FROM entities WHERE household_id=?', hh)).length, 1);
  assert.equal((await all('SELECT 1 FROM fact_quotes WHERE fact_id=?', (results.find(r => r.status === 'written') as any).fact_id)).length, 6);
});

test('CONCURRENCY: parallel conflicting values keep exactly one believed open fact per subject/predicate', async () => {
  const hh = hhId();
  const mk = async (i: number) => { const t = `fee is $${100 + i}`; const ep = await episode(hh, t, { occurred_at: `2025-0${i + 1}-01` }); return fee(hh, ep, t, 100 + i, { quotes: [q(t, `$${100 + i}`)] }); };
  const props = await Promise.all([0, 1, 2, 3, 4, 5].map(mk));
  const results = await Promise.all(props.map(p => writeFact(db, p, { maxAttempts: 12 })));
  assert.ok(results.every(r => r.status === 'written' || r.status === 'rejected'), JSON.stringify(results));
  const believed = await all<any>('SELECT value_cents, valid_from, valid_to FROM facts WHERE household_id=? AND expired_at IS NULL ORDER BY valid_from', hh);
  assert.equal(believed.filter(b => b.valid_to === null).length, 1, JSON.stringify(believed));
  for (let i = 1; i < believed.length; i++) assert.equal(believed[i - 1].valid_to, believed[i].valid_from); // contiguous timeline, no overlaps
  assert.equal(believed.at(-1).value_cents, 10500);
});

test('GUARD: a competing write between read and commit aborts the batch and the writer re-plans', async () => {
  const hh = hhId(); const text = 'fee is $895';
  const e1 = await episode(hh, text, { occurred_at: '2025-09-01' }); const e2 = await episode(hh, text, { occurred_at: '2025-09-02' });
  let batches = 0; let raced = false;
  const racing = new Proxy(db, { get(target, prop, recv) {
    if (prop === 'batch') return async (stmts: D1PreparedStatement[]) => {
      batches++;
      if (!raced) { raced = true; written(await writeFact(db, fee(hh, e2, text, 895))); } // competitor commits first
      return target.batch(stmts);
    };
    const v = Reflect.get(target, prop, recv); return typeof v === 'function' ? v.bind(target) : v;
  } }) as D1Database;
  const r = await writeFact(racing, fee(hh, e1, text, 895));
  assert.equal(r.status, 'reaffirmed', JSON.stringify(r));      // re-planned, saw the competitor's fact, attached support
  assert.equal(batches, 2);                                       // aborted attempt + successful attempt (competitor used the raw db)
  assert.equal((await all('SELECT 1 FROM facts WHERE household_id=?', hh)).length, 1);
  assert.equal((await all('SELECT 1 FROM entities WHERE household_id=?', hh)).length, 1);   // aborted attempt's entity insert rolled back
  assert.equal((await all(`SELECT 1 FROM audit_log WHERE household_id=? AND action='writeFact.guard'`, hh)).length, 0);
  // exhaustion: a writer that always loses returns a rejection value, not an exception
  let n = 0;
  const always = new Proxy(db, { get(target, prop, recv) {
    if (prop === 'batch') return async (stmts: D1PreparedStatement[]) => {
      const t = `fee is $${2000 + n}`; const ep = await episode(hh, t, { occurred_at: '2025-10-01' });
      written(await writeFact(db, fee(hh, ep, t, 2000 + n++, { valid_from: `2026-01-${String(n + 1).padStart(2, '0')}` })));
      return target.batch(stmts);
    };
    const v = Reflect.get(target, prop, recv); return typeof v === 'function' ? v.bind(target) : v;
  } }) as D1Database;
  const t2 = 'fee is $3000'; const e3 = await episode(hh, t2, { occurred_at: '2025-11-01' });
  const lost = await writeFact(always, fee(hh, e3, t2, 3000, { valid_from: '2026-02-01' }), { maxAttempts: 2 });
  rejected(lost, 'conflict_retry_exhausted');
});

test('GUARD in isolation (entity already exists, so no alias race): stale read => abort => re-plan => contiguous timeline', async () => {
  const hh = hhId();
  const t0 = 'fee $700'; const e0 = await episode(hh, t0, { occurred_at: '2025-01-01' });
  written(await writeFact(db, fee(hh, e0, t0, 700)));
  const tA = 'fee $895'; const eA = await episode(hh, tA, { occurred_at: '2025-09-01' });
  const tB = 'fee $795'; const eB = await episode(hh, tB, { occurred_at: '2025-06-01' });
  let batches = 0; let raced = false; let guardAbort = '';
  const racing = new Proxy(db, { get(target, prop, recv) {
    if (prop === 'batch') return async (stmts: D1PreparedStatement[]) => {
      batches++;
      if (!raced) { raced = true; written(await writeFact(db, fee(hh, eB, tB, 795))); }
      try { return await target.batch(stmts); } catch (e) { guardAbort = String(e) + String((e as any).cause ?? ''); throw e; }
    };
    const v = Reflect.get(target, prop, recv); return typeof v === 'function' ? v.bind(target) : v;
  } }) as D1Database;
  const r = written(await writeFact(racing, fee(hh, eA, tA, 895)));
  assert.equal(batches, 2); assert.match(guardAbort, /NOT NULL constraint failed: audit_log\.actor/);
  assert.equal(r.superseded.length, 1); assert.equal(r.superseded[0]!.kind, 'world_change');
  const rows = await all<any>('SELECT value_cents, valid_from, valid_to, expired_at FROM facts WHERE household_id=? ORDER BY valid_from', hh);
  assert.deepEqual(rows, [
    { value_cents: 70000, valid_from: '2025-01-01T00:00:00.000Z', valid_to: '2025-06-01T00:00:00.000Z', expired_at: null },
    { value_cents: 79500, valid_from: '2025-06-01T00:00:00.000Z', valid_to: '2025-09-01T00:00:00.000Z', expired_at: null },
    { value_cents: 89500, valid_from: '2025-09-01T00:00:00.000Z', valid_to: null, expired_at: null },
  ]);
  assert.equal((await all(`SELECT 1 FROM audit_log WHERE household_id=? AND action='writeFact.guard'`, hh)).length, 0);
});

test('non-concurrency database errors are NOT swallowed', async () => {
  const hh = hhId(); const text = 'fee is $895';
  const ep = await episode(hh, text);
  const broken = new Proxy(db, { get(target, prop, recv) {
    if (prop === 'batch') return async () => { throw new Error('D1_ERROR: disk on fire'); };
    const v = Reflect.get(target, prop, recv); return typeof v === 'function' ? v.bind(target) : v;
  } }) as D1Database;
  await assert.rejects(() => writeFact(broken, fee(hh, ep, text, 895)), /disk on fire/);
});
