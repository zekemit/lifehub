# CLAUDE.md — Life Hub (Consumer Life OS & Continuous Context Hub)

## 0. Ground rules
- ALL vendor accounts (Stripe, Cloudflare, Google Cloud, OpenAI/Anthropic, Duffel, affiliates, domains) are owned by the
  guardian-owned LLC ("the Entity"). Never use Zeke's personal identity. Secrets only via `wrangler secret`.
- Users 18+ (attested). No Gmail restricted scopes in v1.
- LLM output = proposals with verbatim quotes; deterministic code validates and writes. The LLM never writes to the graph.
- Never fetch any domain in `policy_blocklist` (Resy, OpenTable, Tock, SevenRooms, api.monarch.com). Engine B = stored
  release rules + step.sleep + human-tapped deep links.
- Never characterize tax treatment ("possible evidence — confirm with your preparer").
- Never hold funds, card numbers, bank passwords or Monarch credentials. R3 = deep link only.
- Redact SSN/ITIN/full account numbers BEFORE any LLM call (src/redact.ts); keep last4.
- Worker calls: count tokens; >90,000 input → split. Never cross 100K (Luna/Haiku price cliff).
- Models: chief GPT-6 Sol, worker GPT-6 Luna; failover Claude Opus 5.5 / Haiku 5.5. All via src/llm/gateway.ts → llm_usage.

## 1. Stack
Workers (Paid), Workflows, D1 (control + shard DBs), R2 (encrypted docs), KV (encrypted OAuth tokens), Email Routing
(catch-all → Email Worker), Email Sending (Receipt), Queues. Pages (React/Vite PWA + Web Push). Stripe Billing (Entity
Standard account). Google scopes: openid email calendar.events.readonly calendar.freebusy. Duffel search only.

## 2. D1 constraints
10 GB hard cap/DB; 1,000 queries/invocation; 100 bound params; 2 MB/row; 30 s/query; 50 MB max result (since 2026-11-04);
serial per DB. Tenancy: `control` DB + `shard_00..03`; shard = fnv1a(household_id) % 4. Every table keyed by household_id;
repository layer injects it. Use db.batch(); paginate at 500 rows.

## 3. Schema

```sql
PRAGMA foreign_keys = ON;

-- CONTROL / TENANCY
CREATE TABLE households (
  household_id TEXT PRIMARY KEY, shard INTEGER NOT NULL,
  alias_local TEXT NOT NULL UNIQUE,                       -- h_<10 base32>@in.lifehub.app
  timezone TEXT NOT NULL DEFAULT 'America/New_York',
  plan TEXT NOT NULL CHECK (plan IN ('trial','monthly','annual','canceled')),
  stripe_customer_id TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), deleted_at TEXT);
CREATE TABLE members (
  member_id TEXT PRIMARY KEY, household_id TEXT NOT NULL REFERENCES households(household_id),
  email TEXT NOT NULL, display_name TEXT, role TEXT NOT NULL CHECK (role IN ('owner','adult')),
  age_attested_18 INTEGER NOT NULL CHECK (age_attested_18 = 1), push_endpoint TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), UNIQUE (household_id, email));
CREATE TABLE connectors (
  connector_id TEXT PRIMARY KEY, household_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('email_alias','google_calendar','spend_csv','spend_pdf','card_list','upload','byo_agent_mcp')),
  status TEXT NOT NULL CHECK (status IN ('pending','active','error','revoked')),
  scopes TEXT, token_kv_key TEXT, sync_cursor TEXT, last_success_at TEXT, last_error TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
CREATE INDEX ix_connectors_hh ON connectors(household_id, kind);

-- EPISODES (immutable raw inputs)
CREATE TABLE episodes (
  episode_id TEXT PRIMARY KEY, household_id TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('email','calendar','csv','pdf','upload','user_input','byo_agent')),
  source_ref TEXT, r2_key TEXT,
  redacted_text TEXT,                                      -- the ONLY text an LLM may see
  sha256 TEXT NOT NULL, occurred_at TEXT,
  received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  redaction_status TEXT NOT NULL CHECK (redaction_status IN ('pending','clean','redacted','quarantined')),
  UNIQUE (household_id, sha256));
CREATE INDEX ix_episodes_hh_time ON episodes(household_id, received_at);

-- ENTITIES + RESOLUTION
CREATE TABLE entities (
  entity_id TEXT PRIMARY KEY, household_id TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('person','household','merchant','card_account','benefit','envelope','recurring_charge',
       'event','window','venue','cuisine','airline','hotel_brand','trip','document','tax_year','product')),
  canonical_name TEXT NOT NULL, attrs_json TEXT NOT NULL DEFAULT '{}',
  merged_into TEXT REFERENCES entities(entity_id),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
CREATE INDEX ix_entities_hh_type ON entities(household_id, type, canonical_name);
CREATE TABLE entity_aliases (
  household_id TEXT NOT NULL, alias_norm TEXT NOT NULL, entity_id TEXT NOT NULL REFERENCES entities(entity_id),
  method TEXT NOT NULL CHECK (method IN ('deterministic','llm_adjudicated','user')), confidence REAL NOT NULL,
  PRIMARY KEY (household_id, alias_norm));

-- PREDICATE REGISTRY (drives deterministic invalidation)
CREATE TABLE predicate_registry (
  predicate TEXT PRIMARY KEY, cardinality TEXT NOT NULL CHECK (cardinality IN ('one','many')),
  value_type TEXT NOT NULL CHECK (value_type IN ('entity','text','num','date','money','interval')),
  decay_days INTEGER,                                      -- NULL = no decay
  domain TEXT NOT NULL CHECK (domain IN ('people','cards','budget','calendar','taste','vault')));

-- FACTS (bi-temporal edges)
CREATE TABLE facts (
  fact_id TEXT PRIMARY KEY, household_id TEXT NOT NULL,
  subject_id TEXT NOT NULL REFERENCES entities(entity_id),
  predicate TEXT NOT NULL REFERENCES predicate_registry(predicate),
  object_id TEXT REFERENCES entities(entity_id),
  value_text TEXT, value_num REAL, value_cents INTEGER, currency TEXT DEFAULT 'USD',
  valid_from TEXT NOT NULL,                                -- world time: became true
  valid_to TEXT,                                           -- world time: stopped being true
  learned_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), -- transaction time start
  expired_at TEXT,                                         -- transaction time end (belief corrected)
  superseded_by TEXT REFERENCES facts(fact_id),
  confidence REAL NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  source_episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
  CHECK (valid_to IS NULL OR valid_to >= valid_from));
CREATE INDEX ix_facts_current ON facts(household_id, subject_id, predicate) WHERE expired_at IS NULL;
CREATE INDEX ix_facts_valid ON facts(household_id, predicate, valid_from, valid_to) WHERE expired_at IS NULL;
CREATE INDEX ix_facts_object ON facts(household_id, object_id) WHERE expired_at IS NULL;
CREATE VIEW facts_current AS SELECT * FROM facts
  WHERE expired_at IS NULL AND valid_from <= strftime('%Y-%m-%dT%H:%M:%fZ','now')
    AND (valid_to IS NULL OR valid_to > strftime('%Y-%m-%dT%H:%M:%fZ','now'));

-- QUOTE GROUNDING + CLAIMS
CREATE TABLE fact_quotes (
  fact_id TEXT NOT NULL REFERENCES facts(fact_id), episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
  quote TEXT NOT NULL,                                     -- exact substring of episodes.redacted_text
  char_start INTEGER NOT NULL, char_end INTEGER NOT NULL, PRIMARY KEY (fact_id, episode_id, char_start));
CREATE TABLE claims (
  claim_id TEXT PRIMARY KEY, household_id TEXT NOT NULL, run_id TEXT, text TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('proposed','grounded','rejected')),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
CREATE TABLE claim_support (
  claim_id TEXT NOT NULL REFERENCES claims(claim_id), fact_id TEXT NOT NULL REFERENCES facts(fact_id),
  PRIMARY KEY (claim_id, fact_id));

-- CARDS & BENEFITS
CREATE TABLE benefit_catalog (
  benefit_key TEXT PRIMARY KEY,                            -- 'amex_platinum.resy_credit'
  card_product TEXT NOT NULL, label TEXT NOT NULL, amount_cents INTEGER NOT NULL,
  cadence TEXT NOT NULL CHECK (cadence IN ('monthly','quarterly','semiannual','annual','cardmember_year')),
  merchant_match TEXT NOT NULL, enrollment_required INTEGER NOT NULL DEFAULT 0,
  verified_on TEXT NOT NULL, source_url TEXT NOT NULL);
CREATE TABLE credit_periods (
  household_id TEXT NOT NULL, card_entity_id TEXT NOT NULL REFERENCES entities(entity_id),
  benefit_key TEXT NOT NULL REFERENCES benefit_catalog(benefit_key),
  period_start TEXT NOT NULL, period_end TEXT NOT NULL, used_cents INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL CHECK (status IN ('open','used','partially_used','expired_unused','unconfirmed')),
  PRIMARY KEY (household_id, card_entity_id, benefit_key, period_start));

-- SHADOW BUDGET
CREATE TABLE transactions (
  txn_id TEXT PRIMARY KEY, household_id TEXT NOT NULL, episode_id TEXT NOT NULL REFERENCES episodes(episode_id),
  posted_on TEXT NOT NULL, amount_cents INTEGER NOT NULL,  -- negative = outflow
  descriptor_norm TEXT NOT NULL, merchant_entity_id TEXT REFERENCES entities(entity_id),
  card_entity_id TEXT REFERENCES entities(entity_id), envelope_key TEXT,
  UNIQUE (household_id, posted_on, amount_cents, descriptor_norm));
CREATE INDEX ix_txn_hh_date ON transactions(household_id, posted_on);
CREATE TABLE envelopes (
  household_id TEXT NOT NULL, envelope_key TEXT NOT NULL,
  monthly_target_cents INTEGER NOT NULL,                   -- default: trailing-3-month median
  PRIMARY KEY (household_id, envelope_key));
CREATE VIEW envelope_headroom AS
  SELECT e.household_id, e.envelope_key, e.monthly_target_cents + COALESCE(SUM(t.amount_cents),0) AS headroom_cents
  FROM envelopes e LEFT JOIN transactions t ON t.household_id=e.household_id AND t.envelope_key=e.envelope_key
   AND t.posted_on >= strftime('%Y-%m-01','now')
  GROUP BY e.household_id, e.envelope_key, e.monthly_target_cents;

-- MISSIONS, RUNS, STEPS
CREATE TABLE mission_templates (
  template_key TEXT PRIMARY KEY, version INTEGER NOT NULL,
  plan_json TEXT NOT NULL,                                 -- [{id, kind, tier, worker_prompt_id, max_input_tokens}]
  max_tier TEXT NOT NULL CHECK (max_tier IN ('R0','R1','R2','R3')), credits_cost INTEGER NOT NULL DEFAULT 1,
  UNIQUE (template_key, version));
CREATE TABLE triggers (
  trigger_key TEXT PRIMARY KEY, template_key TEXT NOT NULL REFERENCES mission_templates(template_key),
  sql_condition TEXT NOT NULL, cooldown_days INTEGER NOT NULL);
CREATE TABLE mission_runs (
  run_id TEXT PRIMARY KEY,                                 -- = Workflows instance id
  household_id TEXT NOT NULL, template_key TEXT NOT NULL, template_version INTEGER NOT NULL,
  origin TEXT NOT NULL CHECK (origin IN ('trigger','user','receipt')), params_json TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('COMPILED','R0_READ','R1_STAGED','GATED','R2_AWAITING','R2_APPROVED',
        'R3_LINK_ISSUED','CONFIRMED','EXPIRED','DECLINED','FAILED_GATE','CANCELED')),
  expires_at TEXT, cost_usd REAL NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
CREATE INDEX ix_runs_hh_state ON mission_runs(household_id, state);
CREATE TABLE steps (
  run_id TEXT NOT NULL REFERENCES mission_runs(run_id), step_no INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('read','llm_worker','llm_chief','gate','stage','approve','deeplink','wait','notify')),
  tier TEXT NOT NULL CHECK (tier IN ('R0','R1','R2','R3')),
  status TEXT NOT NULL CHECK (status IN ('pending','ok','failed','skipped')),
  input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0, cost_usd REAL DEFAULT 0,
  gate_name TEXT, gate_result_json TEXT, started_at TEXT, finished_at TEXT, PRIMARY KEY (run_id, step_no));
CREATE TABLE approvals (
  run_id TEXT PRIMARY KEY REFERENCES mission_runs(run_id), member_id TEXT NOT NULL,
  decision TEXT NOT NULL CHECK (decision IN ('approve','decline')),
  payload_hash TEXT NOT NULL,                              -- hash of exact staged card shown
  decided_at TEXT NOT NULL);
CREATE TABLE deeplinks (
  link_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES mission_runs(run_id),
  merchant_domain TEXT NOT NULL, url TEXT NOT NULL, affiliate_program TEXT, issued_at TEXT NOT NULL, clicked_at TEXT);

-- ENGINE B INPUTS
CREATE TABLE venue_release_rules (
  venue_entity_id TEXT PRIMARY KEY REFERENCES entities(entity_id),
  platform TEXT NOT NULL CHECK (platform IN ('resy','opentable','tock','sevenrooms','direct')),
  venue_url TEXT NOT NULL,                                 -- deep-link target; NEVER fetched
  days_ahead INTEGER NOT NULL, release_local_time TEXT NOT NULL, release_tz TEXT NOT NULL, verified_on TEXT NOT NULL);
CREATE TABLE price_watches (
  watch_id TEXT PRIMARY KEY, household_id TEXT NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('flight')),
  params_json TEXT NOT NULL, ceiling_cents INTEGER NOT NULL,
  checks_per_day INTEGER NOT NULL DEFAULT 1 CHECK (checks_per_day <= 1), active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE policy_blocklist (domain TEXT PRIMARY KEY, reason TEXT NOT NULL);
INSERT INTO policy_blocklist VALUES
 ('resy.com','Unapproved bots/agents banned; Amex may close linked accounts'),('api.resy.com','same'),
 ('opentable.com','ToS restricts bots/AI assistants'),('exploretock.com','ToS'),('sevenrooms.com','ToS'),
 ('api.monarch.com','Personal non-commercial use only; approved apps only');

-- CREDIT LEDGER (append-only)
CREATE TABLE credit_ledger (
  entry_id TEXT PRIMARY KEY, household_id TEXT NOT NULL, delta INTEGER NOT NULL,
  reason TEXT NOT NULL CHECK (reason IN ('monthly_grant','pack_purchase','mission_debit','mission_refund','admin_adjust','expiry')),
  run_id TEXT REFERENCES mission_runs(run_id), stripe_event_id TEXT UNIQUE,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
CREATE INDEX ix_ledger_hh ON credit_ledger(household_id, created_at);
CREATE VIEW credit_balance AS SELECT household_id, SUM(delta) AS balance FROM credit_ledger GROUP BY household_id;
CREATE TRIGGER ledger_no_update BEFORE UPDATE ON credit_ledger BEGIN SELECT RAISE(ABORT,'append-only'); END;
CREATE TRIGGER ledger_no_delete BEFORE DELETE ON credit_ledger BEGIN SELECT RAISE(ABORT,'append-only'); END;

-- COST + AUDIT
CREATE TABLE llm_usage (
  id TEXT PRIMARY KEY, household_id TEXT, run_id TEXT, model TEXT NOT NULL, input_tokens INTEGER NOT NULL,
  cached_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL, cost_usd REAL NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
CREATE TABLE audit_log (
  id TEXT PRIMARY KEY, household_id TEXT, actor TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
```

## 4. Deterministic fact write (src/graph/writeFact.ts)
1. Verify quote: `episodes.redacted_text.slice(char_start,char_end) === quote`, else reject.
2. Resolve entities: aliases → LLM adjudication (top-3, sim ≥ 0.85) → new entity.
3. If cardinality='one' and a current fact differs: new.valid_from > old.valid_from → WORLD CHANGE (old.valid_to =
   new.valid_from); else CORRECTION (old.expired_at = now, superseded_by = new).
4. INSERT fact + fact_quotes in one db.batch().
5. Nightly decay: close facts whose last support is older than decay_days; audit 'decay_invalidate'.

## 5. Workflows
Limits: 10,000 steps/instance (configurable to 25,000); step result ≤ 1 MiB; step.sleep ≤ 365 days; waitForEvent default
24 h (always set explicitly); waiting instances don't count toward concurrency and burn no CPU. Steps (incl. sleep/wait)
billed: 500K/mo included, then $0.80 per 100K.

- IngestEmailWF: MIME → R2 (encrypted) → redact → extract (Luna) → writeFact batch.
- IngestFileWF: CSV/PDF → dedupe → categorize (rules, then Luna) → recompute credit_periods.
- NightlySweepWF (03:00 local): decay → headroom/credits → evaluate triggers → start MissionRunWF.
- LifeReceiptWF (Sun 08:00 local): assemble → ground every claim → email + push.
- DropAlarmWF: step.sleep until release − 120 s → push R2 card → waitForEvent approve.
- PriceWatchWF: loop { Duffel search; if ≤ ceiling → MissionRunWF; step.sleep('1 day') }.

### 5.1 MissionRunWF (R0 → R1 → R2 → R3)
```ts
export class MissionRunWF extends WorkflowEntrypoint<Env, RunParams> {
  async run(event: WorkflowEvent<RunParams>, step: WorkflowStep) {
    const p = event.payload;
    const tpl = await step.do('compile', () => loadTemplate(this.env, p.template_key));     // stored plan, no chief planning
    await step.do('debit_credit', () => debitCredit(this.env, p, tpl.credits_cost));        // fails if balance < cost
    const ctx = await step.do('r0_read', () => readFacts(this.env, p));                    // facts_current, headroom, windows
    const filled = await step.do('r0_fill', { retries: { limit: 2, delay: '10 seconds', backoff: 'exponential' } },
                     () => workerFill(this.env, tpl, ctx));                                 // Luna, <90K tokens each
    const gates = await step.do('gates', () => runGates(this.env, p, filled));
    if (!gates.ok) { await step.do('refund', () => refundCredit(this.env, p)); return markState(this.env, p, 'FAILED_GATE', gates); }
    const card = await step.do('r1_stage', () => stageCard(this.env, p, filled, gates));   // private
    if (card.needsChief) await step.do('chief_check', () => chiefFinalCheck(this.env, card)); // ≤2 escalations + 1 check
    await step.do('notify', () => pushCard(this.env, card));
    let approval;
    try { approval = await step.waitForEvent('await_approval', { type: `approve:${card.run_id}`, timeout: card.timeout }); }
    catch { await step.do('expire', () => expireRun(this.env, p)); return; }
    if (approval.payload.decision !== 'approve' || approval.payload.payload_hash !== card.payload_hash)
      return markState(this.env, p, 'DECLINED');
    const regate = await step.do('r3_regate', () => runGates(this.env, p, filled, { r3: true }));
    if (!regate.ok) return markState(this.env, p, 'FAILED_GATE', regate);
    const link = await step.do('r3_link', () => issueDeepLink(this.env, card));            // affiliate tag where allowed
    await step.do('deliver_link', () => pushLink(this.env, link));
    try { await step.waitForEvent('await_confirmation', { type: `confirmed:${card.run_id}`, timeout: '72 hours' });
          await step.do('confirm', () => markState(this.env, p, 'CONFIRMED')); }
    catch { await step.do('unconfirmed', () => markState(this.env, p, 'R3_LINK_ISSUED')); }
  }
}
```

### 5.2 Deterministic gates (all must pass)
| Gate | Rule |
|---|---|
| G1 grounding | Every card claim maps to ≥1 fact with a verified quote |
| G2 freshness | Supporting facts current; money ≤ 35 days old; calendar ≤ 15 min |
| G3 headroom | price + 10% ≤ envelope headroom |
| G4 calendar | Time range ⊂ free window fetched this step |
| G5 policy | No fetch to blocklisted domains (egress allowlist in fetch wrapper) |
| G6 tier | max_tier respected; R3 only via deeplinks; lint bans payment APIs |
| G7 tax-language | Bans "deductible", "you can deduct", "enter on line" |
| G8 cost | Run ≤ $0.60; worker input ≤ 90K tokens |
| G9 idempotency | No duplicate run within cooldown |

### 5.3 Credits
Grant +10 (monthly) / +8 (annual) on Stripe `invoice.paid`; +10 on pack `checkout.session.completed`; idempotent on
stripe_event_id. Debit 1 at compile; refund on FAILED_GATE/system error; none on DECLINED/EXPIRED. Monthly grants roll
over 1 month max.

## 6. Email ingestion
One catch-all on in.lifehub.app → Email Worker; unknown alias → setReject. 25 MiB cap; larger → reply "please upload".
Accept if SPF/DKIM pass or sender ∈ member forwarding addresses. Raw MIME to R2 (AES-GCM, per-household DEK wrapped by KEK).

## 7. Day-1 Onboarding (60 s) + Home
| t (s) | Screen | Copy / control | Backend |
|---|---|---|---|
| 0–10 | Welcome | "Your life admin, done while you sleep. Nothing gets booked or paid without your tap." [Continue with Google] ☐ "I am 18+" | Household + alias |
| 10–25 | Calendar | "See when you're free (read-only)." [Connect] | Sync last 365 d + next 180 d |
| 25–40 | Cards | Autocomplete chips; "No card numbers, ever." | card_account + credit_periods |
| 40–50 | Drop address | "h_7k2p9x4qwe@in.lifehub.app" [Copy] [Gmail filter guide] "Forward 3 recent receipts now." | Live counter |
| 50–60 | History | "Drag your Monarch or bank CSV. Or skip." | IngestFileWF |
| 60 | Home | 3 Mission Cards | NightlySweepWF now |

Home: header "This week: $X found · Y filed · Z waiting on you"; exactly 3 cards ranked by recoverable $ × urgency, each
with title, why, 2–3 evidence chips (tap → quote), R2 button, "Not for me". Cold-start guarantee (calendar + cards only):
1. "Your $100 Resy credit resets Dec 31 — $0 used this quarter." 2. "Amex Platinum $895 fee posts [date]; credits used
$X of $Y. [Stage retention-call script]" 3. Next anniversary/birthday/trip ≤ 30 days → dining/gift mission; fallback
"Forward 3 receipts to unlock subscription scan". With CSV, card 3 = duplicate/price-increase subscription with both charges.

## 8. 7-Day $0 Concierge Validation Test (before building §5)
Setup: 8 households (5–10), adults 25–55, ≥1 card with fee ≥ $250, dine out ≥ 2×/month. Consent signed with the Entity.
Data: own CSV export + forwarded receipts to a test alias + Calendar in testing mode (<100 users). No bank passwords,
Monarch credentials or SSNs (tax vault tested with dummy forms). Delete all data Day 14. $0 spend: Cloudflare free tier,
per-household spreadsheet graph, synthesis by Zeke with an existing Entity-owned AI subscription (training disabled).
Protocol: Days 1–6, 1–3 hand-built Mission Cards by 8 AM with evidence + deep link; log sent/opened/tapped/completed/$/minutes.
Receipts Day 3 and Day 7. Day 7: Stripe Payment Link (Entity) "Founding member $19.99/mo or $179.99/yr, first charge at
launch" + 10-min interview.

| Metric | Pass threshold |
|---|---|
| Activation | ≥ 6/8 connect calendar + ≥ 5 receipts or 1 CSV by Day 2 |
| Unprompted engagement | ≥ 60% of households tap R2 on ≥ 3 cards without chasing |
| Verified value | Median ≥ $75 recoverable per household |
| Cross-domain magic | ≥ 50% approve ≥ 1 mission joining ≥ 3 domains |
| Accuracy | ≤ 1 wrong card per household; 0 wrong-money claims |
| Willingness to pay | ≥ 3/8 put a card on file at $19.99/mo or $179.99/yr |
| Founder load | ≤ 20 manual min/household/day by Day 6 |
Fail any → do not automate; iterate templates, rerun with new households.

## 9. Build order
Schema + redaction + email → CSV/PDF + headroom + credits → Calendar → NightlySweep + 3 templates → Receipt →
MissionRunWF + Stripe credits → DropAlarm/PriceWatch + affiliate → Google verification → launch → CASA AL1 → Gmail (v2)
→ Plaid (v2).
