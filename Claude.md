# CLAUDE.md — Life Hub (Consumer Life OS & Continuous Context Hub)

## 0. Ground rules
- ALL vendor accounts (Stripe, Cloudflare, Google Cloud, OpenAI/Anthropic, Duffel, affiliates, domains) are owned by the
  guardian-owned LLC ("the Entity"). Never use Zeke's personal identity. Secrets only via `wrangler secret`.
- Users 18+ (attested). No Gmail restricted scopes in v1: the Gmail sync engine (`src/connectors/gmailSync.ts`, Milestone 6) ships
  DORMANT. Sign-in still asks for the two calendar scopes only. The Gmail consent flow exists (Milestone 7) but is behind
  `ENABLE_GMAIL_OAUTH` (`wrangler.toml` `[vars]`, shipped `"false"`): while it is anything but exactly `"true"`,
  `GET /api/auth/google/gmail-connect` and `/gmail-callback` answer 403 `{"error":"CASA_PENDING"}` before a Google URL is built, so nothing requests
  `gmail.readonly` until CASA AL1 passes and the flag is flipped (§9). Turned on, it is PKCE (S256) with the one scope constant in `gmailSync.ts`,
  and the refresh token is stored AES-GCM-encrypted in KV.
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

## 2. D1 constraints, plans and the share card

### 2.1 D1 constraints
10 GB hard cap/DB; 1,000 queries/invocation; 100 bound params; 2 MB/row; 30 s/query; 50 MB max result (since 2026-11-04);
serial per DB. Tenancy: `control` DB + `shard_00..03`; shard = fnv1a(household_id) % 4. Every table keyed by household_id;
repository layer injects it. Use db.batch(); paginate at 500 rows.

### 2.2 Plans, credits and packs (live since Milestone 5; the numbers live in `src/billing/packs.ts` and nowhere else)
| Item | Rule |
|---|---|
| Day-1 trial | +5 credits, once, in the household-creation batch (`stripe_event_id = 'trial:' \|\| household_id`, so it can never grant twice) |
| Monthly plan | +15 on each paid `invoice.paid` |
| Annual plan | +12 on the FIRST paid invoice, then +12 each month from the hourly cron drip (`annual_drip:<household>:<YYYY-MM>`) until `plan_period_end` |
| $0 invoice | grants nothing |
| Rollover | at each renewal, unused SUBSCRIPTION credits above 15 expire first, then the new grant lands. Purchased credits are never touched |
| Top-up packs (never expire) | `pack_10` +10 / $9.99, `pack_25` +25 / $19.99, `pack_60` +60 / $39.99 (Stripe Checkout; metadata `{household_id, pack_sku, credits}`) |
| Auto-recharge | opt-in, saved card, default `pack_10`, default cap $20 per UTC calendar month; failed attempts do not count against the cap |
| Weight | a run debits its template's `credits_cost`: 1 for an ordinary card, 3 for a claim packet (warranty, price-drop, delay) |
| Gate G8 | model spend of a run <= `$0.25 * credit_cost` (linear: 1 -> $0.25, 3 -> $0.75), and no worker call above 90,000 input tokens |

### 2.3 Shareable Achievement Card (`GET /api/receipt/share-card.svg`)
A 1200x630 SVG the member can post. Privacy is the design: it is built from TWO aggregate numbers and the product name, and nothing else.
- Money recovered is bucketed DOWN to a $100 step and shown as "$400+" (`bucketDollars`: 9,999 cents -> $0, 10,000 -> $100, 39,999 -> $300, 40,000 -> $400). Below $100 the
  card says the product "is finding card credits for you" and shows no amount. Recovered = confirmed credit-recovery runs only
  (`expiring_credit`, `anniv_dining`), weighted by confidence like the Home header.
- Documents filed is a plain count of ingested documents that yielded at least one fact. "0 prompts typed" is a constant: the product has no prompt box.
- It NEVER contains a merchant, card or card product, venue, date, place, person, account number, tax value or tax characterization, a household
  or member identifier, or any model-written text. The queries behind it return counts and one sum; the SVG is a fixed template into which only those
  values and the escaped `APP_NAME` are written, so there is no other path for data to reach it.
- Served `private, no-store`, `nosniff`, with `Content-Security-Policy: default-src 'none'` (an SVG opened directly can run script; this one has none).

### 2.4 The 3-tier execution ledger (Milestone 7; the numbers live in `src/goals/tiers.ts` for goals and in the stored templates for cards)
A run costs credits by what it does, and Gate G8 caps its model spend at `$0.25 * credits` (so the cap and the price can never drift apart).
| Tier | Name | Credits | G8 cap | What runs |
|---|---|---|---|---|
| 1 | Quick Mission | 1 credit | $0.25 | a card from a standing trigger (MissionRunWF) |
| 2 | Deep Research & Claim Packet | 3 credits | $0.75 | a claim packet (MissionRunWF) or a member's research goal (`research_plan`, GoalExecutionWF) |
| 3 | Autonomous Micro-App Builder | 5 credits | $1.25 | a member's goal to build and host a small app (`micro_app`, GoalExecutionWF) |
Goals are priced by `classifyGoal()`, a deterministic text rule (a build verb followed by an app noun, or "install ... on my site", is a `micro_app`;
anything else is a `research_plan`). No model decides what a goal costs, and `goals` has a CHECK that pins tier, `credits_reserved` and
`max_cost_usd` together.

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
        'R3_LINK_ISSUED','CONFIRMED','EXPIRED','DECLINED','FAILED_GATE','CANCELED','INSUFFICIENT_CREDITS')),
  expires_at TEXT, cost_usd REAL NOT NULL DEFAULT 0,
  credit_cost INTEGER NOT NULL DEFAULT 1,                  -- credits this run charges (its template's credits_cost); Gate G8 scales by it
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

-- CREDIT LEDGER (append-only; two buckets since migration 0003, see §2.2 and §5.3)
CREATE TABLE credit_ledger (
  entry_id TEXT PRIMARY KEY, household_id TEXT NOT NULL,
  delta INTEGER NOT NULL,                                  -- = sub_delta + pur_delta
  sub_delta INTEGER NOT NULL DEFAULT 0,                    -- subscription bucket (trial + monthly grants; expires down to the rollover cap)
  pur_delta INTEGER NOT NULL DEFAULT 0,                    -- purchased bucket (packs; never expires)
  reason TEXT NOT NULL CHECK (reason IN ('trial_grant','monthly_grant','pack_purchase','mission_debit','mission_refund','admin_adjust','expiry')),
  -- table CHECKs: pack_purchase -> purchased only; trial_grant / monthly_grant -> subscription only; expiry -> subscription only and negative
  run_id TEXT REFERENCES mission_runs(run_id), stripe_event_id TEXT UNIQUE,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
CREATE INDEX ix_ledger_hh ON credit_ledger(household_id, created_at);
CREATE VIEW credit_balance AS SELECT household_id, SUM(delta) AS balance,
  SUM(sub_delta) AS subscription_credits, SUM(pur_delta) AS purchased_credits FROM credit_ledger GROUP BY household_id;
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

### 3.1 Added since this schema (migrations 0002-0004; the migrations are the source of truth)
- 0002: `free_window` predicate; templates and standing triggers (a trigger is ONE stored read-only SELECT using `?1` household, `?2` now, `?3` today).
- 0003: the two-bucket ledger above; `households.stripe_default_payment_method_id`, `auto_recharge_enabled`, `auto_recharge_pack_sku`
  (default `pack_10`), `auto_recharge_monthly_cap_cents` (default 2000), `plan_period_end`; `stripe_events`, `auto_recharges`, `life_receipts`,
  `drop_alarms`; extra `price_watches` columns and the at-most-2-active-watches triggers; templates `drop_alarm`, `flight_watch`.
- 0004: `connectors.kind` admits `'gmail'`; `inbox_connections`, `ingested_messages` (§6.1); predicates `purchase_price_cents`,
  `return_window_closes_on`, `price_drop_detected_cents` (budget), `warranty_expires_on`, `serial_number` (vault), `flight_delay_minutes`,
  `registration_opens_at`, `required_checklist_item`, `event_window` (calendar) - every domain is one of the six the CHECK allows
  (`people, cards, budget, calendar, taste, vault`); templates `return_deadline`, `warranty_claim`, `price_drop_claim`, `delay_claim`,
  `family_event`; triggers `T_CLOSING_RETURN_OR_WARRANTY`, `T_PRICE_DROP_OR_DELAY_CLAIM`, `T_FAMILY_EVENT_AUTOPILOT` (a trigger row may
  return a `template_key` column to stage a different stored template, which is how one trigger stages both a 1-credit and a 3-credit card).

- 0005: `goals`, `goal_tasks`, `micro_apps`, `micro_app_records` (all additive; no existing table is rebuilt) and the predicate `hosted_micro_app`
  (vault; `dietary_restriction` already exists from 0001 in domain `people` and keeps it). `goals.attempt` counts Workflow instances and
  `micro_apps.quota_month` is the UTC month `monthly_ai_calls_used` counts. `micro_app_records` and `micro_apps` carry triggers that refuse a row whose
  household is not its owner's. A goal's debit is an ordinary ledger row keyed by the goal id; `credit_ledger.run_id` references `mission_runs`, so each
  goal has an inert anchor row there (template `goal_<tier>`, state `CANCELED`, which never shows on Home and holds no Home slot).

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

- IngestEmailWF: [funnel layers 0-2, §6.1] MIME → R2 (encrypted) → redact → classify (Luna, layer 3) → bucket-aware extract (Luna) →
  writeFact batch → evaluate the three life-admin triggers and stage new cards.
- IngestFileWF: CSV/PDF → dedupe → categorize (rules, then Luna) → recompute credit_periods.
- NightlySweepWF (03:00 local): decay → headroom/credits → evaluate triggers (every stored trigger, M6's three included) → stage cards.
- LifeReceiptWF (Sun 08:00 local): assemble → ground every claim → email + push.
- DropAlarmWF: step.sleep until release − 120 s → push R2 card → waitForEvent approve.
- PriceWatchWF: loop { Duffel search; if ≤ ceiling → MissionRunWF; step.sleep('1 day') }.
- GoalExecutionWF (§5.4): classify_and_debit → plan_dag → execute_with_self_repair → stage_r2_preview + approval → publish_r3_live.

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
| G8 cost | Run model cost ≤ $0.25 × credit_cost (1 → $0.25, 3 → $0.75); worker input ≤ 90K tokens |
| G9 idempotency | No duplicate run within cooldown |

### 5.3 Credits (two buckets; the numbers are in §2.2)
- `credit_ledger` is append-only and every row moves both buckets: `delta = sub_delta + pur_delta`. Table CHECKs: a pack goes to the purchased
  bucket only, trial and monthly grants to the subscription bucket only, an `expiry` row is subscription-only and negative (so it cannot touch
  purchased credits).
- DEBIT at compile = the template's `credits_cost`, taken by ONE conditional INSERT that decides the split and checks the balance in the same
  statement: the SUBSCRIPTION bucket is drained first (it is the one that expires), then the PURCHASED bucket. This is a bucket order, not per-credit
  lots. A balance short by even one credit refuses the whole debit; the entry id is deterministic per run, so a retried step is a no-op.
- REFUND mirrors the run's debit (same buckets, opposite sign), at most once, on FAILED_GATE or a system error. DECLINED and EXPIRED keep the debit.
  A goal (§5.4) is the exception: every goal that does not end COMPLETED is refunded in full to the buckets it came from, declined or expired included, because
  nothing was delivered.
- GRANTS are idempotent on `stripe_event_id` (UNIQUE): invoices by Stripe event id, packs by `pack:<payment_intent>` (so
  `checkout.session.completed` and `payment_intent.succeeded` for one purchase grant once), annual drips by month. A renewal is one batch:
  expire the subscription excess above 15, then grant.
- INSUFFICIENT_CREDITS: if the buckets cannot cover the run, an opted-in household first gets ONE off-session auto-recharge (saved card, within its
  monthly cap, reserved atomically before Stripe is called). Otherwise the run halts in `INSUFFICIENT_CREDITS` and the card points at
  `POST /api/credits/checkout`. Nothing was taken, so nothing is refunded. The state is not terminal: after a top-up the member taps again and the SAME run
  restarts as a NEW Workflow instance named `<run_id>~<n>` (n = the attempt number, from 1); `failure.attempt` makes a double tap start it once.
- `POST /api/webhooks/stripe`: raw-body HMAC-SHA256 over `t.payload`, 300 s tolerance, idempotent on the event id; handles `invoice.paid`,
  `checkout.session.completed`, `payment_intent.succeeded`, `customer.subscription.deleted`.

### 5.4 GoalExecutionWF (Milestone 7: `src/goals/goalPipeline.ts`, `src/workflows/goalExecutionWF.ts`, `src/workers/goalsApi.ts`)
`POST /api/goals {prompt_text}` redacts the request (a quarantined one is 422, nothing stored), prices it by tier (§2.4), inserts a `goals` row and starts
the Workflow (instance id = the goal id; the app links use the request's own origin). `GET /api/goals`, `GET /api/goals/<id>` (with the task DAG),
`POST /api/goals/<id>/approve {approval_hash}`, `/decline`, `/retry`. Every lookup is scoped by the session's household; another household's goal is 404.
1. `classify_and_debit`: the tier, credits and cap on the row must equal what `classifyGoal()` says (else `TIER_MISMATCH`). The debit is the ordinary
   two-bucket debit keyed by the goal id; a household opted in to auto-recharge gets one recharge first. Short of credits: status
   `INSUFFICIENT_CREDITS`, nothing taken, nothing refunded, and after a top-up `POST /api/goals/<id>/retry` restarts the SAME goal as instance
   `<goal_id>~<n>` (`goals.attempt` makes a double tap start it once; the debit is idempotent, so a goal is never charged or run twice).
2. `plan_dag`: the chief model (gpt-6.2-sol) sees the redacted request and the household's redacted facts and orders the task DAG. Code enforces it:
   the DAG is exactly `context_gather, schema_gen, code_gen, verify_bundle, smoke_test, package_install` for a `micro_app` (the first four for a
   `research_plan`), whatever the model proposes.
3. `execute_with_self_repair`: the worker model (gpt-6-luna) generates; code verifies. BEFORE EVERY model call Gate G8 (spend so far + the call's
   pessimistic cost must not pass the cap) and the 90,000-token ceiling are checked. A failed `verify_bundle` (the sandbox verifier, below) or
   `smoke_test` (the real runtime run over an in-memory store) is fed back as the exact diagnostics for at most 2 repairs (`goal_tasks.retries`
   CHECK <= 2). Gate G7 (tax characterization) fails the goal at once, with no repair. On any failure (`G8_COST`, `TOKEN_CLIFF`, `G7_TAX_LANGUAGE`,
   `REPAIR_EXHAUSTED`, `PROMPT_QUARANTINED`, `PLAN_FAILED`, `SYSTEM_ERROR`) the goal is `FAILED` and the whole reservation is refunded.
4. `stage_r2_preview` + approval: the verified bundle is encrypted into R2 under `apps/<household>/<app_id>/`, the app is `STAGED` (the member's preview
   works; production does not), the member gets a payload-less push, and the Workflow waits for `goal_approval:<goal_id>` (7 days). The
   `approval_hash` covers the goal, the bundle hash, the slug and the install artifact; an approval with another hash is refused by the API (409) and,
   if one reached the Workflow, declines the goal.
5. `publish_r3_live`: the stored bundle is re-read, re-hashed and re-verified (tampered or no longer passing: `REGATE_FAILED`, refunded), then the app is
   `LIVE`, `prod_url` is set, a grounded `hosted_micro_app` fact is written (episode = the request and the published address, verbatim quote) and the
   goal is `COMPLETED`. Declined or expired: the app is `ARCHIVED`, the goal `DECLINED`, the credits refunded.
Nothing is deployed to a member's own site: `install_artifact_json` (`embed_snippet`, `cloudflare_pages_patch`, `github_pr_bundle`) is data the member
applies. The embed carries a read-only token signed for exactly (household_id, app_id).

**Sandbox verifier (`src/builder/sandboxVerifier.ts`)**: deterministic text analysis of a `{manifest, html}` bundle. Rejects `eval`, `new Function`, `Function(`,
`process.env`, any external script, stylesheet, image, form target, `fetch` target or WebSocket, XHR, beacons, dynamic `import()`, cookies, navigation, `document.write`,
`javascript:` URLs, `srcdoc`, meta refresh, `<base>`, workers, a global reached by a computed name (`window[...]`), the `.constructor.constructor` route to `Function`, a timer given a string, any `policy_blocklist` origin (Resy, OpenTable, Tock, SevenRooms, api.monarch.com), any household or app id in the
page (`CROSS_TENANT`), plus manifest checks (known template, slug, routes, collections, AI-call cap <= 100) and accessibility checks (labels, alt text, viewport).
The page can only call its own relative `api/...` routes.

**Micro-app runtime (`src/builder/microAppRuntime.ts`, `/apps/<household_id>/<slug>/*`)**: callers are a signed-in member of that household or a signed token for
exactly (household_id, app_id); anything else is 401 (no lookup) or 404 (another household's app does not exist). `ARCHIVED` is 404 for everyone; `STAGED` is served
only to members and preview tokens, and only `/preview` and `api/`. The flagship recipe book: `POST api/parse-recipe` redacts the text first (a quarantined one is
422 and costs nothing), counts one AI call against `monthly_ai_calls_cap` (100 a month; the 101st is 429 `QUOTA_EXCEEDED`), sends only the REDACTED text to gpt-6-luna under a
4,000-token cap, encrypts any photo into R2 under `apps/<household>/<app_id>/` (no pixel ever goes to a model), and stores the row in `micro_app_records` scoped to
(app_id, household_id); `GET api/search?q=&tag=`; `GET api/suggest` uses the household's live `prefers_cuisine` and `dietary_restriction` facts. Known limit: the app is served
from the same origin as the member API, so isolation rests on the verifier plus a strict Content-Security-Policy (`default-src 'none'`, `connect-src 'self'`); a dedicated
apps origin is the planned hardening.

## 6. Email ingestion
One catch-all on in.lifehub.app → Email Worker; unknown alias → setReject. 25 MiB cap; larger → reply "please upload".
Accept if SPF/DKIM pass or sender ∈ member forwarding addresses. Raw MIME to R2 (AES-GCM, per-household DEK wrapped by KEK).

### 6.1 The inbox funnel (Milestone 6: `src/ingest/inboxTriage.ts`)
A 2,000-message-a-month inbox must not burn model tokens or pollute `facts_current`. Mail reaches the household by two channels, and BOTH go
through the same funnel, R2 vault and IngestEmailWF: auto-forward to the drop address, and the Gmail connector (`src/connectors/gmailSync.ts`).
| Layer | Where | Rule | Cost |
|---|---|---|---|
| 0 dedupe | before R2 | `message_id_norm` = sha256 of the normalized Message-ID (else of From + Subject + Date). The first arrival, by either channel, claims `ingested_messages(household_id, message_id_norm)` atomically; the second is a duplicate | no R2, no model |
| 1 bulk | before R2 | a bulk header (`List-Unsubscribe`, `Precedence: bulk/list/junk`, `Auto-Submitted` other than `no`, `X-Campaign*`) AND none of the high-signal tokens (order, receipt, invoice, total, confirmation, flight, itinerary, airline, delay, warranty, serial, policy, claim, return, refund, statement, due, school, camp, registration, permission, roster, appointment, tax, 1099, w-2, .pdf, .ics) in the subject, the first 20,000 body characters or the attachment names => `DROPPED_BULK`, counted in `inbox_connections.triage_dropped_bulk_count` | no R2, no model |
| 2 forwarding confirmation | before R2, BEFORE `redact()` would quarantine its code | a Gmail (`forwarding-noreply@google.com`), Outlook or iCloud "confirm auto-forwarding" message is recognized only if it is from the provider's own sender or domain AND its topmost Authentication-Results shows DKIM/SPF passing AS that domain. Its confirmation code and a link whose host is the provider's are saved to `inbox_connections` for the member (the link is shown, NEVER fetched); the message is not stored. Anything else, including every other one-time code, bank code, password and PIN, goes on to `redact()` and is quarantined | no R2, no model |
| 3 micro-classifier | inside IngestEmailWF, after `redact()` (a quarantined episode never reaches it) | `from`, `subject` (each redacted) and the first 600 characters of `redacted_text`, at most 2,000 input tokens, to the worker model: one of `RECEIPT_ORDER, BILL_SUBSCRIPTION, TRAVEL_ITINERARY, WARRANTY_PRODUCT, TAX_LEGAL_DOC, FAMILY_SCHOOL_EVENT, PERSONAL_FOLLOWUP_NEEDED, IGNORE`. `IGNORE` is recorded and stops before any fact extraction; any other bucket selects the extraction profile (`src/ingest/extractors.ts`) | one small call |
`forwarding_verified_at` is stamped by the first ordinary message that arrives AFTER a confirmation was captured; the one-time code and link are then cleared.
`GET /api/inbox/setup` returns the drop address, a ready-to-paste Gmail filter, the captured code and link, `forwarding_verified_at`, `gmail_connected`
and `triage_dropped_bulk_count`; `POST /api/inbox/sync` runs one incremental Gmail sync. The Gmail connector reads `users.history.list` +
`users.messages.get` through `src/net/egress.ts` with AES-GCM-encrypted KV tokens; with no cursor, or a 404/410 for it, it does a bounded full read of
the newest 50 messages of the last 90 days. It is dormant (§0): the consent flow that would request `gmail.readonly` exists but is off (`ENABLE_GMAIL_OAUTH` is `"false"`, 403 `CASA_PENDING`).

### 6.2 Life-admin extractors and triggers (Milestone 6)
Facts the model proposes by bucket (subject entity type in brackets; the model proposes, `writeFact()` verifies the quote): `purchase_price_cents`,
`return_window_closes_on`, `warranty_expires_on`, `serial_number`, `price_drop_detected_cents` [product]; `flight_delay_minutes` [trip];
`registration_opens_at`, `required_checklist_item`, `event_window` [event]. Three standing triggers stage R2 cards (the member does the return, the
claim or the registration; nothing is sent or booked), each grounded in verbatim quotes (G1) and weighted by `credits_cost`:
| Trigger | Staged when | Credits | G8 cap |
|---|---|---|---|
| `T_CLOSING_RETURN_OR_WARRANTY` | a return window closes within 5 days on an item of $40+ | 1 (`return_deadline`) | $0.25 |
| | a warranty expires within 30 days, or the member requests the packet (`POST /api/claims/warranty`) | 3 (`warranty_claim`) | $0.75 |
| `T_PRICE_DROP_OR_DELAY_CLAIM` | a price drop of $15+ inside the return/adjustment window of a purchase (both money facts confirmed within 35 days) | 3 (`price_drop_claim`) | $0.75 |
| | a flight delay or cancellation of 180+ minutes with the booking on record, noted in the last 90 days | 3 (`delay_claim`) | $0.75 |
| `T_FAMILY_EVENT_AUTOPILOT` | an event whose registration opens today or later, within 90 days, with up to five checklist items | 1 (`family_event`) | $0.25 |
A claim packet is drafted by the worker model from the verbatim quotes behind the card; it is assistive text, never a claim, and passes Gate G7.

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
MissionRunWF + Stripe credits → DropAlarm/PriceWatch + affiliate → inbox funnel + life-admin triggers + dormant Gmail sync engine (M6) →
GoalExecutionWF + sandboxed micro-apps + the flag-gated Gmail consent flow (M7) →
Google verification → launch → CASA AL1 → flip `ENABLE_GMAIL_OAUTH` (restricted scope, v2) → Plaid (v2).
