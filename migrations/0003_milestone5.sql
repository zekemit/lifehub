-- migrations/0003_milestone5.sql — Milestone 5: two-bucket credits and top-ups, multi-credit missions, the weekly Life Receipt,
-- and the Engine B concierge workflows (drop alarms, flight price watches).
--
-- Two tables are rebuilt because SQLite cannot change a CHECK constraint in place:
--   mission_runs   gains the state INSUFFICIENT_CREDITS and credit_cost
--   credit_ledger  gains the reason 'trial_grant' and the two bucket columns
-- Four tables hold a foreign key to mission_runs (steps, approvals, deeplinks, credit_ledger). To rebuild the parent without ever
-- breaking a foreign key, every row is first copied to a plain backup table, the children and the parent are dropped (children
-- first), all five are recreated, and the rows are restored. Nothing is lost; the backups are dropped at the end.

CREATE TABLE mission_runs_bak AS SELECT * FROM mission_runs;
CREATE TABLE steps_bak AS SELECT * FROM steps;
CREATE TABLE approvals_bak AS SELECT * FROM approvals;
CREATE TABLE deeplinks_bak AS SELECT * FROM deeplinks;
CREATE TABLE credit_ledger_bak AS SELECT * FROM credit_ledger;

DROP VIEW credit_balance;
DROP TABLE steps;
DROP TABLE approvals;
DROP TABLE deeplinks;
DROP TABLE credit_ledger;   -- its append-only triggers go with it and are recreated below
DROP TABLE mission_runs;

-- ── mission_runs: INSUFFICIENT_CREDITS + credit_cost ────────────────────────────────────────────────────────────────
CREATE TABLE mission_runs (
  run_id TEXT PRIMARY KEY,                                 -- = Workflows instance id
  household_id TEXT NOT NULL, template_key TEXT NOT NULL, template_version INTEGER NOT NULL,
  origin TEXT NOT NULL CHECK (origin IN ('trigger','user','receipt')), params_json TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('COMPILED','R0_READ','R1_STAGED','GATED','R2_AWAITING','R2_APPROVED',
        'R3_LINK_ISSUED','CONFIRMED','EXPIRED','DECLINED','FAILED_GATE','CANCELED','INSUFFICIENT_CREDITS')),
  expires_at TEXT, cost_usd REAL NOT NULL DEFAULT 0,
  -- credits this run costs (the template's credits_cost, copied at compile). Gate G8 allows $0.25 of model spend per credit.
  credit_cost INTEGER NOT NULL DEFAULT 1 CHECK (credit_cost >= 0),
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

-- ── credit_ledger: two buckets, trial_grant ─────────────────────────────────────────────────────────────────────────
-- delta = sub_delta + pur_delta on every row.
--   sub_delta  subscription credits: trial_grant and monthly_grant. At each renewal anything above the 15-credit rollover cap expires.
--   pur_delta  purchased credits: pack_purchase. They never expire; no 'expiry' row can touch them (CHECK below).
-- A mission debit records how much it took from each bucket (subscription first); its refund mirrors it exactly.
CREATE TABLE credit_ledger (
  entry_id TEXT PRIMARY KEY, household_id TEXT NOT NULL,
  delta INTEGER NOT NULL, sub_delta INTEGER NOT NULL, pur_delta INTEGER NOT NULL,
  reason TEXT NOT NULL CHECK (reason IN ('trial_grant','monthly_grant','pack_purchase','mission_debit','mission_refund','admin_adjust','expiry')),
  run_id TEXT REFERENCES mission_runs(run_id), stripe_event_id TEXT UNIQUE,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  CHECK (delta = sub_delta + pur_delta),
  CHECK (reason <> 'pack_purchase' OR (sub_delta = 0 AND pur_delta > 0)),
  CHECK (reason NOT IN ('trial_grant','monthly_grant') OR (pur_delta = 0 AND sub_delta > 0)),
  CHECK (reason <> 'expiry' OR (pur_delta = 0 AND sub_delta < 0)));
CREATE INDEX ix_ledger_hh ON credit_ledger(household_id, created_at);
CREATE VIEW credit_balance AS
  SELECT household_id, SUM(delta) AS balance, SUM(sub_delta) AS subscription_credits, SUM(pur_delta) AS purchased_credits
  FROM credit_ledger GROUP BY household_id;
CREATE TRIGGER ledger_no_update BEFORE UPDATE ON credit_ledger BEGIN SELECT RAISE(ABORT,'append-only'); END;
CREATE TRIGGER ledger_no_delete BEFORE DELETE ON credit_ledger BEGIN SELECT RAISE(ABORT,'append-only'); END;

-- ── restore ─────────────────────────────────────────────────────────────────────────────────────────────────────────
INSERT INTO mission_runs (run_id, household_id, template_key, template_version, origin, params_json, state, expires_at, cost_usd, created_at, updated_at)
  SELECT run_id, household_id, template_key, template_version, origin, params_json, state, expires_at, cost_usd, created_at, updated_at FROM mission_runs_bak;
INSERT INTO steps (run_id, step_no, kind, tier, status, input_tokens, output_tokens, cost_usd, gate_name, gate_result_json, started_at, finished_at)
  SELECT run_id, step_no, kind, tier, status, input_tokens, output_tokens, cost_usd, gate_name, gate_result_json, started_at, finished_at FROM steps_bak;
INSERT INTO approvals (run_id, member_id, decision, payload_hash, decided_at) SELECT run_id, member_id, decision, payload_hash, decided_at FROM approvals_bak;
INSERT INTO deeplinks (link_id, run_id, merchant_domain, url, affiliate_program, issued_at, clicked_at)
  SELECT link_id, run_id, merchant_domain, url, affiliate_program, issued_at, clicked_at FROM deeplinks_bak;
-- Credits granted or spent before this migration all lived in one pool, so they become subscription credits; packs were never sold.
INSERT INTO credit_ledger (entry_id, household_id, delta, sub_delta, pur_delta, reason, run_id, stripe_event_id, created_at)
  SELECT entry_id, household_id, delta,
         CASE WHEN reason = 'pack_purchase' THEN 0 ELSE delta END,
         CASE WHEN reason = 'pack_purchase' THEN delta ELSE 0 END,
         reason, run_id, stripe_event_id, created_at FROM credit_ledger_bak;
DROP TABLE mission_runs_bak;
DROP TABLE steps_bak;
DROP TABLE approvals_bak;
DROP TABLE deeplinks_bak;
DROP TABLE credit_ledger_bak;

-- ── households: top-ups, auto-recharge, annual plan period ─────────────────────────────────────────────────────────
ALTER TABLE households ADD COLUMN stripe_default_payment_method_id TEXT;
ALTER TABLE households ADD COLUMN auto_recharge_enabled INTEGER NOT NULL DEFAULT 0 CHECK (auto_recharge_enabled IN (0, 1));
ALTER TABLE households ADD COLUMN auto_recharge_pack_sku TEXT NOT NULL DEFAULT 'pack_10' CHECK (auto_recharge_pack_sku IN ('pack_10','pack_25','pack_60'));
ALTER TABLE households ADD COLUMN auto_recharge_monthly_cap_cents INTEGER NOT NULL DEFAULT 2000 CHECK (auto_recharge_monthly_cap_cents >= 0);
-- End of the paid period (ISO). Annual plans pay once but are granted credits every month until this instant.
ALTER TABLE households ADD COLUMN plan_period_end TEXT;

-- ── Stripe: processed events, and off-session top-ups made on the member's behalf ───────────────────────────────────
CREATE TABLE stripe_events (
  event_id TEXT PRIMARY KEY, type TEXT NOT NULL, household_id TEXT, outcome TEXT NOT NULL,
  received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
-- The monthly auto-recharge cap is enforced by inserting a row here only while (this month's pending + succeeded) + price <= cap.
CREATE TABLE auto_recharges (
  recharge_id TEXT PRIMARY KEY, household_id TEXT NOT NULL,
  pack_sku TEXT NOT NULL CHECK (pack_sku IN ('pack_10','pack_25','pack_60')), credits INTEGER NOT NULL, amount_cents INTEGER NOT NULL,
  run_id TEXT, payment_intent_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending','succeeded','failed')), error TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
CREATE INDEX ix_recharges_hh ON auto_recharges(household_id, created_at);

-- ── Life Receipt ────────────────────────────────────────────────────────────────────────────────────────────────────
-- week_start = the Monday (household-local date) of the receipt's week. One receipt per household per week; regenerating replaces it.
CREATE TABLE life_receipts (
  receipt_id TEXT PRIMARY KEY, household_id TEXT NOT NULL, week_start TEXT NOT NULL,
  payload_json TEXT NOT NULL, found_cents INTEGER NOT NULL DEFAULT 0,
  emailed_at TEXT, pushed_at TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE (household_id, week_start));

-- Return windows are read when present; no extractor writes them yet.
INSERT INTO predicate_registry (predicate, cardinality, value_type, decay_days, domain) VALUES
 ('return_window_ends_on', 'one',  'date',  NULL, 'budget'),
 ('return_eligible_cents', 'one',  'money', NULL, 'budget'),
 -- what the member asked the concierge to watch: written by code from the member's own request, with that request as the quote
 ('drop_alarm_target',     'many', 'text',  NULL, 'calendar'),
 ('price_watch_target',    'many', 'text',  NULL, 'calendar');

-- ── Engine B: drop alarms and flight price watches ─────────────────────────────────────────────────────────────────
CREATE TABLE drop_alarms (
  alarm_id TEXT PRIMARY KEY, household_id TEXT NOT NULL, venue_entity_id TEXT NOT NULL REFERENCES entities(entity_id),
  reservation_date TEXT NOT NULL, party_size INTEGER NOT NULL CHECK (party_size BETWEEN 1 AND 12),
  -- when the venue releases tables for reservation_date (UTC ISO), computed from the stored release rule
  release_at TEXT NOT NULL, grounding_fact_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('scheduled','staged','link_issued','missed','canceled')),
  run_id TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE (household_id, venue_entity_id, reservation_date, party_size));
CREATE INDEX ix_alarms_hh ON drop_alarms(household_id, status, release_at);

ALTER TABLE price_watches ADD COLUMN created_at TEXT;
ALTER TABLE price_watches ADD COLUMN last_checked_at TEXT;
ALTER TABLE price_watches ADD COLUMN last_fare_cents INTEGER;
ALTER TABLE price_watches ADD COLUMN run_id TEXT;
CREATE INDEX ix_watches_hh ON price_watches(household_id, active);
-- At most two active flight watches per household, however a row gets there.
CREATE TRIGGER price_watch_cap_insert BEFORE INSERT ON price_watches
  WHEN NEW.active = 1 AND (SELECT COUNT(*) FROM price_watches WHERE household_id = NEW.household_id AND active = 1) >= 2
  BEGIN SELECT RAISE(ABORT, 'at most 2 active flight watches per household'); END;
CREATE TRIGGER price_watch_cap_reactivate BEFORE UPDATE OF active ON price_watches
  WHEN NEW.active = 1 AND OLD.active = 0 AND (SELECT COUNT(*) FROM price_watches WHERE household_id = NEW.household_id AND active = 1) >= 2
  BEGIN SELECT RAISE(ABORT, 'at most 2 active flight watches per household'); END;

-- ── Mission templates for Engine B (both end in a deep link; the member approves the staged card first) ───────────────
INSERT INTO mission_templates (template_key, version, plan_json, max_tier, credits_cost) VALUES
 ('drop_alarm', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":null,"max_input_tokens":0},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0},{"id":"link","kind":"deeplink","tier":"R3","worker_prompt_id":null,"max_input_tokens":0}]',
  'R3', 1),
 ('flight_watch', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":null,"max_input_tokens":0},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0},{"id":"link","kind":"deeplink","tier":"R3","worker_prompt_id":null,"max_input_tokens":0}]',
  'R3', 1);
