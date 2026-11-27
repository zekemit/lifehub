-- migrations/0005_milestone7.sql — Milestone 7: autonomous goals (GoalExecutionWF), their task DAG, and the hosted micro-apps they build.
--
-- Additive only: four new tables, indexes and triggers, one predicate. No existing table is rebuilt or touched.
--
-- Credits. A goal reserves 3 credits (research_plan, Gate G8 cap $0.75) or 5 (micro_app, cap $1.25): $0.25 per credit, the same linear
-- cap as every other run (src/missions/gates.ts). The CHECK below pins the three numbers together. The debit is an ordinary ledger row
-- keyed by the goal id (src/billing/credits.ts), so credit_ledger.run_id, which references mission_runs, is satisfied by an inert anchor row
-- (template goal_*, state CANCELED, so it never shows on Home, never holds a Home slot and is never counted as a finished mission).
--
-- Predicates: dietary_restriction already exists (migrations/0001, domain 'people') and keeps its domain; a second row would violate the
-- primary key. predicate_registry.domain stays within people/cards/budget/calendar/taste/vault: hosted_micro_app is a vault record.
INSERT INTO predicate_registry (predicate, cardinality, value_type, decay_days, domain) VALUES
 ('hosted_micro_app', 'many', 'text', NULL, 'vault');

CREATE TABLE goals (
  goal_id TEXT PRIMARY KEY,
  household_id TEXT NOT NULL REFERENCES households(household_id),
  submitted_by_member_id TEXT REFERENCES members(member_id),
  prompt_text TEXT NOT NULL,                               -- the member's request AFTER redact(): never raw PII
  tier TEXT NOT NULL CHECK (tier IN ('research_plan', 'micro_app')),
  status TEXT NOT NULL CHECK (status IN ('PLANNING', 'EXECUTING', 'VERIFYING', 'AWAITING_APPROVAL', 'COMPLETED', 'DECLINED', 'FAILED', 'INSUFFICIENT_CREDITS')),
  credits_reserved INTEGER NOT NULL CHECK (credits_reserved IN (3, 5)),
  max_cost_usd REAL NOT NULL,                              -- $0.25 * credits_reserved
  cost_usd REAL NOT NULL DEFAULT 0,
  approval_hash TEXT,                                      -- hash of exactly what the member is asked to approve
  preview_url TEXT,
  prod_url TEXT,
  install_artifact_json TEXT,
  result_summary_json TEXT,
  error_code TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  -- Workflow instance counter: the first instance is the goal id, a restart after a top-up is '<goal_id>~<attempt>'
  attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  CHECK ((tier = 'research_plan' AND credits_reserved = 3 AND max_cost_usd = 0.75) OR (tier = 'micro_app' AND credits_reserved = 5 AND max_cost_usd = 1.25)),
  CHECK (cost_usd >= 0));
CREATE INDEX ix_goals_hh ON goals(household_id, status, created_at);

CREATE TABLE goal_tasks (
  task_id TEXT PRIMARY KEY,
  goal_id TEXT NOT NULL REFERENCES goals(goal_id),
  seq_no INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('decompose', 'context_gather', 'schema_gen', 'code_gen', 'verify_bundle', 'smoke_test', 'package_install')),
  spec_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'PASSED', 'FAILED')),
  retries INTEGER NOT NULL DEFAULT 0 CHECK (retries <= 2),
  output_json TEXT,
  error_text TEXT,
  UNIQUE (goal_id, seq_no));

CREATE TABLE micro_apps (
  app_id TEXT PRIMARY KEY,
  household_id TEXT NOT NULL REFERENCES households(household_id),
  goal_id TEXT NOT NULL REFERENCES goals(goal_id),
  slug TEXT NOT NULL,
  title TEXT NOT NULL,
  bundle_r2_key TEXT NOT NULL,                             -- the verified bundle, AES-GCM encrypted in R2
  manifest_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('STAGED', 'LIVE', 'ARCHIVED')),
  monthly_ai_calls_used INTEGER NOT NULL DEFAULT 0,
  monthly_ai_calls_cap INTEGER NOT NULL DEFAULT 100,
  version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  -- the UTC month ('YYYY-MM') monthly_ai_calls_used counts; the first call of a new month starts the count again
  quota_month TEXT,
  UNIQUE (household_id, slug),
  CHECK (monthly_ai_calls_used >= 0 AND monthly_ai_calls_cap >= 0));
CREATE INDEX ix_apps_goal ON micro_apps(goal_id);

-- Everything a generated app stores lives here, never in a table of its own: a micro-app cannot run DDL, and every query against these rows names
-- both app_id and household_id.
CREATE TABLE micro_app_records (
  record_id TEXT PRIMARY KEY,
  app_id TEXT NOT NULL REFERENCES micro_apps(app_id),
  household_id TEXT NOT NULL REFERENCES households(household_id),
  collection TEXT NOT NULL,
  title TEXT NOT NULL,
  search_tokens TEXT NOT NULL,                             -- ' ' + normalized title, ingredient and tag words + ' ' (tags as '#tag')
  data_json TEXT NOT NULL,
  created_at TEXT NOT NULL);
CREATE INDEX ix_records_app ON micro_app_records(app_id, household_id, collection, created_at);

-- Tenancy in the database itself: a record or an app cannot name a household other than its owner's, whichever code path writes it.
CREATE TRIGGER micro_app_records_tenant BEFORE INSERT ON micro_app_records
  WHEN (SELECT household_id FROM micro_apps WHERE app_id = NEW.app_id) IS NOT NEW.household_id
  BEGIN SELECT RAISE(ABORT, 'a record must belong to its app''s household'); END;
CREATE TRIGGER micro_apps_tenant BEFORE INSERT ON micro_apps
  WHEN (SELECT household_id FROM goals WHERE goal_id = NEW.goal_id) IS NOT NEW.household_id
  BEGIN SELECT RAISE(ABORT, 'an app must belong to its goal''s household'); END;
