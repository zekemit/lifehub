-- migrations/0001_init.sql — Life Hub D1 schema (CLAUDE.md §3, verbatim) + seed data.
--
-- Conventions the application layer relies on (not enforced by SQLite):
--   * Timestamps are canonical UTC ISO-8601 with milliseconds: 'YYYY-MM-DDTHH:MM:SS.sssZ'
--     (identical to strftime('%Y-%m-%dT%H:%M:%fZ','now')). src/graph/writeFact.ts normalizes
--     every valid_from / valid_to / learned_at / expired_at it writes to this shape, so plain
--     string comparison orders them correctly.
--   * entity_aliases.alias_norm = '<entity_type>:<normalized name>' (the PK has no type column).
--   * benefit_catalog.merchant_match = '|'-separated, case-insensitive substrings of
--     transactions.descriptor_norm; a token starting with 'category:' names an issuer-defined
--     merchant category that the matcher resolves itself instead of matching a descriptor.
--   * policy_blocklist matching is by domain suffix (the domain or any subdomain of it).
--
-- ===================== SCHEMA (CLAUDE.md §3) =====================

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

-- ===================== SEED: predicate_registry =====================
-- cardinality 'one'  => writeFact closes/supersedes the current fact when the value changes.
-- cardinality 'many' => facts coexist; an identical assertion only adds supporting quotes.
-- decay_days         => NightlySweepWF closes facts whose newest supporting episode is older (NULL = never).
-- value_type 'interval' is stored in value_text as '<start>/<end>' (ISO dates or UTC datetimes).
INSERT INTO predicate_registry (predicate, cardinality, value_type, decay_days, domain) VALUES
 -- people
 ('birthday',              'one',  'date',   NULL, 'people'),
 ('anniversary_date',      'one',  'date',   NULL, 'people'),
 ('spouse_of',             'one',  'entity', NULL, 'people'),
 ('member_of_household',   'one',  'entity', NULL, 'people'),
 ('home_city',             'one',  'text',   730,  'people'),
 ('dietary_restriction',   'many', 'text',   730,  'people'),
 -- cards
 ('holds_card',            'many', 'entity', NULL, 'cards'),
 ('card_product',          'one',  'text',   NULL, 'cards'),
 ('card_last4',            'one',  'text',   NULL, 'cards'),
 ('card_opened_on',        'one',  'date',   NULL, 'cards'),
 ('annual_fee',            'one',  'money',  400,  'cards'),
 ('annual_fee_posts_on',   'one',  'date',   400,  'cards'),
 ('benefit_enrolled',      'many', 'entity', NULL, 'cards'),
 -- budget
 ('recurring_amount',      'one',  'money',  400,  'budget'),
 ('recurring_cadence',     'one',  'text',   400,  'budget'),
 ('recurring_merchant',    'one',  'entity', 400,  'budget'),
 ('charged_to_card',       'one',  'entity', 400,  'budget'),
 ('next_charge_on',        'one',  'date',   45,   'budget'),
 -- calendar
 ('event_date',            'one',  'date',   NULL, 'calendar'),
 ('event_kind',            'one',  'text',   NULL, 'calendar'),
 ('trip_window',           'one',  'interval', NULL, 'calendar'),
 ('trip_destination',      'one',  'text',   NULL, 'calendar'),
 ('attends',               'many', 'entity', NULL, 'calendar'),
 -- taste
 ('prefers_cuisine',       'many', 'entity', 365,  'taste'),
 ('avoids_cuisine',        'many', 'entity', 365,  'taste'),
 ('dined_at',              'many', 'entity', 730,  'taste'),
 ('prefers_airline',       'many', 'entity', 730,  'taste'),
 ('prefers_hotel_brand',   'many', 'entity', 730,  'taste'),
 -- vault
 ('document_type',         'one',  'text',   NULL, 'vault'),
 ('document_tax_year',     'one',  'entity', NULL, 'vault'),
 ('document_issuer',       'one',  'entity', NULL, 'vault'),
 ('document_expires_on',   'one',  'date',   NULL, 'vault');

-- ===================== SEED: benefit_catalog =====================
-- Every row below was read from the issuer's public card page on 2026-11-26 (verified_on / source_url).
-- amount_cents is the per-period maximum. Uber Cash is seeded at its $15 base so December ($20) is
-- understated, never overstated. Deliberately NOT seeded because the schema cannot express them
-- faithfully: Amex airline-fee credit (merchant = the cardholder's one selected airline), Global
-- Entry/TSA credits (4-year cadence), Chase The Edit credit (period not stated on the page), Venture X
-- anniversary miles (not a statement credit), Chase DashPass/DoorDash promos (promos, not credits).
INSERT INTO benefit_catalog
  (benefit_key, card_product, label, amount_cents, cadence, merchant_match, enrollment_required, verified_on, source_url) VALUES
 -- American Express Platinum Card (annual fee $895)
 ('amex_platinum.resy_credit', 'amex_platinum',
  'Resy credit: $100 per quarter at qualifying U.S. Resy restaurants', 10000, 'quarterly', 'RESY', 1,
  '2026-11-26', 'https://www.americanexpress.com/us/credit-cards/card/platinum/'),
 ('amex_platinum.digital_entertainment_credit', 'amex_platinum',
  'Digital entertainment credit: $25 per month at participating partners', 2500, 'monthly',
  'DISNEY|ESPN|HULU|NYTIMES|NEW YORK TIMES|PARAMOUNT|PEACOCK|WALL STREET JOURNAL|WSJ|YOUTUBE', 1,
  '2026-11-26', 'https://www.americanexpress.com/us/credit-cards/card/platinum/'),
 ('amex_platinum.uber_cash', 'amex_platinum',
  'Uber Cash: $15 per month on U.S. rides and orders ($20 in December)', 1500, 'monthly', 'UBER', 0,
  '2026-11-26', 'https://www.americanexpress.com/us/credit-cards/card/platinum/'),
 ('amex_platinum.uber_one_credit', 'amex_platinum',
  'Uber One credit: up to $120 per calendar year on an auto-renewing Uber One membership', 12000, 'annual', 'UBER ONE', 0,
  '2026-11-26', 'https://www.americanexpress.com/us/credit-cards/card/platinum/'),
 ('amex_platinum.hotel_credit', 'amex_platinum',
  'Hotel credit: $300 per half-year on prepaid Fine Hotels + Resorts / The Hotel Collection via Amex Travel', 30000, 'semiannual',
  'AMEX TRAVEL|AMERICAN EXPRESS TRAVEL|FINE HOTELS|HOTEL COLLECTION', 0,
  '2026-11-26', 'https://www.americanexpress.com/us/credit-cards/card/platinum/'),
 ('amex_platinum.clear_plus_credit', 'amex_platinum',
  'CLEAR+ credit: up to $219 per calendar year on an auto-renewing CLEAR+ membership', 21900, 'annual', 'CLEAR', 0,
  '2026-11-26', 'https://www.americanexpress.com/us/credit-cards/card/platinum/'),
 ('amex_platinum.lululemon_credit', 'amex_platinum',
  'lululemon credit: $75 per quarter at U.S. lululemon stores and lululemon.com', 7500, 'quarterly', 'LULULEMON', 1,
  '2026-11-26', 'https://www.americanexpress.com/us/credit-cards/card/platinum/'),
 ('amex_platinum.walmart_plus_credit', 'amex_platinum',
  'Walmart+ credit: up to $12.95 per month (plus taxes) on one monthly membership', 1295, 'monthly', 'WALMART+|WALMART PLUS', 0,
  '2026-11-26', 'https://www.americanexpress.com/us/credit-cards/card/platinum/'),
 ('amex_platinum.oura_ring_credit', 'amex_platinum',
  'Oura Ring credit: up to $200 per calendar year on an Oura Ring bought at ouraring.com', 20000, 'annual', 'OURA', 1,
  '2026-11-26', 'https://www.americanexpress.com/us/credit-cards/card/platinum/'),
 -- Chase Sapphire Reserve (annual fee $795)
 ('chase_sapphire_reserve.travel_credit', 'chase_sapphire_reserve',
  'Travel credit: $300 per account anniversary year on travel purchases', 30000, 'cardmember_year', 'category:travel', 0,
  '2026-11-26', 'https://creditcards.chase.com/rewards-credit-cards/sapphire/reserve'),
 ('chase_sapphire_reserve.dining_credit', 'chase_sapphire_reserve',
  'Dining credit: $150 per half-year at Sapphire Reserve Exclusive Tables restaurants on OpenTable', 15000, 'semiannual', 'OPENTABLE', 0,
  '2026-11-26', 'https://creditcards.chase.com/rewards-credit-cards/sapphire/reserve'),
 ('chase_sapphire_reserve.stubhub_credit', 'chase_sapphire_reserve',
  'StubHub / viagogo credit: $150 per half-year (activation required; offered through 2027-12-31)', 15000, 'semiannual',
  'STUBHUB|VIAGOGO', 1, '2026-11-26', 'https://creditcards.chase.com/rewards-credit-cards/sapphire/reserve'),
 ('chase_sapphire_reserve.lyft_credit', 'chase_sapphire_reserve',
  'Lyft credit: $10 per month in-app on rides (offered through 2027-09-30)', 1000, 'monthly', 'LYFT', 0,
  '2026-11-26', 'https://creditcards.chase.com/rewards-credit-cards/sapphire/reserve'),
 -- Capital One Venture X (annual fee $395)
 ('capital_one_venture_x.travel_credit', 'capital_one_venture_x',
  'Travel credit: $300 per account year toward purchases made through Capital One Travel', 30000, 'cardmember_year', 'CAPITAL ONE TRAVEL', 0,
  '2026-11-26', 'https://www.capitalone.com/credit-cards/venture-x/');

-- policy_blocklist is seeded by the INSERT inside the schema block above (6 rows from CLAUDE.md §3).
-- It is not repeated here because policy_blocklist.domain is the PRIMARY KEY.
