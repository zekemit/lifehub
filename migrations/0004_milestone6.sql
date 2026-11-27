-- migrations/0004_milestone6.sql — Milestone 6: the inbox funnel (cross-channel dedupe, bulk filter, forwarding confirmation),
-- the Gmail connector kind, nine life-admin predicates, and three standing triggers with their mission templates.
--
-- predicate_registry.domain is CHECKed to ('people','cards','budget','calendar','taste','vault') (migrations/0001_init.sql). Every new
-- predicate below uses one of those six: money and purchase terms -> budget, dates the household acts on -> calendar, product and
-- warranty records -> vault.
--
-- Trigger contract: see migrations/0002_m3.sql. One addition for M6: a trigger's SELECT may return a `template_key` column; when it does,
-- that template is staged for the row instead of the trigger's default (src/missions/engine.ts checks it names a stored template). It lets
-- one standing trigger stage a 1-credit card for one situation and a 3-credit card for another.

-- ── Predicates ──────────────────────────────────────────────────────────────────────────────────────────────────────────
-- Subjects: product (price, return window, warranty, serial, price drop), trip (flight delay), event (registration, checklist, window).
INSERT INTO predicate_registry (predicate, cardinality, value_type, decay_days, domain) VALUES
 ('purchase_price_cents',      'one',  'money',    NULL, 'budget'),
 ('return_window_closes_on',   'one',  'date',     NULL, 'budget'),
 -- a price drop is an observation about now: it stops being believed 90 days after it was last seen
 ('price_drop_detected_cents', 'one',  'money',    90,   'budget'),
 ('warranty_expires_on',       'one',  'date',     NULL, 'vault'),
 ('serial_number',             'one',  'text',     NULL, 'vault'),
 ('flight_delay_minutes',      'one',  'num',      180,  'calendar'),
 -- a date, not a date-time: writeFact validates 'date' as YYYY-MM-DD, and a school's "registration opens Dec 1" rarely names an hour
 ('registration_opens_at',     'one',  'date',     NULL, 'calendar'),
 ('required_checklist_item',   'many', 'text',     365,  'calendar'),
 ('event_window',              'one',  'interval', NULL, 'calendar');

-- ── Inbox funnel state ──────────────────────────────────────────────────────────────────────────────────────────────────
CREATE TABLE inbox_connections (
  household_id TEXT PRIMARY KEY REFERENCES households(household_id),
  gmail_history_id TEXT,
  -- the Gmail / Outlook / iCloud "confirm auto-forwarding" message, captured before redaction can quarantine it (shown to the member, never fetched)
  forwarding_confirm_code TEXT,
  forwarding_confirm_url TEXT,
  forwarding_verified_at TEXT,
  triage_dropped_bulk_count INTEGER NOT NULL DEFAULT 0 CHECK (triage_dropped_bulk_count >= 0),
  last_sync_at TEXT);

-- One row per message the household has ever been offered, whichever channel it arrived by. The PRIMARY KEY is the cross-channel dedupe:
-- the same Message-ID arriving by auto-forward AND by Gmail sync claims the row once. triage_bucket is NULL while the message is claimed
-- but not yet classified.
CREATE TABLE ingested_messages (
  household_id TEXT NOT NULL,
  message_id_norm TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('email_forward','gmail')),
  received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  triage_bucket TEXT CHECK (triage_bucket IS NULL OR triage_bucket IN (
    'DROPPED_BULK','FORWARDING_CONFIRM','QUARANTINED',
    'RECEIPT_ORDER','BILL_SUBSCRIPTION','TRAVEL_ITINERARY','WARRANTY_PRODUCT','TAX_LEGAL_DOC','FAMILY_SCHOOL_EVENT','PERSONAL_FOLLOWUP_NEEDED','IGNORE')),
  episode_id TEXT,
  PRIMARY KEY (household_id, message_id_norm));
CREATE INDEX ix_ingested_hh_time ON ingested_messages(household_id, received_at);

-- ── The Gmail connector ─────────────────────────────────────────────────────────────────────────────────────────────────
-- connectors.kind is CHECKed, so the table is rebuilt to admit 'gmail'. Nothing references connectors, so a plain copy is safe.
CREATE TABLE connectors_m6 (
  connector_id TEXT PRIMARY KEY, household_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('email_alias','google_calendar','gmail','spend_csv','spend_pdf','card_list','upload','byo_agent_mcp')),
  status TEXT NOT NULL CHECK (status IN ('pending','active','error','revoked')),
  scopes TEXT, token_kv_key TEXT, sync_cursor TEXT, last_success_at TEXT, last_error TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
INSERT INTO connectors_m6 (connector_id, household_id, kind, status, scopes, token_kv_key, sync_cursor, last_success_at, last_error, created_at)
  SELECT connector_id, household_id, kind, status, scopes, token_kv_key, sync_cursor, last_success_at, last_error, created_at FROM connectors;
DROP TABLE connectors;
ALTER TABLE connectors_m6 RENAME TO connectors;
CREATE INDEX ix_connectors_hh ON connectors(household_id, kind);

-- ── Mission templates ───────────────────────────────────────────────────────────────────────────────────────────────────
-- All five end at R2: the member does the return, the claim or the registration themselves. credits_cost is the weight the ledger debits
-- and Gate G8 scales its cap by ($0.25 per credit: 1 -> $0.25, 3 -> $0.75).
INSERT INTO mission_templates (template_key, version, plan_json, max_tier, credits_cost) VALUES
 ('return_deadline', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":null,"max_input_tokens":0},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0}]',
  'R2', 1),
 ('warranty_claim', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":"warranty_claim_packet","max_input_tokens":60000},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0}]',
  'R2', 3),
 ('price_drop_claim', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":"price_drop_claim_packet","max_input_tokens":60000},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0}]',
  'R2', 3),
 ('delay_claim', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":"delay_claim_packet","max_input_tokens":60000},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0}]',
  'R2', 3),
 ('family_event', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":null,"max_input_tokens":0},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0}]',
  'R2', 1);

-- ── T_CLOSING_RETURN_OR_WARRANTY ────────────────────────────────────────────────────────────────────────────────────────
-- return_deadline (1 credit):  a return window closes within 5 days on an item bought for $40 or more.
-- warranty_claim  (3 credits): a product warranty expires within 30 days (the claim-packet tier). A member can also request the packet
--                              at any time before expiry (src/missions/triggers.ts requestWarrantyClaim); it shares this dedupe_key.
-- The return card's claim rests on the return-window date alone. The price is read from its fact to apply the $40 floor, but it is not a
-- claim fact: Gate G2 distrusts money facts last confirmed over 35 days ago, and a purchase price does not go stale.
INSERT INTO triggers (trigger_key, template_key, cooldown_days, sql_condition) VALUES
 ('T_CLOSING_RETURN_OR_WARRANTY', 'return_deadline', 30,
  'SELECT ''return|'' || r.subject_id || ''|'' || r.value_text AS dedupe_key,
          ''return_deadline'' AS template_key,
          price.value_cents AS recoverable_cents,
          MAX(0.2, 1.0 - (julianday(r.value_text) - julianday(?3)) / 6.0) AS urgency,
          1.0 AS confidence,
          json_array(json_object(''key'', ''window'', ''facts'', json_array(r.fact_id))) AS claims_json,
          json_object(''kind'', ''return'', ''product'', e.canonical_name, ''price_cents'', price.value_cents, ''closes_on'', r.value_text, ''expires_on'', r.value_text,
                      ''days_left'', CAST(julianday(r.value_text) - julianday(?3) AS INTEGER)) AS data_json
     FROM facts_current r
     JOIN entities e ON e.entity_id = r.subject_id
     JOIN facts_current price ON price.household_id = r.household_id AND price.subject_id = r.subject_id AND price.predicate = ''purchase_price_cents''
    WHERE r.household_id = ?1 AND r.predicate = ''return_window_closes_on''
      AND r.value_text >= ?3 AND r.value_text <= date(?3, ''+5 days'')
      AND price.value_cents >= 4000
   UNION ALL
   SELECT ''warranty|'' || w.subject_id || ''|'' || w.value_text AS dedupe_key,
          ''warranty_claim'' AS template_key,
          0 AS recoverable_cents,
          MAX(0.1, 1.0 - (julianday(w.value_text) - julianday(?3)) / 31.0) AS urgency,
          1.0 AS confidence,
          json_array(json_object(''key'', ''warranty'', ''facts'', json_array(w.fact_id))) AS claims_json,
          json_object(''kind'', ''warranty'', ''product'', e.canonical_name, ''expires_on'', w.value_text,
                      ''days_left'', CAST(julianday(w.value_text) - julianday(?3) AS INTEGER),
                      ''serial'', (SELECT s.value_text FROM facts_current s WHERE s.household_id = w.household_id AND s.subject_id = w.subject_id AND s.predicate = ''serial_number'' LIMIT 1)) AS data_json
     FROM facts_current w
     JOIN entities e ON e.entity_id = w.subject_id
    WHERE w.household_id = ?1 AND w.predicate = ''warranty_expires_on''
      AND w.value_text >= ?3 AND w.value_text <= date(?3, ''+30 days'')');

-- ── T_PRICE_DROP_OR_DELAY_CLAIM (3 credits, R2 claim packet) ────────────────────────────────────────────────────────────
-- price_drop_claim: a retailer price drop of $15 or more on an item still inside its return window (or, when no window was stated, within
--                   30 days of the purchase). Both facts are money facts, so both must have been confirmed within 35 days: otherwise Gate
--                   G2 would refuse the card after the member had tapped it, and it is not offered.
-- delay_claim:      a flight delay or cancellation of 180 minutes or more recorded in the last 90 days on a trip that also has its booking
--                   on record. The card carries verbatim quotes from the original booking and from the update.
INSERT INTO triggers (trigger_key, template_key, cooldown_days, sql_condition) VALUES
 ('T_PRICE_DROP_OR_DELAY_CLAIM', 'price_drop_claim', 30,
  'SELECT ''drop|'' || d.subject_id || ''|'' || d.value_cents AS dedupe_key,
          ''price_drop_claim'' AS template_key,
          d.value_cents AS recoverable_cents,
          MAX(0.2, 1.0 - (julianday(COALESCE(rw.value_text, date(p.valid_from, ''+30 days'')) ) - julianday(?3)) / 30.0) AS urgency,
          1.0 AS confidence,
          json_array(json_object(''key'', ''purchase'', ''facts'', json_array(p.fact_id)), json_object(''key'', ''drop'', ''facts'', json_array(d.fact_id))) AS claims_json,
          json_object(''kind'', ''price_drop'', ''product'', e.canonical_name, ''purchase_cents'', p.value_cents, ''drop_cents'', d.value_cents,
                      ''window_ends'', COALESCE(rw.value_text, date(p.valid_from, ''+30 days'')),
                      ''expires_on'', COALESCE(rw.value_text, date(p.valid_from, ''+30 days''))) AS data_json
     FROM facts_current d
     JOIN entities e ON e.entity_id = d.subject_id
     JOIN facts_current p ON p.household_id = d.household_id AND p.subject_id = d.subject_id AND p.predicate = ''purchase_price_cents''
     LEFT JOIN facts_current rw ON rw.household_id = d.household_id AND rw.subject_id = d.subject_id AND rw.predicate = ''return_window_closes_on''
    WHERE d.household_id = ?1 AND d.predicate = ''price_drop_detected_cents'' AND d.value_cents >= 1500
      AND COALESCE(rw.value_text, date(p.valid_from, ''+30 days'')) >= ?3
      AND (SELECT MAX(x.received_at) FROM fact_quotes q JOIN episodes x ON x.episode_id = q.episode_id WHERE q.fact_id = d.fact_id) >= strftime(''%Y-%m-%dT%H:%M:%fZ'', ?2, ''-35 days'')
      AND (SELECT MAX(x.received_at) FROM fact_quotes q JOIN episodes x ON x.episode_id = q.episode_id WHERE q.fact_id = p.fact_id) >= strftime(''%Y-%m-%dT%H:%M:%fZ'', ?2, ''-35 days'')
   UNION ALL
   SELECT ''delay|'' || f.subject_id || ''|'' || CAST(f.value_num AS INTEGER) AS dedupe_key,
          ''delay_claim'' AS template_key,
          0 AS recoverable_cents,
          0.8 AS urgency,
          1.0 AS confidence,
          json_array(json_object(''key'', ''booking'', ''facts'', json_array(b.fact_id)), json_object(''key'', ''delay'', ''facts'', json_array(f.fact_id))) AS claims_json,
          json_object(''kind'', ''delay'', ''trip'', t.canonical_name, ''delay_minutes'', CAST(f.value_num AS INTEGER), ''booking_predicate'', b.predicate, ''booking_text'', b.value_text,
                      ''noted_on'', substr(f.learned_at, 1, 10),
                      ''expires_on'', date(f.learned_at, ''+90 days'')) AS data_json
     FROM facts_current f
     JOIN entities t ON t.entity_id = f.subject_id AND t.type = ''trip''
     JOIN facts_current b ON b.fact_id = (SELECT x.fact_id FROM facts_current x
                                           WHERE x.household_id = f.household_id AND x.subject_id = f.subject_id AND x.predicate IN (''trip_window'', ''trip_destination'')
                                           ORDER BY CASE x.predicate WHEN ''trip_window'' THEN 0 ELSE 1 END, x.fact_id LIMIT 1)
    WHERE f.household_id = ?1 AND f.predicate = ''flight_delay_minutes'' AND f.value_num >= 180
      AND f.learned_at >= strftime(''%Y-%m-%dT%H:%M:%fZ'', ?2, ''-90 days'')');

-- ── T_FAMILY_EVENT_AUTOPILOT (1 credit) ─────────────────────────────────────────────────────────────────────────────────
-- An event whose registration opens today or later, within 90 days. The card is a calendar/checklist card: one claim that names the
-- registration date and up to five required checklist items, every one of them backed by a verbatim quote from the school's message.
INSERT INTO triggers (trigger_key, template_key, cooldown_days, sql_condition) VALUES
 ('T_FAMILY_EVENT_AUTOPILOT', 'family_event', 14,
  'SELECT ''event|'' || r.subject_id || ''|'' || r.value_text AS dedupe_key,
          ''family_event'' AS template_key,
          0 AS recoverable_cents,
          MAX(0.1, 1.0 - (julianday(r.value_text) - julianday(?3)) / 90.0) AS urgency,
          1.0 AS confidence,
          json_array(json_object(''key'', ''plan'', ''facts'',
            json((SELECT json_group_array(z.fid) FROM (SELECT r.fact_id AS fid, 0 AS ord
                                                       UNION ALL
                                                       SELECT * FROM (SELECT c.fact_id AS fid, 1 AS ord FROM facts_current c
                                                                       WHERE c.household_id = r.household_id AND c.subject_id = r.subject_id AND c.predicate = ''required_checklist_item''
                                                                       ORDER BY c.value_text, c.fact_id LIMIT 5)
                                                       ORDER BY ord) z)))) AS claims_json,
          json_object(''event'', e.canonical_name, ''registration_opens_on'', r.value_text, ''expires_on'', r.value_text,
                      ''event_window'', (SELECT w.value_text FROM facts_current w WHERE w.household_id = r.household_id AND w.subject_id = r.subject_id AND w.predicate = ''event_window'' LIMIT 1),
                      ''event_date'', (SELECT w.value_text FROM facts_current w WHERE w.household_id = r.household_id AND w.subject_id = r.subject_id AND w.predicate = ''event_date'' LIMIT 1),
                      ''checklist'', json((SELECT json_group_array(k.value_text) FROM (SELECT c.value_text FROM facts_current c
                                                                                       WHERE c.household_id = r.household_id AND c.subject_id = r.subject_id AND c.predicate = ''required_checklist_item''
                                                                                       ORDER BY c.value_text, c.fact_id LIMIT 5) k))) AS data_json
     FROM facts_current r
     JOIN entities e ON e.entity_id = r.subject_id AND e.type = ''event''
    WHERE r.household_id = ?1 AND r.predicate = ''registration_opens_at''
      AND r.value_text >= ?3 AND r.value_text <= date(?3, ''+90 days'')');
