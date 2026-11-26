-- migrations/0002_m3.sql — Milestone 3: calendar free windows, mission templates, standing triggers.
--
-- Trigger contract (enforced by src/missions/engine.ts before a trigger is ever run):
--   * sql_condition is ONE read-only SELECT (or WITH ... SELECT) written by engineers in a migration. It is never built
--     from user or model text, never contains ';', and may not use INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/ATTACH/PRAGMA...
--   * Parameters: ?1 = household_id, ?2 = now (ISO timestamp), ?3 = today (YYYY-MM-DD). Every trigger must use ?1.
--   * Result columns (one row per candidate mission):
--       dedupe_key        TEXT     identifies the thing acted on; the cooldown (Gate G9) is per (template, dedupe_key)
--       recoverable_cents INTEGER  money the card helps the household recover or save
--       urgency           REAL     0..1, higher = sooner
--       confidence        REAL     0..1, 0.5 when the data cannot confirm the claim (e.g. unconfirmed credit periods)
--       claims_json       TEXT     [{"key": "...", "facts": [fact_id, ...]}]  every claim must be backed by verified quotes (G1)
--       data_json         TEXT     values the card copy is built from (src/missions/copy.ts)
--   Candidates are ranked by recoverable_cents * confidence * urgency.

-- Free evening windows (Fri/Sat 18:00-22:00 local) computed from Google freeBusy. value_text = '<start>/<end>' in UTC.
-- decay_days = 2: a window that the nightly calendar sync does not re-confirm stops being current.
INSERT INTO predicate_registry (predicate, cardinality, value_type, decay_days, domain) VALUES
 ('free_window', 'one', 'interval', 2, 'calendar');

INSERT INTO mission_templates (template_key, version, plan_json, max_tier, credits_cost) VALUES
 ('expiring_credit', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":null,"max_input_tokens":0},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0},{"id":"link","kind":"deeplink","tier":"R3","worker_prompt_id":null,"max_input_tokens":0}]',
  'R3', 1),
 ('annual_fee_roi', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":"retention_script","max_input_tokens":60000},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0}]',
  'R2', 1),
 ('anniv_dining', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":null,"max_input_tokens":0},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0},{"id":"link","kind":"deeplink","tier":"R3","worker_prompt_id":null,"max_input_tokens":0}]',
  'R3', 1),
 ('subscription_creep', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":"cancel_script","max_input_tokens":60000},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0}]',
  'R2', 1),
 ('occasion_plan', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":null,"max_input_tokens":0},{"id":"approve","kind":"approve","tier":"R2","worker_prompt_id":null,"max_input_tokens":0}]',
  'R2', 1),
 ('unlock_scan', 1,
  '[{"id":"read","kind":"read","tier":"R0","worker_prompt_id":null,"max_input_tokens":0},{"id":"stage","kind":"stage","tier":"R1","worker_prompt_id":null,"max_input_tokens":0}]',
  'R1', 0);

-- ── T_EXPIRING_CREDIT: a card credit with >= $25 left whose period ends within 30 days ──────────────────────────────
INSERT INTO triggers (trigger_key, template_key, cooldown_days, sql_condition) VALUES
 ('T_EXPIRING_CREDIT', 'expiring_credit', 14,
  'SELECT cp.card_entity_id || ''|'' || cp.benefit_key || ''|'' || cp.period_start AS dedupe_key,
          b.amount_cents - cp.used_cents AS recoverable_cents,
          MAX(0.05, (31.0 - (julianday(cp.period_end) - julianday(?3))) / 31.0) AS urgency,
          CASE cp.status WHEN ''unconfirmed'' THEN 0.5 ELSE 1.0 END AS confidence,
          json_array(json_object(''key'', ''credit'', ''facts'', json_array(
            (SELECT f.fact_id FROM facts_current f WHERE f.household_id = cp.household_id AND f.subject_id = cp.card_entity_id AND f.predicate = ''card_product'' LIMIT 1)))) AS claims_json,
          json_object(''card_name'', (SELECT e.canonical_name FROM entities e WHERE e.entity_id = cp.card_entity_id),
                      ''label'', b.label, ''benefit_key'', cp.benefit_key, ''status'', cp.status,
                      ''amount_cents'', b.amount_cents, ''used_cents'', cp.used_cents,
                      ''remaining_cents'', b.amount_cents - cp.used_cents,
                      ''period_end'', cp.period_end, ''expires_on'', cp.period_end) AS data_json
     FROM credit_periods cp JOIN benefit_catalog b ON b.benefit_key = cp.benefit_key
    WHERE cp.household_id = ?1
      AND cp.status IN (''open'', ''partially_used'', ''unconfirmed'')
      AND b.amount_cents - cp.used_cents >= 2500
      AND cp.period_end >= ?3 AND cp.period_end <= date(?3, ''+30 days'')');

-- ── T_ANNUAL_FEE_ROI: the annual fee posts within 30 days and the credits used so far do not cover it ───────────────
INSERT INTO triggers (trigger_key, template_key, cooldown_days, sql_condition) VALUES
 ('T_ANNUAL_FEE_ROI', 'annual_fee_roi', 30,
  'WITH fee AS (
     SELECT p.subject_id AS card_id, p.fact_id AS posts_fact, p.value_text AS posts_on, a.fact_id AS fee_fact, a.value_cents AS fee_cents
       FROM facts_current p
       JOIN facts_current a ON a.household_id = p.household_id AND a.subject_id = p.subject_id AND a.predicate = ''annual_fee''
      WHERE p.household_id = ?1 AND p.predicate = ''annual_fee_posts_on''),
   nxt AS (
     SELECT fee.*,
            CASE WHEN date(strftime(''%Y'', ?3) || substr(posts_on, 5)) >= ?3
                 THEN date(strftime(''%Y'', ?3) || substr(posts_on, 5))
                 ELSE date((CAST(strftime(''%Y'', ?3) AS INTEGER) + 1) || substr(posts_on, 5)) END AS next_post
       FROM fee),
   used AS (
     SELECT n.card_id, COALESCE(SUM(cp.used_cents), 0) AS used_cents
       FROM nxt n
       LEFT JOIN credit_periods cp ON cp.household_id = ?1 AND cp.card_entity_id = n.card_id
            AND cp.status IN (''used'', ''partially_used'')
            AND cp.period_start > date(n.next_post, ''-1 year'') AND cp.period_start <= ?3
      GROUP BY n.card_id)
   SELECT n.card_id || ''|'' || n.next_post AS dedupe_key,
          n.fee_cents - u.used_cents AS recoverable_cents,
          MAX(0.05, (31.0 - (julianday(n.next_post) - julianday(?3))) / 31.0) AS urgency,
          CASE WHEN EXISTS (SELECT 1 FROM transactions t WHERE t.household_id = ?1 AND t.card_entity_id = n.card_id) THEN 1.0 ELSE 0.5 END AS confidence,
          json_array(json_object(''key'', ''fee'', ''facts'', json_array(n.fee_fact, n.posts_fact))) AS claims_json,
          json_object(''card_name'', (SELECT e.canonical_name FROM entities e WHERE e.entity_id = n.card_id),
                      ''fee_cents'', n.fee_cents, ''credits_used_cents'', u.used_cents, ''next_post'', n.next_post,
                      ''has_history'', EXISTS (SELECT 1 FROM transactions t WHERE t.household_id = ?1 AND t.card_entity_id = n.card_id),
                      ''expires_on'', n.next_post) AS data_json
     FROM nxt n JOIN used u ON u.card_id = n.card_id
    WHERE julianday(n.next_post) - julianday(?3) <= 30 AND n.fee_cents > u.used_cents');

-- ── T_ANNIV_DINING: the flagship cross-domain join. ALL FOUR must hold, or nothing fires:
--      calendar : an anniversary/birthday within 14 days AND a free Fri/Sat 18:00-22:00 window within 3 days of it
--      cards    : an unused dining credit (Resy / OpenTable / category:dining) with >= $50 left
--      budget   : dining envelope headroom >= $180
INSERT INTO triggers (trigger_key, template_key, cooldown_days, sql_condition) VALUES
 ('T_ANNIV_DINING', 'anniv_dining', 14,
  'WITH occ AS (
     SELECT ev.entity_id AS event_id, ev.canonical_name AS name, d.fact_id AS date_fact, d.value_text AS occasion_date,
            k.fact_id AS kind_fact, k.value_text AS kind
       FROM facts_current d
       JOIN entities ev ON ev.entity_id = d.subject_id AND ev.type = ''event''
       JOIN facts_current k ON k.household_id = d.household_id AND k.subject_id = d.subject_id AND k.predicate = ''event_kind''
      WHERE d.household_id = ?1 AND d.predicate = ''event_date'' AND k.value_text IN (''anniversary'', ''birthday'')
        AND d.value_text >= ?3 AND d.value_text <= date(?3, ''+14 days'')),
   win AS (
     SELECT w.fact_id AS window_fact, w.value_text AS span, substr(we.canonical_name, 9, 10) AS local_date
       FROM facts_current w JOIN entities we ON we.entity_id = w.subject_id AND we.type = ''window''
      WHERE w.household_id = ?1 AND w.predicate = ''free_window'' AND substr(we.canonical_name, 9, 10) >= ?3),
   credit AS (
     SELECT cp.card_entity_id, cp.benefit_key, cp.status, cp.period_end, b.label, b.amount_cents - cp.used_cents AS remaining,
            (SELECT f.fact_id FROM facts_current f WHERE f.household_id = cp.household_id AND f.subject_id = cp.card_entity_id AND f.predicate = ''card_product'' LIMIT 1) AS card_fact
       FROM credit_periods cp JOIN benefit_catalog b ON b.benefit_key = cp.benefit_key
      WHERE cp.household_id = ?1 AND cp.status IN (''open'', ''partially_used'', ''unconfirmed'') AND cp.period_end >= ?3
        AND b.amount_cents - cp.used_cents >= 5000
        AND (UPPER(b.merchant_match) LIKE ''%RESY%'' OR UPPER(b.merchant_match) LIKE ''%OPENTABLE%'' OR LOWER(b.merchant_match) LIKE ''%category:dining%'')),
   head AS (
     SELECT MAX(headroom_cents) AS headroom FROM envelope_headroom
      WHERE household_id = ?1 AND (envelope_key LIKE ''%dining%'' OR envelope_key LIKE ''%restaurant%''))
   SELECT o.event_id || ''|'' || o.occasion_date AS dedupe_key,
          c.remaining AS recoverable_cents,
          MAX(0.1, (15.0 - (julianday(o.occasion_date) - julianday(?3))) / 15.0) AS urgency,
          CASE c.status WHEN ''unconfirmed'' THEN 0.5 ELSE 1.0 END AS confidence,
          json_array(json_object(''key'', ''occasion'', ''facts'', json_array(o.date_fact, o.kind_fact)),
                     json_object(''key'', ''window'', ''facts'', json_array(w.window_fact)),
                     json_object(''key'', ''credit'', ''facts'', json_array(c.card_fact))) AS claims_json,
          json_object(''occasion'', o.name, ''kind'', o.kind, ''occasion_date'', o.occasion_date,
                      ''window_date'', w.local_date, ''window_span'', w.span,
                      ''credit_label'', c.label, ''credit_remaining_cents'', c.remaining, ''credit_status'', c.status, ''credit_ends'', c.period_end,
                      ''card_name'', (SELECT e.canonical_name FROM entities e WHERE e.entity_id = c.card_entity_id),
                      ''headroom_cents'', h.headroom, ''expires_on'', o.occasion_date) AS data_json
     FROM occ o
     JOIN win w ON w.window_fact = (SELECT w2.window_fact FROM win w2
                                     WHERE w2.local_date BETWEEN date(o.occasion_date, ''-3 days'') AND date(o.occasion_date, ''+3 days'')
                                     ORDER BY ABS(julianday(w2.local_date) - julianday(o.occasion_date)), w2.local_date LIMIT 1)
     JOIN credit c ON c.benefit_key || ''|'' || c.card_entity_id = (SELECT c2.benefit_key || ''|'' || c2.card_entity_id FROM credit c2 ORDER BY c2.remaining DESC, c2.benefit_key LIMIT 1)
     JOIN head h ON h.headroom >= 18000');

-- ── T_SUBSCRIPTION_CREEP: a recurring charge whose price went up in the last 90 days, or two live subscriptions to one merchant
INSERT INTO triggers (trigger_key, template_key, cooldown_days, sql_condition) VALUES
 ('T_SUBSCRIPTION_CREEP', 'subscription_creep', 30,
  'SELECT n.subject_id || ''|up|'' || n.fact_id AS dedupe_key,
          (n.value_cents - o.value_cents) * COALESCE((SELECT CASE c.value_text WHEN ''weekly'' THEN 52 WHEN ''biweekly'' THEN 26 WHEN ''monthly'' THEN 12 WHEN ''quarterly'' THEN 4 WHEN ''annual'' THEN 1 END
                                                        FROM facts_current c WHERE c.household_id = ?1 AND c.subject_id = n.subject_id AND c.predicate = ''recurring_cadence''), 12) AS recoverable_cents,
          MAX(0.2, (91.0 - (julianday(?3) - julianday(substr(n.valid_from, 1, 10)))) / 91.0) AS urgency,
          1.0 AS confidence,
          json_array(json_object(''key'', ''increase'', ''facts'', json_array(o.fact_id, n.fact_id))) AS claims_json,
          json_object(''kind'', ''price_increase'', ''subscription'', e.canonical_name, ''old_cents'', o.value_cents, ''new_cents'', n.value_cents,
                      ''since'', substr(n.valid_from, 1, 10), ''expires_on'', date(?3, ''+30 days'')) AS data_json
     FROM facts n
     JOIN facts o ON o.household_id = n.household_id AND o.subject_id = n.subject_id AND o.predicate = ''recurring_amount''
                 AND o.expired_at IS NULL AND o.valid_to = n.valid_from
     JOIN entities e ON e.entity_id = n.subject_id
    WHERE n.household_id = ?1 AND n.predicate = ''recurring_amount'' AND n.expired_at IS NULL AND n.valid_to IS NULL
      AND n.value_cents > o.value_cents AND substr(n.valid_from, 1, 10) >= date(?3, ''-90 days'')
   UNION ALL
   SELECT m1.object_id || ''|dup'' AS dedupe_key,
          MIN(a1.value_cents, a2.value_cents) * 12 AS recoverable_cents,
          0.5 AS urgency,
          1.0 AS confidence,
          json_array(json_object(''key'', ''duplicate'', ''facts'', json_array(a1.fact_id, a2.fact_id))) AS claims_json,
          json_object(''kind'', ''duplicate'', ''subscription'', (SELECT e.canonical_name FROM entities e WHERE e.entity_id = m1.object_id),
                      ''first'', e1.canonical_name, ''second'', e2.canonical_name, ''first_cents'', a1.value_cents, ''second_cents'', a2.value_cents,
                      ''expires_on'', date(?3, ''+30 days'')) AS data_json
     FROM facts_current m1
     JOIN facts_current m2 ON m2.household_id = m1.household_id AND m2.predicate = ''recurring_merchant'' AND m2.object_id = m1.object_id AND m2.subject_id > m1.subject_id
     JOIN facts_current a1 ON a1.household_id = m1.household_id AND a1.subject_id = m1.subject_id AND a1.predicate = ''recurring_amount''
     JOIN facts_current a2 ON a2.household_id = m1.household_id AND a2.subject_id = m2.subject_id AND a2.predicate = ''recurring_amount''
     JOIN entities e1 ON e1.entity_id = m1.subject_id
     JOIN entities e2 ON e2.entity_id = m2.subject_id
    WHERE m1.household_id = ?1 AND m1.predicate = ''recurring_merchant''');

-- ── T_OCCASION_PLAN (cold-start fallback only): the next anniversary/birthday/trip within 30 days ───────────────────
INSERT INTO triggers (trigger_key, template_key, cooldown_days, sql_condition) VALUES
 ('T_OCCASION_PLAN', 'occasion_plan', 14,
  'SELECT ev.entity_id || ''|'' || d.value_text AS dedupe_key,
          COALESCE((SELECT MAX(b.amount_cents - cp.used_cents) FROM credit_periods cp JOIN benefit_catalog b ON b.benefit_key = cp.benefit_key
                     WHERE cp.household_id = ?1 AND cp.status IN (''open'', ''partially_used'', ''unconfirmed'') AND cp.period_end >= ?3
                       AND (UPPER(b.merchant_match) LIKE ''%RESY%'' OR UPPER(b.merchant_match) LIKE ''%OPENTABLE%'')), 0) AS recoverable_cents,
          MAX(0.1, (31.0 - (julianday(d.value_text) - julianday(?3))) / 31.0) AS urgency,
          1.0 AS confidence,
          json_array(json_object(''key'', ''occasion'', ''facts'', json_array(d.fact_id, k.fact_id))) AS claims_json,
          json_object(''occasion'', ev.canonical_name, ''kind'', k.value_text, ''occasion_date'', d.value_text, ''expires_on'', d.value_text) AS data_json
     FROM facts_current d
     JOIN entities ev ON ev.entity_id = d.subject_id AND ev.type = ''event''
     JOIN facts_current k ON k.household_id = d.household_id AND k.subject_id = d.subject_id AND k.predicate = ''event_kind''
    WHERE d.household_id = ?1 AND d.predicate = ''event_date'' AND k.value_text IN (''anniversary'', ''birthday'')
      AND d.value_text >= ?3 AND d.value_text <= date(?3, ''+30 days'')
   UNION ALL
   SELECT te.entity_id || ''|'' || substr(t.value_text, 1, 10) AS dedupe_key,
          0 AS recoverable_cents,
          MAX(0.1, (31.0 - (julianday(substr(t.value_text, 1, 10)) - julianday(?3))) / 31.0) AS urgency,
          1.0 AS confidence,
          json_array(json_object(''key'', ''trip'', ''facts'', json_array(t.fact_id))) AS claims_json,
          json_object(''occasion'', te.canonical_name, ''kind'', ''trip'', ''occasion_date'', substr(t.value_text, 1, 10), ''expires_on'', substr(t.value_text, 1, 10)) AS data_json
     FROM facts_current t JOIN entities te ON te.entity_id = t.subject_id AND te.type = ''trip''
    WHERE t.household_id = ?1 AND t.predicate = ''trip_window''
      AND substr(t.value_text, 1, 10) >= ?3 AND substr(t.value_text, 1, 10) <= date(?3, ''+30 days'')');
