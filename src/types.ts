/**
 * src/types.ts — shared, strict types for Life Hub.
 *
 * Sections:
 *   1. Worker bindings (Env)
 *   2. Enumerations (one `as const` tuple per CHECK-constrained column; unions are derived from them,
 *      so the runtime lists and the compile-time types can never drift apart)
 *   3. D1 row interfaces — one per table/view in migrations/0001_init.sql, column-for-column
 *   4. Episode records (insert shape)
 *   5. Bi-temporal fact payloads (proposal in, result out) used by src/graph/writeFact.ts
 *   6. Mission tiers R0–R3 and the mission state machine of MissionRunWF (CLAUDE.md §5.1)
 *
 * Conventions:
 *   - SQLite NULL  => `| null`.  SQLite 0/1 flags => `0 | 1` numbers.
 *   - Every timestamp is a canonical UTC ISO-8601 string with milliseconds ("2026-11-26T08:30:00.000Z"),
 *     the same shape as strftime('%Y-%m-%dT%H:%M:%fZ','now'). Plain string comparison therefore orders
 *     them chronologically.
 *   - Character offsets (char_start / char_end) are UTF-16 code-unit indices, i.e. exactly what
 *     String.prototype.slice() uses.
 */

// ───────────────────────────────────────────────────────────────────────────
// 1. Worker bindings
// ───────────────────────────────────────────────────────────────────────────

export interface Env {
  /** Control + tenancy D1 database (wrangler.toml binding "DB"). */
  DB: D1Database;
  /** R2 bucket holding encrypted raw documents (wrangler.toml binding "DOCS_BUCKET"). */
  DOCS_BUCKET: R2Bucket;
  /** Product name for user-facing text (wrangler.toml [vars]). Never hardcode it in source. */
  APP_NAME: string;
  /** Domain of the inbound drop addresses, e.g. the part after '@' (wrangler.toml [vars]). Never hardcode it. */
  INBOUND_DOMAIN: string;
  /** Cloudflare Workflows bindings (wrangler.toml [[workflows]]). */
  INGEST_EMAIL_WF: Workflow<IngestEmailParams>;
  INGEST_FILE_WF: Workflow<IngestFileParams>;
  CALENDAR_SYNC_WF: Workflow<CalendarSyncParams>;
  NIGHTLY_SWEEP_WF: Workflow<NightlySweepParams>;
  MISSION_RUN_WF: Workflow<MissionRunParams>;
  LIFE_RECEIPT_WF: Workflow<LifeReceiptParams>;
  DROP_ALARM_WF: Workflow<DropAlarmParams>;
  PRICE_WATCH_WF: Workflow<PriceWatchParams>;
  /** Encrypted OAuth tokens (wrangler.toml [[kv_namespaces]]). Values are AES-GCM under the household DEK. */
  KV: KVNamespace;
  /** Secrets (`wrangler secret put`): 32-byte base64 key-encryption key, and the two LLM provider keys. */
  KEK_B64: string;
  /** Google OAuth client (calendar.events.readonly + calendar.freebusy only) and the HMAC key for API session tokens. */
  GOOGLE_CLIENT_ID: string;
  GOOGLE_CLIENT_SECRET: string;
  SESSION_SECRET: string;
  OPENAI_API_KEY: string;
  ANTHROPIC_API_KEY: string;
  /** Optional model overrides; must be ids listed in src/llm/pricing.ts. Defaults: gpt-6.2-sol (chief), gpt-6-luna (worker). */
  CHIEF_MODEL?: string;
  WORKER_MODEL?: string;
  /**
   * Optional affiliate programs, a JSON object keyed by merchant domain:
   * {"shop.example": {"program": "name", "param": "ref", "value": "tag"}}. Domains in policy_blocklist are never tagged.
   */
  AFFILIATE_TAGS?: string;
  /** Web Push (VAPID). Public key (base64url, 65-byte point) and subject (mailto: or https: URL) are vars; the private key is a secret. */
  VAPID_PUBLIC_KEY?: string;
  VAPID_PRIVATE_KEY?: string;
  VAPID_SUBJECT?: string;
  /** Stripe (secrets): API key for Checkout and off-session top-ups, and the webhook signing secret. Both are optional so the Worker still runs without billing. */
  STRIPE_SECRET_KEY?: string;
  STRIPE_WEBHOOK_SECRET?: string;
  /** Duffel search-only API key (secret). Without it price watches check nothing. */
  DUFFEL_API_KEY?: string;
  /** Cloudflare Email Sending binding for the weekly Life Receipt. Without it receipts are stored and pushed but not e-mailed. */
  RECEIPT_EMAIL?: EmailSender;
}

/** The part of Cloudflare's Email Sending binding the app uses. */
export interface EmailSender {
  send(message: { to: string; from: string; subject: string; text: string; html: string }): Promise<unknown>;
}

/** Payload of a MissionRunWF instance. Everything else is read from the mission_runs row. */
export interface MissionRunParams {
  run_id: string;
  household_id: string;
}

/** Payload of a LifeReceiptWF instance. `notify` sends the e-mail and push (the Sunday cron does; an on-demand receipt does not). */
export interface LifeReceiptParams {
  household_id: string;
  notify: boolean;
  /** ISO clock override for tests and replays. */
  as_of?: IsoTimestamp;
}

/** Payload of a DropAlarmWF instance. The alarm row holds everything else. */
export interface DropAlarmParams {
  alarm_id: string;
  household_id: string;
}

/** Payload of a PriceWatchWF instance. The price_watches row holds everything else. */
export interface PriceWatchParams {
  watch_id: string;
  household_id: string;
}

/** Payload of an IngestEmailWF instance. The raw MIME is already encrypted in R2 at `r2_key`. */
export interface IngestEmailParams {
  household_id: string;
  r2_key: string;
  /** sha256 (hex) of the raw MIME bytes. */
  sha256: string;
  size_bytes: number;
  /** SMTP envelope sender (lower-cased). */
  envelope_from: string;
}

/** Payload of a CalendarSyncWF instance. When `then_sweep` is set the workflow starts NightlySweepWF when it finishes. */
export interface CalendarSyncParams {
  household_id: string;
  then_sweep: boolean;
}

/** Payload of a NightlySweepWF instance. `as_of` (ISO) overrides the clock; used by tests and replays. */
export interface NightlySweepParams {
  household_id: string;
  as_of?: IsoTimestamp;
}

export type SignConvention = 'negative_is_outflow' | 'positive_is_outflow' | 'auto';

/** Payload of an IngestFileWF instance. The raw upload is already encrypted in R2 at `r2_key`. */
export interface IngestFileParams {
  household_id: string;
  r2_key: string;
  file_name: string;
  /** 'csv' = bank / card / Monarch export; 'pdf_text' = text already extracted from a PDF statement. */
  kind: 'csv' | 'pdf_text';
  /** sha256 (hex) of the raw file bytes. */
  sha256: string;
  /** Card the file belongs to, when the file itself does not say (bank/card CSV without an account column). */
  default_card_entity_id?: string | null;
  /** Default 'auto'. Amex-style exports (positive = charge) are detected automatically. */
  sign_convention?: SignConvention;
  /** Year for statement lines that only carry MM/DD. Default: derived from `as_of`. */
  statement_year?: number | null;
  /** Reference "now" for the run (ISO). Default: the workflow clock. */
  as_of?: IsoTimestamp;
}

/** Canonical UTC ISO-8601 timestamp with milliseconds, e.g. "2026-11-26T08:30:00.000Z". */
export type IsoTimestamp = string;

// ───────────────────────────────────────────────────────────────────────────
// 2. Enumerations
// ───────────────────────────────────────────────────────────────────────────

export const PLANS = ['trial', 'monthly', 'annual', 'canceled'] as const;
export type Plan = (typeof PLANS)[number];

export const MEMBER_ROLES = ['owner', 'adult'] as const;
export type MemberRole = (typeof MEMBER_ROLES)[number];

export const CONNECTOR_KINDS = [
  'email_alias',
  'google_calendar',
  'spend_csv',
  'spend_pdf',
  'card_list',
  'upload',
  'byo_agent_mcp',
] as const;
export type ConnectorKind = (typeof CONNECTOR_KINDS)[number];

export const CONNECTOR_STATUSES = ['pending', 'active', 'error', 'revoked'] as const;
export type ConnectorStatus = (typeof CONNECTOR_STATUSES)[number];

export const EPISODE_SOURCES = [
  'email',
  'calendar',
  'csv',
  'pdf',
  'upload',
  'user_input',
  'byo_agent',
] as const;
export type EpisodeSource = (typeof EPISODE_SOURCES)[number];

export const REDACTION_STATUSES = ['pending', 'clean', 'redacted', 'quarantined'] as const;
export type RedactionStatus = (typeof REDACTION_STATUSES)[number];

export const ENTITY_TYPES = [
  'person',
  'household',
  'merchant',
  'card_account',
  'benefit',
  'envelope',
  'recurring_charge',
  'event',
  'window',
  'venue',
  'cuisine',
  'airline',
  'hotel_brand',
  'trip',
  'document',
  'tax_year',
  'product',
] as const;
export type EntityType = (typeof ENTITY_TYPES)[number];

export const ALIAS_METHODS = ['deterministic', 'llm_adjudicated', 'user'] as const;
export type AliasMethod = (typeof ALIAS_METHODS)[number];

export const PREDICATE_CARDINALITIES = ['one', 'many'] as const;
export type PredicateCardinality = (typeof PREDICATE_CARDINALITIES)[number];

export const VALUE_TYPES = ['entity', 'text', 'num', 'date', 'money', 'interval'] as const;
export type ValueType = (typeof VALUE_TYPES)[number];

export const PREDICATE_DOMAINS = ['people', 'cards', 'budget', 'calendar', 'taste', 'vault'] as const;
export type PredicateDomain = (typeof PREDICATE_DOMAINS)[number];

export const CLAIM_STATUSES = ['proposed', 'grounded', 'rejected'] as const;
export type ClaimStatus = (typeof CLAIM_STATUSES)[number];

export const BENEFIT_CADENCES = ['monthly', 'quarterly', 'semiannual', 'annual', 'cardmember_year'] as const;
export type BenefitCadence = (typeof BENEFIT_CADENCES)[number];

export const CREDIT_PERIOD_STATUSES = ['open', 'used', 'partially_used', 'expired_unused', 'unconfirmed'] as const;
export type CreditPeriodStatus = (typeof CREDIT_PERIOD_STATUSES)[number];

export const MISSION_TIERS = ['R0', 'R1', 'R2', 'R3'] as const;
export type MissionTier = (typeof MISSION_TIERS)[number];

export const MISSION_STATES = [
  'COMPILED',
  'R0_READ',
  'R1_STAGED',
  'GATED',
  'R2_AWAITING',
  'R2_APPROVED',
  'R3_LINK_ISSUED',
  'CONFIRMED',
  'EXPIRED',
  'DECLINED',
  'FAILED_GATE',
  'CANCELED',
  'INSUFFICIENT_CREDITS',
] as const;
export type MissionState = (typeof MISSION_STATES)[number];

export const MISSION_ORIGINS = ['trigger', 'user', 'receipt'] as const;
export type MissionOrigin = (typeof MISSION_ORIGINS)[number];

export const STEP_KINDS = [
  'read',
  'llm_worker',
  'llm_chief',
  'gate',
  'stage',
  'approve',
  'deeplink',
  'wait',
  'notify',
] as const;
export type StepKind = (typeof STEP_KINDS)[number];

export const STEP_STATUSES = ['pending', 'ok', 'failed', 'skipped'] as const;
export type StepStatus = (typeof STEP_STATUSES)[number];

export const APPROVAL_DECISIONS = ['approve', 'decline'] as const;
export type ApprovalDecision = (typeof APPROVAL_DECISIONS)[number];

export const VENUE_PLATFORMS = ['resy', 'opentable', 'tock', 'sevenrooms', 'direct'] as const;
export type VenuePlatform = (typeof VENUE_PLATFORMS)[number];

export const PRICE_WATCH_KINDS = ['flight'] as const;
export type PriceWatchKind = (typeof PRICE_WATCH_KINDS)[number];

export const LEDGER_REASONS = [
  'monthly_grant',
  'pack_purchase',
  'mission_debit',
  'mission_refund',
  'admin_adjust',
  'expiry',
] as const;
export type LedgerReason = (typeof LEDGER_REASONS)[number];

// ───────────────────────────────────────────────────────────────────────────
// 3. D1 row interfaces (column-for-column with migrations/0001_init.sql)
// ───────────────────────────────────────────────────────────────────────────

// ── Control / tenancy ──

export interface HouseholdRow {
  household_id: string;
  shard: number;
  /** Local part of the drop address: h_<10 base32>  (full address: <alias_local>@<INBOUND_DOMAIN>). */
  alias_local: string;
  /** IANA time zone, default 'America/New_York'. */
  timezone: string;
  plan: Plan;
  stripe_customer_id: string | null;
  created_at: IsoTimestamp;
  deleted_at: IsoTimestamp | null;
}

export interface MemberRow {
  member_id: string;
  household_id: string;
  email: string;
  display_name: string | null;
  role: MemberRole;
  /** Constrained to 1 by the schema: users are 18+ (attested). */
  age_attested_18: 1;
  push_endpoint: string | null;
  created_at: IsoTimestamp;
}

export interface ConnectorRow {
  connector_id: string;
  household_id: string;
  kind: ConnectorKind;
  status: ConnectorStatus;
  scopes: string | null;
  /** Key of the encrypted OAuth token blob in KV. Never the token itself. */
  token_kv_key: string | null;
  sync_cursor: string | null;
  last_success_at: IsoTimestamp | null;
  last_error: string | null;
  created_at: IsoTimestamp;
}

// ── Episodes (immutable raw inputs) ──

export interface EpisodeRow {
  episode_id: string;
  household_id: string;
  source: EpisodeSource;
  source_ref: string | null;
  /** R2 object key of the encrypted raw document. */
  r2_key: string | null;
  /** The ONLY text an LLM may see. NULL until redaction finishes, and NULL forever when quarantined. */
  redacted_text: string | null;
  sha256: string;
  /** When the thing happened in the world (email Date header, transaction date, event start). */
  occurred_at: IsoTimestamp | null;
  received_at: IsoTimestamp;
  redaction_status: RedactionStatus;
}

// ── Entities + resolution ──

export interface EntityRow {
  entity_id: string;
  household_id: string;
  type: EntityType;
  canonical_name: string;
  /** JSON object serialized as text. */
  attrs_json: string;
  /** Set when this entity was merged into another; follow the chain to the canonical entity. */
  merged_into: string | null;
  created_at: IsoTimestamp;
}

export interface EntityAliasRow {
  household_id: string;
  /** '<entity_type>:<normalized name>' — see aliasKey() in src/graph/writeFact.ts. */
  alias_norm: string;
  entity_id: string;
  method: AliasMethod;
  confidence: number;
}

// ── Predicate registry + facts (bi-temporal edges) ──

export interface PredicateRegistryRow {
  predicate: string;
  cardinality: PredicateCardinality;
  value_type: ValueType;
  /** Days of silence after which the nightly sweep closes the fact; NULL = never decays. */
  decay_days: number | null;
  domain: PredicateDomain;
}

/**
 * One bi-temporal edge.
 *   World time        : valid_from .. valid_to      (when it was true in the world)
 *   Transaction time  : learned_at .. expired_at    (when we believed it)
 * A fact is "currently believed" while expired_at IS NULL.
 */
export interface FactRow {
  fact_id: string;
  household_id: string;
  subject_id: string;
  predicate: string;
  object_id: string | null;
  value_text: string | null;
  value_num: number | null;
  value_cents: number | null;
  currency: string | null;
  valid_from: IsoTimestamp;
  valid_to: IsoTimestamp | null;
  learned_at: IsoTimestamp;
  expired_at: IsoTimestamp | null;
  superseded_by: string | null;
  confidence: number;
  source_episode_id: string;
}

/** Row of the `facts_current` view: believed now AND valid now. Same columns as `facts`. */
export type FactCurrentRow = FactRow;

export interface FactQuoteRow {
  fact_id: string;
  episode_id: string;
  /** Exact substring of episodes.redacted_text: redacted_text.slice(char_start, char_end) === quote. */
  quote: string;
  char_start: number;
  char_end: number;
}

export interface ClaimRow {
  claim_id: string;
  household_id: string;
  run_id: string | null;
  text: string;
  status: ClaimStatus;
  created_at: IsoTimestamp;
}

export interface ClaimSupportRow {
  claim_id: string;
  fact_id: string;
}

// ── Cards & benefits ──

export interface BenefitCatalogRow {
  /** e.g. 'amex_platinum.resy_credit'. */
  benefit_key: string;
  /** e.g. 'amex_platinum' — matches the value of the `card_product` fact. */
  card_product: string;
  label: string;
  amount_cents: number;
  cadence: BenefitCadence;
  /** '|'-separated, case-insensitive descriptor substrings; 'category:<name>' tokens name issuer categories. */
  merchant_match: string;
  enrollment_required: 0 | 1;
  /** Date the row was last checked against source_url. */
  verified_on: string;
  source_url: string;
}

export interface CreditPeriodRow {
  household_id: string;
  card_entity_id: string;
  benefit_key: string;
  period_start: string;
  period_end: string;
  used_cents: number;
  status: CreditPeriodStatus;
}

// ── Shadow budget ──

export interface TransactionRow {
  txn_id: string;
  household_id: string;
  episode_id: string;
  posted_on: string;
  /** Negative = outflow. */
  amount_cents: number;
  descriptor_norm: string;
  merchant_entity_id: string | null;
  card_entity_id: string | null;
  envelope_key: string | null;
}

export interface EnvelopeRow {
  household_id: string;
  envelope_key: string;
  /** Default: trailing-3-month median. */
  monthly_target_cents: number;
}

/** Row of the `envelope_headroom` view. */
export interface EnvelopeHeadroomRow {
  household_id: string;
  envelope_key: string;
  headroom_cents: number;
}

// ── Missions, runs, steps ──

export interface MissionTemplateRow {
  template_key: string;
  version: number;
  /** JSON: [{ id, kind, tier, worker_prompt_id, max_input_tokens }]. */
  plan_json: string;
  max_tier: MissionTier;
  credits_cost: number;
}

export interface TriggerRow {
  trigger_key: string;
  template_key: string;
  sql_condition: string;
  cooldown_days: number;
}

export interface MissionRunRow {
  /** Equals the Workflows instance id. */
  run_id: string;
  household_id: string;
  template_key: string;
  template_version: number;
  origin: MissionOrigin;
  params_json: string;
  state: MissionState;
  expires_at: IsoTimestamp | null;
  cost_usd: number;
  created_at: IsoTimestamp;
  updated_at: IsoTimestamp;
}

export interface StepRow {
  run_id: string;
  step_no: number;
  kind: StepKind;
  tier: MissionTier;
  status: StepStatus;
  input_tokens: number | null;
  output_tokens: number | null;
  cost_usd: number | null;
  gate_name: string | null;
  gate_result_json: string | null;
  started_at: IsoTimestamp | null;
  finished_at: IsoTimestamp | null;
}

export interface ApprovalRow {
  run_id: string;
  member_id: string;
  decision: ApprovalDecision;
  /** Hash of the exact staged card the member was shown. */
  payload_hash: string;
  decided_at: IsoTimestamp;
}

export interface DeeplinkRow {
  link_id: string;
  run_id: string;
  merchant_domain: string;
  url: string;
  affiliate_program: string | null;
  issued_at: IsoTimestamp;
  clicked_at: IsoTimestamp | null;
}

// ── Engine B inputs ──

export interface VenueReleaseRuleRow {
  venue_entity_id: string;
  platform: VenuePlatform;
  /** Deep-link target. NEVER fetched. */
  venue_url: string;
  days_ahead: number;
  /** Local wall-clock time of the release, e.g. '09:00'. */
  release_local_time: string;
  release_tz: string;
  verified_on: string;
}

export interface PriceWatchRow {
  watch_id: string;
  household_id: string;
  kind: PriceWatchKind;
  params_json: string;
  ceiling_cents: number;
  /** Schema caps this at 1. */
  checks_per_day: number;
  active: 0 | 1;
}

export interface PolicyBlocklistRow {
  domain: string;
  reason: string;
}

// ── Credit ledger (append-only) ──

export interface CreditLedgerRow {
  entry_id: string;
  household_id: string;
  delta: number;
  reason: LedgerReason;
  run_id: string | null;
  stripe_event_id: string | null;
  created_at: IsoTimestamp;
}

/** Row of the `credit_balance` view. */
export interface CreditBalanceRow {
  household_id: string;
  balance: number;
}

// ── Cost + audit ──

export interface LlmUsageRow {
  id: string;
  household_id: string | null;
  run_id: string | null;
  model: string;
  input_tokens: number;
  cached_tokens: number;
  output_tokens: number;
  cost_usd: number;
  created_at: IsoTimestamp;
}

export interface AuditLogRow {
  id: string;
  household_id: string | null;
  actor: string;
  action: string;
  detail_json: string | null;
  created_at: IsoTimestamp;
}

// ───────────────────────────────────────────────────────────────────────────
// 4. Episode records
// ───────────────────────────────────────────────────────────────────────────

/**
 * Insert shape for `episodes`. `redacted_text` / `redaction_status` come straight from
 * redact() in src/redact.ts; received_at is filled by the database default.
 * A quarantined episode is stored with redacted_text = null so no LLM can ever read it.
 */
export interface NewEpisode {
  episode_id: string;
  household_id: string;
  source: EpisodeSource;
  source_ref?: string | null;
  r2_key?: string | null;
  redacted_text: string | null;
  /** sha256 (hex) of the RAW bytes, computed before redaction; (household_id, sha256) is unique. */
  sha256: string;
  occurred_at?: IsoTimestamp | null;
  redaction_status: Exclude<RedactionStatus, 'pending'>;
}

// ───────────────────────────────────────────────────────────────────────────
// 5. Bi-temporal fact payloads
// ───────────────────────────────────────────────────────────────────────────

/** Reference to an entity that already exists in the household. */
export interface EntityIdRef {
  entity_id: string;
}

/** Reference by type + surface name; resolved via aliases, then (optional) adjudication, then created. */
export interface EntityNameRef {
  type: EntityType;
  name: string;
  /** Only used when the entity has to be created. */
  attrs?: Readonly<Record<string, unknown>>;
}

export type EntityRef = EntityIdRef | EntityNameRef;

/** One piece of grounding evidence. LLMs propose the text; code locates and verifies the offsets. */
export interface QuoteProposal {
  quote: string;
  char_start: number;
  char_end: number;
}

/**
 * A proposed fact. This is what an LLM extraction step (after validation/shaping by code) hands to
 * writeFact(). Exactly the value fields matching the predicate's value_type may be set:
 *   entity   -> object
 *   text     -> value_text
 *   num      -> value_num
 *   date     -> value_text as 'YYYY-MM-DD'
 *   money    -> value_cents (+ optional currency, default 'USD')
 *   interval -> value_text as '<start>/<end>' (ISO dates or UTC datetimes)
 */
export interface FactProposal {
  household_id: string;
  source_episode_id: string;
  subject: EntityRef;
  predicate: string;
  object?: EntityRef | null;
  value_text?: string | null;
  value_num?: number | null;
  value_cents?: number | null;
  currency?: string | null;
  /** World time it became true. Accepts 'YYYY-MM-DD' or any ISO-8601 instant. Default: episode.occurred_at ?? received_at. */
  valid_from?: string | null;
  /** World time it stopped being true. Normally omitted: closing is done by the writer (world change). */
  valid_to?: string | null;
  /** 0..1. */
  confidence: number;
  /** At least one verbatim quote from the source episode's redacted_text. */
  quotes: readonly QuoteProposal[];
}

export const FACT_REJECTION_REASONS = [
  'invalid_input',
  'unknown_predicate',
  'invalid_value',
  'episode_not_found',
  'episode_not_redacted',
  'no_quotes',
  'quote_mismatch',
  'entity_not_found',
  'entity_type_mismatch',
  'partial_overlap',
  'stale_evidence',
  'conflict_retry_exhausted',
] as const;
export type FactRejectionReason = (typeof FACT_REJECTION_REASONS)[number];

/** How the writer retired an earlier, conflicting cardinality='one' fact. */
export type FactSupersessionKind = 'world_change' | 'correction';

export interface FactSupersession {
  fact_id: string;
  kind: FactSupersessionKind;
}

export interface FactWritten {
  status: 'written';
  fact_id: string;
  learned_at: IsoTimestamp;
  valid_from: IsoTimestamp;
  valid_to: IsoTimestamp | null;
  /** Earlier facts closed (world_change: valid_to set) or expired (correction: expired_at set + superseded_by). */
  superseded: readonly FactSupersession[];
  /** Entities created by this write (subject/object that matched no alias). */
  created_entity_ids: readonly string[];
}

/** An equal fact was already believed; the new quotes were attached as extra support and nothing else changed. */
export interface FactReaffirmed {
  status: 'reaffirmed';
  fact_id: string;
}

export interface FactRejected {
  status: 'rejected';
  reason: FactRejectionReason;
  detail: string;
}

export type WriteFactResult = FactWritten | FactReaffirmed | FactRejected;

// ───────────────────────────────────────────────────────────────────────────
// 6. Mission tiers R0–R3 and state machine
// ───────────────────────────────────────────────────────────────────────────

/**
 * R0 read (facts, headroom, windows)        — no side effects
 * R1 stage (private card, nothing sent)      — no external effect
 * R2 member approval (exact staged payload)  — human tap
 * R3 deep link only                          — member completes the action at the merchant
 */
export function missionTierRank(tier: MissionTier): number {
  return MISSION_TIERS.indexOf(tier);
}

/** G6 (tier gate): true when `tier` does not exceed the template's `maxTier`. */
export function isTierAllowed(tier: MissionTier, maxTier: MissionTier): boolean {
  return missionTierRank(tier) <= missionTierRank(maxTier);
}

/** Tier a run has reached in each state; null for end states that can occur at several tiers. */
export const MISSION_STATE_TIER: Readonly<Record<MissionState, MissionTier | null>> = {
  COMPILED: 'R0',
  R0_READ: 'R0',
  GATED: 'R0',
  R1_STAGED: 'R1',
  R2_AWAITING: 'R2',
  R2_APPROVED: 'R2',
  R3_LINK_ISSUED: 'R3',
  CONFIRMED: 'R3',
  EXPIRED: null,
  DECLINED: null,
  FAILED_GATE: null,
  CANCELED: null,
  INSUFFICIENT_CREDITS: 'R0',
};

/**
 * Legal transitions of mission_runs.state, following MissionRunWF (CLAUDE.md §5.1):
 *   compile+debit -> r0_read/r0_fill -> gates -> r1_stage -> notify -> await_approval
 *   -> r3_regate -> r3_link -> await_confirmation.
 * CANCELED is reachable from every state that has not reached an end state or issued the link.
 */
export const MISSION_STATE_TRANSITIONS: Readonly<Record<MissionState, readonly MissionState[]>> = {
  COMPILED: ['R0_READ', 'INSUFFICIENT_CREDITS', 'CANCELED'],
  // The balance could not cover the run (and auto-recharge could not or may not): nothing was taken. A top-up and a new tap start it again.
  INSUFFICIENT_CREDITS: ['COMPILED', 'DECLINED', 'CANCELED'],
  R0_READ: ['GATED', 'FAILED_GATE', 'CANCELED'],
  GATED: ['R1_STAGED', 'CANCELED'],
  // 'COMPILED': the member tapped the staged card, so MissionRunWF takes it over. 'DECLINED': "Not for me" on a staged card.
  R1_STAGED: ['R2_AWAITING', 'COMPILED', 'DECLINED', 'CANCELED'],
  R2_AWAITING: ['R2_APPROVED', 'DECLINED', 'EXPIRED', 'CANCELED'],
  // 'CONFIRMED' straight from R2_APPROVED is for templates whose max tier is R2 (a script the member carries out themselves).
  R2_APPROVED: ['R3_LINK_ISSUED', 'CONFIRMED', 'FAILED_GATE', 'CANCELED'],
  R3_LINK_ISSUED: ['CONFIRMED'],
  CONFIRMED: [],
  EXPIRED: [],
  DECLINED: [],
  FAILED_GATE: [],
  CANCELED: [],
};

export function canTransitionMissionState(from: MissionState, to: MissionState): boolean {
  return MISSION_STATE_TRANSITIONS[from].includes(to);
}

export function isTerminalMissionState(state: MissionState): boolean {
  return MISSION_STATE_TRANSITIONS[state].length === 0;
}
