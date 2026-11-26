/**
 * src/connectors/calendarPipeline.ts — CalendarSyncWF's logic. Deterministic: no model is involved.
 *
 *   events           list (incremental or full) -> classify occasions and trips -> episode + grounded proposals
 *   close-events     facts of events that were cancelled or renamed since the last sync
 *   write-events-k   writeFact() for <= 45 proposals per step
 *   windows          freeBusy -> free Fri/Sat 18:00-22:00 windows -> episode + proposals + windows that are now busy
 *   write-windows-k  writeFact() for <= 45 proposals per step
 *   retire-windows   close the facts of windows that are no longer free
 *   commit           persist the sync cursor only after every write succeeded (a failed run re-reads from the old cursor)
 *   start-sweep      optionally start NightlySweepWF
 *
 * Episodes are derived text (source 'calendar'). Every one starts with a header carrying the sync date, so each day's
 * episode is new and re-confirming a fact attaches a fresh quote whose timestamp keeps the fact from decaying.
 * Quotes are the exact lines of the REDACTED episode text. Only the title of an occasion/trip is ever stored.
 */

import { sha256Hex } from '../crypto/vault';
import { aliasKey, locateQuote, type WriteFactOptions } from '../graph/writeFact';
import { closeFacts, type CloseTarget } from '../graph/closeFacts';
import { redact } from '../redact';
import type { CalendarSyncParams, FactProposal } from '../types';
import {
  SYNC_FUTURE_DAYS,
  classifyEvent,
  computeFreeWindows,
  findCalendarConnector,
  isoMs,
  listEvents,
  parseCursor,
  queryBusy,
  windowEntityName,
  windowLine,
  type CalendarDeps,
  type ClassifiedEvent,
  type SyncCursor,
} from './googleCalendar';
import {
  DEFAULT_STEP_CONFIG,
  MAX_FACTS_PER_STEP,
  chunk,
  mergeSummaries,
  writeFactsChunk,
  type StepLike,
  type WriteSummary,
} from '../ingest/steps';

export interface CalendarPipelineDeps {
  cal: CalendarDeps;
  writeOptions?: WriteFactOptions;
  /** Starts NightlySweepWF for the household (only called when params.then_sweep is set). */
  startSweep?: (household_id: string) => Promise<void>;
}

interface EpisodeInfo {
  episode_id: string;
  text: string | null;
}

async function storeDerivedEpisode(db: D1Database, household_id: string, sourceRef: string, raw: string, occurredAt: string): Promise<EpisodeInfo & { quarantined: boolean }> {
  const redacted = redact(raw);
  const sha = await sha256Hex(`${sourceRef}\n${raw}`);
  const episode_id = `ep_${(await sha256Hex(`${household_id}:${sha}`)).slice(0, 32)}`;
  await db
    .prepare(
      `INSERT OR IGNORE INTO episodes (episode_id, household_id, source, source_ref, redacted_text, sha256, occurred_at, redaction_status)
       VALUES (?, ?, 'calendar', ?, ?, ?, ?, ?)`,
    )
    .bind(episode_id, household_id, sourceRef, redacted.redacted_text, sha, occurredAt, redacted.redaction_status)
    .run();
  return { episode_id, text: redacted.redacted_text, quarantined: redacted.redaction_status === 'quarantined' };
}

/** Proposal quoting the whole line `line` of the (redacted) episode text. */
function lineQuote(text: string, line: string): { quote: string; char_start: number; char_end: number } | null {
  const at = locateQuote(text, line);
  return at === null ? null : { quote: line, ...at };
}

type EventCloseRef = { kind: 'occasion' | 'trip'; name: string };

function occasionLine(e: ClassifiedEvent): string {
  return e.kind === 'trip' ? `Trip: ${e.name} | ${e.start} to ${e.end}` : `Occasion: ${e.name} | ${e.kind} | ${e.start} | yearly`;
}

interface EventsStepResult {
  status: 'ok' | 'not_connected';
  mode: 'full' | 'incremental';
  seen: number;
  episode_id: string | null;
  proposals: FactProposal[];
  close: EventCloseRef[];
  cursor: SyncCursor | null;
  occasions: number;
  trips: number;
}

interface WindowsStepResult {
  episode_id: string | null;
  proposals: FactProposal[];
  vanished: CloseTarget[];
  windows: number;
}

export interface CalendarSyncResult {
  status: 'done' | 'not_connected';
  mode?: 'full' | 'incremental';
  events_seen?: number;
  occasions?: number;
  trips?: number;
  windows?: number;
  writes?: WriteSummary;
  closed?: number;
  steps: number;
}

export async function runCalendarSync(deps: CalendarPipelineDeps, params: CalendarSyncParams, step: StepLike): Promise<CalendarSyncResult> {
  const { db } = { db: deps.cal.env.DB };
  const household_id = params.household_id;
  const now = deps.cal.now;
  const today = (): string => now().toISOString().slice(0, 10);
  let steps = 0;
  const run = async <T>(name: string, fn: () => Promise<T>): Promise<T> => {
    steps += 1;
    return step.do(name, DEFAULT_STEP_CONFIG, fn);
  };

  // ── events ──
  const events = await run('events', async (): Promise<EventsStepResult> => {
    const connector = await findCalendarConnector(db, household_id);
    if (connector === null) {
      return { status: 'not_connected', mode: 'full', seen: 0, episode_id: null, proposals: [], close: [], cursor: null, occasions: 0, trips: 0 };
    }
    const previous = parseCursor(connector.sync_cursor);
    const page = await listEvents(deps.cal, connector, previous);
    const day = today();
    const tracked: SyncCursor['tracked'] = page.mode === 'incremental' && previous !== null ? { ...previous.tracked } : {};
    const close: EventCloseRef[] = [];
    const current = new Map<string, ClassifiedEvent>();

    for (const e of page.events) {
      const classified = classifyEvent(e, day);
      const was = tracked[e.id];
      if (classified === null) {
        if (was !== undefined) {
          close.push(was);
          delete tracked[e.id];
        }
        continue;
      }
      current.set(e.id, classified);
    }
    if (page.mode === 'full' && previous !== null) {
      for (const [id, was] of Object.entries(previous.tracked)) if (!current.has(id)) close.push(was);
    }

    // earliest upcoming instance per title and kind
    const best = new Map<string, ClassifiedEvent>();
    for (const e of current.values()) {
      const key = `${e.kind === 'trip' ? 'trip' : 'occasion'}|${aliasKey(e.kind === 'trip' ? 'trip' : 'event', e.name) ?? e.name}`;
      const have = best.get(key);
      if (have === undefined || e.start < have.start) best.set(key, e);
    }
    for (const e of best.values()) tracked[e.id] = { kind: e.kind === 'trip' ? 'trip' : 'occasion', name: e.name };

    const chosen = [...best.values()].sort((a, b) => a.start.localeCompare(b.start) || a.name.localeCompare(b.name));
    const cursor: SyncCursor = { syncToken: page.nextSyncToken, window_start: page.window_start, window_end: page.window_end, tracked };
    const result: EventsStepResult = { status: 'ok', mode: page.mode, seen: page.events.length, episode_id: null, proposals: [], close, cursor, occasions: 0, trips: 0 };
    if (chosen.length === 0) return result;

    const lines = chosen.map(occasionLine);
    const header = `Google Calendar occasions and trips, synced ${day}`;
    const episode = await storeDerivedEpisode(db, household_id, 'google_calendar:events', [header, ...lines].join('\n'), now().toISOString());
    result.episode_id = episode.episode_id;
    if (episode.quarantined || episode.text === null) return result;

    const redactedLines = episode.text.split('\n');
    for (const [i, e] of chosen.entries()) {
      const quote = lineQuote(episode.text, redactedLines[i + 1] ?? '');
      if (quote === null) continue;
      const base = { household_id, source_episode_id: episode.episode_id, confidence: 1, quotes: [quote] };
      if (e.kind === 'trip') {
        result.trips += 1;
        result.proposals.push({ ...base, subject: { type: 'trip', name: e.name }, predicate: 'trip_window', value_text: `${e.start}/${e.end}` });
      } else {
        result.occasions += 1;
        result.proposals.push(
          { ...base, subject: { type: 'event', name: e.name }, predicate: 'event_date', value_text: e.start },
          { ...base, subject: { type: 'event', name: e.name }, predicate: 'event_kind', value_text: e.kind },
        );
      }
    }
    return result;
  });

  if (events.status === 'not_connected') return { status: 'not_connected', steps };

  // ── close facts of events that disappeared, BEFORE writing (a renamed/re-added event must end up open) ──
  let closed = await run('close-events', async () => {
    let total = 0;
    for (const ref of events.close) {
      const type = ref.kind === 'trip' ? 'trip' : 'event';
      const key = aliasKey(type, ref.name);
      if (key === null) continue;
      const predicates = ref.kind === 'trip' ? ['trip_window'] : ['event_date', 'event_kind'];
      const { results } = await db
        .prepare(
          `SELECT f.fact_id FROM facts f JOIN entity_aliases a ON a.entity_id = f.subject_id AND a.household_id = f.household_id
            WHERE f.household_id = ? AND a.alias_norm = ? AND f.predicate IN (${predicates.map(() => '?').join(', ')})
              AND f.valid_to IS NULL AND f.expired_at IS NULL`,
        )
        .bind(household_id, key, ...predicates)
        .all<{ fact_id: string }>();
      total += await closeFacts(db, household_id, results.map((r) => ({ fact_id: r.fact_id, detail: { name: ref.name } })), now().toISOString(), 'calendar_event_removed', 'calendar-sync');
    }
    return total;
  });

  const writes: WriteSummary[] = [];
  for (const [k, group] of chunk(events.proposals, MAX_FACTS_PER_STEP).entries()) {
    writes.push(await run(`write-events-${k}`, () => writeFactsChunk(db, group, deps.writeOptions)));
  }

  // ── free windows ──
  const windows = await run('windows', async (): Promise<WindowsStepResult> => {
    const connector = await findCalendarConnector(db, household_id);
    if (connector === null) return { episode_id: null, proposals: [], vanished: [], windows: 0 };
    const hh = await db.prepare('SELECT timezone FROM households WHERE household_id = ?').bind(household_id).first<{ timezone: string }>();
    const tz = hh?.timezone ?? 'America/New_York';
    const busy = await queryBusy(deps.cal, connector, tz, SYNC_FUTURE_DAYS);
    const day = today();
    const free = computeFreeWindows(busy, tz, day, SYNC_FUTURE_DAYS, now().getTime());

    const out: WindowsStepResult = { episode_id: null, proposals: [], vanished: [], windows: free.length };
    const fresh = new Set(free.map((w) => windowEntityName(w.date)));
    const { results: existing } = await db
      .prepare(
        `SELECT f.fact_id, e.canonical_name FROM facts f JOIN entities e ON e.entity_id = f.subject_id
          WHERE f.household_id = ? AND f.predicate = 'free_window' AND f.valid_to IS NULL AND f.expired_at IS NULL`,
      )
      .bind(household_id)
      .all<{ fact_id: string; canonical_name: string }>();
    for (const row of existing) {
      if (!fresh.has(row.canonical_name)) out.vanished.push({ fact_id: row.fact_id, detail: { window: row.canonical_name } });
    }
    if (free.length === 0) return out;

    const lines = free.map((w) => windowLine(w, tz));
    const episode = await storeDerivedEpisode(db, household_id, 'google_calendar:freebusy', [`Free evening windows for ${tz}, computed ${day}`, ...lines].join('\n'), now().toISOString());
    out.episode_id = episode.episode_id;
    if (episode.quarantined || episode.text === null) return out;
    const redactedLines = episode.text.split('\n');
    for (const [i, w] of free.entries()) {
      const quote = lineQuote(episode.text, redactedLines[i + 1] ?? '');
      if (quote === null) continue;
      out.proposals.push({
        household_id,
        source_episode_id: episode.episode_id,
        subject: { type: 'window', name: windowEntityName(w.date) },
        predicate: 'free_window',
        value_text: `${isoMs(w.startMs)}/${isoMs(w.endMs)}`,
        confidence: 1,
        quotes: [quote],
      });
    }
    return out;
  });

  for (const [k, group] of chunk(windows.proposals, MAX_FACTS_PER_STEP).entries()) {
    writes.push(await run(`write-windows-${k}`, () => writeFactsChunk(db, group, deps.writeOptions)));
  }
  closed += await run('retire-windows', () => closeFacts(db, household_id, windows.vanished, now().toISOString(), 'window_no_longer_free', 'calendar-sync'));

  // ── commit the cursor last ──
  await run('commit', async () => {
    await db
      .prepare(`UPDATE connectors SET sync_cursor = ?, last_success_at = ?, last_error = NULL WHERE household_id = ? AND kind = 'google_calendar' AND status = 'active'`)
      .bind(JSON.stringify(events.cursor), now().toISOString(), household_id)
      .run();
    return true;
  });
  if (params.then_sweep && deps.startSweep !== undefined) {
    const start = deps.startSweep;
    await run('start-sweep', async () => {
      await start(household_id);
      return true;
    });
  }

  return {
    status: 'done',
    mode: events.mode,
    events_seen: events.seen,
    occasions: events.occasions,
    trips: events.trips,
    windows: windows.windows,
    writes: mergeSummaries(writes),
    closed,
    steps,
  };
}
