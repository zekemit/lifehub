/**
 * src/missions/runState.ts — reading a mission run, moving it through the state machine, and recording its steps.
 *
 * Every transition is checked against MISSION_STATE_TRANSITIONS (src/types.ts) and applied with a conditional UPDATE
 * (`WHERE state = <what we read>`), so two actors cannot both move the same run. Moving to the state a run is already
 * in is a no-op that still applies the patch, which makes Workflow step retries safe.
 */

import { canTransitionMissionState, type IsoTimestamp, type MissionState, type MissionTier, type StepKind } from '../types';

export interface RunRecord {
  run_id: string;
  household_id: string;
  template_key: string;
  template_version: number;
  state: MissionState;
  expires_at: string | null;
  cost_usd: number;
  created_at: string;
  params: Record<string, unknown>;
}

interface RunRow {
  run_id: string;
  household_id: string;
  template_key: string;
  template_version: number;
  state: MissionState;
  expires_at: string | null;
  cost_usd: number;
  created_at: string;
  params_json: string;
}

export async function getRun(db: D1Database, household_id: string, run_id: string): Promise<RunRecord | null> {
  const row = await db
    .prepare(
      `SELECT run_id, household_id, template_key, template_version, state, expires_at, cost_usd, created_at, params_json
         FROM mission_runs WHERE run_id = ? AND household_id = ?`,
    )
    .bind(run_id, household_id)
    .first<RunRow>();
  if (row === null) return null;
  let params: Record<string, unknown> = {};
  try {
    params = JSON.parse(row.params_json) as Record<string, unknown>;
  } catch {
    params = {};
  }
  const { params_json: _ignored, ...rest } = row;
  void _ignored;
  return { ...rest, params };
}

export class IllegalTransitionError extends Error {
  override readonly name = 'IllegalTransitionError';
  constructor(readonly run_id: string, readonly from: MissionState, readonly to: MissionState) {
    super(`mission run ${run_id}: ${from} -> ${to} is not a legal transition`);
  }
}

/**
 * Moves the run to `to` and merges `patch` into params_json (JSON merge-patch: a null value deletes the key).
 * Returns 'already' when it was in `to` already, 'moved' otherwise. Throws IllegalTransitionError for an illegal move.
 */
export async function transitionRun(
  db: D1Database,
  household_id: string,
  run_id: string,
  to: MissionState,
  at: IsoTimestamp,
  patch: Record<string, unknown> = {},
): Promise<'moved' | 'already'> {
  for (let attempt = 0; attempt < 3; attempt++) {
    const run = await getRun(db, household_id, run_id);
    if (run === null) throw new Error(`mission run ${run_id} not found`);
    const patchJson = JSON.stringify(patch);
    if (run.state === to) {
      if (Object.keys(patch).length > 0) {
        await db.prepare(`UPDATE mission_runs SET params_json = json_patch(params_json, ?), updated_at = ? WHERE run_id = ? AND household_id = ?`).bind(patchJson, at, run_id, household_id).run();
      }
      return 'already';
    }
    if (!canTransitionMissionState(run.state, to)) throw new IllegalTransitionError(run_id, run.state, to);
    const res = await db
      .prepare(
        `UPDATE mission_runs SET state = ?, params_json = json_patch(params_json, ?), updated_at = ?
          WHERE run_id = ? AND household_id = ? AND state = ?`,
      )
      .bind(to, patchJson, at, run_id, household_id, run.state)
      .run();
    if ((res.meta.changes ?? 0) > 0) return 'moved';
  }
  throw new Error(`mission run ${run_id}: could not move to ${to} (concurrent updates)`);
}

/** Fixed step numbers for MissionRunWF; the staging done by the nightly sweep owns steps 1-4. */
export const STEP_NO = {
  compile: 10,
  debit_credit: 11,
  r0_read: 12,
  r0_fill: 13,
  gates: 14,
  r1_stage: 15,
  notify: 16,
  await_approval: 17,
  r3_regate: 18,
  r3_link: 19,
  deliver_link: 20,
  await_confirmation: 21,
  confirm: 22,
} as const;

export async function recordStep(
  db: D1Database,
  run_id: string,
  step_no: number,
  o: {
    kind: StepKind;
    tier: MissionTier;
    status: 'ok' | 'failed' | 'skipped';
    gate_name?: string | null;
    gate_result?: unknown;
    input_tokens?: number;
    output_tokens?: number;
    cost_usd?: number;
    at: IsoTimestamp;
  },
): Promise<void> {
  await db
    .prepare(
      `INSERT OR REPLACE INTO steps (run_id, step_no, kind, tier, status, input_tokens, output_tokens, cost_usd, gate_name, gate_result_json, started_at, finished_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    )
    .bind(
      run_id, step_no, o.kind, o.tier, o.status, o.input_tokens ?? 0, o.output_tokens ?? 0, o.cost_usd ?? 0,
      o.gate_name ?? null, o.gate_result === undefined ? null : JSON.stringify(o.gate_result), o.at, o.at,
    )
    .run();
}
