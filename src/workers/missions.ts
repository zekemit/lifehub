/**
 * src/workers/missions.ts — the member's side of MissionRunWF.
 *
 *   POST /api/missions/<run_id>/start     tap a staged card: starts MissionRunWF (instance id = run_id) which takes the card over.
 *   POST /api/missions/<run_id>/approve   body {payload_hash}: the member approves the exact card they saw (R2). Sent to the waiting
 *                                         Workflow as event `approve:<run_id>`.
 *   POST /api/missions/<run_id>/decline   "Not for me". A staged card is declined directly; a card awaiting approval gets a decline event.
 *   POST /api/missions/<run_id>/confirm   "I did it" after the deep link: event `confirmed:<run_id>`.
 *   POST /api/push/subscribe              body {endpoint, keys?}: stores the member's Web Push subscription (allowed push hosts only).
 *
 * Every lookup is scoped by the authenticated household; a run id from another household answers 404, never 403.
 */

import { autoRechargeAllowed, autoRechargeSettings } from '../billing/autoRecharge';
import { creditBalance } from '../billing/credits';
import { getRun, transitionRun, type RunRecord } from '../missions/runState';
import { isAllowedPushEndpoint } from '../notify/webPush';
import type { Env } from '../types';
import type { Auth } from './auth';
import { error, isAlreadyExists, json } from './http';

const RUN_ID = /^run_[0-9a-f]{32}$/;
/** States in which MissionRunWF is already running (or has finished) for a run. */
const STARTED_STATES = ['COMPILED', 'R0_READ', 'GATED', 'R2_AWAITING', 'R2_APPROVED', 'R3_LINK_ISSUED', 'CONFIRMED', 'FAILED_GATE', 'CANCELED'];

/**
 * The Workflow that owns a run, and its instance id. Most runs belong to MissionRunWF under their own id; a drop alarm's run lives inside
 * its DropAlarmWF instance; a run restarted after a top-up lives in a newer MissionRunWF instance (`<run_id>~<attempt>`).
 */
function workflowOf(env: Env, run: RunRecord): { binding: Workflow<unknown>; id: string } {
  const instance = typeof run.params['workflow_instance'] === 'string' ? run.params['workflow_instance'] : run.run_id;
  const binding = run.params['workflow'] === 'drop_alarm' ? env.DROP_ALARM_WF : env.MISSION_RUN_WF;
  return { binding: binding as Workflow<unknown>, id: instance };
}

export function parseMissionPath(path: string): { run_id: string; action: 'start' | 'approve' | 'decline' | 'confirm' } | null {
  const m = /^\/api\/missions\/([^/]+)\/(start|approve|decline|confirm)$/.exec(path);
  if (m === null || !RUN_ID.test(m[1]!)) return null;
  return { run_id: m[1]!, action: m[2] as 'start' | 'approve' | 'decline' | 'confirm' };
}

async function readJson(request: Request): Promise<Record<string, unknown> | null> {
  try {
    const body = (await request.json()) as unknown;
    return body !== null && typeof body === 'object' && !Array.isArray(body) ? (body as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

const stateOf = (run: RunRecord): string => run.state;

async function start(env: Env, auth: Auth, run: RunRecord, now: Date): Promise<Response> {
  // a run another Workflow owns (a drop alarm's card) is already running there
  if (typeof run.params['workflow'] === 'string' || STARTED_STATES.includes(run.state)) return json(200, { status: 'already_started', run_id: run.run_id, state: stateOf(run) });
  if (run.state !== 'R1_STAGED' && run.state !== 'INSUFFICIENT_CREDITS') return error(409, 'not_startable', `A card in state ${run.state} cannot be started.`);
  if (run.expires_at !== null && Date.parse(run.expires_at) < now.getTime()) return error(409, 'expired', 'This card has expired.');
  const tpl = await env.DB.prepare('SELECT max_tier, credits_cost FROM mission_templates WHERE template_key = ? AND version = ?')
    .bind(run.template_key, run.template_version)
    .first<{ max_tier: string; credits_cost: number }>();
  if (tpl === null || tpl.max_tier === 'R1') return error(409, 'not_runnable', 'This card has no action to run.');
  const balance = await creditBalance(env.DB, auth.household_id);
  if (balance < tpl.credits_cost) {
    // short of credits: the run may still go ahead if the household opted in to auto-recharge and it is allowed right now
    const auto = await autoRechargeSettings(env.DB, auth.household_id, now);
    if (auto === null || !autoRechargeAllowed(auto).ok) {
      return json(402, { error: 'insufficient_credits', message: 'You are out of credits.', balance, needed: tpl.credits_cost, top_up: '/api/credits/checkout' });
    }
  }
  // a run halted for credits starts again as a NEW Workflow instance (the first one has finished). If the newest instance is
  // already running (it started after the halt we are looking at, but has not moved the run on yet), a second tap starts nothing.
  const startedAttempt = typeof run.params['attempt'] === 'number' ? run.params['attempt'] : 0;
  const failure = run.params['failure'] as { attempt?: unknown } | null | undefined;
  const haltedAttempt = typeof failure?.attempt === 'number' ? failure.attempt : 0;
  if (run.state === 'INSUFFICIENT_CREDITS' && startedAttempt > haltedAttempt) return json(200, { status: 'already_started', run_id: run.run_id, state: stateOf(run) });
  const attempt = run.state === 'INSUFFICIENT_CREDITS' ? startedAttempt + 1 : 0;
  const instance = attempt === 0 ? run.run_id : `${run.run_id}~${attempt}`;
  if (attempt > 0) {
    await env.DB
      .prepare('UPDATE mission_runs SET params_json = json_patch(params_json, ?), updated_at = ? WHERE run_id = ? AND household_id = ?')
      .bind(JSON.stringify({ attempt, workflow_instance: instance }), now.toISOString(), run.run_id, auth.household_id)
      .run();
  }
  try {
    await env.MISSION_RUN_WF.create({ id: instance, params: { run_id: run.run_id, household_id: auth.household_id } });
  } catch (err) {
    if (isAlreadyExists(err)) return json(200, { status: 'already_started', run_id: run.run_id, state: stateOf(run) });
    throw err;
  }
  return json(202, { status: 'accepted', run_id: run.run_id });
}

async function decide(env: Env, auth: Auth, run: RunRecord, decision: 'approve' | 'decline', claimedHash: string | null, now: Date): Promise<Response> {
  if ((run.state === 'R1_STAGED' || run.state === 'INSUFFICIENT_CREDITS') && decision === 'decline' && typeof run.params['workflow'] !== 'string') {
    // Never started, or halted before anything was taken: no Workflow is waiting. "Not for me" is recorded directly.
    const at = now.toISOString();
    await transitionRun(env.DB, auth.household_id, run.run_id, 'DECLINED', at, { declined_reason: 'declined' });
    return json(200, { status: 'declined', run_id: run.run_id });
  }
  if (run.state !== 'R2_AWAITING') {
    return TERMINAL_OR_LATER.includes(run.state)
      ? json(200, { status: 'already_decided', run_id: run.run_id, state: stateOf(run) })
      : error(409, 'not_awaiting', `A card in state ${run.state} is not waiting for a decision.`);
  }
  const stored = typeof run.params['payload_hash'] === 'string' ? run.params['payload_hash'] : null;
  if (stored === null) return error(409, 'not_awaiting', 'This card has not been staged yet.');
  if (decision === 'approve' && claimedHash !== stored) return error(409, 'payload_changed', 'The card changed since you opened it. Reload and review it again.');
  await env.DB.prepare(
    `INSERT INTO approvals (run_id, member_id, decision, payload_hash, decided_at) VALUES (?, ?, ?, ?, ?)
     ON CONFLICT (run_id) DO NOTHING`,
  )
    .bind(run.run_id, auth.member_id, decision, stored, now.toISOString())
    .run();
  const wf = workflowOf(env, run);
  await wf.binding.get(wf.id).then((instance) =>
    instance.sendEvent({ type: `approve:${run.run_id}`, payload: { decision, payload_hash: stored, member_id: auth.member_id } }),
  );
  return json(202, { status: decision === 'approve' ? 'approval_sent' : 'decline_sent', run_id: run.run_id });
}

const TERMINAL_OR_LATER = ['R2_APPROVED', 'R3_LINK_ISSUED', 'CONFIRMED', 'EXPIRED', 'DECLINED', 'FAILED_GATE', 'CANCELED'];

async function confirm(env: Env, run: RunRecord): Promise<Response> {
  if (run.state === 'CONFIRMED') return json(200, { status: 'already_confirmed', run_id: run.run_id });
  if (run.state !== 'R3_LINK_ISSUED') return error(409, 'no_link_issued', 'There is no issued link to confirm.');
  const wf = workflowOf(env, run);
  await wf.binding.get(wf.id).then((instance) => instance.sendEvent({ type: `confirmed:${run.run_id}`, payload: {} }));
  return json(202, { status: 'confirmation_sent', run_id: run.run_id });
}

export async function handleMission(request: Request, env: Env, auth: Auth, route: NonNullable<ReturnType<typeof parseMissionPath>>, now: Date): Promise<Response> {
  const run = await getRun(env.DB, auth.household_id, route.run_id);
  if (run === null) return error(404, 'not_found', 'No such card.');
  switch (route.action) {
    case 'start':
      return start(env, auth, run, now);
    case 'approve': {
      const body = await readJson(request);
      const hash = body !== null && typeof body['payload_hash'] === 'string' ? body['payload_hash'] : null;
      if (hash === null) return error(400, 'payload_hash_required', 'Send the payload_hash of the card you reviewed.');
      return decide(env, auth, run, 'approve', hash, now);
    }
    case 'decline':
      return decide(env, auth, run, 'decline', null, now);
    case 'confirm':
      return confirm(env, run);
  }
}

export async function handlePushSubscribe(request: Request, env: Env, auth: Auth): Promise<Response> {
  const body = await readJson(request);
  const endpoint = body !== null && typeof body['endpoint'] === 'string' ? body['endpoint'] : null;
  if (endpoint === null || endpoint.length > 2048 || !isAllowedPushEndpoint(endpoint)) {
    return error(400, 'bad_endpoint', 'endpoint must be an https URL on a supported push service.');
  }
  // Only the endpoint is kept: pushes carry no payload, so the encryption keys are never needed.
  await env.DB.prepare('UPDATE members SET push_endpoint = ? WHERE member_id = ? AND household_id = ?')
    .bind(JSON.stringify({ endpoint }), auth.member_id, auth.household_id)
    .run();
  return json(200, { status: 'subscribed' });
}
