/**
 * src/workers/goalsApi.ts — the member's side of GoalExecutionWF.
 *
 *   POST /api/goals                    {"prompt_text": "..."}  redact, price by tier (micro_app 5 credits / research_plan 3), start GoalExecutionWF.
 *   GET  /api/goals                    the household's goals, newest first.
 *   GET  /api/goals/<id>               one goal with its task DAG, preview and production links.
 *   POST /api/goals/<id>/approve       {"approval_hash"}  publish exactly what was previewed (event goal_approval:<id>).
 *   POST /api/goals/<id>/decline       decline (refunds the reservation).
 *   POST /api/goals/<id>/retry         a goal halted for credits starts again as instance <goal_id>~<n> after a top-up.
 *
 * The household id always comes from the session. A goal id from another household answers 404, never 403.
 */

import { autoRechargeAllowed, autoRechargeSettings } from '../billing/autoRecharge';
import { creditBalance } from '../billing/credits';
import { classifyGoal, creditsFor, maxCostFor } from '../goals/tiers';
import { instanceIdFor, loadGoal, type GoalRow } from '../goals/goalPipeline';
import { redact } from '../redact';
import type { Env } from '../types';
import type { Auth } from './auth';
import { error, isAlreadyExists, json, newId } from './http';

const GOAL_ID = /^goal_[0-9a-f]{32}$/;
export const MAX_GOAL_PROMPT_CHARS = 2_000;

export function parseGoalPath(path: string): { goal_id: string | null; action: 'create_or_list' | 'get' | 'approve' | 'decline' | 'retry' } | null {
  if (path === '/api/goals') return { goal_id: null, action: 'create_or_list' };
  const m = /^\/api\/goals\/([^/]+)(?:\/(approve|decline|retry))?$/.exec(path);
  if (m === null || !GOAL_ID.test(m[1]!)) return null;
  return { goal_id: m[1]!, action: (m[2] as 'approve' | 'decline' | 'retry' | undefined) ?? 'get' };
}

const readJson = async (request: Request): Promise<Record<string, unknown> | null> => {
  try {
    const body = (await request.json()) as unknown;
    return body !== null && typeof body === 'object' && !Array.isArray(body) ? (body as Record<string, unknown>) : null;
  } catch {
    return null;
  }
};

const view = (g: GoalRow) => ({
  goal_id: g.goal_id,
  tier: g.tier,
  status: g.status,
  prompt_text: g.prompt_text,
  credits_reserved: g.credits_reserved,
  max_cost_usd: g.max_cost_usd,
  cost_usd: g.cost_usd,
  approval_hash: g.status === 'AWAITING_APPROVAL' ? g.approval_hash : null,
  preview_url: g.preview_url,
  prod_url: g.prod_url,
  install_artifact: g.install_artifact_json === null ? null : (JSON.parse(g.install_artifact_json) as unknown),
  result_summary: g.result_summary_json === null ? null : (JSON.parse(g.result_summary_json) as unknown),
  error_code: g.error_code,
  created_at: g.created_at,
  updated_at: g.updated_at,
});

async function createGoal(request: Request, env: Env, auth: Auth, now: Date): Promise<Response> {
  const body = await readJson(request);
  const raw = body?.['prompt_text'];
  if (typeof raw !== 'string' || raw.trim() === '') return error(400, 'bad_request', 'Send {"prompt_text": "..."}.');
  if (raw.length > MAX_GOAL_PROMPT_CHARS) return error(413, 'too_long', `A goal may be at most ${MAX_GOAL_PROMPT_CHARS} characters.`);
  const red = redact(raw.trim());
  if (red.redaction_status === 'quarantined') return error(422, 'quarantined', 'That request contains a card number, password or similar secret. Remove it and try again.');
  const prompt = red.redacted_text;
  const tier = classifyGoal(prompt);
  const goal_id = newId('goal');
  const at = now.toISOString();
  await env.DB
    .prepare(
      `INSERT INTO goals (goal_id, household_id, submitted_by_member_id, prompt_text, tier, status, credits_reserved, max_cost_usd, cost_usd, created_at, updated_at, attempt)
       VALUES (?, ?, ?, ?, ?, 'PLANNING', ?, ?, 0, ?, ?, 0)`,
    )
    .bind(goal_id, auth.household_id, auth.member_id, prompt, tier, creditsFor(tier), maxCostFor(tier), at, at)
    .run();
  await env.GOAL_EXECUTION_WF.create({ id: goal_id, params: { goal_id, household_id: auth.household_id, attempt: 0, base_url: new URL(request.url).origin } });
  return json(202, { status: 'accepted', goal_id, tier, credits: creditsFor(tier), max_cost_usd: maxCostFor(tier) });
}

async function listGoals(env: Env, auth: Auth): Promise<Response> {
  const rows = (await env.DB.prepare('SELECT * FROM goals WHERE household_id = ? ORDER BY created_at DESC, goal_id DESC LIMIT 50').bind(auth.household_id).all<GoalRow>()).results;
  return json(200, { goals: rows.map(view) });
}

async function getGoal(env: Env, auth: Auth, goal_id: string): Promise<Response> {
  const goal = await loadGoal(env.DB, goal_id, auth.household_id);
  if (goal === null) return error(404, 'not_found', 'No such goal.');
  const tasks = (
    await env.DB.prepare('SELECT seq_no, kind, status, retries, error_text FROM goal_tasks WHERE goal_id = ? ORDER BY seq_no').bind(goal_id).all<{ seq_no: number; kind: string; status: string; retries: number; error_text: string | null }>()
  ).results;
  return json(200, { ...view(goal), tasks });
}

async function decide(request: Request, env: Env, auth: Auth, goal_id: string, decision: 'approve' | 'decline', now: Date): Promise<Response> {
  const goal = await loadGoal(env.DB, goal_id, auth.household_id);
  if (goal === null) return error(404, 'not_found', 'No such goal.');
  if (decision === 'decline' && goal.status === 'INSUFFICIENT_CREDITS') {
    // nothing was debited and no Workflow is waiting
    await env.DB.prepare(`UPDATE goals SET status = 'DECLINED', error_code = 'declined', updated_at = ? WHERE goal_id = ? AND household_id = ? AND status = 'INSUFFICIENT_CREDITS'`).bind(now.toISOString(), goal_id, auth.household_id).run();
    return json(200, { status: 'declined', goal_id });
  }
  if (goal.status !== 'AWAITING_APPROVAL' || goal.approval_hash === null) {
    return ['COMPLETED', 'DECLINED', 'FAILED'].includes(goal.status) ? json(200, { status: 'already_decided', goal_id, state: goal.status }) : error(409, 'not_awaiting', `A goal in state ${goal.status} is not waiting for a decision.`);
  }
  if (decision === 'approve') {
    const body = await readJson(request);
    if (body?.['approval_hash'] !== goal.approval_hash) return error(409, 'approval_changed', 'The preview changed since you opened it. Reload and review it again.');
  }
  const instance = await env.GOAL_EXECUTION_WF.get(instanceIdFor(goal_id, goal.attempt));
  await instance.sendEvent({ type: `goal_approval:${goal_id}`, payload: { decision, approval_hash: goal.approval_hash, member_id: auth.member_id } });
  return json(202, { status: decision === 'approve' ? 'approval_sent' : 'decline_sent', goal_id });
}

async function retryGoal(request: Request, env: Env, auth: Auth, goal_id: string, now: Date): Promise<Response> {
  const goal = await loadGoal(env.DB, goal_id, auth.household_id);
  if (goal === null) return error(404, 'not_found', 'No such goal.');
  if (goal.status !== 'INSUFFICIENT_CREDITS') return json(200, { status: 'already_started', goal_id, state: goal.status });
  const balance = await creditBalance(env.DB, auth.household_id);
  if (balance < goal.credits_reserved) {
    const auto = await autoRechargeSettings(env.DB, auth.household_id, now);
    if (auto === null || !autoRechargeAllowed(auto).ok) return json(402, { error: 'insufficient_credits', message: 'You are out of credits.', balance, needed: goal.credits_reserved, top_up: '/api/credits/checkout' });
  }
  // exactly one tap wins the attempt counter; the instance id then makes a second create a no-op
  const next = goal.attempt + 1;
  const claimed = await env.DB
    .prepare(`UPDATE goals SET attempt = ?, status = 'PLANNING', updated_at = ? WHERE goal_id = ? AND household_id = ? AND status = 'INSUFFICIENT_CREDITS' AND attempt = ?`)
    .bind(next, now.toISOString(), goal_id, auth.household_id, goal.attempt)
    .run();
  if ((claimed.meta.changes ?? 0) === 0) return json(200, { status: 'already_started', goal_id });
  try {
    await env.GOAL_EXECUTION_WF.create({ id: instanceIdFor(goal_id, next), params: { goal_id, household_id: auth.household_id, attempt: next, base_url: new URL(request.url).origin } });
  } catch (err) {
    if (isAlreadyExists(err)) return json(200, { status: 'already_started', goal_id });
    throw err;
  }
  return json(202, { status: 'accepted', goal_id, instance_id: instanceIdFor(goal_id, next) });
}

export async function handleGoal(request: Request, env: Env, auth: Auth, route: NonNullable<ReturnType<typeof parseGoalPath>>, now: Date): Promise<Response> {
  const post = request.method === 'POST';
  switch (route.action) {
    case 'create_or_list':
      return post ? createGoal(request, env, auth, now) : request.method === 'GET' ? listGoals(env, auth) : error(405, 'method_not_allowed', 'Use GET or POST.');
    case 'get':
      return request.method === 'GET' ? getGoal(env, auth, route.goal_id!) : error(405, 'method_not_allowed', 'Use GET.');
    default:
      if (!post) return error(405, 'method_not_allowed', 'Use POST.');
      if (route.action === 'retry') return retryGoal(request, env, auth, route.goal_id!, now);
      return decide(request, env, auth, route.goal_id!, route.action, now);
  }
}
