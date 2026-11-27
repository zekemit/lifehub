/**
 * src/workflows/goalExecutionWF.ts — Cloudflare Workflow wrapper for GoalExecutionWF (Claude.md §5.4). The state machine lives in
 * src/goals/goalPipeline.ts; this class only supplies the durable `step` and production wiring.
 *
 * The instance id is the goal id; a goal restarted after a top-up is instance `<goal_id>~<attempt>`. The member's approval arrives as the
 * event `goal_approval:<goal_id>` ({decision, approval_hash, member_id}).
 */

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import { runGoal, type GoalDeps } from '../goals/goalPipeline';
import { missionDepsFromEnv } from '../missions/runPipeline';
import type { StepLike } from '../ingest/steps';
import type { Env, GoalExecutionParams } from '../types';

export class GoalExecutionWF extends WorkflowEntrypoint<Env, GoalExecutionParams> {
  override async run(event: WorkflowEvent<GoalExecutionParams>, step: WorkflowStep): Promise<void> {
    const { goal_id, household_id } = event.payload;
    const base = missionDepsFromEnv(this.env, goal_id, household_id);
    const deps: GoalDeps = { ...base, env: this.env };
    await runGoal(step as unknown as StepLike, deps, { ...event.payload, goal_id });
  }
}
