/**
 * Milestone 7 fixtures: a scripted model for goals (planner, schema, code, repair, recipe parse), and a runner that drives GoalExecutionWF's
 * state machine (runGoal) step for step, with the member's approval scripted as an event. Only the network and the Workflows engine are fake.
 */

import { randomUUID } from 'node:crypto';
import { runGoal, type GoalApprovalEvent, type GoalDeps, type GoalOutcome } from '../../src/goals/goalPipeline';
import { CANONICAL_DAG } from '../../src/goals/goalPipeline';
import { referenceRecipeBundle } from '../../src/builder/recipeTemplate';
import type { Bundle } from '../../src/builder/sandboxVerifier';
import { createGateway } from '../../src/llm/gateway';
import { createGuardedFetch } from '../../src/net/egress';
import type { TestRuntime } from './index';
import { directStep, fakeProvider, openaiReply, testEnv, type EventScript, type ProviderCall, type TestClock } from './m2';
import type { Env, GoalExecutionParams } from '../../src/types';

export const WIFE_PROMPT =
  "Build my wife a recipe app for her blog at jos-kitchen.example: she can photograph a recipe, I want instant search by ingredient, and a What Should We Cook? button that uses what we like.";
export const RESEARCH_PROMPT = 'Research the best way to compare 529 college savings plans for our two kids and write me a plan.';

export const SLUG = 'recipe-book';
export const TITLE = 'Jo Recipe Book';

export interface GoalModelScript {
  /** The bundle the code model returns, per attempt (0 = first generation, 1 = first repair, ...). The last one repeats. */
  bundles?: Array<(slug: string, title: string) => unknown>;
  /** Usage (tokens) a given purpose reports, to push spend toward the cap. */
  usage?: Partial<Record<'plan' | 'schema' | 'code' | 'repair' | 'parse', { prompt_tokens: number; completion_tokens: number }>>;
  plan?: unknown;
  planText?: string;
  schemaText?: string;
  plans?: unknown[];
  parse?: (text: string) => unknown;
  /** What the code model returns for a research plan. */
  research?: unknown;
}

export interface GoalModel {
  handler: (call: ProviderCall) => Response;
  calls: ProviderCall[];
  count: (marker: string) => number;
  /** All text sent for calls whose system prompt carries `marker`. */
  sent: (marker: string) => string[];
}

export const REF = (slug = SLUG, title = TITLE): Bundle => referenceRecipeBundle({ slug, title });

export const defaultParse = (text: string): unknown => {
  const lines = text.split('\n').map((l) => l.trim()).filter(Boolean);
  const ingredients = /ingredients?:\s*(.+)/i.exec(text)?.[1]?.split(',').map((s) => s.trim()).filter(Boolean) ?? ['rice noodles', 'shrimp'];
  const tags = /thai/i.test(text) ? ['thai', 'noodles'] : ['dinner'];
  return { title: lines[0] ?? 'Recipe', ingredients, steps: ['Cook it.'], tags, prep_minutes: 20 };
};

export function goalModel(script: GoalModelScript = {}): GoalModel {
  const calls: ProviderCall[] = [];
  let codeAttempt = 0;
  let slug = SLUG;
  let title = TITLE;
  const handler = (call: ProviderCall): Response => {
    calls.push(call);
    const s = call.sentText;
    const use = (k: keyof NonNullable<GoalModelScript['usage']>) => {
      const u = script.usage?.[k];
      return u === undefined ? { prompt_tokens: 2_000, completion_tokens: 600, cached: 0 } : { ...u, cached: 0 };
    };
    if (s.includes('GOAL-PLANNER')) {
      const tier = /Tier: (\w+)/.exec(s)?.[1] === 'research_plan' ? 'research_plan' : 'micro_app';
      return openaiReply(
        script.plan ?? { title: TITLE, slug: SLUG, summary: 'A recipe book she can add to from her phone.', tasks: CANONICAL_DAG[tier].map((kind) => ({ kind, notes: `do ${kind}` })) },
        use('plan'),
      );
    }
    if (s.includes('GOAL-SCHEMA')) return openaiReply(script.schemaText ?? { collections: [{ name: 'recipes', fields: ['title', 'ingredients', 'steps', 'tags'] }], ui_notes: 'one page' }, use('schema'));
    if (s.includes('GOAL-CODE. You write a research plan')) {
      if (script.research !== undefined) return openaiReply(script.research, use('code'));
      return openaiReply({ title: 'College savings plan comparison', summary: 'How to compare the plans.', sections: [{ heading: 'Fees', body: 'Compare expense ratios.' }, { heading: 'State benefits', body: 'Check what each state program offers its residents.' }, { heading: 'Investment menu', body: 'Age-based and static options.' }], next_steps: ['List the plans', 'Compare fees'] }, use('code'));
    }
    if (s.includes('GOAL-CODE') || s.includes('GOAL-REPAIR')) {
      const repair = s.includes('GOAL-REPAIR');
      const named = /template "recipe_book", slug "([^"]+)", title "([^"]+)"/.exec(s);
      if (named !== null) {
        slug = named[1]!;
        title = named[2]!;
      }
      const bundles = script.bundles ?? [REF];
      const make = bundles[Math.min(codeAttempt, bundles.length - 1)]!;
      codeAttempt += 1;
      return openaiReply(make(slug, title), use(repair ? 'repair' : 'code'));
    }
    if (s.includes('APP-PARSE-RECIPE')) {
      const text = /<<<\n([\s\S]*?)\n>>>/.exec(s)?.[1] ?? '';
      return openaiReply((script.parse ?? defaultParse)(text), use('parse'));
    }
    throw new Error(`unscripted model call: ${s.slice(0, 120)}`);
  };
  return { handler, calls, count: (m) => calls.filter((c) => c.sentText.includes(m)).length, sent: (m) => calls.filter((c) => c.sentText.includes(m)).map((c) => c.sentText) };
}

export interface GoalRig {
  env: Env;
  deps: GoalDeps;
  model: GoalModel;
  params: GoalExecutionParams;
  workflows: ReturnType<typeof testEnv>['workflows'];
}

export const BASE = 'https://app.example';

export function goalRig(rt: TestRuntime, h: { household_id: string }, o: { script?: GoalModelScript; goal_id?: string; attempt?: number; envExtra?: Partial<Env>; now?: () => Date; smoke?: GoalDeps['smoke'] } = {}): GoalRig {
  const { env, workflows } = testEnv(rt, { WORKER_MODEL: 'gpt-6-luna', CHIEF_MODEL: 'gpt-6.2-sol', ...o.envExtra });
  const model = goalModel(o.script);
  const provider = fakeProvider(model.handler);
  const fetch = createGuardedFetch({ db: rt.db, fetchImpl: provider.fetch, ttlMs: 0 });
  const now = o.now ?? ((): Date => new Date());
  const gateway = createGateway({ db: rt.db, apiKeys: { openai: 'sk-test', anthropic: 'ak-test' }, fetch, now, models: { chief: 'gpt-6.2-sol', worker: 'gpt-6-luna' } });
  const deps: GoalDeps = { env, fetch, gateway, stripe: null, now, ...(o.smoke === undefined ? {} : { smoke: o.smoke }) };
  const goal_id = o.goal_id ?? `goal_${randomUUID().replace(/-/g, '')}`;
  return { env, deps, model, workflows, params: { goal_id, household_id: h.household_id, attempt: o.attempt ?? 0, base_url: BASE } };
}

/** Inserts the goal row the API would create. */
export async function insertGoal(rt: TestRuntime, rig: GoalRig, o: { prompt?: string; tier?: 'micro_app' | 'research_plan' } = {}): Promise<string> {
  const tier = o.tier ?? 'micro_app';
  const at = new Date().toISOString();
  await rt.db
    .prepare(
      `INSERT INTO goals (goal_id, household_id, submitted_by_member_id, prompt_text, tier, status, credits_reserved, max_cost_usd, cost_usd, created_at, updated_at, attempt)
       VALUES (?, ?, ?, ?, ?, 'PLANNING', ?, ?, 0, ?, ?, ?)`,
    )
    .bind(rig.params.goal_id, rig.params.household_id, `mem_${rig.params.household_id}`, o.prompt ?? WIFE_PROMPT, tier, tier === 'micro_app' ? 5 : 3, tier === 'micro_app' ? 1.25 : 0.75, at, at, rig.params.attempt)
    .run();
  return rig.params.goal_id;
}

/** Runs the whole state machine. `approval` scripts the member's tap: an event payload, a function that reads the DB, or TIMEOUT. */
export async function runToEnd(rig: GoalRig, approval: EventScript | ((hash: string) => GoalApprovalEvent), rt: TestRuntime, clock?: TestClock): Promise<{ outcome: GoalOutcome; steps: string[]; waits: Array<{ name: string; type: string; timeout: number | string | undefined }> }> {
  const { step, log, waits } = directStep(
    undefined,
    {
      [`goal_approval:${rig.params.goal_id}`]: async () => {
        const row = await rt.db.prepare('SELECT approval_hash FROM goals WHERE goal_id = ?').bind(rig.params.goal_id).first<{ approval_hash: string | null }>();
        return typeof approval === 'function' ? (approval as (h: string) => unknown)(row?.approval_hash ?? '') : approval;
      },
    },
    clock,
  );
  const outcome = await runGoal(step, rig.deps, rig.params);
  return { outcome, steps: log.map((l) => l.name), waits };
}

export const approve = (member = 'mem_x') => (hash: string): GoalApprovalEvent => ({ decision: 'approve', approval_hash: hash, member_id: member });
export const decline = (hash: string): GoalApprovalEvent => ({ decision: 'decline', approval_hash: hash });
