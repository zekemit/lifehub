/**
 * src/goals/tiers.ts — the 3-tier execution ledger. A run costs credits by what it does, and Gate G8 caps its model spend at $0.25 per credit:
 *
 *   Tier 1  Quick Mission                     1 credit   $0.25   a card from a standing trigger (src/missions/*)
 *   Tier 2  Deep Research / Claim Packet      3 credits  $0.75   a claim packet (M6) or a member's research goal ('research_plan')
 *   Tier 3  Autonomous Micro-App Builder      5 credits  $1.25   a member's goal to build and host a small app ('micro_app')
 *
 * Tiers 2 (goals) and 3 are executed by GoalExecutionWF. This is the only place those numbers live for goals; migrations/0005 pins the same
 * three numbers together in a CHECK.
 */

import { maxRunCostUsd } from '../missions/gates';

export const QUICK_MISSION_CREDITS = 1;

export const GOAL_TIERS = {
  research_plan: { credits: 3 },
  micro_app: { credits: 5 },
} as const;

export type GoalTier = keyof typeof GOAL_TIERS;
export const GOAL_TIER_KEYS = Object.keys(GOAL_TIERS) as GoalTier[];

export const creditsFor = (tier: GoalTier): number => GOAL_TIERS[tier].credits;
/** Gate G8: $0.25 per credit reserved. */
export const maxCostFor = (tier: GoalTier): number => maxRunCostUsd(GOAL_TIERS[tier].credits);

const BUILD_VERB = /\b(?:build|make|create|design|develop|code|set\s*up|spin\s*up)\b/i;
const APP_NOUN = /\b(?:app|application|tracker|dashboard|hub|widget|website|web\s*page|tool|planner|organi[sz]er|portal|site|calculator)\b/i;
const INSTALL_ON_SITE = /\binstall\b[^.?!]{0,60}\b(?:site|website|home\s*page|homepage|blog)\b/i;

/** Deterministic: a request to build something hosted is a micro_app; anything else is a research_plan. No model decides what a goal costs. */
export function classifyGoal(prompt: string): GoalTier {
  const text = prompt.slice(0, 4000);
  const build = BUILD_VERB.exec(text);
  if (build !== null && APP_NOUN.test(text.slice(build.index))) return 'micro_app';
  return INSTALL_ON_SITE.test(text) ? 'micro_app' : 'research_plan';
}
