/**
 * src/goals/planVerifier.ts — the deterministic check on a research_plan goal's deliverable (the micro_app tier has sandboxVerifier instead).
 * A plan is { title, summary, sections: [{ heading, body }], next_steps: [string] }. Problems come back as the same structured diagnostics, so the
 * repair loop treats both tiers alike.
 */

import type { Diagnostic } from '../builder/sandboxVerifier';

export const MIN_PLAN_SECTIONS = 3;
export const MAX_PLAN_CHARS = 20_000;

export interface ResearchPlan {
  title: string;
  summary: string;
  sections: Array<{ heading: string; body: string }>;
  next_steps: string[];
}

export function verifyPlan(plan: unknown): { ok: boolean; diagnostics: Diagnostic[] } {
  const out: Diagnostic[] = [];
  const d = (code: string, message: string, where?: string): void => void out.push({ code, message, ...(where === undefined ? {} : { where }) });
  if (plan === null || typeof plan !== 'object' || Array.isArray(plan)) return { ok: false, diagnostics: [{ code: 'PLAN_SHAPE', message: 'the plan must be an object' }] };
  const p = plan as Record<string, unknown>;
  if (typeof p['title'] !== 'string' || p['title'].trim() === '' || p['title'].length > 120) d('PLAN_TITLE', 'title must be 1-120 characters', 'title');
  if (typeof p['summary'] !== 'string' || p['summary'].trim() === '') d('PLAN_SUMMARY', 'summary must be a non-empty string', 'summary');
  const sections = p['sections'];
  if (!Array.isArray(sections)) d('PLAN_SECTIONS', 'sections must be an array of { heading, body }', 'sections');
  else {
    if (sections.length < MIN_PLAN_SECTIONS) d('TOO_FEW_SECTIONS', `the plan needs at least ${MIN_PLAN_SECTIONS} sections`, 'sections');
    sections.forEach((s, i) => {
      const x = s as { heading?: unknown; body?: unknown } | null;
      if (x === null || typeof x.heading !== 'string' || x.heading.trim() === '' || x.heading.length > 100 || typeof x.body !== 'string' || x.body.trim() === '' || x.body.length > 3_000) {
        d('SECTION_SHAPE', 'every section needs a heading (1-100 characters) and a body (1-3,000 characters)', `sections[${i}]`);
      }
    });
  }
  const steps = p['next_steps'];
  if (!Array.isArray(steps) || steps.length === 0 || !steps.every((s) => typeof s === 'string' && s.trim() !== '')) d('PLAN_NEXT_STEPS', 'next_steps must be a non-empty array of strings', 'next_steps');
  if (JSON.stringify(plan).length > MAX_PLAN_CHARS) d('TOO_LARGE', `the plan is over ${MAX_PLAN_CHARS} characters`);
  return { ok: out.length === 0, diagnostics: out };
}
