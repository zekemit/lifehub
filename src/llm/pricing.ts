/**
 * src/llm/pricing.ts — USD price per million tokens for every model the gateway may call.
 *
 * Sources, read 2026-11-26:
 *   Anthropic: https://platform.claude.com/docs/en/about-claude/pricing
 *   OpenAI:    https://platform.openai.com/docs/pricing
 *
 * Where a published row did not exist, the entry uses the closest published rate and says so in `basis`
 * (only gpt-6-luna, whose Standard row is not shown, is derived; the choice over-states cost, never under-states it). Re-check this table when
 * either vendor changes prices; `llm_usage.cost_usd` is computed from it at call time.
 */

export const LLM_MODELS = ['gpt-6.2-sol', 'gpt-6.1-sol', 'gpt-6-luna', 'claude-opus-5-5', 'claude-haiku-5-5'] as const;
export type LlmModel = (typeof LLM_MODELS)[number];

export interface RateCard {
  input: number;
  output: number;
  cached_input: number;
}

export interface ModelPrice extends RateCard {
  /** Prompts longer than this many input tokens (cache reads included) are billed at `above_cliff`. */
  cliff_tokens?: number;
  above_cliff?: RateCard;
  basis: string;
}

export const MODEL_PRICING: Readonly<Record<LlmModel, ModelPrice>> = {
  'gpt-6.2-sol': {
    input: 2,
    output: 10,
    cached_input: 0.1,
    basis: 'OpenAI Standard rates for gpt-6.2-sol: $2.00 input / $10.00 output / $0.10 cached input (read 2026-11-26).',
  },
  'gpt-6.1-sol': {
    input: 2,
    output: 10,
    cached_input: 0.1,
    basis: 'OpenAI Standard rates for gpt-6.1-sol: $2.00 / $10.00 / $0.10 (read 2026-11-26). Selectable via CHIEF_MODEL.',
  },
  'gpt-6-luna': {
    input: 0.1,
    output: 0.5,
    cached_input: 0.01,
    basis:
      'OpenAI lists gpt-6-luna only in the Flex table ($0.05 / $0.25 / $0.005). Flex is half of Standard for every other ' +
      'model on the page, so Standard is taken as 2x Flex. Not shown on the page; verify before relying on it.',
  },
  'claude-opus-5-5': {
    input: 4,
    output: 20,
    cached_input: 0.2,
    basis: 'Anthropic base rates for Claude Opus 5.5 (input $4, output $20, cache hit $0.20).',
  },
  'claude-haiku-5-5': {
    input: 0.1,
    output: 0.5,
    cached_input: 0.01,
    cliff_tokens: 100_000,
    above_cliff: { input: 0.5, output: 2.5, cached_input: 0.05 },
    basis:
      'Anthropic Claude Haiku 5.5: prompts up to 100,000 tokens $0.10 / $0.50 / $0.01; over 100,000 tokens the higher ' +
      'tier ($0.50 / $2.50) applies to the whole request. Cache-hit rate above the cliff is 0.1x input.',
  },
};

/** Price of one call in USD. `inputTokens` includes cached tokens. */
export function costUsd(
  model: LlmModel,
  usage: { input_tokens: number; cached_tokens: number; output_tokens: number },
): number {
  const price = MODEL_PRICING[model];
  const rates: RateCard =
    price.cliff_tokens !== undefined && price.above_cliff !== undefined && usage.input_tokens > price.cliff_tokens
      ? price.above_cliff
      : price;
  const cached = Math.min(usage.cached_tokens, usage.input_tokens);
  const fresh = usage.input_tokens - cached;
  const usd = (fresh * rates.input + cached * rates.cached_input + usage.output_tokens * rates.output) / 1_000_000;
  return Math.round(usd * 1e9) / 1e9;
}
