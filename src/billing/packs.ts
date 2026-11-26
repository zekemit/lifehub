/**
 * src/billing/packs.ts — what credits cost and how many a plan grants. The only place these numbers live.
 *
 * Top-up packs never expire. Subscription credits (trial and monthly grants) roll over at most ROLLOVER_CAP at each renewal.
 */

export const PACKS = {
  pack_10: { credits: 10, price_cents: 999 },
  pack_25: { credits: 25, price_cents: 1999 },
  pack_60: { credits: 60, price_cents: 3999 },
} as const;

export type PackSku = keyof typeof PACKS;
export const PACK_SKUS = Object.keys(PACKS) as PackSku[];

export function isPackSku(value: unknown): value is PackSku {
  return typeof value === 'string' && Object.prototype.hasOwnProperty.call(PACKS, value);
}

/** Granted once, when a household is created, so its three cold-start cards and one multi-credit goal can run at once. */
export const TRIAL_CREDITS = 5;
/** Granted on each paid monthly invoice. */
export const MONTHLY_GRANT_CREDITS = 15;
/** Granted each month of an annual plan (the plan is paid once; the grants are dripped monthly). */
export const ANNUAL_MONTHLY_GRANT_CREDITS = 12;
/** At each renewal, unused subscription credits above this expire before the new grant lands. Purchased credits are never touched. */
export const ROLLOVER_CAP = 15;
/** Upper bound a member can set for the monthly auto-recharge spend. */
export const MAX_AUTO_RECHARGE_CAP_CENTS = 50_000;
export const DEFAULT_AUTO_RECHARGE_CAP_CENTS = 2000;
