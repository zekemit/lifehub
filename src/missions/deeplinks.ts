/**
 * src/missions/deeplinks.ts — R3 is a deep link and nothing else (CLAUDE.md §0: "R3 = deep link only"). We never book, pay or
 * call a merchant; we hand the member a URL and they finish it themselves.
 *
 * Target domains come from fixed tables (never from model text). Affiliate tags (env.AFFILIATE_TAGS) are added only for
 * domains the tag table names AND that are not in policy_blocklist, so Resy, OpenTable, Tock and SevenRooms links stay
 * untagged even if someone lists them.
 */

/** Issuer site for each card product in benefit_catalog. */
export const ISSUER_DOMAINS: Readonly<Record<string, string>> = {
  amex_platinum: 'americanexpress.com',
  chase_sapphire_reserve: 'chase.com',
  capital_one_venture_x: 'capitalone.com',
};

/** Where each benefit_catalog.merchant_match token leads. */
export const MERCHANT_DOMAINS: Readonly<Record<string, string>> = {
  RESY: 'resy.com',
  OPENTABLE: 'opentable.com',
  UBER: 'uber.com',
  'UBER ONE': 'uber.com',
  CLEAR: 'clearme.com',
  LULULEMON: 'lululemon.com',
  'WALMART+': 'walmart.com',
  'WALMART PLUS': 'walmart.com',
  OURA: 'ouraring.com',
  STUBHUB: 'stubhub.com',
  VIAGOGO: 'viagogo.com',
  LYFT: 'lyft.com',
  'CAPITAL ONE TRAVEL': 'capitalonetravel.com',
  'AMEX TRAVEL': 'amextravel.com',
  'AMERICAN EXPRESS TRAVEL': 'americanexpress.com',
  'FINE HOTELS': 'americanexpress.com',
  'HOTEL COLLECTION': 'americanexpress.com',
  DISNEY: 'disneyplus.com',
  ESPN: 'espn.com',
  HULU: 'hulu.com',
  NYTIMES: 'nytimes.com',
  'NEW YORK TIMES': 'nytimes.com',
  PARAMOUNT: 'paramountplus.com',
  PEACOCK: 'peacocktv.com',
  'WALL STREET JOURNAL': 'wsj.com',
  WSJ: 'wsj.com',
  YOUTUBE: 'youtube.com',
};

/** First merchant token with a known site, else the card issuer's site, else null (no link can be offered). */
export function resolveDomain(merchantMatch: string, cardProduct: string | null): string | null {
  for (const raw of merchantMatch.split('|')) {
    const token = raw.trim().toUpperCase();
    const domain = MERCHANT_DOMAINS[token];
    if (domain !== undefined) return domain;
  }
  return cardProduct === null ? null : (ISSUER_DOMAINS[cardProduct] ?? null);
}

export interface AffiliateTag {
  program: string;
  param: string;
  value: string;
}

export function parseAffiliateTags(raw: string | undefined): Record<string, AffiliateTag> {
  if (raw === undefined || raw.trim() === '') return {};
  try {
    const parsed = JSON.parse(raw) as Record<string, Partial<AffiliateTag>>;
    const out: Record<string, AffiliateTag> = {};
    for (const [domain, t] of Object.entries(parsed)) {
      if (typeof t.program === 'string' && typeof t.param === 'string' && /^[A-Za-z0-9_-]{1,32}$/.test(t.param) && typeof t.value === 'string' && t.value !== '') {
        out[domain.toLowerCase()] = { program: t.program, param: t.param, value: t.value };
      }
    }
    return out;
  } catch {
    return {};
  }
}

export interface LinkTarget {
  merchant_domain: string;
  path: string;
  query: Record<string, string>;
}

export interface DeepLink {
  merchant_domain: string;
  url: string;
  affiliate_program: string | null;
}

/** Builds the final URL. `isBlocked(domain)` says whether the domain is in policy_blocklist. */
export function buildDeepLink(target: LinkTarget, tags: Record<string, AffiliateTag>, isBlocked: (domain: string) => boolean): DeepLink {
  const url = new URL(`https://${target.merchant_domain}${target.path}`);
  for (const [k, v] of Object.entries(target.query)) url.searchParams.set(k, v);
  const tag = tags[target.merchant_domain.toLowerCase()];
  if (tag !== undefined && !isBlocked(target.merchant_domain)) {
    url.searchParams.set(tag.param, tag.value);
    return { merchant_domain: target.merchant_domain, url: url.href, affiliate_program: tag.program };
  }
  return { merchant_domain: target.merchant_domain, url: url.href, affiliate_program: null };
}
