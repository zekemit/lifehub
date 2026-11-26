/**
 * src/net/egress.ts — the ONLY place in the codebase allowed to call the global fetch().
 *
 * CLAUDE.md §0: never fetch any domain in `policy_blocklist` (Resy, OpenTable, Tock, SevenRooms, api.monarch.com).
 * Gate G5 (policy): "No fetch to blocklisted domains (egress allowlist in fetch wrapper)".
 *
 * Every outbound HTTP call in the app goes through a guarded fetch created here:
 *   - the blocklist is read from D1 (`policy_blocklist`), cached for `ttlMs`, and FAILS CLOSED: if it cannot be
 *     loaded, nothing is fetched;
 *   - a host is blocked when it equals a blocklisted domain or is any subdomain of one (suffix match on labels);
 *   - redirects are followed manually and every hop is checked, so an allowed host cannot bounce the request to a
 *     blocked one;
 *   - only http(s) URLs are allowed.
 * A test (test/milestone2.test.ts) fails the build if any other source file calls fetch() directly.
 */

export class PolicyBlockedError extends Error {
  override readonly name = 'PolicyBlockedError';
  constructor(
    readonly host: string,
    readonly domain: string,
    readonly reason: string,
  ) {
    super(`egress to ${host} is blocked by policy (${domain}): ${reason}`);
  }
}

export class EgressError extends Error {
  override readonly name = 'EgressError';
}

export interface BlocklistEntry {
  domain: string;
  reason: string;
}

export interface GuardedFetchOptions {
  db: D1Database;
  /** The real network. Defaults to the global fetch. Tests inject a fake here; production never does. */
  fetchImpl?: typeof fetch;
  /** How long a loaded blocklist is trusted. Default 60 s. */
  ttlMs?: number;
  /** Maximum redirects followed. Default 5. */
  maxRedirects?: number;
  /** Clock in ms. Default Date.now. */
  nowMs?: () => number;
  /** Called (and awaited) when a request is refused because its host is on policy_blocklist, before PolicyBlockedError is thrown. */
  onBlocked?: (blocked: PolicyBlockedError) => Promise<void> | void;
}

function normalizeHost(host: string): string {
  return host.toLowerCase().replace(/\.+$/, '');
}

/** Returns the matching blocklist entry for `host` (exact or subdomain match), or null. */
export function matchBlocklist(host: string, entries: readonly BlocklistEntry[]): BlocklistEntry | null {
  const h = normalizeHost(host);
  for (const entry of entries) {
    const d = normalizeHost(entry.domain);
    if (h === d || h.endsWith(`.${d}`)) return entry;
  }
  return null;
}

function urlOf(input: RequestInfo | URL): URL {
  const raw = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
  try {
    return new URL(raw);
  } catch {
    throw new EgressError(`invalid URL: ${raw}`);
  }
}

/** Creates the fetch used for all outbound calls. */
export function createGuardedFetch(options: GuardedFetchOptions): typeof fetch {
  const { db } = options;
  const real: typeof fetch = options.fetchImpl ?? ((input, init) => fetch(input, init));
  const ttlMs = options.ttlMs ?? 60_000;
  const maxRedirects = options.maxRedirects ?? 5;
  const nowMs = options.nowMs ?? Date.now;

  let cache: { at: number; entries: BlocklistEntry[] } | null = null;

  async function blocklist(): Promise<BlocklistEntry[]> {
    const now = nowMs();
    if (cache !== null && now - cache.at < ttlMs) return cache.entries;
    const { results } = await db.prepare('SELECT domain, reason FROM policy_blocklist').all<BlocklistEntry>();
    cache = { at: now, entries: results };
    return results;
  }

  async function assertAllowed(url: URL): Promise<void> {
    if (url.protocol !== 'https:' && url.protocol !== 'http:') {
      throw new EgressError(`only http(s) URLs may be fetched, got ${url.protocol}`);
    }
    const hit = matchBlocklist(url.hostname, await blocklist());
    if (hit !== null) {
      const blocked = new PolicyBlockedError(normalizeHost(url.hostname), hit.domain, hit.reason);
      await options.onBlocked?.(blocked);
      throw blocked;
    }
  }

  const guarded = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    let url = urlOf(input);
    let method = (init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase();
    let body = init?.body ?? null;
    for (let hop = 0; ; hop++) {
      await assertAllowed(url);
      const response = await real(url.href, { ...init, method, body, redirect: 'manual' });
      const location = response.headers.get('location');
      const isRedirect = [301, 302, 303, 307, 308].includes(response.status) && location !== null;
      if (!isRedirect) return response;
      if (hop >= maxRedirects) throw new EgressError(`too many redirects fetching ${urlOf(input).hostname}`);
      url = new URL(location, url);
      if (response.status === 303 || ((response.status === 301 || response.status === 302) && method === 'POST')) {
        method = 'GET';
        body = null;
      }
    }
  };
  return guarded as typeof fetch;
}
