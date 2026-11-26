/** src/workers/http.ts — tiny response helpers shared by the API modules. */

const JSON_HEADERS = { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' };

/** `extra` header values may be arrays (several Set-Cookie headers). */
export function json(status: number, body: unknown, extra: Record<string, string | string[]> = {}): Response {
  const headers = new Headers(JSON_HEADERS);
  for (const [k, v] of Object.entries(extra)) for (const value of Array.isArray(v) ? v : [v]) headers.append(k, value);
  return new Response(JSON.stringify(body), { status, headers });
}

export const error = (status: number, code: string, message: string, extra: Record<string, string | string[]> = {}): Response =>
  json(status, { error: code, message }, extra);

export function isAlreadyExists(err: unknown): boolean {
  return /already|exist/i.test(err instanceof Error ? err.message : String(err));
}

export const newId = (prefix: string): string => `${prefix}_${crypto.randomUUID().replace(/-/g, '')}`;
