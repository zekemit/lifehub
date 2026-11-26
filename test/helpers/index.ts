/**
 * Shared test helpers. Everything here runs against REAL local D1 and R2 (miniflare/workerd) obtained through
 * wrangler's getPlatformProxy; only the network (LLM providers) and the Workflows engine are faked.
 */

import { createRequire } from 'node:module';
import { resolve } from 'node:path';

// ── real D1 / R2 ──

export interface TestRuntime {
  db: D1Database;
  bucket: R2Bucket;
  kv: KVNamespace;
  dispose(): Promise<void>;
}

export async function openTestRuntime(): Promise<TestRuntime> {
  const root = process.env['PROJECT_ROOT'] ?? process.cwd();
  const require = createRequire(resolve(root, 'package.json'));
  const { getPlatformProxy } = require('wrangler') as {
    getPlatformProxy: (o: unknown) => Promise<{ env: { DB: D1Database; DOCS_BUCKET: R2Bucket; KV: KVNamespace }; dispose(): Promise<void> }>;
  };
  const proxy = await getPlatformProxy({
    configPath: resolve(root, 'test/wrangler.test.toml'),
    persist: { path: process.env['STATE_DIR'] },
  });
  return { db: proxy.env.DB, bucket: proxy.env.DOCS_BUCKET, kv: proxy.env.KV, dispose: proxy.dispose };
}

// ── query counting (D1 allows 1,000 queries per invocation; every statement of a batch counts) ──

export interface QueryCounter {
  db: D1Database;
  count: number;
  reset(): void;
}

export function countingDb(real: D1Database): QueryCounter {
  const unwrap = new WeakMap<object, D1PreparedStatement>();
  const state = { count: 0 };
  const wrapStatement = (stmt: D1PreparedStatement): D1PreparedStatement => {
    const proxy: D1PreparedStatement = new Proxy(stmt, {
      get(target, prop) {
        if (prop === 'bind') return (...args: unknown[]) => wrapStatement(target.bind(...args));
        if (prop === 'first' || prop === 'all' || prop === 'run' || prop === 'raw') {
          return (...args: unknown[]) => {
            state.count += 1;
            return (target as never as Record<string, (...a: unknown[]) => unknown>)[prop as string]!(...args);
          };
        }
        const v = Reflect.get(target, prop) as unknown;
        return typeof v === 'function' ? (v as (...a: unknown[]) => unknown).bind(target) : v;
      },
    });
    unwrap.set(proxy, stmt);
    return proxy;
  };
  const db = new Proxy(real, {
    get(target, prop) {
      if (prop === 'prepare') return (sql: string) => wrapStatement(target.prepare(sql));
      if (prop === 'batch') {
        return (stmts: D1PreparedStatement[]) => {
          state.count += stmts.length;
          return target.batch(stmts.map((s) => unwrap.get(s) ?? s));
        };
      }
      if (prop === 'exec') return (sql: string) => { state.count += 1; return target.exec(sql); };
      const v = Reflect.get(target, prop) as unknown;
      return typeof v === 'function' ? (v as (...a: unknown[]) => unknown).bind(target) : v;
    },
  }) as D1Database;
  return {
    db,
    get count() {
      return state.count;
    },
    reset() {
      state.count = 0;
    },
  } as QueryCounter;
}
