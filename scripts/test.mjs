// npm test: applies migrations to a fresh LOCAL D1 (miniflare/workerd), bundles test/*.test.ts and runs them with node:test.
// Usage: npm test [-- <substring of a test file name>]
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { build } from 'esbuild';

const filter = process.argv[2] ?? '';
const files = readdirSync('test')
  .filter((f) => f.endsWith('.test.ts') && f.includes(filter))
  .sort();
if (files.length === 0) {
  console.error(`no test files match "${filter}"`);
  process.exit(1);
}

const state = mkdtempSync(join(tmpdir(), 'hub-test-state-'));
const bundles = mkdtempSync(join(tmpdir(), 'hub-test-bundle-'));
let code = 1;
try {
  const migrate = spawnSync(
    'npx',
    ['wrangler', 'd1', 'migrations', 'apply', 'DB', '--local', '-c', 'test/wrangler.test.toml', '--persist-to', state],
    { stdio: 'inherit', env: { ...process.env, CI: '1', WRANGLER_SEND_METRICS: 'false' } },
  );
  if (migrate.status !== 0) throw new Error('migration failed');

  await build({
    entryPoints: files.map((f) => join('test', f)),
    bundle: true,
    platform: 'node',
    format: 'esm',
    outdir: bundles,
    outExtension: { '.js': '.mjs' },
    external: ['wrangler'],
    sourcemap: 'inline',
    logLevel: 'error',
  });

  code = await new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      ['--enable-source-maps', '--test', '--test-concurrency=1', '--test-timeout=180000', ...files.map((f) => join(bundles, f.replace(/\.ts$/, '.mjs')))],
      { stdio: 'inherit', env: { ...process.env, STATE_DIR: join(state, 'v3'), PROJECT_ROOT: process.cwd() } },
    );
    child.on('exit', (c) => resolve(c ?? 1));
  });
} catch (err) {
  console.error(err);
} finally {
  rmSync(state, { recursive: true, force: true });
  rmSync(bundles, { recursive: true, force: true });
}
process.exit(code);
