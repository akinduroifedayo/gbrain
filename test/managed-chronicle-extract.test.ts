/**
 * #5523 — Life Chronicle extraction on a managed brain.
 *
 * Drives the real registered `chronicle_extract` job handler against a source
 * with an active canonical owner. Event pages and their depth-page timeline
 * projections must publish through the coordinator (committed receipts, no
 * guard bypass), stay DB-only, be idempotent on re-run, and never resurrect an
 * event page an operator deleted.
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { BrainEngine } from '../src/core/engine.ts';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { registerBuiltinHandlers } from '../src/commands/jobs.ts';
import { claimWorktree } from '../src/core/persistence/ownership.ts';
import { submitPageMutation } from '../src/core/persistence/page-mutations.ts';
import { disposePersistenceConsumer } from '../src/core/persistence/service.ts';
import { withCoordinatedWrite } from '../src/core/persistence/context.ts';
import { configureGateway, resetGateway, __setChatTransportForTests } from '../src/core/ai/gateway.ts';
import { isolatedPersistencePostgres } from './helpers/persistence-postgres.ts';
import { withEnv } from './helpers/with-env.ts';
import { testBackends } from './helpers/test-backends.ts';

const backends = testBackends();
const engines: BrainEngine[] = [];
const dataDir = mkdtempSync(join(tmpdir(), 'gbrain-chronicle-db-'));
let closePostgres: (() => Promise<void>) | undefined;
beforeAll(async () => {
  configureGateway({ embedding_model: 'openai:text-embedding-3-large', embedding_dimensions: 1536, env: {} });
  if (backends.includes('pglite')) {
    const engine = new PGLiteEngine();
    await engine.connect({ database_path: dataDir }); await engine.initSchema(); engines.push(engine);
  }
  if (backends.includes('postgres')) {
    const pg = await isolatedPersistencePostgres(process.env.DATABASE_URL!);
    engines.push(pg.engine); closePostgres = pg.close;
  }
}, 120_000);
afterAll(async () => {
  for (const engine of engines) { await disposePersistenceConsumer(engine); await engine.disconnect(); }
  await closePostgres?.(); resetGateway(); rmSync(dataDir, { recursive: true, force: true });
});

const EVENTS = [
  { when: '2026-09-20', who: ['people/example'], what: 'Planned the managed rollout', kind: 'meeting' },
  { when: '2026-09-21', who: ['people/example'], what: 'Decided to keep coordinator writes', kind: 'decision' },
];

test('managed chronicle_extract publishes events and projections through the coordinator (#5523)', async () => {
  for (const engine of engines) {
    const dir = mkdtempSync(join(tmpdir(), 'gbrain-chronicle-'));
    const root = join(dir, 'brain'); mkdirSync(root);
    const sourceId = `chron-${randomUUID().slice(0, 8)}`;
    __setChatTransportForTests(async opts => {
      const text = JSON.stringify(EVENTS);
      return { text, blocks: [{ type: 'text', text }], stopReason: 'end',
        usage: { input_tokens: 10, output_tokens: 10, cache_read_tokens: 0, cache_creation_tokens: 0 },
        model: opts.model ?? 'anthropic:claude-sonnet-4-6', providerId: 'anthropic' } as never;
    });
    try {
      await withEnv({ GBRAIN_HOME: join(dir, 'home'), ANTHROPIC_API_KEY: 'sk-test-chronicle' }, async () => {
        await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
        await engine.executeRaw('INSERT INTO sources(id,name,local_path) VALUES($1,$1,$2)', [sourceId, root]);
        await engine.setConfig('sync.write_through', 'true');
        await claimWorktree(engine, sourceId, root);
        const ctx = { engine, sourceId, remote: false as const, config: { engine: engine.kind, embedding_disabled: true },
          dryRun: false, logger: { info() {}, warn() {}, error() {} } };
        await submitPageMutation(ctx, { operation: 'put_page', params: { slug: 'meetings/rollout',
          content: '---\ntitle: Rollout sync\ntype: meeting\ndate: 2026-09-20\n---\nWe planned the rollout and decided on coordinator writes.',
          request_id: randomUUID() } });
        await engine.executeRaw('UPDATE persistence_brain SET enabled=true WHERE singleton=1');

        const handlers = new Map<string, (job: unknown) => Promise<any>>();
        await registerBuiltinHandlers({ register(name: string, fn: (job: unknown) => Promise<any>) { handlers.set(name, fn); } } as never, engine);
        const run = () => handlers.get('chronicle_extract')!({ id: 5523, data: { slug: 'meetings/rollout', sourceId } });

        const first = await run();
        expect(first).toMatchObject({ status: 'extracted', events_written: 2 });
        const events = await engine.executeRaw<{ slug: string; source_path: string | null; effective_date: unknown }>(
          "SELECT slug,source_path,effective_date FROM pages WHERE source_id=$1 AND slug LIKE 'life/events/%' AND deleted_at IS NULL ORDER BY slug", [sourceId]);
        expect(events).toHaveLength(2);
        expect(events.every(e => e.source_path === null && e.effective_date !== null)).toBe(true);
        for (const e of events) expect(existsSync(join(root, `${e.slug}.md`))).toBe(false);
        const timeline = await engine.executeRaw<{ summary: string; date: unknown }>(`SELECT t.summary,t.date FROM timeline_entries t
          JOIN pages d ON d.id=t.page_id WHERE d.source_id=$1 AND d.slug='meetings/rollout' AND t.event_page_id IS NOT NULL ORDER BY t.date`, [sourceId]);
        expect(timeline.map(t => t.summary)).toEqual(EVENTS.map(e => e.what));
        const receipts = await engine.executeRaw<{ state: string }>(
          "SELECT state FROM persistence_requests WHERE source_id=$1 AND operation='submit_job' AND slug LIKE 'life/events/%'", [sourceId]);
        expect(receipts.map(r => r.state)).toEqual(['committed', 'committed']);

        // Idempotent re-run: same pages, same projections, no new timeline rows.
        expect(await run()).toMatchObject({ status: 'extracted', events_written: 2 });
        const again = await engine.executeRaw<{ n: number }>(`SELECT count(*)::int AS n FROM timeline_entries t
          JOIN pages d ON d.id=t.page_id WHERE d.source_id=$1 AND d.slug='meetings/rollout' AND t.event_page_id IS NOT NULL`, [sourceId]);
        expect(Number(again[0].n)).toBe(2);

        // An operator-deleted event is never resurrected by a later run.
        const deleted = events[0].slug;
        // Fixture tombstone for a DB-only page, written under the coordinator's
        // own source capability (delete_page targets file-backed pages here).
        await engine.transaction(tx => withCoordinatedWrite(tx, [sourceId], () => tx.softDeletePage(deleted, { sourceId })));
        expect(await run()).toMatchObject({ status: 'extracted', events_written: 1 });
        const tomb = await engine.executeRaw<{ deleted: boolean }>('SELECT deleted_at IS NOT NULL AS deleted FROM pages WHERE source_id=$1 AND slug=$2', [sourceId, deleted]);
        expect(tomb[0].deleted).toBe(true);
      });
    } finally {
      __setChatTransportForTests(null);
      await disposePersistenceConsumer(engine);
      await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
      rmSync(dir, { recursive: true, force: true });
    }
  }
}, 120_000);
