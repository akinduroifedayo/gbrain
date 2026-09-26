/**
 * #5484 — the scheduled `autopilot-global-maintenance` lane on a managed brain.
 *
 * Drives the real registered job handler over every MAINTENANCE_PHASES entry
 * against a source with an active canonical owner. Eligible atoms reach concept
 * synthesis, a soft-band take with fresh timeline evidence reaches drift, and an
 * expired soft-deleted page reaches purge. The lane must finish without a failed
 * phase, publish its canonical outputs through the coordinator (database row,
 * canonical Markdown file and durable receipt), and stamp global freshness.
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { BrainEngine } from '../src/core/engine.ts';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { registerBuiltinHandlers } from '../src/commands/jobs.ts';
import { LAST_GLOBAL_AT_KEY, MAINTENANCE_PHASES } from '../src/core/cycle.ts';
import { claimWorktree } from '../src/core/persistence/ownership.ts';
import { submitPageMutation } from '../src/core/persistence/page-mutations.ts';
import { disposePersistenceConsumer } from '../src/core/persistence/service.ts';
import { configureGateway, resetGateway, __setChatTransportForTests, __setEmbedTransportForTests } from '../src/core/ai/gateway.ts';
import { isolatedPersistencePostgres } from './helpers/persistence-postgres.ts';
import { withEnv } from './helpers/with-env.ts';
import { testBackends } from './helpers/test-backends.ts';

const backends = testBackends();
const engines: BrainEngine[] = [];
const dataDir = mkdtempSync(join(tmpdir(), 'gbrain-global-lane-db-'));
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

async function captureGlobalHandler(engine: BrainEngine) {
  const handlers = new Map<string, (job: unknown) => Promise<any>>();
  await registerBuiltinHandlers({ register(name: string, fn: (job: unknown) => Promise<any>) { handlers.set(name, fn); } } as never, engine);
  return handlers.get('autopilot-global-maintenance')!;
}

/** Fixture data is seeded before activation, exactly like a brain that predates managed mode. */
async function seedLegacyCorpus(engine: BrainEngine, sourceId: string) {
  const ctx = { engine, sourceId, remote: false as const, config: { engine: engine.kind, embedding_disabled: true },
    dryRun: false, logger: { info() {}, warn() {}, error() {} } };
  await submitPageMutation(ctx, { operation: 'put_page', params: {
    slug: 'people/example', content: '---\ntitle: Example\ntype: person\n---\nExample person.', request_id: randomUUID() } });
  // A concept that already has a canonical file (older brains file-back concepts).
  await submitPageMutation(ctx, { operation: 'put_page', params: {
    slug: 'concepts/owner-writes', content: '---\ntitle: owner writes\ntype: concept\n---\nOld narrative.', request_id: randomUUID() } });
  for (const [slug, concepts] of [
    ['atoms/durable-memory-one', ['durable-memory']],
    ['atoms/durable-memory-two', ['durable-memory', 'owner-writes']],
    ['atoms/owner-writes-one', ['owner-writes']],
  ] as const) {
    await engine.executeRaw(`INSERT INTO pages(source_id,slug,type,title,compiled_truth,frontmatter)
      VALUES($1,$2,'atom',$3,$4,$5::text::jsonb)`, [sourceId, slug, slug.split('/')[1], `Insight for ${slug}.`,
      JSON.stringify({ type: 'atom', concepts })]);
  }
  const person = (await engine.getPage('people/example', { sourceId }))!;
  await engine.addTakesBatch([{ page_id: person.id, row_num: 1, claim: 'Example prefers durable writes',
    kind: 'take', holder: 'self', weight: 0.6 }]);
  await engine.addTimelineEntriesBatch([{ slug: 'people/example', date: new Date().toISOString().slice(0, 10),
    source: 'test', summary: 'Example chose a different write path', source_id: sourceId }]);
  await engine.executeRaw(`INSERT INTO pages(source_id,slug,type,title,compiled_truth,deleted_at)
    VALUES($1,'notes/expired','note','Expired','Gone.',now()-interval '80 hours')`, [sourceId]);
}

test('managed global maintenance lane completes every applicable phase and stamps freshness (#5484)', async () => {
  for (const engine of engines) {
    const dir = mkdtempSync(join(tmpdir(), 'gbrain-global-lane-'));
    const root = join(dir, 'brain'); mkdirSync(root);
    const sourceId = `lane-${randomUUID().slice(0, 8)}`;
    let calls = 0;
    __setChatTransportForTests(async opts => {
      calls++;
      const text = JSON.stringify({ drifted: true, confidence: 0.9, reasoning: 'New evidence' });
      return { text, blocks: [{ type: 'text', text }], stopReason: 'end',
        usage: { input_tokens: 10, output_tokens: 10, cache_read_tokens: 0, cache_creation_tokens: 0 },
        model: opts.model ?? 'anthropic:claude-sonnet-4-6', providerId: 'anthropic' } as never;
    });
    __setEmbedTransportForTests((async ({ values }: { values: string[] }) => ({
      embeddings: values.map(() => Array.from({ length: 1536 }, () => 0.01)), values, usage: { tokens: values.length } })) as never);
    try {
      await withEnv({ GBRAIN_HOME: join(dir, 'home'), GBRAIN_SCHEMA_PACK: 'gbrain-everything', ANTHROPIC_API_KEY: 'sk-test-lane', OPENAI_API_KEY: 'sk-test-lane' }, async () => {
        await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
        await engine.executeRaw('INSERT INTO sources(id,name,local_path) VALUES($1,$1,$2)', [sourceId, root]);
        for (const [key, value] of [['sync.write_through', 'true'], ['dream.synthesize.enabled', 'false'],
          ['dream.patterns.enabled', 'false'], ['dream.drift.enabled', 'true'], ['embedding_disabled', 'true'],
          ['models.drift', 'anthropic:claude-sonnet-4-6']]) await engine.setConfig(key, value);
        await engine.executeRaw('DELETE FROM config WHERE key=$1', [LAST_GLOBAL_AT_KEY]);
        await claimWorktree(engine, sourceId, root);
        await seedLegacyCorpus(engine, sourceId);
        await engine.executeRaw('UPDATE persistence_brain SET enabled=true WHERE singleton=1');

        const handler = await captureGlobalHandler(engine);
        const result = await handler({ id: 5484, data: { phases: (process.env.LANE_PHASES ? process.env.LANE_PHASES.split(",") : MAINTENANCE_PHASES), repoPath: root }, signal: undefined });
        const phases = result.report.phases as Array<{ phase: string; status: string; summary: string; error?: { message: string } }>;
        const failed = phases.filter(p => p.status === 'fail').map(p => `${p.phase}: ${p.error?.message ?? p.summary}`);
        if (failed.length) console.log("FAILED_PHASES", JSON.stringify(failed));
        expect(failed).toEqual([]);
        const byPhase = new Map(phases.map(p => [p.phase, p]));
        expect(byPhase.get('synthesize_concepts')?.status).toBe('ok');
        expect(byPhase.get('drift')?.status).not.toBe('skipped');
        expect(byPhase.get('purge')?.status).toBe('ok');
        expect(await engine.getConfig(LAST_GLOBAL_AT_KEY)).not.toBeNull();

        // Concept pages land in the cycle's source. A new concept stays DB-only
        // (legacy shape); the file-backed concept is republished to its file.
        for (const concept of ['durable-memory', 'owner-writes']) {
          const snapshot = await engine.readPageSnapshot(`concepts/${concept}`, { sourceId });
          expect(snapshot?.page.type).toBe('concept');
          expect(snapshot?.page.frontmatter.synthesized_by).toBe('synthesize_concepts-v0.41');
        }
        expect(existsSync(join(root, 'concepts/durable-memory.md'))).toBe(false);
        const ownerFile = readFileSync(join(root, 'concepts/owner-writes.md'), 'utf8');
        expect(ownerFile).toContain('synthesize_concepts-v0.41');
        expect(ownerFile).not.toContain('Old narrative.');
        const receipts = await engine.executeRaw<{ slug: string; state: string }>(
          "SELECT slug,state FROM persistence_requests WHERE source_id=$1 AND operation='submit_job' AND slug LIKE 'concepts/%' ORDER BY slug", [sourceId]);
        expect(receipts.map(r => [r.slug, r.state])).toEqual([['concepts/durable-memory', 'committed'], ['concepts/owner-writes', 'committed']]);
        // Provenance edges in both directions, pinned to the cycle source.
        const edges = await engine.executeRaw<{ n: number }>(`SELECT count(*)::int AS n FROM links l
          JOIN pages f ON f.id=l.from_page_id JOIN pages t ON t.id=l.to_page_id
          WHERE l.link_source='concept-provenance' AND f.source_id=$1 AND t.source_id=$1`, [sourceId]);
        expect(Number(edges[0].n)).toBe(8); // 2 concepts x 2 member atoms x both directions
        // The drift report is published by the coordinator into the cycle's
        // source (PR #5537 semantics): committed receipt, row and canonical file.
        const driftSlug = `reports/drift-${new Date().toISOString().slice(0, 10)}`;
        const report = await engine.executeRaw<{ state: string }>(`SELECT r.state FROM persistence_requests r
          WHERE r.source_id=$1 AND r.operation='submit_job' AND r.slug=$2`, [sourceId, driftSlug]);
        expect(report.map(r => r.state)).toEqual(['committed']);
        expect((await engine.getPage(driftSlug, { sourceId }))?.compiled_truth).toContain('Example prefers durable writes');
        expect(readFileSync(join(root, `${driftSlug}.md`), 'utf8')).toContain('Example prefers durable writes');
        // The expired tombstone was purged through a committed delete_page receipt.
        expect(await engine.executeRaw("SELECT id FROM pages WHERE source_id=$1 AND slug='notes/expired'", [sourceId])).toHaveLength(0);
        expect((await engine.executeRaw<{ state: string }>(
          "SELECT state FROM persistence_requests WHERE source_id=$1 AND operation='delete_page' AND slug='notes/expired'", [sourceId]))
          .map(r => r.state)).toEqual(['committed']);
        expect(calls).toBeGreaterThan(0);
      });
    } finally {
      __setChatTransportForTests(null); __setEmbedTransportForTests(null);
      await disposePersistenceConsumer(engine);
      await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
      rmSync(dir, { recursive: true, force: true });
    }
  }
}, 120_000);
