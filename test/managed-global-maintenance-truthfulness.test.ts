/**
 * #5484 — the managed global maintenance lane must preserve what it cannot
 * republish losslessly and must not claim freshness for unfinished work.
 *
 * Drives the real `autopilot-global-maintenance` handler (synthesize_concepts
 * + purge) on a managed brain and asserts the handler-level outcome: phase
 * status, counters, summary and `autopilot.last_global_at`.
 *
 *  - F2: an existing concept whose original fences are ambiguous (duplicate
 *    Facts fence, unterminated Takes fence, fence inside the timeline) is held
 *    byte-identical: no publication, no fact expiry, no take deletion.
 *  - F5: a concept publication deferred by a concurrent writer withholds the
 *    freshness stamp (the next run retries it); an operational purge failure
 *    fails the purge phase and withholds the stamp; neither is reported as a
 *    clean run.
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { mkdirSync, mkdtempSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { BrainEngine } from '../src/core/engine.ts';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { registerBuiltinHandlers } from '../src/commands/jobs.ts';
import { LAST_GLOBAL_AT_KEY } from '../src/core/cycle.ts';
import { dispatchGlobalMaintenance } from '../src/commands/autopilot-fanout.ts';
import { claimWorktree } from '../src/core/persistence/ownership.ts';
import { submitPageMutation } from '../src/core/persistence/page-mutations.ts';
import { disposePersistenceConsumer } from '../src/core/persistence/service.ts';
import { configureGateway, resetGateway } from '../src/core/ai/gateway.ts';
import { isolatedPersistencePostgres } from './helpers/persistence-postgres.ts';
import { withEnv } from './helpers/with-env.ts';
import { recordFactWithdrawal } from '../src/core/facts/withdrawal.ts';
import { testBackends } from './helpers/test-backends.ts';

const backends = testBackends();
const LAST_GLOBAL_ATTEMPT_AT_KEY = 'autopilot.last_global_attempt_at';
const engines: BrainEngine[] = [];
const dataDir = mkdtempSync(join(tmpdir(), 'gbrain-global-truth-db-'));
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

const FACTS_HEAD = ['| # | claim | kind | confidence | visibility | notability | valid_from | valid_until | source | context |',
  '|---|-------|------|------------|------------|------------|------------|-------------|--------|---------|'];
const TAKES_HEAD = ['| # | claim | kind | who | weight | since | source |', '|---|-------|------|-----|--------|-------|--------|'];
const factsFence = (n: number, claim: string) => ['<!--- gbrain:facts:begin -->', '', ...FACTS_HEAD,
  `| ${n} | ${claim} | fact | 1.0 | world | high | 2026-09-01 |  | fence |  |`, '<!--- gbrain:facts:end -->'].join('\n');
const takesRows = ['<!--- gbrain:takes:begin -->', '', ...TAKES_HEAD, '| 1 | Kept take | take | self | 0.8 | 2026-09-01 | fence |'].join('\n');

/** Ambiguous originals, stored exactly as an older writer left them. */
const AMBIGUOUS: Record<string, { body: string; timeline: string }> = {
  'dup-takes': { body: `Old narrative.\n\n## Takes\n\n${takesRows}\n<!--- gbrain:takes:end -->\n\n${takesRows}\n<!--- gbrain:takes:end -->`, timeline: '' },
  'dup-facts': { body: `Old narrative.\n\n## Facts\n\n${factsFence(1, 'First fence claim')}\n\n## More facts\n\n${factsFence(2, 'Second fence claim')}`, timeline: '' },
  'open-takes': { body: `Old narrative.\n\n## Facts\n\n${factsFence(1, 'Open takes claim')}\n\n## Takes\n\n${takesRows}`, timeline: '' },
  'fence-in-timeline': { body: `Old narrative.\n\n## Facts\n\n${factsFence(1, 'Above sentinel claim')}`,
    timeline: `- **2026-09-02** | test — entry\n\n${factsFence(2, 'Below sentinel claim')}` },
};

async function captureGlobalHandler(engine: BrainEngine) {
  const handlers = new Map<string, (job: unknown) => Promise<any>>();
  await registerBuiltinHandlers({ register(name: string, fn: (job: unknown) => Promise<any>) { handlers.set(name, fn); } } as never, engine);
  return handlers.get('autopilot-global-maintenance')!;
}

type Phase = { phase: string; status: string; summary: string; details: Record<string, any>; error?: { message: string } };

async function lane(run: (ctx: { engine: BrainEngine; sourceId: string; root: string; dir: string;
  put: (slug: string, content: string, expected?: string) => Promise<unknown> }) => Promise<void>) {
  for (const engine of engines) {
    const dir = mkdtempSync(join(tmpdir(), 'gbrain-global-truth-'));
    const root = join(dir, 'brain'); mkdirSync(root);
    const sourceId = `truth-${randomUUID().slice(0, 8)}`;
    try {
      await withEnv({ GBRAIN_HOME: join(dir, 'home'), GBRAIN_SCHEMA_PACK: 'gbrain-everything' }, async () => {
        await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
        await engine.executeRaw('INSERT INTO sources(id,name,local_path) VALUES($1,$1,$2)', [sourceId, root]);
        for (const [key, value] of [['sync.write_through', 'true'], ['dream.synthesize.enabled', 'false'],
          ['dream.patterns.enabled', 'false'], ['dream.drift.enabled', 'false'], ['embedding_disabled', 'true']]) await engine.setConfig(key, value);
        await engine.executeRaw('DELETE FROM config WHERE key=$1', [LAST_GLOBAL_AT_KEY]);
        await engine.executeRaw('DELETE FROM config WHERE key=$1', [LAST_GLOBAL_ATTEMPT_AT_KEY]);
        await claimWorktree(engine, sourceId, root);
        const ctx = { engine, sourceId, remote: false as const, config: { engine: engine.kind, embedding_disabled: true },
          dryRun: false, logger: { info() {}, warn() {}, error() {} } };
        const put = (slug: string, content: string, expected?: string) => submitPageMutation(ctx, { operation: 'put_page',
          params: { slug, content, request_id: randomUUID(), ...(expected ? { expected_revision: expected } : {}) } });
        await run({ engine, sourceId, root, dir, put });
      });
    } finally {
      await disposePersistenceConsumer(engine);
      await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
      rmSync(dir, { recursive: true, force: true });
    }
  }
}

async function atoms(engine: BrainEngine, sourceId: string, concept: string, n = 2) {
  for (let i = 0; i < n; i++) {
    await engine.executeRaw(`INSERT INTO pages(source_id,slug,type,title,compiled_truth,frontmatter)
      VALUES($1,$2,'atom',$2,'Insight.',$3::text::jsonb)`, [sourceId, `atoms/${concept}-${i}`, JSON.stringify({ type: 'atom', concepts: [concept] })]);
  }
}

async function runLane(engine: BrainEngine, root: string): Promise<{ result: any; phases: Map<string, Phase> }> {
  const handler = await captureGlobalHandler(engine);
  const result = await handler({ id: 5484, data: { phases: ['synthesize_concepts', 'purge'], repoPath: root }, signal: undefined });
  return { result, phases: new Map((result.report.phases as Phase[]).map(p => [p.phase, p])) };
}

test('ambiguous concept originals are held byte-identical with their index; the run is not reported clean (F2)', async () => {
  await lane(async ({ engine, sourceId, root }) => {
    mkdirSync(join(root, 'concepts'), { recursive: true });
    const files = new Map<string, Buffer>();
    for (const [name, { body, timeline }] of Object.entries(AMBIGUOUS)) {
      const file = join(root, `concepts/${name}.md`);
      const bytes = Buffer.from(`---\ntitle: ${name}\ntype: concept\n---\n${body}\n\n<!-- gbrain:timeline -->\n${timeline}\n`);
      writeFileSync(file, bytes); files.set(file, bytes);
      await engine.executeRaw(`INSERT INTO pages(source_id,slug,type,title,compiled_truth,timeline,frontmatter)
        VALUES($1,$2,'concept',$3,$4,$5,'{}'::jsonb)`, [sourceId, `concepts/${name}`, name, body, timeline]);
      await engine.executeRaw(`INSERT INTO facts(source_id,entity_slug,fact,kind,visibility,notability,source,row_num,source_markdown_slug)
        VALUES($1,$2,$3,'fact','world','high','fence',1,$2),($1,$2,$4,'fact','world','high','fence',2,$2)`,
      [sourceId, `concepts/${name}`, `${name} indexed one`, `${name} indexed two`]);
      const [pg] = await engine.executeRaw<{ id: number }>('SELECT id FROM pages WHERE source_id=$1 AND slug=$2', [sourceId, `concepts/${name}`]);
      await engine.addTakesBatch([{ page_id: pg.id, row_num: 1, claim: `${name} kept take`, kind: 'take', holder: 'self', weight: 0.8 }]);
      await atoms(engine, sourceId, name);
    }
    const [withdraw] = await engine.executeRaw<{ id: number }>('SELECT id FROM facts WHERE source_id=$1 ORDER BY id LIMIT 1', [sourceId]);
    await recordFactWithdrawal(engine, Number(withdraw.id), sourceId);
    const index = () => engine.executeRaw('SELECT * FROM facts WHERE source_id=$1 ORDER BY id', [sourceId]);
    const takesIndex = () => engine.executeRaw(`SELECT t.* FROM takes t JOIN pages p ON p.id=t.page_id
      WHERE p.source_id=$1 ORDER BY t.id`, [sourceId]);
    const withdrawals = () => engine.executeRaw('SELECT * FROM fact_withdrawals WHERE source_id=$1 ORDER BY fact_hash', [sourceId]);
    const [beforeIndex, beforeTakes, beforeWithdrawals] = await Promise.all([index(), takesIndex(), withdrawals()]);
    const before = await engine.executeRaw<{ slug: string; compiled_truth: string; timeline: string; revision: string }>(
      `SELECT slug,compiled_truth,timeline,knowledge_revision::text AS revision FROM pages WHERE source_id=$1 AND type='concept' ORDER BY slug`, [sourceId]);
    await engine.executeRaw('UPDATE persistence_brain SET enabled=true WHERE singleton=1');

    const { phases, result } = await runLane(engine, root);
    const concepts = phases.get('synthesize_concepts')!;
    expect(concepts.status).toBe('warn');
    expect((concepts.details.publication_held as Array<{ concept: string }>).map(h => h.concept).sort())
      .toEqual(Object.keys(AMBIGUOUS).sort());
    expect(concepts.details.failures).toEqual([]);
    expect(concepts.summary).not.toContain('LLM-failed');
    expect(result.partial).toBe(true);
    // Byte-identical pages, no admitted publication, facts unexpired, takes kept.
    const after = await engine.executeRaw<{ slug: string; compiled_truth: string; timeline: string; revision: string }>(
      `SELECT slug,compiled_truth,timeline,knowledge_revision::text AS revision FROM pages WHERE source_id=$1 AND type='concept' ORDER BY slug`, [sourceId]);
    expect(after).toEqual(before);
    expect(await engine.executeRaw("SELECT id FROM persistence_requests WHERE source_id=$1 AND slug LIKE 'concepts/%'", [sourceId])).toHaveLength(0);
    const expired = await engine.executeRaw(`SELECT id FROM facts WHERE source_id=$1 AND source_markdown_slug LIKE 'concepts/%' AND expired_at IS NOT NULL`, [sourceId]);
    expect(expired).toHaveLength(1);
    const takes = await engine.executeRaw(`SELECT t.id FROM takes t JOIN pages p ON p.id=t.page_id WHERE p.source_id=$1 AND p.type='concept'`, [sourceId]);
    expect(takes).toHaveLength(Object.keys(AMBIGUOUS).length);
    expect(await index()).toEqual(beforeIndex);
    expect(await takesIndex()).toEqual(beforeTakes);
    expect(await withdrawals()).toEqual(beforeWithdrawals);
    for (const [file, bytes] of files) expect(readFileSync(file).equals(bytes)).toBe(true);
    expect(await engine.getConfig(LAST_GLOBAL_AT_KEY)).toBeNull();
    expect(await engine.getConfig(LAST_GLOBAL_ATTEMPT_AT_KEY)).not.toBeNull();
    const options = { repoPath: root, slot: 'test', timeoutMs: 1000, jsonMode: true, emit() {} };
    expect(await dispatchGlobalMaintenance(engine, { add() { throw new Error('Backoff must not dispatch'); } } as never, options))
      .toEqual({ dispatched: false, reason: 'backoff' });
    await engine.setConfig(LAST_GLOBAL_ATTEMPT_AT_KEY, '2000-01-01T00:00:00Z');
    let dispatched = 0;
    expect(await dispatchGlobalMaintenance(engine, { async add() { dispatched++; return { id: 1 }; } } as never, options))
      .toEqual({ dispatched: true, reason: 'stale' });
    expect(dispatched).toBe(1);
    expect(await engine.getConfig(LAST_GLOBAL_AT_KEY)).toBeNull();
  });
}, 180_000);

test('a concept publication deferred by a concurrent writer withholds global freshness; the next run publishes it (F5)', async () => {
  await lane(async ({ engine, sourceId, root, put }) => {
    await put('concepts/raced', '---\ntitle: raced\ntype: concept\n---\nOld narrative.\n');
    const stale = (await engine.readPageSnapshot('concepts/raced', { sourceId }))!;
    // Revision B commits; the phase then publishes from a revision-A read
    // (the state a writer racing between the phase's read and admission leaves).
    // Admission and preparation read the live revision and must refuse.
    await put('concepts/raced', '---\ntitle: raced\ntype: concept\n---\nA concurrent edit.\n', stale.revision);
    await atoms(engine, sourceId, 'raced');
    await engine.executeRaw('UPDATE persistence_brain SET enabled=true WHERE singleton=1');
    const original = engine.readPageSnapshot;
    let reads = 0;
    engine.readPageSnapshot = async function (this: BrainEngine, slug, opts) {
      // `this`, not `engine`: transaction-scoped clones inherit this override.
      // Only the phase's own reads (made directly from synthesize-concepts.ts)
      // see revision A; the coordinator's admission/preparation reads are live.
      const caller = new Error().stack?.split('\n')[2] ?? '';
      if (slug === 'concepts/raced' && caller.includes('synthesize-concepts.ts')) { reads++; return stale; }
      return original.call(this, slug, opts);
    };
    let first;
    try { first = await runLane(engine, root); } finally { delete (engine as Partial<BrainEngine>).readPageSnapshot; }
    expect(reads).toBeGreaterThan(0);
    const concepts = first.phases.get('synthesize_concepts')!;
    expect(concepts.status).toBe('warn');
    expect((concepts.details.publication_deferred as Array<{ reason: string }>).map(d => d.reason.split(':')[0])).toEqual(['revision_conflict']);
    expect(concepts.details.failures).toEqual([]);
    expect(first.result.partial).toBe(true);
    expect(await engine.getConfig(LAST_GLOBAL_AT_KEY)).toBeNull();
    expect((await engine.readPageSnapshot('concepts/raced', { sourceId }))!.page.compiled_truth).toContain('A concurrent edit.');

    const second = await runLane(engine, root);
    expect(second.phases.get('synthesize_concepts')!.status).toBe('ok');
    expect(await engine.getConfig(LAST_GLOBAL_AT_KEY)).not.toBeNull();
    expect((await engine.readPageSnapshot('concepts/raced', { sourceId }))!.page.frontmatter.synthesized_by).toBe('synthesize_concepts-v0.41');
  });
}, 180_000);

test('an operational managed purge failure fails the purge phase and withholds global freshness (F5)', async () => {
  await lane(async ({ engine, sourceId, root, dir }) => {
    // A second managed source whose canonical owner is unavailable on this
    // host: its expired tombstone cannot be purged through the coordinator.
    const orphanRoot = join(dir, 'orphan'); mkdirSync(orphanRoot);
    const orphan = `${sourceId}-orphan`;
    await engine.executeRaw('INSERT INTO sources(id,name,local_path) VALUES($1,$1,$2)', [orphan, orphanRoot]);
    // Bound to a canonical worktree whose host binding is gone from this host.
    await claimWorktree(engine, orphan, orphanRoot);
    await engine.executeRaw(`DELETE FROM persistence_host_bindings WHERE worktree_id IN
      (SELECT worktree_id FROM persistence_source_bindings WHERE source_id=$1)`, [orphan]);
    await engine.executeRaw(`INSERT INTO pages(source_id,slug,type,title,compiled_truth,deleted_at)
      VALUES($1,'notes/expired','note','Expired','Gone.',now()-interval '80 hours'),($2,'notes/expired','note','Expired','Gone.',now()-interval '80 hours')`, [sourceId, orphan]);
    await engine.executeRaw('UPDATE persistence_brain SET enabled=true WHERE singleton=1');
    try {
      const { result, phases } = await runLane(engine, root);
      const purge = phases.get('purge')!;
      expect(purge.status).toBe('fail');
      expect(purge.details.purged_pages_failed).toBe(1);
      expect(purge.details.purged_page_slugs).toEqual(['notes/expired']); // the healthy source still purged
      expect(purge.error?.message).toContain(orphan);
      expect(result.partial).toBe(true);
      expect(await engine.getConfig(LAST_GLOBAL_AT_KEY)).toBeNull();
      // Never forced: the orphan's tombstone is still there.
      expect(await engine.executeRaw('SELECT id FROM pages WHERE source_id=$1', [orphan])).toHaveLength(1);
    } finally {
      await disposePersistenceConsumer(engine);
      await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
      await engine.executeRaw('DELETE FROM pages WHERE source_id=$1', [orphan]);
      await engine.executeRaw('DELETE FROM persistence_source_bindings WHERE source_id=$1', [orphan]);
      await engine.executeRaw('DELETE FROM sources WHERE id=$1', [orphan]);
    }
  });
}, 180_000);
