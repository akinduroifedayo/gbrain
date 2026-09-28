/**
 * Managed extract_facts: the fence -> facts index reconcile on a managed brain.
 *
 * Pre-fix the phase threw `legacy fact-fence reconciliation cannot mutate a
 * managed brain` on every source every tick. It must now re-project drifted
 * pages through committed coordinator receipts (DB-only: canonical files are
 * byte-identical afterwards), leave in-sync pages unadmitted, and preserve —
 * never guess — pages whose fence is absent, unparseable, or shares its row
 * keyspace with conversation facts.
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { BrainEngine } from '../src/core/engine.ts';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { runExtractFacts } from '../src/core/cycle/extract-facts.ts';
import { claimWorktree } from '../src/core/persistence/ownership.ts';
import { submitPageMutation } from '../src/core/persistence/page-mutations.ts';
import { disposePersistenceConsumer } from '../src/core/persistence/service.ts';
import { configureGateway, resetGateway } from '../src/core/ai/gateway.ts';
import { isolatedPersistencePostgres } from './helpers/persistence-postgres.ts';
import { withEnv } from './helpers/with-env.ts';
import { testBackends } from './helpers/test-backends.ts';

const backends = testBackends();
const engines: BrainEngine[] = [];
const dataDir = mkdtempSync(join(tmpdir(), 'gbrain-managed-facts-db-'));
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

const HEADER = ['| # | claim | kind | confidence | visibility | notability | valid_from | valid_until | source | context |',
  '|---|-------|------|------------|------------|------------|------------|-------------|--------|---------|'];
function page(title: string, rows: string[]): string {
  return ['---', `title: ${title}`, 'type: person', '---', `${title} profile.`, '',
    '## Facts', '', '<!--- gbrain:facts:begin -->', '', ...HEADER, ...rows, '<!--- gbrain:facts:end -->', ''].join('\n');
}
const row = (n: number, claim: string, visibility = 'world') =>
  `| ${n} | ${claim} | fact | 1.0 | ${visibility} | medium | 2026-09-01 |  | fence |  |`;

interface Fact { row_num: number | null; fact: string; visibility: string; expired: boolean }
async function facts(engine: BrainEngine, sourceId: string, slug: string): Promise<Fact[]> {
  const rows = await engine.executeRaw<Fact>(`SELECT row_num,fact,visibility,expired_at IS NOT NULL AS expired FROM facts
    WHERE source_id=$1 AND source_markdown_slug=$2 ORDER BY row_num NULLS LAST, fact`, [sourceId, slug]);
  return rows.map(r => ({ ...r, row_num: r.row_num === null ? null : Number(r.row_num) }));
}

test('managed extract_facts re-projects drifted fences through committed DB-only receipts and preserves the rest', async () => {
  for (const engine of engines) {
    const dir = mkdtempSync(join(tmpdir(), 'gbrain-managed-facts-'));
    const root = join(dir, 'brain'); mkdirSync(root);
    const sourceId = `facts-${randomUUID().slice(0, 8)}`;
    try {
      await withEnv({ GBRAIN_HOME: join(dir, 'home') }, async () => {
        // Seed while unmanaged, exactly like a brain that predates managed mode.
        await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
        await engine.executeRaw('INSERT INTO sources(id,name,local_path) VALUES($1,$1,$2)', [sourceId, root]);
        await engine.setConfig('sync.write_through', 'true');
        await claimWorktree(engine, sourceId, root);
        const ctx = { engine, sourceId, remote: false as const, config: { engine: engine.kind, embedding_disabled: true },
          dryRun: false, logger: { info() {}, warn() {}, error() {} } };
        const put = (slug: string, content: string) => submitPageMutation(ctx, { operation: 'put_page',
          params: { slug, content, request_id: randomUUID() } });
        await put('people/drift', page('Drift', [row(1, 'Alpha holds'), row(2, 'Beta holds'), row(3, 'Gamma holds', 'private')]));
        await put('people/steady', page('Steady', [row(1, 'Steady holds')]));
        await put('people/nofence', '---\ntitle: No fence\ntype: person\n---\nNo fence here.\n');
        await put('people/broken', page('Broken', [row(1, 'Broken holds')]));
        await put('people/talk', page('Talk', [row(1, 'Talk holds')]));

        // Drift the index behind the fences (fixture writes while unmanaged).
        await engine.executeRaw("DELETE FROM facts WHERE source_id=$1 AND source_markdown_slug='people/drift' AND row_num=2", [sourceId]);
        await engine.executeRaw("UPDATE facts SET fact='Gamma stale', visibility='world' WHERE source_id=$1 AND source_markdown_slug='people/drift' AND row_num=3", [sourceId]);
        await engine.executeRaw(`INSERT INTO facts(source_id,entity_slug,fact,kind,visibility,notability,source,row_num,source_markdown_slug)
          VALUES($1,'people/nofence','Indexed without a fence','fact','world','medium','fence',1,'people/nofence')`, [sourceId]);
        await engine.executeRaw(`INSERT INTO facts(source_id,entity_slug,fact,kind,visibility,notability,source,row_num,source_markdown_slug)
          VALUES($1,'people/talk','Said in conversation','fact','world','medium','cli:extract-conversation-facts',7,'people/talk')`, [sourceId]);
        // A malformed canonical body (cache and file agree) — the parse is not authoritative.
        const brokenBody = page('Broken', [row(1, 'Broken holds'), '| 2 | only two cells |']).split('---\n').slice(2).join('---\n');
        await engine.executeRaw("UPDATE pages SET compiled_truth=$2 WHERE source_id=$1 AND slug='people/broken'", [sourceId, brokenBody.trim()]);
        await engine.executeRaw("DELETE FROM facts WHERE source_id=$1 AND source_markdown_slug='people/broken'", [sourceId]);
        const driftFile = readFileSync(join(root, 'people/drift.md'), 'utf8');
        await engine.executeRaw('UPDATE persistence_brain SET enabled=true WHERE singleton=1');

        const first = await runExtractFacts(engine, { sourceId, brainDir: root });
        expect(first.guardTriggered).toBe(false);
        expect(first.managed).toMatchObject({ pagesReconciled: 1, pagesInSync: 1 }); // steady only
        expect(first.warnings.some(w => w.startsWith('people/broken:'))).toBe(true);
        expect(first.managed!.pagesPreserved).toBe(2); // broken (parse) + talk (conversation keyspace)
        expect(first.managed!.pagesFenceAbsent).toBe(1); // nofence: rows kept, never read as a withdrawal
        expect(first.warnings.some(w => w.includes('people/talk') && w.includes('FACTS_CONVERSATION_COLLISION'))).toBe(true);

        // The drifted page now matches its fence; the replaced row is expired and detached, not deleted.
        expect(await facts(engine, sourceId, 'people/drift')).toEqual([
          { row_num: 1, fact: 'Alpha holds', visibility: 'world', expired: false },
          { row_num: 2, fact: 'Beta holds', visibility: 'world', expired: false },
          { row_num: 3, fact: 'Gamma holds', visibility: 'private', expired: false },
          { row_num: null, fact: 'Gamma stale', visibility: 'world', expired: true },
        ]);
        // Preserved pages are untouched.
        expect(await facts(engine, sourceId, 'people/nofence')).toEqual([{ row_num: 1, fact: 'Indexed without a fence', visibility: 'world', expired: false }]);
        expect(await facts(engine, sourceId, 'people/broken')).toEqual([]);
        expect((await facts(engine, sourceId, 'people/talk')).map(f => f.fact).sort()).toEqual(['Said in conversation', 'Talk holds']);
        // Exactly one committed, DB-only coordinator receipt; the canonical file is byte-identical.
        const receipts = await engine.executeRaw<{ slug: string; state: string; worktree_id: string | null }>(
          `SELECT slug,state,worktree_id FROM persistence_requests WHERE source_id=$1 AND operation='submit_job'
            AND intent->>'kind'='managed_maintenance_facts'`, [sourceId]);
        expect(receipts.map(r => [r.slug, r.state, r.worktree_id])).toEqual([['people/drift', 'committed', null]]);
        expect(readFileSync(join(root, 'people/drift.md'), 'utf8')).toBe(driftFile);

        // Steady state: nothing admitted, nothing changed.
        const second = await runExtractFacts(engine, { sourceId, brainDir: root });
        expect(second.managed).toMatchObject({ pagesReconciled: 0, pagesInSync: 2, pagesPreserved: 2 });
        const after = await engine.executeRaw<{ n: number }>(`SELECT count(*)::int AS n FROM persistence_requests
          WHERE source_id=$1 AND intent->>'kind'='managed_maintenance_facts'`, [sourceId]);
        expect(Number(after[0].n)).toBe(1);

        // Dry run never admits work.
        await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
        await engine.executeRaw("DELETE FROM facts WHERE source_id=$1 AND source_markdown_slug='people/steady'", [sourceId]);
        await engine.executeRaw('UPDATE persistence_brain SET enabled=true WHERE singleton=1');
        const dry = await runExtractFacts(engine, { sourceId, brainDir: root, dryRun: true });
        expect(dry.managed).toMatchObject({ pagesReconciled: 1 });
        expect(await facts(engine, sourceId, 'people/steady')).toEqual([]);
      });
    } finally {
      await disposePersistenceConsumer(engine);
      await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
      rmSync(dir, { recursive: true, force: true });
    }
  }
}, 180_000);
