/**
 * Managed extract_facts — retry identity (F3) and complete convergence (F4).
 *
 * The managed reconcile only changes the derived facts index, never the page
 * revision, so:
 *  - a page whose index drifts again after a committed reconcile, with no page
 *    edit, must be repaired again (not answered with the old receipt), and the
 *    run's counters must describe work this run did;
 *  - a reconcile that failed terminally must be retryable once its cause is
 *    gone;
 *  - "in sync" must cover every column the projection owns (confidence,
 *    validity, supersession), existing rows keep their ids, and a converged
 *    page is never re-admitted.
 * Withdrawal, privacy and duplicate-claim cases pin the preservation policy:
 * the fence is the system of record for fence-owned rows; a replaced row is
 * expired and detached, never deleted; a withdrawn fingerprint stays expired.
 */
import { afterAll, beforeAll, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { BrainEngine } from '../src/core/engine.ts';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { runExtractFacts } from '../src/core/cycle/extract-facts.ts';
import { factsInSync, type IndexedFact } from '../src/core/cycle/extract-facts-managed.ts';
import { claimWorktree } from '../src/core/persistence/ownership.ts';
import { submitPageMutation } from '../src/core/persistence/page-mutations.ts';
import { disposePersistenceConsumer } from '../src/core/persistence/service.ts';
import { recordFactWithdrawal } from '../src/core/facts/withdrawal.ts';
import { configureGateway, resetGateway } from '../src/core/ai/gateway.ts';
import { isolatedPersistencePostgres } from './helpers/persistence-postgres.ts';
import { withEnv } from './helpers/with-env.ts';
import { testBackends } from './helpers/test-backends.ts';

const backends = testBackends();
const engines: BrainEngine[] = [];
const dataDir = mkdtempSync(join(tmpdir(), 'gbrain-facts-converge-db-'));
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
const page = (title: string, rows: string[]) => ['---', `title: ${title}`, 'type: person', '---', `${title} profile.`, '',
  '## Facts', '', '<!--- gbrain:facts:begin -->', '', ...HEADER, ...rows, '<!--- gbrain:facts:end -->', ''].join('\n');
const row = (n: number, claim: string, o: { visibility?: string; confidence?: string; from?: string; until?: string; context?: string } = {}) =>
  `| ${n} | ${claim} | fact | ${o.confidence ?? '1.0'} | ${o.visibility ?? 'world'} | medium | ${o.from ?? '2026-09-01'} | ${o.until ?? ''} | fence | ${o.context ?? ''} |`;

interface Fact { id: number; row_num: number | null; fact: string; visibility: string; expired: boolean; confidence: number;
  valid_from: string | null; superseded_by: number | null }
async function facts(engine: BrainEngine, sourceId: string, slug: string): Promise<Fact[]> {
  const rows = await engine.executeRaw<Fact>(`SELECT id,row_num,fact,visibility,expired_at IS NOT NULL AS expired,confidence,
      to_char(valid_from AT TIME ZONE 'UTC','YYYY-MM-DD') AS valid_from,superseded_by
    FROM facts WHERE source_id=$1 AND source_markdown_slug=$2 ORDER BY row_num NULLS LAST, fact, id`, [sourceId, slug]);
  return rows.map(r => ({ ...r, id: Number(r.id), row_num: r.row_num === null ? null : Number(r.row_num), confidence: Number(r.confidence),
    superseded_by: r.superseded_by === null ? null : Number(r.superseded_by) }));
}
async function receipts(engine: BrainEngine, sourceId: string, slug: string): Promise<string[]> {
  return (await engine.executeRaw<{ state: string }>(`SELECT state FROM persistence_requests WHERE source_id=$1 AND slug=$2
    AND intent->>'kind'='managed_maintenance_facts' ORDER BY sequence`, [sourceId, slug])).map(r => r.state);
}

/** Seed pages while unmanaged (a brain that predates managed mode), then run `body` managed. */
async function brain(seed: Record<string, string>, body: (ctx: { engine: BrainEngine; sourceId: string; root: string;
  unmanaged: (fn: () => Promise<unknown>) => Promise<void>; run: () => ReturnType<typeof runExtractFacts> }) => Promise<void>) {
  for (const engine of engines) {
    const dir = mkdtempSync(join(tmpdir(), 'gbrain-facts-converge-'));
    const root = join(dir, 'brain'); mkdirSync(root);
    const sourceId = `conv-${randomUUID().slice(0, 8)}`;
    try {
      await withEnv({ GBRAIN_HOME: join(dir, 'home') }, async () => {
        await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
        await engine.executeRaw('INSERT INTO sources(id,name,local_path) VALUES($1,$1,$2)', [sourceId, root]);
        await engine.setConfig('sync.write_through', 'true');
        await claimWorktree(engine, sourceId, root);
        const ctx = { engine, sourceId, remote: false as const, config: { engine: engine.kind, embedding_disabled: true },
          dryRun: false, logger: { info() {}, warn() {}, error() {} } };
        for (const [slug, content] of Object.entries(seed)) {
          await submitPageMutation(ctx, { operation: 'put_page', params: { slug, content, request_id: randomUUID() } });
        }
        const unmanaged = async (fn: () => Promise<unknown>) => {
          await disposePersistenceConsumer(engine);
          await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
          try { await fn(); } finally { await engine.executeRaw('UPDATE persistence_brain SET enabled=true WHERE singleton=1'); }
        };
        await engine.executeRaw('UPDATE persistence_brain SET enabled=true WHERE singleton=1');
        await body({ engine, sourceId, root, unmanaged, run: () => runExtractFacts(engine, { sourceId, brainDir: root }) });
      });
    } finally {
      await disposePersistenceConsumer(engine);
      await engine.executeRaw('UPDATE persistence_brain SET enabled=false WHERE singleton=1');
      rmSync(dir, { recursive: true, force: true });
    }
  }
}

test('re-drift after a committed reconcile, with no page edit, is repaired again and counted once per run (F3)', async () => {
  await brain({ 'people/redrift': page('Redrift', [row(1, 'Alpha holds'), row(2, 'Beta holds')]) }, async ({ engine, sourceId, unmanaged, run }) => {
    const drop = () => unmanaged(() => engine.executeRaw("DELETE FROM facts WHERE source_id=$1 AND source_markdown_slug='people/redrift' AND row_num=2", [sourceId]));
    const revision = (await engine.readPageSnapshot('people/redrift', { sourceId }))!.revision;
    await drop();
    const first = await run();
    expect(first.managed).toMatchObject({ pagesReconciled: 1 });
    expect(first.factsInserted).toBe(1);
    expect((await facts(engine, sourceId, 'people/redrift')).map(f => [f.row_num, f.fact, f.expired])).toEqual([[1, 'Alpha holds', false], [2, 'Beta holds', false]]);

    await drop(); // the derived index drifts again; the page (and its revision) does not change
    expect((await engine.readPageSnapshot('people/redrift', { sourceId }))!.revision).toBe(revision);
    const second = await run();
    expect(second.managed).toMatchObject({ pagesReconciled: 1, pagesInSync: 0 });
    expect(second.factsInserted).toBe(1);
    expect((await facts(engine, sourceId, 'people/redrift')).map(f => [f.row_num, f.fact, f.expired])).toEqual([[1, 'Alpha holds', false], [2, 'Beta holds', false]]);
    expect(await receipts(engine, sourceId, 'people/redrift')).toEqual(['committed', 'committed']);

    const steady = await run();
    expect(steady.managed).toMatchObject({ pagesReconciled: 0, pagesInSync: 1 });
    expect(steady.factsInserted).toBe(0);
    expect(await receipts(engine, sourceId, 'people/redrift')).toHaveLength(2);
  });
}, 180_000);

test('a terminally failed reconcile is retried once its cause is removed (F3)', async () => {
  await brain({ 'people/retry': page('Retry', [row(1, 'Alpha holds'), row(2, 'Beta holds')]) }, async ({ engine, sourceId, unmanaged, run }) => {
    await unmanaged(() => engine.executeRaw("DELETE FROM facts WHERE source_id=$1 AND source_markdown_slug='people/retry' AND row_num=2", [sourceId]));
    // A storage fault that makes the reconcile's publication fail.
    await engine.executeRaw(`CREATE OR REPLACE FUNCTION gbrain_test_refuse_fact() RETURNS trigger LANGUAGE plpgsql AS $fn$
      BEGIN RAISE EXCEPTION 'test storage fault'; END $fn$`);
    await engine.executeRaw(`CREATE TRIGGER gbrain_test_refuse_fact BEFORE INSERT ON facts FOR EACH ROW
      WHEN (NEW.source_markdown_slug = 'people/retry') EXECUTE FUNCTION gbrain_test_refuse_fact()`);
    let failed: unknown;
    try {
      await run();
    } catch (error) {
      failed = error;
    } finally {
      await engine.executeRaw('DROP TRIGGER IF EXISTS gbrain_test_refuse_fact ON facts');
      await engine.executeRaw('DROP FUNCTION IF EXISTS gbrain_test_refuse_fact()');
    }
    expect(failed).toBeDefined(); // the run reports the failure; it is not counted as a reconcile
    const states = await receipts(engine, sourceId, 'people/retry');
    expect(states).toHaveLength(1);
    expect(['failed', 'conflict', 'cancelled']).toContain(states[0]);
    expect((await facts(engine, sourceId, 'people/retry')).map(f => f.row_num)).toEqual([1]);

    const retried = await run();
    expect(retried.managed).toMatchObject({ pagesReconciled: 1 });
    expect(retried.factsInserted).toBe(1);
    expect((await facts(engine, sourceId, 'people/retry')).map(f => [f.row_num, f.fact])).toEqual([[1, 'Alpha holds'], [2, 'Beta holds']]);
    expect((await receipts(engine, sourceId, 'people/retry')).slice(1)).toEqual(['committed']);
  });
}, 180_000);

test('confidence, validity and supersession drift on existing rows converge with stable ids and no churn (F4)', async () => {
  const content = page('Converge', [
    row(1, '~~Old plan~~', { context: 'superseded by #2' }),
    row(2, 'New plan', { confidence: '0.7', from: '2026-08-15' }),
    row(3, 'Bounded fact', { until: '2027-01-01' }),
  ]);
  await brain({ 'people/converge': content }, async ({ engine, sourceId, unmanaged, run }) => {
    const before = await facts(engine, sourceId, 'people/converge');
    const ids = before.map(f => f.id);
    expect(before[0].superseded_by).toBe(before[1].id);
    // Drift only columns the old in-sync predicate ignored.
    await unmanaged(async () => {
      await engine.executeRaw(`UPDATE facts SET superseded_by=NULL WHERE id=$1`, [ids[0]]);
      await engine.executeRaw(`UPDATE facts SET confidence=0.2, valid_from='2020-01-01' WHERE id=$1`, [ids[1]]);
      await engine.executeRaw(`UPDATE facts SET valid_until=NULL WHERE id=$1`, [ids[2]]);
    });
    const first = await run();
    expect(first.managed).toMatchObject({ pagesReconciled: 1 });
    const after = await facts(engine, sourceId, 'people/converge');
    expect(after.map(f => f.id)).toEqual(ids); // same identities, updated in place
    expect(after[0]).toMatchObject({ expired: true, superseded_by: ids[1] });
    expect(after[1].confidence).toBeCloseTo(0.7, 5);
    expect(after[1].valid_from).toBe('2026-08-15');
    const [until] = await engine.executeRaw<{ d: string }>(`SELECT to_char(valid_until AT TIME ZONE 'UTC','YYYY-MM-DD') AS d FROM facts WHERE id=$1`, [ids[2]]);
    expect(until.d).toBe('2027-01-01');
    // Converged: the next run admits nothing.
    const steady = await run();
    expect(steady.managed).toMatchObject({ pagesReconciled: 0, pagesInSync: 1 });
    expect(await receipts(engine, sourceId, 'people/converge')).toEqual(['committed']);
  });
}, 180_000);

test('withdrawal, privacy and repeated claims follow the fence without resurrecting withdrawn facts (F4)', async () => {
  await brain({
    'people/withdrawn': page('Withdrawn', [row(1, 'Withdrawn claim'), row(2, 'Kept claim')]),
    'people/privacy': page('Privacy', [row(1, 'Shared claim', { visibility: 'world' }), row(2, 'Shared claim', { visibility: 'private' })]),
    'people/repeat': page('Repeat', [row(1, 'Same fact twice'), row(2, 'Other fact'), row(3, 'Same fact twice')]),
  }, async ({ engine, sourceId, unmanaged, run }) => {
    // Withdrawn fact whose index row is later lost: re-projection re-inserts it expired.
    const [target] = await facts(engine, sourceId, 'people/withdrawn');
    await unmanaged(() => recordFactWithdrawal(engine, target.id, sourceId));
    await unmanaged(() => engine.executeRaw("DELETE FROM facts WHERE source_id=$1 AND source_markdown_slug='people/withdrawn'", [sourceId]));
    // Privacy disagreement: the index says private where the fence says world.
    const privacyIds = (await facts(engine, sourceId, 'people/privacy')).map(f => f.id);
    await unmanaged(() => engine.executeRaw("UPDATE facts SET visibility='private' WHERE id=$1", [privacyIds[0]]));
    // Repeated claim at distinct rows (the prior duplicate-identity planner case): drift one copy.
    await unmanaged(() => engine.executeRaw("DELETE FROM facts WHERE source_id=$1 AND source_markdown_slug='people/repeat' AND row_num=3", [sourceId]));

    const first = await run();
    expect(first.managed!.pagesReconciled).toBe(3);
    expect((await facts(engine, sourceId, 'people/withdrawn')).map(f => [f.row_num, f.fact, f.expired])).toEqual([
      [1, 'Withdrawn claim', true], [2, 'Kept claim', false]]);
    // Fence visibility wins; the disagreeing row is expired and detached, never deleted.
    const privacy = await facts(engine, sourceId, 'people/privacy');
    expect(privacy.map(f => [f.row_num, f.visibility, f.expired])).toEqual([[1, 'world', false], [2, 'private', false], [null, 'private', true]]);
    expect(privacy.find(f => f.row_num === null)!.id).toBe(privacyIds[0]);
    expect(privacy.find(f => f.row_num === 2)!.id).toBe(privacyIds[1]);
    // Both copies of the repeated claim are indexed at their own row numbers.
    expect((await facts(engine, sourceId, 'people/repeat')).map(f => [f.row_num, f.fact, f.expired])).toEqual([
      [1, 'Same fact twice', false], [2, 'Other fact', false], [3, 'Same fact twice', false]]);

    const steady = await run();
    expect(steady.managed).toMatchObject({ pagesReconciled: 0, pagesInSync: 3 });
  });
}, 180_000);

test('the in-sync predicate compares typed claims and tolerates REAL confidence rounding (F4)', () => {
  const base: IndexedFact = { id: 1, slug: 'p', row_num: 1, fact: 'MRR is 50K', visibility: 'world', notability: 'medium', kind: 'fact',
    context: null, source: 'fence', expired: false, withdrawn: false, confidence: 0.699999988079071, valid_from: '2026-09-01T00:00:00Z',
    valid_until: null, superseded_by: null, claim_metric: 'mrr', claim_value: '50000', claim_unit: 'USD', claim_period: 'monthly' };
  const want = { fact: 'MRR is 50K', kind: 'fact' as const, entity_slug: 'p', visibility: 'world' as const, notability: 'medium' as const,
    context: null, valid_from: new Date('2026-09-01T00:00:00Z'), valid_until: null, expired_at: null, source: 'fence', confidence: 0.7,
    row_num: 1, source_markdown_slug: 'p', claim_metric: 'mrr', claim_value: 50000, claim_unit: 'USD', claim_period: 'monthly' };
  expect(factsInSync([want], [base])).toBe(true);
  expect(factsInSync([want], [{ ...base, claim_value: '60000' }])).toBe(false);
  expect(factsInSync([want], [{ ...base, claim_unit: null }])).toBe(false);
  expect(factsInSync([{ ...want, claim_metric: null }], [base])).toBe(false);
});
