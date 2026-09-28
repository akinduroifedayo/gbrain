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
import { factsInSync, observedIndexDigest, type IndexedFact } from '../src/core/cycle/extract-facts-managed.ts';
import { maintenancePreflight, submitMaintenanceFactsReconcile } from '../src/core/persistence/prepared-maintenance.ts';
import { submitForgetMutation } from '../src/core/persistence/memory-mutations.ts';
import { withCoordinatedWrite } from '../src/core/persistence/context.ts';
import { prepareCanonicalFactsProjection } from '../src/core/persistence/canonical-projections.ts';
import { claimWorktree } from '../src/core/persistence/ownership.ts';
import { submitPageMutation } from '../src/core/persistence/page-mutations.ts';
import { disposePersistenceConsumer } from '../src/core/persistence/service.ts';
import { runPersistenceEffects } from '../src/core/persistence/effects.ts';
import { localHostId } from '../src/core/persistence/identity.ts';
import { waitFor } from './helpers/wait-for.ts';
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

    // Successful episodes do not consume a lifetime failure allowance.
    for (let episode = 3; episode <= 5; episode++) {
      await drop();
      const repaired = await run();
      expect(repaired.managed).toMatchObject({ pagesReconciled: 1 });
      expect(repaired.factsInserted).toBe(1);
    }
    const steady = await run();
    expect(steady.managed).toMatchObject({ pagesReconciled: 0, pagesInSync: 1 });
    expect(steady.factsInserted).toBe(0);
    expect(await receipts(engine, sourceId, 'people/redrift')).toHaveLength(5);
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
      for (let attempt = 0; attempt < 3; attempt++) {
        try { await run(); } catch (error) { failed = error; }
      }
    } catch (error) {
      failed = error;
    } finally {
      await engine.executeRaw('DROP TRIGGER IF EXISTS gbrain_test_refuse_fact ON facts');
      await engine.executeRaw('DROP FUNCTION IF EXISTS gbrain_test_refuse_fact()');
    }
    expect(failed).toBeDefined(); // the run reports the failure; it is not counted as a reconcile
    const states = await receipts(engine, sourceId, 'people/retry');
    expect(states).toHaveLength(3);
    expect(['failed', 'conflict', 'cancelled']).toContain(states[0]);
    expect((await facts(engine, sourceId, 'people/retry')).map(f => f.row_num)).toEqual([1]);

    // A tight fourth retry must be held, but expiry of backoff allows recovery.
    const held = await run();
    expect(held.managed!.pagesReconciled).toBe(0);
    expect(await receipts(engine, sourceId, 'people/retry')).toHaveLength(3);
    await unmanaged(() => engine.executeRaw(`UPDATE persistence_requests SET completed_at=now()-interval '1 hour'
      WHERE source_id=$1 AND intent->>'kind'='managed_maintenance_facts'`, [sourceId]));
    const retried = await run();
    expect(retried.managed).toMatchObject({ pagesReconciled: 1 });
    expect(retried.factsInserted).toBe(1);
    expect((await facts(engine, sourceId, 'people/retry')).map(f => [f.row_num, f.fact])).toEqual([[1, 'Alpha holds'], [2, 'Beta holds']]);
    expect((await receipts(engine, sourceId, 'people/retry')).slice(3)).toEqual(['committed']);
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

test('struck authored validity converges exactly, including null, with stable ids (R3)', async () => {
  await brain({ 'people/struck': page('Struck', [
    row(1, '~~Dated old~~', { until: '2026-09-01', context: 'superseded by #3' }),
    row(2, '~~Undated old~~', { context: 'superseded by #3' }),
    row(3, 'Current'),
  ]) }, async ({ engine, sourceId, unmanaged, run }) => {
    const ids = (await facts(engine, sourceId, 'people/struck')).map(f => f.id);
    for (const wrong of ['2020-01-01', null]) {
      await unmanaged(async () => {
        await engine.executeRaw('UPDATE facts SET valid_until=$2::timestamptz WHERE id=$1', [ids[0], wrong]);
        await engine.executeRaw("UPDATE facts SET valid_until='2020-01-01' WHERE id=$1", [ids[1]]);
      });
      expect((await run()).managed!.pagesReconciled).toBe(1);
      const rows = await engine.executeRaw<{ id: number; until: string | null }>(`SELECT id,
        to_char(valid_until AT TIME ZONE 'UTC','YYYY-MM-DD') AS until FROM facts
        WHERE source_id=$1 AND source_markdown_slug='people/struck' ORDER BY row_num`, [sourceId]);
      expect(rows.map(r => Number(r.id))).toEqual(ids);
      expect(rows.map(r => r.until)).toEqual(['2026-09-01', null, null]);
      expect((await run()).managed).toMatchObject({ pagesReconciled: 0, pagesInSync: 1 });
    }
  });
}, 180_000);

test('concurrent observed drift is admitted once; stale observers consume no requests (R2)', async () => {
  await brain({ 'people/race': page('Race', [row(1, 'Only fact')]) }, async ({ engine, sourceId, root, unmanaged }) => {
    await unmanaged(() => engine.executeRaw("DELETE FROM facts WHERE source_id=$1 AND source_markdown_slug='people/race'", [sourceId]));
    const authority = (await maintenancePreflight(engine, sourceId, root))!;
    const revision = (await engine.readPageSnapshot('people/race', { sourceId }))!.revision;
    const submit = () => submitMaintenanceFactsReconcile(engine, authority, 'people/race', revision, observedIndexDigest([]));
    const results = await Promise.all([submit(), submit()]);
    expect(results.filter(r => r.kind === 'admitted')).toHaveLength(1);
    expect(await receipts(engine, sourceId, 'people/race')).toEqual(['committed']);
    // The stale digest must not consume another attempt after completion.
    for (let i = 0; i < 4; i++) expect((await submit()).kind).not.toBe('admitted');
    expect(await receipts(engine, sourceId, 'people/race')).toEqual(['committed']);
  });
}, 180_000);

test('generated expiry and withdrawal caps survive repair of another row (R3)', async () => {
  await brain({ 'people/expiry': page('Expiry', [
    row(1, '~~Generated end~~', { context: 'forgotten: old' }),
    row(2, 'Withdraw me', { until: '2030-01-01' }), row(3, 'Drift me'),
  ]) }, async ({ engine, sourceId, unmanaged, run }) => {
    const ids = (await facts(engine, sourceId, 'people/expiry')).map(f => f.id);
    await unmanaged(async () => {
      await engine.executeRaw("UPDATE facts SET valid_until='2020-01-01',expired_at='2020-01-01' WHERE id=$1", [ids[0]]);
      await recordFactWithdrawal(engine, ids[1], sourceId);
      await engine.executeRaw('UPDATE facts SET confidence=0.2 WHERE id=$1', [ids[2]]);
    });
    const dates = () => engine.executeRaw(`SELECT id,valid_until,expired_at FROM facts WHERE id=ANY($1::int[]) ORDER BY id`, [ids.slice(0, 2)]);
    const before = await dates();
    expect((await run()).managed!.pagesReconciled).toBe(1);
    expect(await dates()).toEqual(before);
    expect((await facts(engine, sourceId, 'people/expiry')).map(f => f.id)).toEqual(ids);
    expect((await run()).managed).toMatchObject({ pagesReconciled: 0, pagesInSync: 1 });
  });
}, 180_000);

test('two discovery readers count one actual repair, not the shared receipt (R2)', async () => {
  await brain({ 'people/counters': page('Counters', [row(1, 'Lost')]) }, async ({ engine, sourceId, unmanaged, run }) => {
    await unmanaged(() => engine.executeRaw('DELETE FROM facts WHERE source_id=$1', [sourceId]));
    const original = engine.executeRaw;
    let readers = 0;
    let release!: () => void;
    const bothRead = new Promise<void>(resolve => { release = resolve; });
    engine.executeRaw = async function (this: BrainEngine, sql, params, opts) {
      const result = await original.call(this, sql, params, opts);
      // Gate only the discovery query, never transaction-scoped revalidation.
      if (this === engine && sql.includes('f.source_markdown_slug AS slug')) {
        if (++readers === 2) release();
        await bothRead;
      }
      return result as never;
    };
    let results;
    try { results = await Promise.all([run(), run()]); }
    finally { release(); delete (engine as Partial<BrainEngine>).executeRaw; }
    expect(readers).toBe(2);
    expect(results.reduce((n, r) => n + r.managed!.pagesReconciled, 0)).toBe(1);
    expect(results.reduce((n, r) => n + r.factsInserted, 0)).toBe(1);
    expect(await receipts(engine, sourceId, 'people/counters')).toEqual(['committed']);
  });
}, 180_000);

test('withdrawal winning after repair preparation keeps its ledger, ids and expiry (R2)', async () => {
  await brain({ 'people/late-withdrawal': page('Late withdrawal', [row(1, 'Withdraw while preparing'), row(2, 'Missing')]) },
    async ({ engine, sourceId, unmanaged, run }) => {
      const [target] = await facts(engine, sourceId, 'people/late-withdrawal');
      await unmanaged(() => engine.executeRaw('DELETE FROM facts WHERE source_id=$1 AND row_num=2', [sourceId]));
      const original = engine.readPageSnapshot;
      let reached!: () => void, release!: () => void;
      const atPrepare = new Promise<void>(resolve => { reached = resolve; });
      const resume = new Promise<void>(resolve => { release = resolve; });
      let paused = false;
      engine.readPageSnapshot = async function (this: BrainEngine, slug, opts) {
        const snapshot = await original.call(this, slug, opts);
        if (!paused && slug === 'people/late-withdrawal' && new Error().stack?.includes('prepareFactsReconcile')) {
          paused = true; reached(); await resume;
        }
        return snapshot;
      };
      const repairing = run();
      try {
        await atPrepare;
        await submitForgetMutation({ engine, sourceId, remote: false, config: { engine: engine.kind, embedding_disabled: true },
          dryRun: false, logger: { info() {}, warn() {}, error() {} } }, 'forget', { id: String(target.id), request_id: randomUUID() });
      } finally { release(); }
      let result;
      try { result = await repairing; } finally { delete (engine as Partial<BrainEngine>).readPageSnapshot; }
      expect(paused).toBe(true);
      expect(result.managed!.pagesReconciled).toBe(0);
      expect((await facts(engine, sourceId, 'people/late-withdrawal'))[0]).toMatchObject({ id: target.id, expired: true });
      expect(await engine.executeRaw('SELECT * FROM fact_withdrawals WHERE source_id=$1', [sourceId])).toHaveLength(1);
      // Forget commits before its asynchronous file/cache mirror. A scan during
      // that handoff may correctly preserve a stale cache, not count a repair.
      await waitFor(async () => {
        const mirrors = await engine.executeRaw<{ state: string }>(
          "SELECT state FROM persistence_effects WHERE source_id=$1 AND kind='withdrawal-mirror'", [sourceId]);
        return mirrors.length === 1 && mirrors[0].state === 'committed';
      }, { label: 'withdrawal mirror committed' });
      const ledger = await engine.executeRaw('SELECT * FROM fact_withdrawals WHERE source_id=$1', [sourceId]);
      const caps = await engine.executeRaw('SELECT id,valid_until,expired_at FROM facts WHERE id=$1', [target.id]);
      expect(await facts(engine, sourceId, 'people/late-withdrawal')).toHaveLength(1);
      // Once the mirror is settled, a fresh observation must repair the missing row.
      const fresh = await run();
      expect(fresh.managed).toMatchObject({ pagesReconciled: 1, pagesPreserved: 0 });
      expect(fresh.factsInserted).toBe(1);
      expect((await facts(engine, sourceId, 'people/late-withdrawal')).map(f => [f.row_num, f.fact, f.expired])).toEqual([
        [1, 'Withdraw while preparing', true], [2, 'Missing', false]]);
      expect(await engine.executeRaw('SELECT id,valid_until,expired_at FROM facts WHERE id=$1', [target.id])).toEqual(caps);
      expect(await engine.executeRaw('SELECT * FROM fact_withdrawals WHERE source_id=$1', [sourceId])).toEqual(ledger);
      expect((await run()).managed).toMatchObject({ pagesReconciled: 0, pagesInSync: 1 });
      expect(await receipts(engine, sourceId, 'people/late-withdrawal')).toEqual(['conflict', 'committed']);
    });
}, 180_000);

test('index repaired after admission is revalidated before publication and not counted (R2)', async () => {
  await brain({ 'people/late-index': page('Late index', [row(1, 'Missing')]) }, async ({ engine, sourceId, unmanaged, run }) => {
    await unmanaged(() => engine.executeRaw('DELETE FROM facts WHERE source_id=$1', [sourceId]));
    const snapshot = (await engine.readPageSnapshot('people/late-index', { sourceId }))!;
    const projection = prepareCanonicalFactsProjection([snapshot.page.compiled_truth], 'people/late-index', sourceId);
    const original = engine.readPageSnapshot;
    let reached!: () => void, release!: () => void;
    const atPrepare = new Promise<void>(resolve => { reached = resolve; });
    const resume = new Promise<void>(resolve => { release = resolve; });
    let paused = false;
    engine.readPageSnapshot = async function (this: BrainEngine, slug, opts) {
      const current = await original.call(this, slug, opts);
      if (!paused && slug === 'people/late-index' && new Error().stack?.includes('prepareFactsReconcile')) {
        paused = true; reached(); await resume;
      }
      return current;
    };
    const repairing = run();
    let repaired;
    try {
      await atPrepare;
      // Simulate another guarded projection worker completing the same repair.
      await engine.transaction(async tx => {
        await tx.lockPageKeys([{ sourceId, slug: 'people/late-index' }]);
        await withCoordinatedWrite(tx, [sourceId], () => projection.apply(tx));
      });
      repaired = await facts(engine, sourceId, 'people/late-index');
    } finally { release(); }
    let result;
    try { result = await repairing; } finally { delete (engine as Partial<BrainEngine>).readPageSnapshot; }
    expect(paused).toBe(true);
    expect(result.managed!.pagesReconciled).toBe(0);
    expect(result.factsInserted).toBe(0);
    expect(await facts(engine, sourceId, 'people/late-index')).toEqual(repaired);
    const [receipt] = await engine.executeRaw<{ outcome: Record<string, unknown> }>(`SELECT outcome FROM persistence_requests
      WHERE source_id=$1 AND intent->>'kind'='managed_maintenance_facts'`, [sourceId]);
    expect(receipt.outcome).toMatchObject({ noop: true, facts_inserted: 0, facts_expired: 0 });
    expect((await run()).managed!.pagesInSync).toBe(1);
  });
}, 180_000);

test('old maintenance authority cannot repair a replacement source with the same slug (R2)', async () => {
  await brain({ 'people/recreated': page('Recreated', [row(1, 'Retained')]) }, async ({ engine, sourceId, root, unmanaged }) => {
    const authority = (await maintenancePreflight(engine, sourceId, root))!;
    const revision = (await engine.readPageSnapshot('people/recreated', { sourceId }))!.revision;
    await unmanaged(async () => {
      await engine.executeRaw('DELETE FROM sources WHERE id=$1', [sourceId]);
      await engine.executeRaw('INSERT INTO sources(id,name) VALUES($1,$1)', [sourceId]);
      await engine.executeRaw(`INSERT INTO pages(source_id,slug,type,title,compiled_truth,knowledge_revision)
        VALUES($1,'people/recreated','person','Replacement','Untouched',$2::uuid)`, [sourceId, revision]);
    });
    await expect(submitMaintenanceFactsReconcile(engine, authority, 'people/recreated', revision, observedIndexDigest([])))
      .rejects.toMatchObject({ code: 'source_changed' });
    expect((await engine.readPageSnapshot('people/recreated', { sourceId }))!.page.compiled_truth).toBe('Untouched');
    expect(await receipts(engine, sourceId, 'people/recreated')).toHaveLength(0);
  });
}, 180_000);

test('a withdrawal mirror overtaking discovery preserves the stale observation, then converges (qualification R4)', async () => {
  const slug = 'people/mirror-race';
  await brain({ [slug]: page('Mirror race', [row(1, 'Withdraw across scan'), row(2, 'Private missing', { visibility: 'private' })]) },
    async ({ engine, sourceId, unmanaged, run }) => {
      const [target] = await facts(engine, sourceId, slug);
      // Stop the consumer so this test, not a timer, owns mirror scheduling.
      await unmanaged(() => engine.executeRaw('DELETE FROM facts WHERE source_id=$1 AND row_num=2', [sourceId]));
      const config = { engine: engine.kind, embedding_disabled: true };
      await submitForgetMutation({ engine, sourceId, remote: false, config,
        dryRun: false, logger: { info() {}, warn() {}, error() {} } }, 'forget', { id: String(target.id), request_id: randomUUID() });
      // Retain prior fixture effects but keep them out of this explicitly driven schedule.
      await engine.executeRaw("UPDATE persistence_effects SET next_attempt_at=now()+interval '1 hour' WHERE source_id<>$1", [sourceId]);
      const ledger = await engine.executeRaw('SELECT * FROM fact_withdrawals WHERE source_id=$1', [sourceId]);
      const caps = await engine.executeRaw('SELECT id,valid_until,expired_at FROM facts WHERE id=$1', [target.id]);
      const original = engine.executeRaw;
      let overtook = false;
      engine.executeRaw = async function (this: BrainEngine, sql, params, opts) {
        const result = await original.call(this, sql, params, opts);
        if (this === engine && !overtook && sql.includes('SELECT p.slug,p.compiled_truth,p.timeline,p.effective_date')) {
          overtook = true;
          // Keep the actual old discovery result while the real mirror publishes
          // its file and materializes the withdrawal overlay in the page cache.
          await waitFor(async () => {
            await runPersistenceEffects(engine, config, { hostId: localHostId(), limit: 1 });
            const mirrors = await original.call(engine,
              "SELECT state FROM persistence_effects WHERE source_id=$1 AND kind='withdrawal-mirror'", [sourceId]) as Array<{ state: string }>;
            return mirrors.length === 1 && mirrors[0].state === 'committed';
          }, { label: 'explicitly driven withdrawal mirror' });
        }
        return result as never;
      };
      let stale;
      try { stale = await run(); } finally { delete (engine as Partial<BrainEngine>).executeRaw; }
      expect(overtook).toBe(true);
      expect(stale.warnings.some(w => w.includes('FACTS_PAGE_CACHE_STALE'))).toBe(true);
      expect(stale.managed).toMatchObject({ pagesReconciled: 0, pagesPreserved: 1, pagesInSync: 0 });
      expect(stale.factsInserted).toBe(0);
      expect(await receipts(engine, sourceId, slug)).toEqual([]);
      expect(await facts(engine, sourceId, slug)).toHaveLength(1);
      const fresh = await run();
      expect(fresh.managed).toMatchObject({ pagesReconciled: 1, pagesPreserved: 0 });
      expect(fresh.factsInserted).toBe(1);
      expect((await facts(engine, sourceId, slug)).map(f => [f.row_num, f.fact, f.visibility, f.expired])).toEqual([
        [1, 'Withdraw across scan', 'world', true], [2, 'Private missing', 'private', false]]);
      expect(await engine.executeRaw('SELECT id,valid_until,expired_at FROM facts WHERE id=$1', [target.id])).toEqual(caps);
      expect(await engine.executeRaw('SELECT * FROM fact_withdrawals WHERE source_id=$1', [sourceId])).toEqual(ledger);
      expect((await run()).managed).toMatchObject({ pagesReconciled: 0, pagesInSync: 1 });
      expect(await receipts(engine, sourceId, slug)).toEqual(['committed']);
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
