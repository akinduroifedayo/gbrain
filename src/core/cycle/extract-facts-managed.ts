/**
 * Managed-brain extract_facts: re-project drifted `## Facts` fences into the
 * facts index through the persistence coordinator.
 *
 * On a managed brain the legacy reconcile (direct insertFacts / hard delete /
 * inline embedding / phantom file rewrites) is refused by
 * assertUnmanagedCanonicalWriter. This path keeps the same contract — the
 * fence is the system of record for fence-owned rows — while publishing each
 * drifted page as one DB-only `managed_maintenance_facts` request pinned to
 * the page revision it was compared against. The prepared mutation reuses the
 * canonical facts projection (keyed by row_num; replaced rows are expired and
 * detached, never deleted; withdrawals stay enforced by the facts trigger).
 *
 * Deterministic preservation rules (a page is left untouched, never guessed):
 *   - parse warnings, a fence below the timeline sentinel, or duplicate fence
 *     markers — the parse is not authoritative;
 *   - the canonical file differs from the pages cache — sync owns the import;
 *   - no fence rows (fence absent or empty) — absence is not a withdrawal;
 *   - `cli:` conversation facts share the page coordinate — the projection's
 *     row_num keyspace would collide with them.
 * Pages already in sync are not admitted, so a steady-state run writes nothing.
 * No embedding provider is called; NULL vectors are counted for the managed
 * fact-embedding lane.
 */
import { existsSync, readFileSync } from 'node:fs';
import type { BrainEngine } from '../engine.ts';
import { FACTS_FENCE_BEGIN, FACTS_FENCE_END, parseFactsFence } from '../facts-fence.ts';
import { extractFactsFromFenceText, FENCE_SOURCE_DEFAULT, type FenceExtractedFact } from '../facts/extract-from-fence.ts';
import { parseMarkdown } from '../markdown.ts';
import { isWriteThroughDisabled, resolvePageWriteTarget } from '../write-through.ts';
import { isAborted } from '../abort-check.ts';
import { maintenancePreflight, submitMaintenanceFactsReconcile, type MaintenanceAuthority } from '../persistence/prepared-maintenance.ts';

export interface ManagedFactsResult {
  pagesScanned: number;
  pagesWithFacts: number;
  pagesInSync: number;
  pagesReconciled: number;
  pagesPreserved: number;
  pagesFenceAbsent: number;
  factsInserted: number;
  factsExpired: number;
  factsMissingEmbedding: number;
  warnings: string[];
}

interface CandidatePage { slug: string; compiled_truth: string; timeline: string | null; effective_date: Date | string | null; revision: string }
interface IndexedFact {
  slug: string; row_num: number | string; fact: string; visibility: string; notability: string; kind: string;
  context: string | null; source: string | null; expired: boolean; withdrawn: boolean;
}

function markerCount(text: string, marker: string): number { return text.split(marker).length - 1; }

/**
 * Whether the indexed rows already equal what the canonical projection would
 * write. Only dimensions the projection actually converges are compared, so an
 * admitted page is in sync on the next run (no churn): row set, claim,
 * visibility, and the attribute columns its UPDATE rewrites. Expiry compares
 * NULL-ness (the mapper stamps struck rows with today's date); a withdrawn
 * fingerprint is expired by the facts trigger regardless of the fence.
 */
export function factsInSync(desired: FenceExtractedFact[], indexed: IndexedFact[]): boolean {
  if (desired.length !== indexed.length) return false;
  const byRow = new Map(indexed.map(row => [Number(row.row_num), row]));
  for (const want of desired) {
    const have = byRow.get(want.row_num);
    if (!have) return false;
    if (have.fact !== want.fact || have.visibility !== want.visibility || have.notability !== (want.notability ?? 'medium') ||
      have.kind !== (want.kind ?? 'fact') || (have.context ?? null) !== (want.context ?? null) ||
      (have.source ?? null) !== (want.source ?? FENCE_SOURCE_DEFAULT)) return false;
    if (have.expired !== (want.expired_at != null || have.withdrawn)) return false;
  }
  return true;
}

async function canonicalFileMatches(engine: BrainEngine, page: CandidatePage, sourceId: string): Promise<'fresh' | 'stale' | 'unavailable'> {
  if (await isWriteThroughDisabled(engine)) return 'unavailable';
  const target = await resolvePageWriteTarget(engine, page.slug, sourceId);
  if (!target.ok || !existsSync(target.filePath)) return 'unavailable';
  try {
    const canonical = parseMarkdown(readFileSync(target.filePath, 'utf-8'), target.filePath);
    return canonical.compiled_truth === page.compiled_truth.trim() && canonical.timeline === (page.timeline ?? '').trim() ? 'fresh' : 'stale';
  } catch {
    return 'stale';
  }
}

export async function runManagedExtractFacts(engine: BrainEngine, opts: {
  sourceId: string; slugs?: string[]; dryRun?: boolean; brainDir?: string; signal?: AbortSignal;
}): Promise<ManagedFactsResult> {
  const { sourceId } = opts;
  const result: ManagedFactsResult = { pagesScanned: 0, pagesWithFacts: 0, pagesInSync: 0, pagesReconciled: 0,
    pagesPreserved: 0, pagesFenceAbsent: 0, factsInserted: 0, factsExpired: 0, factsMissingEmbedding: 0, warnings: [] };
  if (opts.slugs !== undefined && opts.slugs.length === 0) return result;
  const scope = opts.slugs === undefined ? '' : ' AND p.slug = ANY($3::text[])';
  const params: unknown[] = [sourceId, `%${FACTS_FENCE_BEGIN}%`, ...(opts.slugs === undefined ? [] : [opts.slugs])];
  const pages = await engine.executeRaw<CandidatePage>(`SELECT p.slug,p.compiled_truth,p.timeline,p.effective_date,p.knowledge_revision::text AS revision
      FROM pages p WHERE p.source_id=$1 AND p.deleted_at IS NULL
       AND (p.compiled_truth LIKE $2 OR COALESCE(p.timeline,'') LIKE $2)${scope} ORDER BY p.slug`, params);
  const indexedRows = await engine.executeRaw<IndexedFact>(`SELECT f.source_markdown_slug AS slug,f.row_num,f.fact,f.visibility,f.notability,f.kind,
        f.context,f.source,f.expired_at IS NOT NULL AS expired,
        EXISTS (SELECT 1 FROM fact_withdrawals w WHERE w.source_id=f.source_id AND w.visibility=f.visibility
          AND w.fact_hash=gbrain_fact_fingerprint(f.fact)) AS withdrawn
      FROM facts f WHERE f.source_id=$1 AND f.row_num IS NOT NULL AND f.source_markdown_slug IS NOT NULL
       ${opts.slugs === undefined ? '' : 'AND f.source_markdown_slug = ANY($2::text[])'}`,
  opts.slugs === undefined ? [sourceId] : [sourceId, opts.slugs]);
  const indexed = new Map<string, IndexedFact[]>();
  for (const row of indexedRows) (indexed.get(row.slug) ?? indexed.set(row.slug, []).get(row.slug)!).push(row);
  const fenced = new Set(pages.map(p => p.slug));
  for (const [slug, rows] of indexed) if (!fenced.has(slug) && rows.some(r => !(r.source ?? '').startsWith('cli:') && !r.expired)) result.pagesFenceAbsent++;

  let authority: MaintenanceAuthority | null = null;
  for (const page of pages) {
    if (isAborted(opts.signal)) { result.warnings.push('extract_facts: managed reconcile deferred after cancellation'); break; }
    result.pagesScanned++;
    const fields = [page.compiled_truth ?? '', page.timeline ?? ''];
    if (fields.some(f => markerCount(f, FACTS_FENCE_BEGIN) > 1 || markerCount(f, FACTS_FENCE_END) > 1)) {
      result.pagesPreserved++; result.warnings.push(`${page.slug}: FACTS_FENCE_DUPLICATE: more than one facts fence; existing index preserved`); continue;
    }
    if (fields[1].includes(FACTS_FENCE_BEGIN)) {
      result.pagesPreserved++;
      result.warnings.push(`${page.slug}: FACTS_FENCE_BELOW_SENTINEL: a ## Facts fence sits below the timeline sentinel; existing index preserved`);
      continue;
    }
    const parsed = parseFactsFence(fields[0]);
    if (parsed.warnings.length) { result.pagesPreserved++; result.warnings.push(...parsed.warnings.map(w => `${page.slug}: ${w}`)); continue; }
    if (parsed.facts.length === 0) { result.pagesFenceAbsent++; continue; }
    result.pagesWithFacts++;
    const rows = indexed.get(page.slug) ?? [];
    if (rows.some(r => (r.source ?? '').startsWith('cli:'))) {
      result.pagesPreserved++;
      result.warnings.push(`${page.slug}: FACTS_CONVERSATION_COLLISION: conversation facts share this page's row keyspace; existing index preserved`);
      continue;
    }
    const effective = page.effective_date ? new Date(page.effective_date) : null;
    const desired = extractFactsFromFenceText(parsed.facts, page.slug, sourceId, { pageEffectiveDate: effective });
    if (new Set(desired.map(d => d.row_num)).size !== desired.length) {
      result.pagesPreserved++; result.warnings.push(`${page.slug}: FACTS_ROW_NUM_DUPLICATE: fence row numbers repeat; existing index preserved`); continue;
    }
    if (factsInSync(desired, rows)) { result.pagesInSync++; continue; }
    if (await canonicalFileMatches(engine, page, sourceId) === 'stale') {
      result.pagesPreserved++;
      result.warnings.push(`${page.slug}: FACTS_PAGE_CACHE_STALE: canonical Markdown differs from the pages cache; reconcile waits for sync`);
      continue;
    }
    if (opts.dryRun) { result.pagesReconciled++; continue; }
    authority ??= await maintenancePreflight(engine, sourceId, opts.brainDir);
    if (!authority) throw new Error('managed extract_facts requires managed persistence');
    try {
      const receipt = await submitMaintenanceFactsReconcile(engine, authority, page.slug, page.revision);
      result.pagesReconciled++;
      result.factsInserted += Number(receipt.facts_inserted ?? 0);
      result.factsExpired += Number(receipt.facts_expired ?? 0);
    } catch (error) {
      const code = (error as { code?: string }).code;
      // A page edited between comparison and publication is reconciled by
      // the edit's own canonical projection; everything else is surfaced.
      if (code !== 'revision_conflict') throw error;
      result.pagesPreserved++;
      result.warnings.push(`${page.slug}: FACTS_PAGE_CHANGED: page changed during reconcile; its own publication owns the projection`);
    }
  }
  if (!opts.dryRun && result.pagesReconciled > 0) {
    const [missing] = await engine.executeRaw<{ n: number | string }>(`SELECT count(*) AS n FROM facts
      WHERE source_id=$1 AND row_num IS NOT NULL AND expired_at IS NULL AND embedding IS NULL`, [sourceId]);
    result.factsMissingEmbedding = Number(missing?.n ?? 0);
  }
  return result;
}
