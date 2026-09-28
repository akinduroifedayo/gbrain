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
import { digest } from '../persistence/digest.ts';
import { isInt4RowRef, resolveSupersededByRow } from '../facts/supersede-resolve.ts';

export interface ManagedFactsResult {
  pagesScanned: number;
  pagesWithFacts: number;
  pagesInSync: number;
  pagesReconciled: number;
  pagesPreserved: number;
  /** Drifted pages whose identical reconcile was already in flight (another run admitted it). */
  pagesJoined: number;
  /** Admitted reconciles still running when this run stopped waiting. */
  pagesPending: number;
  pagesFenceAbsent: number;
  factsInserted: number;
  factsExpired: number;
  factsMissingEmbedding: number;
  warnings: string[];
}

interface CandidatePage { slug: string; compiled_truth: string; timeline: string | null; effective_date: Date | string | null; revision: string }
export interface IndexedFact {
  id: number | string; slug: string; row_num: number | string; fact: string; visibility: string; notability: string; kind: string;
  context: string | null; source: string | null; expired: boolean; withdrawn: boolean;
  confidence: number | string | null; valid_from: Date | string | null; valid_until: Date | string | null;
  superseded_by: number | string | null;
  claim_metric: string | null; claim_value: number | string | null; claim_unit: string | null; claim_period: string | null;
}

function markerCount(text: string, marker: string): number { return text.split(marker).length - 1; }

const time = (v: Date | string | null | undefined): number | null => v == null ? null : new Date(v).getTime();
const num = (v: number | string | null | undefined): number | null => v == null ? null : Number(v);

/**
 * Whether the indexed rows are the converged projection of the fence: the
 * state the canonical facts projection writes, compared on every column it
 * owns, so a page is admitted exactly when a reconcile would change it and is
 * in sync on the next run (no churn). Mirrors the unmanaged reconcile's drift
 * definition (extract-facts.ts) and extends it to the attribute columns the
 * projection's UPDATE rewrites:
 *   - identity: one row per fence row_num with the same claim + visibility;
 *   - attributes: kind, notability, context, source, confidence (REAL, so a
 *     float4 tolerance), typed-claim metric/value/unit/period;
 *   - validity: valid_from when the fence (or page date) supplies one — the
 *     projection keeps the stored value otherwise; valid_until when explicit;
 *     expiry and a derived valid_until compare NULL-ness only, since the
 *     mapper stamps struck rows with today's date; a withdrawn fingerprint is
 *     expired by the facts trigger regardless of the fence;
 *   - supersession: a `superseded by #N` row's stored target equals the id the
 *     shared resolver yields against the page's current rows (an unsafe
 *     reference resolves to NULL and matches NULL, so it never churns). Rows
 *     without a reference are not compared: supersession recorded by other
 *     writers is never cleared by this path.
 * Pure: exported for tests.
 */
export function factsInSync(desired: FenceExtractedFact[], indexed: IndexedFact[], slug = ''): boolean {
  if (desired.length !== indexed.length) return false;
  const byRow = new Map(indexed.map(row => [Number(row.row_num), row]));
  if (byRow.size !== indexed.length) return false;
  for (const want of desired) {
    const have = byRow.get(want.row_num);
    if (!have) return false;
    if (have.fact !== want.fact || have.visibility !== want.visibility || have.notability !== (want.notability ?? 'medium') ||
      have.kind !== (want.kind ?? 'fact') || !contextMatches(have, want.context ?? null) ||
      (have.source ?? null) !== (want.source ?? FENCE_SOURCE_DEFAULT)) return false;
    const wantConfidence = want.confidence ?? 1.0;
    if (num(have.confidence) === null || Math.abs(num(have.confidence)! - wantConfidence) > 1e-6) return false;
    if ((have.claim_metric ?? null) !== (want.claim_metric ?? null) || (have.claim_unit ?? null) !== (want.claim_unit ?? null) ||
      (have.claim_period ?? null) !== (want.claim_period ?? null) || num(have.claim_value) !== num(want.claim_value)) return false;
    if (want.valid_from && time(have.valid_from) !== time(want.valid_from)) return false;
    const struck = want.expired_at != null;
    if (have.expired !== (struck || have.withdrawn)) return false;
    // valid_until: explicit fence value → exact; derived today-stamp → NULL-ness.
    // A withdrawn row's valid_until is capped by the withdrawal trigger, not the fence.
    if (!struck && !have.withdrawn && time(have.valid_until) !== time(want.valid_until)) return false;
    if (struck && want.valid_until && (have.valid_until == null)) return false;
    if (want.superseded_by_row !== undefined) {
      const target = byRow.get(want.superseded_by_row);
      const resolved = resolveSupersededByRow(want.row_num, want.superseded_by_row,
        target && isInt4RowRef(want.superseded_by_row) ? { id: Number(target.id), struck: target.expired } : undefined, slug).superseded_by;
      if (num(have.superseded_by) !== resolved) return false;
    }
  }
  return true;
}

/**
 * A withdrawn row's context is owned by the withdrawal overlay
 * (withdrawal-overlay.ts): `forgotten: <reason>` optionally followed by
 * ` | <fence context>`. Any other row compares exactly.
 */
function contextMatches(have: IndexedFact, want: string | null): boolean {
  const context = have.context ?? null;
  if (context === want) return true;
  if (!have.withdrawn || !context || !/^forgotten\s*:/i.test(context)) return false;
  if (want === null || /^forgotten\s*:/i.test(want)) return true;
  return context.split(' | ').slice(1).join(' | ') === want;
}

/** Stable digest of the indexed rows a reconcile was admitted against (retry identity, F3). */
export function observedIndexDigest(rows: IndexedFact[]): string {
  return digest([...rows].sort((a, b) => Number(a.row_num) - Number(b.row_num)).map(r => [
    Number(r.id), Number(r.row_num), r.fact, r.visibility, r.notability, r.kind, r.context, r.source, r.expired, r.withdrawn,
    num(r.confidence), time(r.valid_from), time(r.valid_until), num(r.superseded_by),
    r.claim_metric, num(r.claim_value), r.claim_unit, r.claim_period]));
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
    pagesPreserved: 0, pagesJoined: 0, pagesPending: 0, pagesFenceAbsent: 0, factsInserted: 0, factsExpired: 0, factsMissingEmbedding: 0, warnings: [] };
  if (opts.slugs !== undefined && opts.slugs.length === 0) return result;
  const scope = opts.slugs === undefined ? '' : ' AND p.slug = ANY($3::text[])';
  const params: unknown[] = [sourceId, `%${FACTS_FENCE_BEGIN}%`, ...(opts.slugs === undefined ? [] : [opts.slugs])];
  const pages = await engine.executeRaw<CandidatePage>(`SELECT p.slug,p.compiled_truth,p.timeline,p.effective_date,p.knowledge_revision::text AS revision
      FROM pages p WHERE p.source_id=$1 AND p.deleted_at IS NULL
       AND (p.compiled_truth LIKE $2 OR COALESCE(p.timeline,'') LIKE $2)${scope} ORDER BY p.slug`, params);
  const indexedRows = await engine.executeRaw<IndexedFact>(`SELECT f.id,f.source_markdown_slug AS slug,f.row_num,f.fact,f.visibility,f.notability,f.kind,
        f.context,f.source,f.expired_at IS NOT NULL AS expired,f.confidence,f.valid_from,f.valid_until,f.superseded_by,
        f.claim_metric,f.claim_value,f.claim_unit,f.claim_period,
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
    if (factsInSync(desired, rows, page.slug)) { result.pagesInSync++; continue; }
    if (await canonicalFileMatches(engine, page, sourceId) === 'stale') {
      result.pagesPreserved++;
      result.warnings.push(`${page.slug}: FACTS_PAGE_CACHE_STALE: canonical Markdown differs from the pages cache; reconcile waits for sync`);
      continue;
    }
    if (opts.dryRun) { result.pagesReconciled++; continue; }
    authority ??= await maintenancePreflight(engine, sourceId, opts.brainDir);
    if (!authority) throw new Error('managed extract_facts requires managed persistence');
    try {
      const outcome = await submitMaintenanceFactsReconcile(engine, authority, page.slug, page.revision, observedIndexDigest(rows));
      if (outcome.kind === 'exhausted') {
        result.pagesPreserved++;
        result.warnings.push(`${page.slug}: FACTS_RECONCILE_RETRY_EXHAUSTED: ${outcome.attempts} reconcile attempts at this revision left the same drift; existing index preserved`);
        continue;
      }
      if (outcome.kind === 'joined') { result.pagesJoined++; continue; }
      result.pagesReconciled++;
      result.factsInserted += Number(outcome.receipt.facts_inserted ?? 0);
      result.factsExpired += Number(outcome.receipt.facts_expired ?? 0);
    } catch (error) {
      const code = (error as { code?: string }).code;
      // A page edited between comparison and publication is reconciled by
      // the edit's own canonical projection; an accepted request still in
      // flight completes on its own. Everything else is surfaced.
      if (code === 'revision_conflict') {
        result.pagesPreserved++;
        result.warnings.push(`${page.slug}: FACTS_PAGE_CHANGED: page changed during reconcile; its own publication owns the projection`);
      } else if (code === 'write_pending') {
        result.pagesPending++;
        result.warnings.push(`${page.slug}: FACTS_RECONCILE_PENDING: the accepted reconcile is still running; the next cycle re-checks it`);
      } else throw error;
    }
  }
  if (!opts.dryRun && result.pagesReconciled > 0) {
    const [missing] = await engine.executeRaw<{ n: number | string }>(`SELECT count(*) AS n FROM facts
      WHERE source_id=$1 AND row_num IS NOT NULL AND expired_at IS NULL AND embedding IS NULL`, [sourceId]);
    result.factsMissingEmbedding = Number(missing?.n ?? 0);
  }
  return result;
}
