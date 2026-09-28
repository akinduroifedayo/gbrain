import { randomUUID } from 'node:crypto';
import type { BrainEngine } from '../engine.ts';
import { loadConfig } from '../config.ts';
import { submitPageMutation } from '../persistence/page-mutations.ts';

/** A tombstone that moved under a concurrent writer; the next run retries it. */
const DEFERRAL_CODES = new Set(['revision_conflict', 'page_not_found', 'page_identity_changed']);

export interface ManagedPurgeResult {
  slugs: string[];
  count: number;
  /** Every tombstone not purged this run, with its reason (deferred + failed). */
  blocked: Array<{ source_id: string; slug: string; reason: string; code: string | null }>;
  /** Benign concurrency deferrals (page changed/restored/gone mid-run). */
  deferred: number;
  /** Operational failures (owner/binding/storage/protocol). The phase must not report success. */
  failed: number;
  /** Set when `failed > 0`: the purge phase fails (no global freshness); deferrals alone stay ok. */
  error?: { class: string; code: string; message: string };
}

/**
 * #5484: coordinated purge of expired tombstones on a managed brain. Pages in
 * archived sources are left to the source-lifecycle purge. A page that cannot
 * be purged is reported, never forced; concurrency deferrals are separated
 * from operational failures so the caller can report the phase truthfully.
 */
export async function purgeExpiredPagesManaged(engine: BrainEngine, olderThanHours: number): Promise<ManagedPurgeResult> {
  const hours = Math.max(0, Math.floor(olderThanHours));
  const rows = await engine.executeRaw<{ source_id: string; slug: string }>(
    `SELECT p.source_id, p.slug FROM pages p JOIN sources s ON s.id = p.source_id
      WHERE p.deleted_at IS NOT NULL AND p.deleted_at < now() - ($1 || ' hours')::interval AND NOT s.archived
      ORDER BY p.deleted_at ASC, p.source_id ASC, p.slug ASC`, [String(hours)]);
  const config = loadConfig() ?? { engine: engine.kind };
  const slugs: string[] = [];
  const blocked: ManagedPurgeResult['blocked'] = [];
  let deferred = 0, failed = 0;
  for (const row of rows) {
    try {
      const snapshot = await engine.readPageSnapshot(row.slug, { sourceId: row.source_id, includeDeleted: true });
      if (!snapshot?.page.deleted_at) continue; // restored or already gone since the scan
      await submitPageMutation({ engine, config, remote: false, sourceId: row.source_id, dryRun: false,
        logger: { info() {}, warn() {}, error() {} } } as never,
        { operation: 'delete_page', params: { slug: row.slug, source_id: row.source_id, purge: true,
          expected_revision: snapshot.revision, request_id: randomUUID() } });
      slugs.push(row.slug);
    } catch (error) {
      const code = typeof (error as { code?: unknown })?.code === 'string' ? (error as { code: string }).code : null;
      if (code && DEFERRAL_CODES.has(code)) deferred++; else failed++;
      blocked.push({ ...row, code, reason: error instanceof Error ? error.message : String(error) });
    }
  }
  const error = failed > 0 ? { class: 'ManagedPurgeFailed', code: 'managed_purge_failed',
    message: `${failed} expired page(s) could not be purged through the coordinator: ${blocked.filter(b => !b.code || !DEFERRAL_CODES.has(b.code))
      .slice(0, 3).map(b => `${b.source_id}/${b.slug}: ${b.reason}`).join('; ')}` } : undefined;
  return { slugs, count: slugs.length, blocked, deferred, failed, ...(error ? { error } : {}) };
}
