import { randomUUID } from 'node:crypto';
import type { BrainEngine } from '../engine.ts';
import { loadConfig } from '../config.ts';
import { submitPageMutation } from '../persistence/page-mutations.ts';

/**
 * #5484: coordinated purge of expired tombstones on a managed brain. Pages in
 * archived sources are left to the source-lifecycle purge. A page that cannot
 * be purged (owner unavailable, revision moved) is reported, never forced.
 */
export async function purgeExpiredPagesManaged(engine: BrainEngine, olderThanHours: number):
  Promise<{ slugs: string[]; count: number; blocked: Array<{ source_id: string; slug: string; reason: string }> }> {
  const hours = Math.max(0, Math.floor(olderThanHours));
  const rows = await engine.executeRaw<{ source_id: string; slug: string }>(
    `SELECT p.source_id, p.slug FROM pages p JOIN sources s ON s.id = p.source_id
      WHERE p.deleted_at IS NOT NULL AND p.deleted_at < now() - ($1 || ' hours')::interval AND NOT s.archived
      ORDER BY p.deleted_at ASC, p.source_id ASC, p.slug ASC`, [String(hours)]);
  const config = loadConfig() ?? { engine: engine.kind };
  const slugs: string[] = [];
  const blocked: Array<{ source_id: string; slug: string; reason: string }> = [];
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
      blocked.push({ ...row, reason: error instanceof Error ? error.message : String(error) });
    }
  }
  return { slugs, count: slugs.length, blocked };
}
