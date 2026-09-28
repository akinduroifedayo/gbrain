import type { BrainEngine } from '../engine.ts';
import { digest } from '../persistence/digest.ts';

/** Only fence-owned semantic columns; embeddings do not invalidate a repair. */
export interface IndexedFact {
  id: number | string; slug: string; row_num: number | string; fact: string; visibility: string; notability: string; kind: string;
  context: string | null; source: string | null; expired: boolean; withdrawn: boolean;
  confidence: number | string | null; valid_from: Date | string | null; valid_until: Date | string | null;
  superseded_by: number | string | null;
  claim_metric: string | null; claim_value: number | string | null; claim_unit: string | null; claim_period: string | null;
}

export async function readFenceIndex(engine: BrainEngine, sourceId: string, slugs?: string[]): Promise<IndexedFact[]> {
  return engine.executeRaw<IndexedFact>(`SELECT f.id,f.source_markdown_slug AS slug,f.row_num,f.fact,f.visibility,f.notability,f.kind,
      f.context,f.source,f.expired_at IS NOT NULL AS expired,f.confidence,f.valid_from,f.valid_until,f.superseded_by,
      f.claim_metric,f.claim_value,f.claim_unit,f.claim_period,
      EXISTS (SELECT 1 FROM fact_withdrawals w WHERE w.source_id=f.source_id AND w.visibility=f.visibility
        AND w.fact_hash=gbrain_fact_fingerprint(f.fact)) AS withdrawn
    FROM facts f WHERE f.source_id=$1 AND f.row_num IS NOT NULL AND f.source_markdown_slug IS NOT NULL
      ${slugs === undefined ? '' : 'AND f.source_markdown_slug = ANY($2::text[])'}`,
  slugs === undefined ? [sourceId] : [sourceId, slugs]);
}

/** One representation shared by discovery, atomic admission and publication. */
export function observedIndexDigest(rows: IndexedFact[]): string {
  const num = (v: number | string | null) => v == null ? null : Number(v);
  const time = (v: Date | string | null) => v == null ? null : new Date(v).getTime();
  return digest([...rows].sort((a, b) => Number(a.row_num) - Number(b.row_num) || Number(a.id) - Number(b.id)).map(r => [
    Number(r.id), Number(r.row_num), r.fact, r.visibility, r.notability, r.kind, r.context, r.source, r.expired, r.withdrawn,
    num(r.confidence), time(r.valid_from), time(r.valid_until), num(r.superseded_by),
    r.claim_metric, num(r.claim_value), r.claim_unit, r.claim_period]));
}
