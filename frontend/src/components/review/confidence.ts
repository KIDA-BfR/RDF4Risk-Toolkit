// Shared confidence-band model used by the review grid (cell render, sort, filter) and the
// keyboard triage mode. Thresholds follow the common AI-confidence framework: >=0.8 high,
// 0.5–0.79 medium, <0.5 low.
export type ConfidenceBand = 'high' | 'medium' | 'low' | 'none';

export function toConfidenceNumber(value: unknown): number | null {
  if (value == null || value === '') return null;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

export function confidenceBand(value: unknown): ConfidenceBand {
  const n = toConfidenceNumber(value);
  if (n == null) return 'none';
  if (n >= 0.8) return 'high';
  if (n >= 0.5) return 'medium';
  return 'low';
}

export const BAND_META: Record<ConfidenceBand, { label: string; color: 'success' | 'warning' | 'error' | 'default'; order: number }> = {
  high: { label: 'High', color: 'success', order: 3 },
  medium: { label: 'Medium', color: 'warning', order: 2 },
  low: { label: 'Low — verify', color: 'error', order: 1 },
  none: { label: '—', color: 'default', order: 0 },
};
