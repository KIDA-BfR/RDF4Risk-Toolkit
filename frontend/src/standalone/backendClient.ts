import type { AppEvent } from '../shared/appBridge';
import type { ServiceId } from './services';

export type BackendPayload = { service: string; args: Record<string, unknown> };

const API_BASE = (import.meta as any).env?.VITE_RDF4RISK_API_BASE || 'http://127.0.0.1:8766';

// Map known backend failure fragments to actionable, human-readable copy.
const ERROR_HINTS: Array<[RegExp, string]> = [
  [/keyring/i, "Secure sign-in storage is unavailable on this machine. Install the project requirements (pip install -r requirements.txt), then sign in again."],
];

// The backend's catch-all handler responds with `{"error": str(exc), "type": ExcClass}`.
// Surface the message text only — never the raw JSON envelope or exception class.
function humanizeBackendError(body: string, status: number, statusText: string): string {
  let message = body.trim() || `The server responded with ${status} ${statusText}.`;
  try {
    const parsed = JSON.parse(message);
    if (parsed && typeof parsed.error === 'string' && parsed.error.trim()) {
      message = parsed.error.trim();
    }
  } catch {
    // not JSON — keep the raw text
  }
  for (const [pattern, hint] of ERROR_HINTS) {
    if (pattern.test(message)) return hint;
  }
  return message;
}

async function parseError(response: Response): Promise<Error> {
  const body = await response.text();
  return new Error(humanizeBackendError(body, response.status, response.statusText));
}

export async function fetchSnapshot(service: ServiceId): Promise<BackendPayload | null> {
  if (service === 'home') return null;
  const response = await fetch(`${API_BASE}/api/services/${service}/snapshot?_=${Date.now()}`, {
    cache: 'no-store',
  });
  if (!response.ok) throw await parseError(response);
  return response.json();
}

export async function postEvent(service: ServiceId, event: AppEvent): Promise<BackendPayload> {
  const response = await fetch(`${API_BASE}/api/services/${service}/event`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(event),
  });
  if (!response.ok) throw await parseError(response);
  return response.json();
}

export type ProjectMeta = { id: string; name: string; saved_at: number | null };
export type HistoryEntry = { index: number; label: string; service: string; ts: number };
export type WorkspaceSnapshot = { projects: ProjectMeta[]; history: HistoryEntry[] } & Record<string, unknown>;

export async function fetchWorkspace(): Promise<WorkspaceSnapshot> {
  const response = await fetch(`${API_BASE}/api/workspace/snapshot`);
  if (!response.ok) throw await parseError(response);
  return response.json();
}

export async function postWorkspaceEvent(event: Record<string, unknown>): Promise<WorkspaceSnapshot> {
  const response = await fetch(`${API_BASE}/api/workspace/event`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(event),
  });
  if (!response.ok) throw await parseError(response);
  return response.json();
}

// ---- BioPortal ontology registry maintenance (admin) ----
export type RegistryJobStatus = {
  job_id: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  mode: 'catalog' | 'full_context';
  all_bioportal: boolean;
  dry_run: boolean;
  total: number;
  processed: number;
  percent: number;
  current_acronym: string | null;
  fetched: number;
  skipped: number;
  failed: number;
  errors_sample: Array<{ acronym: string; error: string }>;
  error: string;
} | null;

export type RegistryJobPreview = {
  total_discovered: number;
  already_in_registry: number;
  planned_for_fetch: number;
  has_api_key?: boolean;
  error?: string;
};

export async function fetchRegistryJobPreview(): Promise<RegistryJobPreview> {
  const response = await fetch(`${API_BASE}/api/agent/registry-job/preview?_=${Date.now()}`, { cache: 'no-store' });
  if (!response.ok) throw await parseError(response);
  return response.json();
}

export async function fetchRegistryJobStatus(jobId?: string): Promise<RegistryJobStatus> {
  const q = jobId ? `&job_id=${encodeURIComponent(jobId)}` : '';
  const response = await fetch(`${API_BASE}/api/agent/registry-job/status?_=${Date.now()}${q}`, { cache: 'no-store' });
  if (!response.ok) throw await parseError(response);
  const body = await response.json();
  return body?.status ?? null;
}

export async function startRegistryJob(payload: {
  mode: 'catalog' | 'full_context';
  all_bioportal?: boolean;
  dry_run?: boolean;
  confirmed: boolean;
  max_requests?: number | null;
  refresh?: boolean;
}): Promise<{ job_id?: string; status?: RegistryJobStatus; error?: string }> {
  const response = await fetch(`${API_BASE}/api/agent/registry-job/start`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!response.ok) throw await parseError(response);
  return response.json();
}

export async function cancelRegistryJob(jobId: string): Promise<{ cancelled: boolean; status: RegistryJobStatus }> {
  const response = await fetch(`${API_BASE}/api/agent/registry-job/cancel`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ job_id: jobId }),
  });
  if (!response.ok) throw await parseError(response);
  return response.json();
}
