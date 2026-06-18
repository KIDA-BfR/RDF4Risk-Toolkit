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
