import type { AppEvent } from '../shared/appBridge';
import type { ServiceId } from './services';

export type BackendPayload = { service: string; args: Record<string, unknown> };

const API_BASE = (import.meta as any).env?.VITE_RDF4RISK_API_BASE || 'http://127.0.0.1:8765';

async function parseError(response: Response): Promise<Error> {
  const body = await response.text();
  return new Error(body || `${response.status} ${response.statusText}`);
}

export async function fetchSnapshot(service: ServiceId): Promise<BackendPayload | null> {
  if (service === 'home') return null;
  const response = await fetch(`${API_BASE}/api/services/${service}/snapshot`);
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
