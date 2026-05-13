export type AppEvent = { type: string; [key: string]: unknown };

let eventHandler: ((event: AppEvent) => void) | null = null;

export function setAppEventHandler(handler: ((event: AppEvent) => void) | null): () => void {
  eventHandler = handler;
  return () => {
    if (eventHandler === handler) {
      eventHandler = null;
    }
  };
}

export function emitAppEvent(event: AppEvent): void {
  eventHandler?.({ ...event, nonce: Date.now() });
  window.setTimeout(notifyLayoutChanged, 0);
}

export function notifyLayoutChanged(): void {
  window.dispatchEvent(new Event('rdf4risk-layout-change'));
}