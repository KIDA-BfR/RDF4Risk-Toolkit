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

// Backends drop an event whose nonce equals the previous one (duplicate delivery). The nonce must
// therefore be unique per event: Date.now() alone repeats when one action sends two events in the
// same millisecond (e.g. Continue saving a typed field, then navigating), and the second was lost.
let eventSequence = 0;

export function emitAppEvent(event: AppEvent): void {
  eventSequence += 1;
  eventHandler?.({ ...event, nonce: `${Date.now()}-${eventSequence}` });
  window.setTimeout(notifyLayoutChanged, 0);
}

export function notifyLayoutChanged(): void {
  window.dispatchEvent(new Event('rdf4risk-layout-change'));
}