import React from 'react';
import type { AppEvent } from './appBridge';

// Cross-tool navigation provided by the shell. Back on a tool's first stage and Continue on its
// last stage step through the pipeline (Overview → Prepare → Reconcile → Generate → Inspect), so
// Back/Continue keep one meaning across the whole app. Labels are the destination tool names,
// used for tooltips and default hints — never for the button labels themselves.
export type ToolNavigation = {
  goBack?: () => void;
  goNext?: () => void;
  backLabel?: string;
  nextLabel?: string;
  // Hand a result to the next tool: sends `event` to the next tool's service and opens it only if
  // that succeeds (e.g. RDF generator → RDF inspector loads the generated catalog). Resolves false
  // on failure; the error then shows in the current tool's status area.
  continueWith?: (event: AppEvent) => Promise<boolean>;
};

const ToolNavigationContext = React.createContext<ToolNavigation | null>(null);

export const ToolNavigationProvider = ToolNavigationContext.Provider;

export function useToolNavigation(): ToolNavigation | null {
  return React.useContext(ToolNavigationContext);
}

// Request errors raised by the shell (failed events, snapshot loads). WizardPage renders them in
// the stage's status area so every error appears in one place, under the stage header.
export type ShellStatus = { error: string | null; dismissError: () => void; reload: () => void };

const ShellStatusContext = React.createContext<ShellStatus | null>(null);

export const ShellStatusProvider = ShellStatusContext.Provider;

export function useShellStatus(): ShellStatus | null {
  return React.useContext(ShellStatusContext);
}
