// Design tokens shared by the theme and the layout primitives. DESIGN.md is the human-readable
// source of truth; change a value here and there together.

// Shell geometry. The fixed WorkflowNav offsets itself by the permanent drawer, so both must read
// the same width and breakpoint.
export const DRAWER_WIDTH = 264;
export const WIDE_LAYOUT_QUERY = '(min-width:1200px)';
export const CONTENT_MAX_WIDTH = 1200;

// Bottom padding (theme spacing units) every wizard page reserves so the fixed WorkflowNav never
// covers the last row. On xs the nav adds a second line for the hint.
export const NAV_CLEARANCE = { xs: 16, sm: 12 } as const;
// Bottom offset (px) for Snackbars so toasts sit above the WorkflowNav instead of on it.
export const NAV_SNACKBAR_OFFSET = { xs: 120, sm: 88 } as const;
// Scroll margins so focused fields are not hidden under the sticky top bar or the fixed nav.
export const SCROLL_MARGIN = { top: 96, bottom: 120 } as const;

export const radius = { control: '6px', surface: '8px', overlay: '12px', pill: '999px' } as const;

export const fonts = {
  sans: ['system-ui', '-apple-system', '"Segoe UI"', 'Roboto', '"Helvetica Neue"', 'Arial', 'sans-serif'].join(','),
  mono: ['ui-monospace', '"SF Mono"', '"Cascadia Mono"', 'Menlo', 'Consolas', 'monospace'].join(','),
} as const;

// Motion: quiet and purposeful. Overlays scale from their trigger; stage/tool changes don't animate.
export const motion = {
  easeOut: 'cubic-bezier(0.23, 1, 0.32, 1)',
  easeInOut: 'cubic-bezier(0.65, 0, 0.35, 1)',
  popover: { enter: 200, exit: 150 },
  collapse: 200,
  dialog: { enter: 240, exit: 180 },
  drawer: { enter: 300, exit: 200 },
} as const;
