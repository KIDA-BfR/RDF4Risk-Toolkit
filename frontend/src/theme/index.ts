import { experimental_extendTheme as extendTheme } from '@mui/material/styles';
import { fonts, motion, NAV_SNACKBAR_OFFSET, radius } from './tokens';

declare module '@mui/material/styles' {
  interface Palette {
    rdf: { control: string; subtle: string };
  }
  interface PaletteOptions {
    rdf?: { control?: string; subtle?: string };
  }
}

// "Lab Ledger" theme (see DESIGN.md). CSS-variable theme with light + dark colorSchemes: palette
// lives per scheme; typography, shape, motion and component overrides are shared and read theme
// tokens (theme.vars) so both schemes stay consistent. Every text/background pair clears WCAG AA
// (body text 6.4:1+, primary button 6.8:1, dark-mode text 7:1+); `rdf.control` gives input borders
// the 3:1 non-text contrast the default rgba border misses.
export const theme = extendTheme({
  cssVarPrefix: 'rdf4risk',
  colorSchemes: {
    light: {
      palette: {
        primary: { main: '#1d5aa8' },
        success: { main: '#047857' },
        warning: { main: '#b45309' },
        info: { main: '#0369a1' },
        error: { main: '#c62828' },
        background: { default: '#f4f6f8', paper: '#ffffff' },
        text: { primary: '#14202b', secondary: '#4d5b69' },
        divider: '#dde3e9',
        rdf: { control: '#8796a5', subtle: '#f8fafb' },
      },
    },
    dark: {
      palette: {
        primary: { main: '#7fb0ec' },
        success: { main: '#34d399' },
        warning: { main: '#fbbf24' },
        info: { main: '#38bdf8' },
        error: { main: '#f87171' },
        background: { default: '#0e141b', paper: '#151d26' },
        text: { primary: '#e3e9ef', secondary: '#9aa8b6' },
        divider: 'rgba(148, 163, 184, 0.20)',
        rdf: { control: '#5b6b7b', subtle: 'rgba(148, 163, 184, 0.06)' },
      },
    },
  },
  // Unit for numeric sx radii: borderRadius 1.5 = 6px (control), 2 = 8px (surface), 3 = 12px (overlay).
  shape: { borderRadius: 4 },
  typography: {
    fontFamily: fonts.sans,
    fontWeightMedium: 500,
    fontWeightBold: 600,
    h1: { fontSize: '2rem', fontWeight: 600, lineHeight: 1.2, letterSpacing: '-0.02em' },
    h2: { fontSize: '1.75rem', fontWeight: 600, lineHeight: 1.2, letterSpacing: '-0.02em' },
    h3: { fontSize: '1.5rem', fontWeight: 600, lineHeight: 1.25, letterSpacing: '-0.015em' },
    h4: { fontSize: '1.375rem', fontWeight: 600, lineHeight: 1.25, letterSpacing: '-0.01em' },
    h5: { fontSize: '1.5rem', fontWeight: 600, lineHeight: 1.25, letterSpacing: '-0.015em' },
    h6: { fontSize: '1.125rem', fontWeight: 600, lineHeight: 1.3, letterSpacing: '-0.01em' },
    subtitle1: { fontSize: '1rem', fontWeight: 600, lineHeight: 1.4 },
    subtitle2: { fontSize: '0.875rem', fontWeight: 600, lineHeight: 1.4 },
    body1: { lineHeight: 1.5 },
    body2: { lineHeight: 1.5 },
    caption: { lineHeight: 1.45 },
    // Sentence case everywhere: neutralise MUI's all-caps overline/button defaults.
    overline: { textTransform: 'none', letterSpacing: 0, fontWeight: 600, fontSize: '0.75rem', lineHeight: 1.5 },
    button: { fontWeight: 600, textTransform: 'none', letterSpacing: 0 },
  },
  transitions: {
    easing: {
      easeOut: motion.easeOut,
      easeInOut: motion.easeInOut,
    },
    duration: {
      shortest: 120,
      shorter: 160,
      short: 200,
      standard: 240,
      complex: 300,
      enteringScreen: 240,
      leavingScreen: 180,
    },
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        'code, kbd, samp, pre': { fontFamily: fonts.mono },
        // Reduced motion: drop all non-essential motion app-wide (spinners keep running).
        '@media (prefers-reduced-motion: reduce)': {
          '*, *::before, *::after': {
            animationDuration: '0.01ms !important',
            animationIterationCount: '1 !important',
            transitionDuration: '0.01ms !important',
            scrollBehavior: 'auto !important',
          },
          '.MuiCircularProgress-indeterminate, .MuiCircularProgress-indeterminate *, .MuiLinearProgress-root *': {
            animationDuration: 'revert !important',
            animationIterationCount: 'infinite !important',
          },
        },
      },
    },
    // Visible keyboard-focus ring on every ButtonBase descendant — WCAG 2.4.7.
    MuiButtonBase: {
      styleOverrides: {
        root: ({ theme: t }) => ({
          '&.Mui-focusVisible': {
            outline: `2px solid ${t.vars.palette.primary.main}`,
            outlineOffset: 2,
          },
        }),
      },
    },
    MuiButton: {
      defaultProps: { disableElevation: true },
      styleOverrides: {
        root: ({ theme: t }) => ({
          borderRadius: radius.control,
          textTransform: 'none',
          whiteSpace: 'nowrap',
          // Default disabled text (rgba(0,0,0,0.26)) measures 1.88:1; keep disabled CTAs legible.
          '&.Mui-disabled': { color: t.vars.palette.text.secondary },
        }),
        contained: ({ theme: t }) => ({
          '&.Mui-disabled': { backgroundColor: t.vars.palette.action.disabledBackground },
        }),
      },
    },
    MuiIconButton: {
      styleOverrides: { root: { borderRadius: radius.control } },
    },
    MuiPaper: {
      styleOverrides: {
        root: { backgroundImage: 'none' },
        rounded: { borderRadius: radius.surface },
      },
    },
    MuiCard: { styleOverrides: { root: { borderRadius: radius.surface } } },
    MuiPopover: {
      defaultProps: { transitionDuration: motion.popover },
      styleOverrides: { paper: { borderRadius: radius.overlay } },
    },
    MuiMenu: {
      defaultProps: { transitionDuration: motion.popover },
      styleOverrides: { paper: { borderRadius: radius.overlay } },
    },
    MuiDialog: {
      defaultProps: { transitionDuration: motion.dialog },
      styleOverrides: { paper: { borderRadius: radius.overlay } },
    },
    MuiDrawer: {
      defaultProps: { transitionDuration: motion.drawer },
    },
    MuiTooltip: {
      defaultProps: { enterDelay: 300, enterNextDelay: 0, TransitionProps: { timeout: 125 } },
    },
    MuiCollapse: {
      defaultProps: { timeout: motion.collapse, easing: motion.easeOut },
    },
    // One expandable style app-wide (see components/layout/Disclosure).
    MuiAccordion: {
      defaultProps: { disableGutters: true, elevation: 0 },
      styleOverrides: {
        root: ({ theme: t }) => ({
          border: `1px solid ${t.vars.palette.divider}`,
          borderRadius: radius.surface,
          backgroundColor: t.vars.palette.background.paper,
          '&:before': { display: 'none' },
          '&:first-of-type, &:last-of-type': { borderRadius: radius.surface },
        }),
      },
    },
    MuiAccordionSummary: {
      styleOverrides: {
        root: { minHeight: 48, borderRadius: radius.surface },
        content: { margin: '10px 0', minWidth: 0 },
      },
    },
    MuiOutlinedInput: {
      styleOverrides: {
        root: ({ theme: t }) => ({
          borderRadius: radius.control,
          '&:not(.Mui-focused):not(.Mui-error):not(.Mui-disabled) .MuiOutlinedInput-notchedOutline': {
            borderColor: t.vars.palette.rdf.control,
          },
        }),
      },
    },
    MuiTextField: { defaultProps: { size: 'small' } },
    MuiSelect: { defaultProps: { size: 'small' } },
    MuiChip: { styleOverrides: { root: { fontWeight: 500, borderRadius: radius.control } } },
    MuiAlert: { styleOverrides: { root: { borderRadius: radius.surface } } },
    MuiListItemButton: { styleOverrides: { root: { borderRadius: radius.control } } },
    MuiTabs: { styleOverrides: { root: { minHeight: 40 } } },
    MuiTab: { styleOverrides: { root: { minHeight: 40, textTransform: 'none', fontWeight: 500 } } },
    MuiStepLabel: {
      styleOverrides: {
        label: ({ theme: t }) => ({
          fontSize: '0.875rem',
          fontWeight: 400,
          '&.Mui-active': { fontWeight: 600, color: t.vars.palette.text.primary },
          '&.Mui-completed': { fontWeight: 500 },
        }),
      },
    },
    MuiTableCell: {
      styleOverrides: {
        head: ({ theme: t }) => ({ fontSize: '0.75rem', fontWeight: 600, color: t.vars.palette.text.secondary }),
      },
    },
    // Toasts sit above the fixed WorkflowNav instead of covering Back/Continue.
    MuiSnackbar: {
      styleOverrides: {
        anchorOriginBottomCenter: { bottom: NAV_SNACKBAR_OFFSET.xs, '@media (min-width:600px)': { bottom: NAV_SNACKBAR_OFFSET.sm } },
        anchorOriginBottomLeft: { bottom: NAV_SNACKBAR_OFFSET.xs, '@media (min-width:600px)': { bottom: NAV_SNACKBAR_OFFSET.sm } },
        anchorOriginBottomRight: { bottom: NAV_SNACKBAR_OFFSET.xs, '@media (min-width:600px)': { bottom: NAV_SNACKBAR_OFFSET.sm } },
      },
    },
  },
});
