import React, { useEffect, useRef, useState } from 'react';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import { alpha, useTheme } from '@mui/material/styles';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import { EditorState, type Extension } from '@codemirror/state';
import { EditorView, lineNumbers, highlightActiveLine, keymap } from '@codemirror/view';
import { foldGutter, syntaxHighlighting, bracketMatching, HighlightStyle } from '@codemirror/language';
import { searchKeymap, highlightSelectionMatches } from '@codemirror/search';
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
import { json } from '@codemirror/lang-json';
import { tags } from '@lezer/highlight';
import { turtle } from 'codemirror-lang-turtle';
import { fonts, radius } from '../../theme/tokens';

export type RdfCodeMode = 'turtle' | 'json' | 'auto';

// Detect JSON-LD by content (the language prop in the app is sometimes a human label, not a
// syntax mode), otherwise treat as Turtle/TriG/SPARQL.
function resolveMode(value: string, mode: RdfCodeMode): 'turtle' | 'json' {
  if (mode !== 'auto') return mode;
  const head = value.trimStart()[0];
  return head === '{' || head === '[' ? 'json' : 'turtle';
}

// Syntax-highlighted, read-only viewer for RDF/Turtle/TriG/JSON-LD/SPARQL previews. Line numbers,
// code folding, search (Cmd/Ctrl-F), bracket matching and a copy button. Every colour comes from
// the active colour scheme's palette tokens (DESIGN.md §5), so light and dark stay legible; the
// editor is rebuilt when the scheme changes.
export function RdfCodeView({ value, mode = 'auto', maxHeight = 360 }: { value?: string; mode?: RdfCodeMode; maxHeight?: number }) {
  const theme = useTheme();
  const host = useRef<HTMLDivElement | null>(null);
  const view = useRef<EditorView | null>(null);
  const [copied, setCopied] = useState(false);
  const text = value ?? '';
  const dark = theme.palette.mode === 'dark';

  useEffect(() => {
    if (!host.current) return;
    const { palette } = theme;
    const lang = resolveMode(text, mode) === 'json' ? json() : turtle();
    // Token roles: keywords and prefixed names in ink blue, IRIs and datatypes in info, literals
    // in success, numbers/booleans in warning, punctuation and comments in secondary text.
    const highlighting = HighlightStyle.define([
      { tag: tags.keyword, color: palette.primary.main, fontWeight: 600 },
      { tag: [tags.namespace, tags.propertyName], color: palette.primary.main },
      { tag: [tags.url, tags.typeName, tags.link], color: palette.info.main },
      { tag: [tags.string, tags.special(tags.string)], color: palette.success.main },
      { tag: [tags.number, tags.integer, tags.float, tags.bool, tags.null, tags.atom], color: palette.warning.main },
      { tag: [tags.comment, tags.annotation, tags.separator, tags.squareBracket, tags.brace, tags.punctuation], color: palette.text.secondary },
      { tag: tags.invalid, color: palette.error.main },
    ]);
    const editorTheme = EditorView.theme(
      {
        '&': {
          backgroundColor: palette.rdf.subtle,
          color: palette.text.primary,
          border: `1px solid ${palette.divider}`,
          borderRadius: radius.surface,
          fontSize: '0.8125rem',
          maxHeight: `${maxHeight}px`,
        },
        '.cm-scroller': { overflow: 'auto', fontFamily: fonts.mono, lineHeight: 1.6 },
        '.cm-gutters': { backgroundColor: 'transparent', color: palette.text.secondary, border: 'none' },
        '.cm-activeLine': { backgroundColor: palette.action.hover },
        '.cm-activeLineGutter': { backgroundColor: 'transparent' },
        '.cm-selectionBackground, &.cm-focused .cm-selectionBackground': { backgroundColor: alpha(palette.primary.main, 0.2) },
        '&.cm-focused .cm-matchingBracket': { backgroundColor: alpha(palette.primary.main, 0.2) },
        '&.cm-focused .cm-nonmatchingBracket': { backgroundColor: alpha(palette.error.main, 0.2) },
        '.cm-content': { padding: '8px 4px' },
        // Keyboard focus (the scrollable code area is a Tab stop in most browsers) gets the app's
        // focus ring on the whole viewer, gutters included; pointer focus stays unmarked.
        '&.cm-focused': { outline: 'none' },
        '&:has(:focus-visible)': { outline: `2px solid ${palette.primary.main}`, outlineOffset: '2px' },
        '.cm-scroller:focus-visible, .cm-content:focus-visible': { outline: 'none' },
      },
      { dark },
    );
    const extensions: Extension[] = [
      lineNumbers(),
      foldGutter(),
      highlightActiveLine(),
      highlightSelectionMatches(),
      bracketMatching(),
      history(),
      syntaxHighlighting(highlighting),
      lang,
      keymap.of([...defaultKeymap, ...historyKeymap, ...searchKeymap]),
      EditorState.readOnly.of(true),
      EditorView.editable.of(false),
      editorTheme,
    ];
    const state = EditorState.create({ doc: text, extensions });
    view.current = new EditorView({ state, parent: host.current });
    return () => {
      view.current?.destroy();
      view.current = null;
    };
    // The palette only changes with the colour scheme, which `dark` tracks.
  }, [text, mode, dark, maxHeight]);

  const copy = () => {
    navigator.clipboard?.writeText(text).then(() => {
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    });
  };

  return (
    <Box sx={{ position: 'relative', minWidth: 0 }}>
      <Box ref={host} />
      <Tooltip title={copied ? 'Copied' : 'Copy'}>
        <IconButton
          size="small"
          aria-label="Copy code"
          onClick={copy}
          sx={{ position: 'absolute', top: 8, right: 8, color: 'text.secondary', bgcolor: 'background.paper', border: 1, borderColor: 'divider', '&:hover': { bgcolor: 'background.paper', color: 'text.primary' } }}
        >
          {copied ? <CheckIcon sx={{ fontSize: 16 }} /> : <ContentCopyIcon sx={{ fontSize: 16 }} />}
        </IconButton>
      </Tooltip>
    </Box>
  );
}
