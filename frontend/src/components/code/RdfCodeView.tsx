import React, { useEffect, useRef, useState } from 'react';
import Box from '@mui/material/Box';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import { useTheme } from '@mui/material/styles';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import { EditorState, type Extension } from '@codemirror/state';
import { EditorView, lineNumbers, highlightActiveLine, keymap } from '@codemirror/view';
import { foldGutter, syntaxHighlighting, defaultHighlightStyle, bracketMatching } from '@codemirror/language';
import { searchKeymap, highlightSelectionMatches } from '@codemirror/search';
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
import { json } from '@codemirror/lang-json';
import { turtle } from 'codemirror-lang-turtle';

export type RdfCodeMode = 'turtle' | 'json' | 'auto';

// Detect JSON-LD by content (the language prop in the app is sometimes a human label, not a
// syntax mode), otherwise treat as Turtle/TriG/SPARQL.
function resolveMode(value: string, mode: RdfCodeMode): 'turtle' | 'json' {
  if (mode !== 'auto') return mode;
  const head = value.trimStart()[0];
  return head === '{' || head === '[' ? 'json' : 'turtle';
}

// Syntax-highlighted, read-only viewer for RDF/Turtle/TriG/JSON-LD/SPARQL previews. Replaces
// the plain monospace <pre>. Line numbers, code folding, search (Cmd/Ctrl-F), bracket matching,
// and a copy button. Theme-aware (light/dark) via MUI tokens.
export function RdfCodeView({ value, mode = 'auto', maxHeight = 360 }: { value?: string; mode?: RdfCodeMode; maxHeight?: number }) {
  const theme = useTheme();
  const host = useRef<HTMLDivElement | null>(null);
  const view = useRef<EditorView | null>(null);
  const [copied, setCopied] = useState(false);
  const text = value ?? '';
  const dark = theme.palette.mode === 'dark';

  useEffect(() => {
    if (!host.current) return;
    const lang = resolveMode(text, mode) === 'json' ? json() : turtle();
    const editorTheme = EditorView.theme(
      {
        '&': {
          backgroundColor: dark ? '#0b1220' : '#0f172a',
          color: '#e2e8f0',
          borderRadius: '8px',
          fontSize: '12.5px',
          maxHeight: `${maxHeight}px`,
        },
        '.cm-scroller': { overflow: 'auto', fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace', lineHeight: 1.6 },
        '.cm-gutters': { backgroundColor: 'transparent', color: '#64748b', border: 'none' },
        '.cm-activeLine': { backgroundColor: 'rgba(148,163,184,0.08)' },
        '.cm-activeLineGutter': { backgroundColor: 'transparent' },
        '.cm-selectionBackground, &.cm-focused .cm-selectionBackground': { backgroundColor: 'rgba(96,165,250,0.25)' },
        '.cm-content': { padding: '10px 4px' },
      },
      { dark: true },
    );
    const extensions: Extension[] = [
      lineNumbers(),
      foldGutter(),
      highlightActiveLine(),
      highlightSelectionMatches(),
      bracketMatching(),
      history(),
      syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
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
  }, [text, mode, dark, maxHeight]);

  const copy = () => {
    navigator.clipboard?.writeText(text).then(() => {
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    });
  };

  return (
    <Box sx={{ position: 'relative' }}>
      <Box ref={host} sx={{ '& .cm-editor': { borderRadius: 2 }, '& .cm-editor.cm-focused': { outline: 'none' } }} />
      <Tooltip title={copied ? 'Copied' : 'Copy'}>
        <IconButton
          size="small"
          aria-label="Copy code"
          onClick={copy}
          sx={{ position: 'absolute', top: 6, right: 6, color: '#94a3b8', bgcolor: 'rgba(15,23,42,0.6)', '&:hover': { bgcolor: 'rgba(15,23,42,0.85)' } }}
        >
          {copied ? <CheckIcon sx={{ fontSize: 16 }} /> : <ContentCopyIcon sx={{ fontSize: 16 }} />}
        </IconButton>
      </Tooltip>
    </Box>
  );
}
