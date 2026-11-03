import React, { useLayoutEffect, useState } from 'react';
import TextField, { type TextFieldProps } from '@mui/material/TextField';

type CommitTextFieldProps = Omit<TextFieldProps, 'value' | 'defaultValue' | 'onChange'> & {
  value: string;
  // Called on blur or Enter with the (normalized) text, only when it differs from `value`.
  onCommit: (next: string) => void;
  // Canonical form of the text (e.g. a trimmed, upper-cased comma list) used for the comparison.
  normalize?: (text: string) => string;
};

// A free-text setting that keeps its own draft and saves on blur or Enter; choices elsewhere still
// save immediately. Saving every keystroke lets a late snapshot echo overwrite what is being typed,
// and normalizers (trim, split on commas) would eat characters mid-word.
export function CommitTextField({ value, onCommit, normalize, onBlur, onKeyDown, multiline, ...props }: CommitTextFieldProps) {
  const [draft, setDraft] = useState(value);
  // A new saved value (echo, provider switch, project restore) replaces the draft before paint.
  useLayoutEffect(() => setDraft(value), [value]);
  const commit = () => {
    const next = normalize ? normalize(draft) : draft;
    if (next !== value) onCommit(next);
    // Show what is saved: a changed value replaces this via the layout effect, and a value the
    // parent normalized back (e.g. an emptied default) stays visible instead of the stale draft.
    setDraft(value);
  };
  return (
    <TextField
      {...props}
      multiline={multiline}
      value={draft}
      onChange={(event) => setDraft(event.target.value)}
      onBlur={(event) => { commit(); onBlur?.(event); }}
      onKeyDown={(event) => {
        if (event.key === 'Enter' && !multiline) { event.preventDefault(); commit(); }
        onKeyDown?.(event);
      }}
    />
  );
}
