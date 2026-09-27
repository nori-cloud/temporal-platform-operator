#!/usr/bin/env bash
set -euo pipefail

for file in "$@"; do
  if [[ -f "$file" ]] && ! grep -q '^# yamllint disable-file$' "$file"; then
    tmp="${file}.tmp"
    {
      printf '%s\n' '# yamllint disable-file'
      cat "$file"
    } > "$tmp"
    mv "$tmp" "$file"
  fi
done
