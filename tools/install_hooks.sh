#!/bin/sh
# Activates the secret-scan hooks (.githooks) for this clone. Run once after cloning.
set -eu
cd "$(git rev-parse --show-toplevel)"
current="$(git config --get core.hooksPath || true)"
if [ -n "$current" ] && [ "$current" != ".githooks" ]; then
  echo "core.hooksPath is already set to $current; leaving it unchanged." >&2
  exit 0
fi
git config core.hooksPath .githooks
echo "secret-scan hooks active (pre-commit, commit-msg, pre-push)."
