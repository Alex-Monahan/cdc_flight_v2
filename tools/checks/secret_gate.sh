#!/bin/sh
# P1 check: commits that carry a planted fake credential are rejected by the secret-scan hooks,
# in the staged content (pre-commit) and in the commit message (commit-msg). Nothing stays behind.
# The fake values are assembled at run time so that this file holds nothing that looks like a credential.
set -u
cd "$(git rev-parse --show-toplevel)"
git diff --cached --quiet || { echo "the index has staged changes; run this with a clean index" >&2; exit 2; }
head_before=$(git rev-parse HEAD)
fake_token="gh""p_$(printf 'A1b2C3d4E5f6%.0s' 1 2 3)"
fake_key="-----BEGIN RSA PRIVATE"" KEY-----"
file="planted_fake_credential_$$.txt"
rc=0
cleanup() {
  git reset -q -- "$file" 2>/dev/null
  rm -f "$file"
  if [ "$(git rev-parse HEAD)" != "$head_before" ]; then git reset -q --mixed "$head_before"; fi
}
trap cleanup EXIT

try_content() {  # $1 = label, $2 = content that must be rejected
  printf '%s\n' "$2" > "$file"
  git add -- "$file"
  if git commit -q -m "planted fake credential: must be rejected" >/dev/null 2>&1; then
    echo "FAIL  $1: the commit was accepted"; rc=1
    git reset -q --mixed "$head_before"
  else
    echo "PASS  $1: the commit was rejected"
  fi
  git reset -q -- "$file"; rm -f "$file"
}

try_content "staged file with a token-format string" "token for the test: $fake_token"
try_content "staged file with a private-key block" "$fake_key"

if git commit -q --allow-empty -m "message carrying $fake_token" >/dev/null 2>&1; then
  echo "FAIL  commit message with a token-format string: the commit was accepted"; rc=1
else
  echo "PASS  commit message with a token-format string: the commit was rejected"
fi

[ "$(git rev-parse HEAD)" = "$head_before" ] && echo "PASS  HEAD is unchanged" || { echo "FAIL  HEAD moved"; rc=1; }
echo "hooks directory in use: $(git config --get core.hooksPath | sed -E "s#^$HOME#~#; s#^(~/)\.claude/.*#outside the repository (pinned copy)#")"
[ "$rc" = 0 ] && echo "RESULT: PASS" || echo "RESULT: FAIL"
exit "$rc"
