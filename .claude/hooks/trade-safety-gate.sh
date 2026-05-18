#!/bin/bash
# PreToolUse(Bash) gate for futures-live: refuse `git commit` on trade-safety
# critical paths until trade-safety-reviewer agent has written an approval
# marker for the exact staged-diff hash.
#
# Bypass: agent (or operator) writes /tmp/claude-trade-safety-approved-<sha1>
# where <sha1> = sha1(`git diff --cached`). Restaging invalidates the marker.

set -u

input=$(cat)
cmd=$(echo "$input" | jq -r '.tool_input.command // ""')

# Match `git commit` only — not `git commit-tree`, `git log`, etc.
if ! echo "$cmd" | grep -qE '(^|[^a-zA-Z0-9_-])git[[:space:]]+commit([[:space:]]|$)'; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR" 2>/dev/null || exit 0

staged=$(git diff --cached --name-only 2>/dev/null) || exit 0
[ -z "$staged" ] && exit 0

# futures-live critical paths.
critical_re='^(futures_executor/strategy/aggregator\.py|futures_executor/execution/[^/]+\.py|futures_executor/config/settings\.yaml)$'

matches=$(echo "$staged" | grep -E "$critical_re" || true)
[ -z "$matches" ] && exit 0

diff_hash=$(git diff --cached | shasum -a 1 | awk '{print $1}')
marker="/tmp/claude-trade-safety-approved-${diff_hash}"

if [ -f "$marker" ]; then
  exit 0
fi

reason="Trade-safety critical paths in staged diff — agent review required before commit.

Matched files:
${matches}

To proceed: invoke the trade-safety-reviewer agent on the staged diff.
On APPROVED verdict the agent will write:
  ${marker}

Then retry the commit. Re-staging changes the hash and invalidates the marker."

jq -n --arg r "$reason" '{
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    permissionDecision: "deny",
    permissionDecisionReason: $r
  }
}'
exit 0
