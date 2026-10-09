#!/usr/bin/env bash
# Runs the Kyverno CLI tests for Chronicle's healing guardrails.
#
# `kyverno test` (1.19) exits 0 when a policy fails to load: every expectation
# is reported as "Skip / Invalid Policy" and nothing fails. So a skipped or
# invalid result is treated as a failure here, not only a failed one.
set -uo pipefail

cd "$(dirname "$0")/.."
command -v kyverno >/dev/null || { echo "kyverno CLI not found (brew install kyverno)" >&2; exit 2; }

out="$(kyverno test deploy/kyverno/test --detailed-results --remove-color 2>&1)"
status=$?
echo "$out" | grep -E 'Test Summary'

fail() { echo "kyverno guardrail tests FAILED: $1" >&2; echo "$out" | grep -E '│ (Fail|Skip) ' >&2; exit 1; }
[ "$status" -eq 0 ] || fail "kyverno test exited $status"
echo "$out" | grep -q 'Invalid Policy' && fail "the policy did not load"
echo "$out" | grep -qE '│ Skip +│' && fail "an expectation was skipped"
echo "$out" | grep -qE '[1-9][0-9]* tests failed' && fail "an expectation failed"
echo "$out" | grep -q 'Test Summary' || fail "no test summary"
echo "kyverno guardrail tests passed"
