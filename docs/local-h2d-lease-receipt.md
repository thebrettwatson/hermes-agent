# H2-D Lease Adjudication Receipt

**Branch:** `local/hermes-integration-route-h1-botmode-20260828`
**Date:** 2026-08-28
**Scope:** Parent H2-D adjudication — durable SessionDB lease for routed Gemini composition

## Decision

**Do not add a new lease schema** on this integration branch.

## Evidence

- A durable SessionDB lease already exists:
  - Cross-process SQLite row keyed by `conversation_id`
  - Owner-checked release
  - Expiry and refresh semantics
- Existing tests: **31 passed** via Homebrew pytest (SessionDB lease suite).
- A two-process OS-level contention test now exists in `tests/state/test_session_turn_lease_subprocess.py` (tmp `state.db` only; no live gateway mutation).

## Residual

**ACCEPTED_RESIDUAL** — routed Gemini composition is not proven against live gateway `state.db` instances. The subprocess contention test covers cross-process lease serialization on an isolated database; no H2 lease/mutex schema changes accompany it.

## Related work on this branch

Bot Mode alias-map custody (`tools/bot_mode_dm.py`, `tools/bot_mode_probe.py`) is landed separately from H2 lease work. No lease schema changes accompany that custody commit.
