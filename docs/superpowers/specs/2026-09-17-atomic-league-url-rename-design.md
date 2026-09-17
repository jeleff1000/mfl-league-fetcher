# Atomic League URL Rename Design

**Date:** 2026-09-17
**Status:** Approved

## Problem

The settings-page URL rename currently performs many independent SQL writes from the web request. A partial rename can therefore split one logical league across two `db_name` values. That happened to Agusta Fantasy League: historical facts remain under `agustafantasyleague`, while current-season facts, draft history, registry records, and the public page use `agusta_fantasy_league`. A weekly update then sees only the target key and can rebuild all-time outputs from a single season.

A rename is an identity operation, not a copy, merge, or re-import. It must preserve the league's history, provider renewal chains, credentials, payment entitlement, aliases, and user configuration while changing the canonical URL key everywhere.

## Required outcome

Renaming a league in Settings queues one idempotent server-side operation. On success:

- every canonical league row uses the new `db_name`;
- provider credentials and renewal-chain registries resolve to the new `db_name`;
- paid/grandfathered eligibility and user configuration remain attached;
- generated aggregates are rebuilt from the complete history;
- the old URL redirects to the new URL;
- weekly updates use the renamed URL, correct credentials, and complete chain;
- retries cannot duplicate rows or lose history.

Agusta is the production acceptance case. Its canonical result is `agusta_fantasy_league`, with Yahoo history from 2009–2018, Sleeper history from 2019–2026, and all existing aliases/configuration retained.

## Architecture

### 1. Settings queues; the worker owns the mutation

The frontend validates the requested slug and dispatches a public-worker `league_admin_rename` event. It no longer discovers tables or issues rename SQL itself. The response contains an operation ID and run URL; the settings UI shows a pending state and routes to the new URL only after the operation commits.

The public worker invokes one specialized Fly rename endpoint. It does not fetch provider data, download league snapshots, or run an import.

### 2. Rename only the canonical table registry

The Fly endpoint uses `canonical_table_registry()` as its allowlist. Backup, quarantine, staging, and ad-hoc tables are never scanned or mutated. This prevents the broad writes that made the failed recovery slow and unsafe.

Tables are handled by role:

- **Year-scoped source tables:** move source-only year partitions to the target. When a partial prior rename left the same year on both keys, the target partition wins and the stale source partition is removed.
- **Identity/config tables:** merge by their stable keys, preferring an existing target row and otherwise moving the source row.
- **Generated league/career/homepage tables:** clear both source and target outputs, then rebuild the target from the consolidated canonical facts using the existing shared aggregation functions.

The operation records before/after manifests and verifies that the target contains the union of source and target history, expected identities are present, and no canonical source rows remain.

### 3. Preserve control-plane identity

The operation updates the corresponding `___ops` records by stable provider identity, not by display name. It preserves and retargets:

- league inventory and canonical `league_db`;
- paid/grandfathered eligibility;
- encrypted Yahoo OAuth credentials;
- encrypted ESPN credentials;
- Sleeper league registry entries;
- import/update jobs, claims, freshness records, and pending paid-import state;
- aliases and manager/franchise configuration.

If the target already exists, it is accepted only when its stable provider identities belong to the same logical league. Conflicting identities abort before publication.

### 4. Idempotency and recovery

The operation ID is deterministic from old key, new key, and requested display name. A durable rename receipt tracks phases and manifests. Retrying the same operation resumes the first incomplete phase.

The data phase and control-plane phase are state-aware: either key may already contain part of the league because of a prior failed rename. Re-execution converges to one target key without copying overlapping rows or discarding target-only data.

The old URL remains resolvable through a durable URL-alias record. Before completion it continues to resolve safely; after completion it redirects to the canonical target URL.

### 5. Weekly-update guardrail

All platform refresh entry points retain the canonical-history preflight. If another alias contains pre-active seasons missing from the target key, the refresh fails before provider fetch or publication with an actionable rename-repair error. Once a rename commits, the same check passes and the weekly worker operates normally on the renamed key.

## Failure behavior

- Invalid slug or conflicting target identity: reject before mutation.
- Fly or worker timeout: operation remains retryable; no import watermark advances.
- Incomplete phase: a retry resumes from the durable receipt and current table state.
- Aggregate rebuild failure: rename is not marked complete and the old URL remains resolvable.
- Credential absence/expiry: rename still preserves the credential record; the next update uses the existing reconnect behavior rather than silently changing auth mode.

No credential values may appear in logs, receipts, or API responses.

## Verification

Automated tests cover normal rename, partial prior rename, overlapping years, target-only draft history, stable-identity conflict, retry after each phase, credential/entitlement preservation, URL redirect, aggregate rebuilding, and weekly-update lookup under the new key.

Production completion for Agusta requires direct Fly evidence that:

- `agusta_fantasy_league` has the complete 2009–2026 chain;
- `agustafantasyleague` has zero canonical league rows;
- draft and current 2026 data remain present;
- one canonical paid inventory record remains;
- Yahoo OAuth and Sleeper registry identities point to the target key;
- aliases and franchise identities remain intact;
- the renamed page shows complete history and all-time aggregates;
- an actual UI-dispatched weekly update resolves the renamed URL and credentials and commits successfully.

