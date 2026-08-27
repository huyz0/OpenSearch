# Phase 7 — the allowlisted surface

- Code: `serverless/shell/src/main/java/org/opensearch/serverless/rest/`
- Tests: `ServerlessAdminRestTests` (4)
- Result: **65 tests, 0 failures**; `check` green on both projects.

> Built out of order — phase 8 came first. That is recorded rather than tidied away, because phase 8's
> gossip gate cited "evidence from phases 1–7" it did not have. See the addendum at the end.

## The surface

Every endpoint is one or two object-store reads. None consults a cluster-manager, because there isn't
one.

| Endpoint | What it is |
|---|---|
| `PUT /{index}?shards=N` | one put-if-absent; body is the mapping |
| `GET /{index}` | one register read |
| `DELETE /{index}` | heads then descriptor |
| `GET /_serverless/indices` | a listing |
| `GET /_serverless/shards/{index}` | shard-heads plus manifest state |
| `GET /_serverless/nodes` | live leases |

Creation cost does not depend on how many indices exist. That is the point of the whole metadata plane:
`plan-area-h-metadata-off-cluster-state.md` measured classic creation at 7.4 ms with 200 indices present
and 98.8 ms at 6,000, because `Metadata.Builder.build()` rebuilds name lookups across every index on
every create.

## The names are different on purpose

`_cat/shards` and `_cluster/state` promise a cluster-wide answer computed by whoever is asked. These
promise a read of the object store, which is a weaker and truer claim:

- `/_serverless/nodes` reports `"source": "live_leases_observed_by_this_node"`. Two nodes may return
  different answers and neither is wrong — membership is derived, not agreed (§10.1).
- `/_serverless/shards/{index}` distinguishes **`never_activated`** from **`unowned`**. A
  pre-provisioned shard nobody has written to has no head at all and costs nothing, and collapsing that
  into "unassigned" would lose the distinction that makes lazy heads worth having.

## Three states that are refusals, not empty answers

| Situation | Answer |
|---|---|
| Index does not exist | 404 `index_not_found` |
| Index already exists | 400 `index_already_exists` |
| No metadata plane configured | 503 `no_metadata_plane` |
| Classic endpoint (`_cluster/health`, `_cat/*`, `_nodes`) | 501, with the reason |

The third is the one worth naming: a node with no metadata plane could plausibly answer "no indices".
It answers 503 instead, because "I cannot tell you" and "there are none" are different facts and
`HANDOFF.md` records eight bugs that came from conflating them.

## Canaries — both D2 properties made to fail

| Canary | Failure produced |
|---|---|
| A missing index returns 200 with an empty body | `expected:<404> but was:<200>` |
| Stop registering the classic-endpoint refusals | `classic endpoint /_cluster/health stopped refusing expected:<501> but was:<400>` |

The second is why the 501s are registered handlers rather than left to the router: without them the
path still fails, but with 400 "no handler for uri", which reads like a typo rather than a decision.

## Route shadowing — checked, because `/{index}` is a wildcard

Registering `GET /{index}` could plausibly have swallowed `/`, `/_serverless/health` and the 501s. A
test asserts all of them still answer as before. This is the kind of regression that would otherwise be
found by a user typing a URL.

## What phase 7 does NOT establish

- **No document APIs.** No `POST /{index}/_doc`, no `_bulk`, no `_search` over REST. Writes still go
  directly to `IndexShard` in tests, and search fan-out is the `action/` layer, deferred since phase 4.
  The admin surface is complete; the data surface is not.
- **No auth, no multi-tenancy.** Anyone who can reach the port can delete an index.
- **`PUT /{index}` takes the mapping as a raw body** and shards as a query parameter, rather than the
  classic `{"settings":…,"mappings":…}` envelope. Deliberate — D2 says this is not a drop-in — but it
  means an existing client cannot create an index here.
- **Delete does not stop a live writer.** `DELETE /{index}` removes heads and the descriptor; a node
  currently holding the shard finds out on its next heartbeat. There is no drain.
- **Nothing about S3 or GCS** (D5/R11), unchanged.

## Addendum: what this changes about phase 8's gossip gate

Phase 8 measured 3.0 object-store operations per reconciliation tick per shard and noted that phase 7's
REST traffic was absent from the evidence §10.3 asked for. With phase 7 built, the honest update is that
**it does not move the number**: these endpoints are operator-rate, not request-rate — an operator
creating an index is not a per-second cost, and none of them runs on a data path. The steady-state
polling cost is still dominated by per-shard lease renewal, and batching that (§7) is still the
mitigation that comes before gossip.

What remains genuinely missing from that evidence is the data surface — `_bulk` and `_search` over
REST — which does not exist in any phase yet.
