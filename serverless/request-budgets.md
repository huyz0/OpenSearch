# Object-store request budgets

What the shell's key paths cost in object-store requests and in sequential round trips, pinned so that a change which
adds one fails a test. The gate tests read this table from the test classpath (`RequestBudgets`); no number here is
repeated in a test.

Run the gates with the rest of the testkit suite, or alone:

```
./gradlew :serverless:testkit:test --tests '*RequestBudget*' --tests '*IdleIndexRequestTests'
```

## The rules

- **Every row is `enforced` or `blocked`.** An enforced row names the test that checks it as `Class#method`; a blocked row
  names what blocks it. There is no third status: a third status is how "we aim for zero" gets written by someone who
  measured nothing. `RequestBudgetTableTests` checks this, and that each enforced row's test passes the row's key.
- **`exact` rows ratchet.** A measurement above the budget is a regression; below it, the test fails too and asks for the
  budget to be lowered, so a saving is recorded in this table and cannot be lost silently. Raising a budget is allowed
  when the new requests are worth paying: change the row in the same pull request and say why there.
- **`max` rows are ceilings,** for measurements that vary between runs by a known amount: round-trip depth (a parallel
  pair can be seen as one round apart when the machine is loaded) and the first take of several shards, whose
  concurrent rollup marks batch differently from run to run.
- **Requests are counted as S3 bills them.** A compare-and-swap is a GET and a conditional PUT (two requests, two round
  trips); an existence check is a HEAD, billed as a GET; a LIST counts once per thousand names asked for. Counts come from
  `CountingBlobStore` on a filesystem store and a simulated clock; per-index lines come from the shell's own
  `StoreUsage` attribution.
- **Round trips** are the depth of the chain a caller waits through (`RoundTripDepthBlobStore`): requests sent together
  share a round, a request sent after another's answer is the next one. Each request is held 50 ms so that requests
  sent together overlap.

To see where a count's requests go, record instead of checking; the tests then log each measurement with a by-caller
breakdown, and the requests in the order they were sent, each with its round and who sent it:

```
./gradlew :serverless:testkit:test --tests '*RequestBudgetTests' -Dtests.request_budgets.record=true -Dtests.output=true
```

## Budgets

The lifecycle cases run on one writer holding four one-shard indices, then one search node.

| Objective | Case | Budget | Bound | Status | Gate |
|---|---|---|---|---|---|
| `take.fresh.requests` | First take of four never-written shards | 50 | max | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `write.first.requests` | The node's first write after the take: opens its log epoch | 4 | exact | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `write.first.round_trips` | Round trips that first write waits through | 5 | max | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `write.next.requests` | The next write to the same shard, inside the descriptor fence window: one log PUT | 1 | exact | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `write.next.round_trips` | Round trips that write waits through | 1 | max | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `publish.requests` | One write to each of the four shards, then a publish of all four | 23 | exact | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `held_idle.index_requests_per_hour` | An hour with the four shards held and nothing written or searched: requests charged to the indices | 64 | exact | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `held_idle.node_requests_per_hour` | The same hour: requests charged to the node (lease renewal, stored-script marker, sweeps) | 1227 | exact | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `held_idle.index_requests_target` | An idle held index makes no periodic request | 0 | exact | blocked | Head verification reads each held shard's head every 5 minutes (12 per shard-hour), the 5-minute descriptor refresh backstop reads each descriptor (12 an hour in service; the simulated hour sees one, because the backstop skips a descriptor read recently by the wall clock), and the orphan sweep reads each descriptor about 3 times an hour. Zero needs head checks carried by the lease or a change feed, and the backstop replaced by the change stripes |
| `release.requests` | Releasing the four idle shards | 43 | exact | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `take.released.requests` | Taking one shard its writer released cleanly | 13 | exact | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `take.released.round_trips` | Round trips the reconcile pass that takes it waits through: ten for the take, the rest its own lease renewal and stored-script check | 13 | max | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `search.cold.requests` | A search node opens a shard it never served and runs one search | 7 | exact | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `search.cold.round_trips` | Round trips that open and first search wait through | 3 | max | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `search.warm.requests` | The next search on that shard | 0 | exact | enforced | `RequestBudgetTests#testIndexLifecycleBudgets` |
| `search.held_idle.index_requests_target` | An idle shard a search node holds open makes no periodic request | 0 | exact | blocked | Not measured: a search node's reader checks run on its own wall-clock timers, which the simulated clock does not drive. Needs a reader-side meter on the simulated clock |
| `idle_released.node_requests_per_hour` | An hour of a writer's periodic work once four written indices were released | 1168 | exact | enforced | `IdleIndexRequestTests#testIdleIndicesMakeNoPeriodicRequests` |
