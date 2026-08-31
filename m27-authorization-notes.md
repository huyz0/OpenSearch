# M27 — A plugin can say no

Every milestone since M24 ended with the same sentence: the shell authenticates and does not authorize.
Privilege evaluation in OpenSearch is keyed on action names at the `ActionFilter` layer, and §6.3 left that
layer unbuilt, so a plugin could learn who was calling and could do nothing about what they then did. That
was the largest remaining gap in the plugin host, named as such twice.

It is closed. A plugin's `ActionFilter`s now run, unmodified, against every operation the shell performs.

## What was actually needed, and what was not

An `ActionFilter` needs two things: an action name and a request describing what is about to happen. It
does **not** need a `TransportAction`, an `ActionModule`, an action registry, or any of the `action/`
package §6.3 declined to build. Both of the things it needs can be produced at the handful of places where
the shell does work.

[`ActionGate`](serverless/shell/src/main/java/org/opensearch/serverless/shell/ActionGate.java) builds the
chain, drives it, and runs the operation as the chain's terminal step. So a filter that refuses before
calling `chain.proceed` prevents the work, and one that wraps the listener runs after it.

**What a filter does not get is the response.** The shell's operations return their own types — a `Read`, a
`SearchOutcome` — and manufacturing an `ActionResponse` to carry them would be lossy invention. The
listener is completed with a marker saying only that the work was done. **A filter that inspects or
rewrites responses will not work here; one that decides whether a caller may proceed will**, and that is
the one authorization is made of.

Indices are reported to filters as *unresolved*, via `ActionRequestMetadata.empty()`. That is the truth
rather than a shortcut, for the reasons in `RefusingIndexNameExpressionResolver`: a node cannot resolve an
index expression here, and a filter told "unknown" can decide what to do about it instead of being handed a
subset that looks complete.

## The demonstration

One node, two plugins: the shell's own authentication, and a plugin whose entire contribution is a single
`ActionFilter` that reads the authenticated principal from the thread context and refuses writes to anyone
who is not the administrator.

The administrator writes. The reader reads what the administrator wrote, and is refused the write — with
the plugin's own status and the plugin's own words reaching the caller, because nothing in the gate
reinterprets a filter's exception. And the refused write is asserted **not to have happened**, not merely to
have been reported on.

**The thread-context hop is the part that could quietly not have worked.** The principal is left by the REST
wrapper on the thread that read the request, and every operation is dispatched to another pool before the
filter runs. Had transients not survived that hop, the filter would have seen no caller and been forced to
fail open or closed for everybody — so the test asserts on *who* was refused, not that something was.

## Writing the coverage table found two holes

The gate is not in one place. It is in `ShardOperations` for the operations that funnel through it, and in
`DocumentHandler`, `SearchHandler`, `BulkHandler` and `IndexAdminHandler` for the paths that predate it and
carry their own routing and response vocabulary.

Two of those call sites exist **because the coverage table was written and came up short**: the REST
single-document write path and the REST search path both went around `ShardOperations`, so the first
version of this work produced a filter that saw a plugin's internal reads and none of the user's traffic.
Authorization that guards everything except what users actually send is worse than none, because it looks
like it is working.

That is why `testEveryDataEndpointReachesAFilter` exists: it drives every endpoint that reads or changes
data and asserts an action name reached a filter for each. Adding an endpoint means adding a row. The
failure mode it guards against is silent — an ungated endpoint works perfectly for everybody.

| Endpoint | Action |
| --- | --- |
| `PUT /{index}/_doc/{id}` | `indices:data/write/index` |
| `DELETE /{index}/_doc/{id}` | `indices:data/write/delete` |
| `GET /{index}/_doc/{id}` | `indices:data/read/get` |
| `POST /{index}/_search` | `indices:data/read/search` |
| `POST /_bulk` | `indices:data/write/bulk` |
| `PUT /{index}` | `indices:admin/create` |
| `GET /{index}` | `indices:admin/get` |
| `DELETE /{index}` | `indices:admin/delete` |

**Bulk arrives as a bulk**, once, under `indices:data/write/bulk`, carrying a `BulkRequest` built from the
items that survived routing. Filtering each document under `indices:data/write/index` instead would have
been stricter, and would have shown a filter an action name it never registered for — which, for a filter
that only guards bulk, is a hole rather than a difference.

## A pre-existing bug the assertion exposed

`IndexAdminHandler` executed inline on the transport thread. Adding the gate there tripped OpenSearch's
`Expected current thread [...transport_worker...] to not be a transport thread` — and the handler had
**already** been doing object-store IO on the HTTP event loop, invisibly, because blob IO does not assert
about it the way a future does. Creating and deleting an index now dispatch like every other handler.

Deleting an index also happened while *preparing* the request, before the channel consumer ran. That is
wrong independently of filters — it deleted the index and then asked — and it is now inside the consumer,
after the gate.

## A hazard worth writing down

A plugin's own client calls carry the **current caller's identity**. The authentication plugin reads the
account index through the client on the request's own thread, so a filter that denied reads of that index
would lock everybody out, including the administrator who would have to fix it.

Classic OpenSearch handles this with a system context that its security plugin recognises. The shell has
half of that already — `IdentityService#getPluginSubject(plugin).runAs(...)` stashes the context, and works
— but there is no shared convention for what a filter should make of a call with no principal, and
inventing one here would be a design decision dressed as a bug fix. It is documented rather than guessed
at.

## Canaries

| Defect | Caught by |
| --- | --- |
| The REST write path skips the gate | 2 tests |
| A filter's refusal is ignored and the work runs anyway | 2 tests |

## What is still missing

- **Filters see requests, not responses.** Response-shaping filters — document-level security, field
  redaction — cannot work. Only admission decisions can.
- **Node-to-node forwarding still carries no identity**, so filters run on the coordinating node only.
  That is the right place, but it means the transport port remains trusted infrastructure.
- **`CatalogHandler`** (`/_serverless/shards/{index}`) is ungated. It reveals which nodes hold which shards
  and nothing about documents, and there is no core action name that honestly describes it.
- **No system-privilege convention** for a plugin acting as itself, as above.
- **The real OpenSearch Security plugin has still not been pointed at this.** It now has a seam to run in,
  which it did not before, so the spike is finally worth doing — and it should report what breaks rather
  than be scheduled as a milestone.

245 tests green across `test` (210), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched; §4 rule 1 verified.
