# RFC: The Serverless Shell — a new node process over the existing data plane


> **Current state:** the per-milestone notes below record what was true when each was written; several of their
> "still missing" lists have since been closed. [`serverless-status.md`](serverless-status.md) is the single
> up-to-date statement of what the shell does, what it deliberately refuses, and what is genuinely missing.

- Status: DRAFT — design record, no implementation attached
- Branch: `feature/serverlessplusplus`, cut from `feature/serverless` at `152bfd87536`
- Amends: nothing. Supersedes the *delivery vehicle* assumed by `rfc-serverless-opensearch.md` §2.5
  ("delivered predominantly as a plugin/module ... core changes stay minimal") and blocks
  `rfc-serverless-control-cell-diet.md` pending the decision here.
- Question answered: given that the seam-carving path has been pushed about as far as it goes,
  what exactly do we build instead — and how much of `server/` survives the change.

---

## 1. The decision, stated once

**Keep the data plane. Replace the shell and the control plane.**

Build a new node process — its own `main`, its own bootstrap, its own wiring, no Guice, no
`Coordinator`, no `GatewayMetaState`, no `AllocationService`, no discovery — that *depends on*
OpenSearch's indexing and search code as a library rather than being installed into it as a plugin.

The thing that makes this cheap rather than heroic, and the central claim of this document:

> **`ClusterState` stays. The protocol that maintains it goes.**

`ClusterState` stops being a consensus object replicated to every node by a publish/diff protocol,
and becomes a **node-local materialized view** — computed from object-store registers that are
themselves mutated by compare-and-swap, with no consensus process anywhere in the system (§9) — that the shell computes from object-store truth and
hands to the data plane through an interface that already exists and is already public. The data
plane cannot tell the difference. That is not an aspiration — §2 is the measurement, and §5 is the
mechanism.

### 1.1 Decisions taken (2026-08-27)

Four questions that measurement cannot answer were put to the owner and are settled. They are recorded
here because later sections now assume them.

| # | Decision | Effect |
|---|---|---|
| D1 | **S0 first, then decide.** No shell work begins until the spike reports. | §11 is a gate, not a first step. If S0 falsifies §5, this RFC is withdrawn and the cell-diet design is approved instead. |
| D2 | **Allowlisted API surface, grown on demand.** Not a drop-in for existing clients or Dashboards. | §6.3 confirmed. Unimplemented endpoints return 501 with a reason — never an empty success. |
| D3 | **Fs, S3 and GCS are targets. Azure is not.** | R11's conformance suite covers three backends. The in-tree Azure register implementation is left alone but untested and unsupported. |
| D4 | **Coupling is interface-mediated, and shared interfaces are preferred over concrete-class reach-through.** `serverless/` may depend on `server`; `server` may gain narrow shared interfaces; `server` may never reference `serverless/`. | Replaces the earlier "`server/` is untouched" rule. See §4 and §8.1. |
| D5 | **R11 (provider CAS conformance) is deferred.** Not cancelled — deferred. | Phases 1 and 2 are unaffected: neither touches an object store. **Phase 3 is the consequence** — it either runs against `FsBlobContainer` only and ships behind a flag that says so, or it waits. Nothing that claims durability on S3 or GCS may ship until R11 closes. |

D4 deserves a word, because it is a third position rather than one of the two originally offered. The
pristine-fork rule protected upstream merges but forced the shell to consume god objects — passing a
whole `ClusterService` into `IndicesService` so it can read two settings. The hard-fork rule fixed the
ergonomics and gave up the merges. D4 takes the ergonomics *and* keeps the merges by making the
coupling an interface: the awkwardness was never the edit, it was depending on a concrete class for a
narrow capability. §8.1 shows the resulting interface set is small, and every member of it is a change
we would be willing to send upstream on its own merits.

## 2. What was measured

Every number and line reference below was read on `152bfd87536`, not assumed from public surface.

### 2.1 The data plane barely knows the control plane exists

| Class | Lines | `ClusterState`/`ClusterService` references | What they actually are |
|---|---|---|---|
| [`IndexShard`](server/src/main/java/org/opensearch/index/shard/IndexShard.java) | 6,767 | 5 | **Zero `clusterService.` calls.** Implements the `IndicesClusterStateService.Shard` *callback* interface ([:294](server/src/main/java/org/opensearch/index/shard/IndexShard.java#L294)); takes a `long applyingClusterStateVersion` — a number, not a state. |
| [`IndexService`](server/src/main/java/org/opensearch/index/IndexService.java) | 2,034 | 6 | **One** real call: `clusterService.getClusterApplierService()` at [:911](server/src/main/java/org/opensearch/index/IndexService.java#L911), passed straight through to the shard. |
| [`IndicesService`](server/src/main/java/org/opensearch/indices/IndicesService.java) | 2,647 | 14 | **Every** call is `getClusterSettings()`/`getSettings()` (:566–:694) — dynamic settings registration. `ClusterState` appears only as a *parameter* on methods the control plane calls inward (:1672, :1792, :1847, :2374). |
| [`SearchService`](server/src/main/java/org/opensearch/search/SearchService.java) | 2,166 | 6 | Settings registration (:536–:607), `localNode().getId()` for task attribution, and one `state().nodes().getMinNodeVersion()` at [:1386](server/src/main/java/org/opensearch/search/SearchService.java#L1386). |

The data plane's dependency on `ClusterService` is, in aggregate, **a settings bus plus local-node
identity**. It is not a dependency on cluster state.

Sizes, for scale: `index/` 212,853 + `search/` 170,087 + `indices/` 33,416 ≈ **416k lines** of data
plane, against `cluster/` 95,710 + `gateway/` 21,068 + `discovery/` 2,298 ≈ **119k lines** of control
plane and ~12k of shell (`node/` 6,579, `bootstrap/` 5,463).

### 2.2 The one real seam is already a value-object interface

`IndexShard.updateShardState(...)` ([:765](server/src/main/java/org/opensearch/index/shard/IndexShard.java#L765)) is the
entire control-plane→shard interface:

```java
void updateShardState(ShardRouting newRouting, long newPrimaryTerm,
                      BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
                      long applyingClusterStateVersion, Set<String> inSyncAllocationIds,
                      IndexShardRoutingTable routingTable, DiscoveryNodes discoveryNodes)
```

Seven value objects. No `ClusterState`, no `ClusterService`, no publish protocol. Anything that can
synthesize a `ShardRouting`, an `IndexShardRoutingTable` and a `DiscoveryNodes` can drive a shard
through its full lifecycle. The shard's only other node-awareness is
`discoveryNodes.get(nodeId).isRemoteStoreNode()` at [:651](server/src/main/java/org/opensearch/index/shard/IndexShard.java#L651).

### 2.3 `ClusterService` is a local machine, not a distributed one

[`ClusterService`](server/src/main/java/org/opensearch/cluster/service/ClusterService.java) is **382 lines**:
a facade over `ClusterManagerService` + `ClusterApplierService`. Its constructor is
`(Settings, ClusterSettings, ThreadPool)` ([:96](server/src/main/java/org/opensearch/cluster/service/ClusterService.java#L96)),
and its `doStart()` starts those two and nothing else — **no coordinator, no discovery, no gateway**.

`ClusterManagerService.doStart()` requires two injected collaborators, both interfaces:

```java
Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
Objects.requireNonNull(clusterStateSupplier, "please set a cluster state supplier before starting");
```

Today `Coordinator` supplies the publisher. Nothing requires that it be `Coordinator`.

**Correction from S0 (F1):** `ClusterApplierService.doStart()` requires a third — a
`NodeConnectionsService` — plus a non-null initial state. An earlier draft of this section said two,
having read `ClusterService.doStart()` without reading what its children require. In the shell the
connections service is real, not a stub: its job is to hold transport connections to the nodes named
in the applied state, driven by the membership view (§10).

And [`ClusterApplier`](server/src/main/java/org/opensearch/cluster/service/ClusterApplier.java#L65) — the interface
`ClusterApplierService` implements — exposes state injection as public API:

```java
void setInitialState(ClusterState initialState);
void onNewClusterState(String source, Supplier<ClusterState> supplier, ClusterApplyListener listener);
```

**That pair is the entire integration point for a new control plane.** `Coordinator` is one caller of
`onNewClusterState`. The shell becomes another.

### 2.4 Guice is a service locator here, not a framework

398 files import `org.opensearch.common.inject`, which looks fatal. It isn't: `Node.java`'s binding
block ([:1857–:1990](server/src/main/java/org/opensearch/node/Node.java#L1857)) is almost entirely
`b.bind(X.class).toInstance(alreadyConstructedX)`. Objects are built by hand in `Node`'s 2,783-line
constructor and *then* registered for lookup. Replacing that with a plain holder record is mechanical,
not a redesign. The Guice-shaped risk is concentrated in plugin `createComponents` contracts, not in
core construction.

### 2.5 Where the coupling genuinely is

| Package | Lines | Files touching cluster state |
|---|---|---|
| `transport/` | 22,467 | `TransportService`: 9 refs |
| `rest/` | 25,388 | 13 of 195 |
| `action/` | 146,277 | **213 of 1,028** |

`action/` is the real work. That is where `TransportBulkAction`, the search coordination phases, and
every `TransportClusterManagerNodeAction` admin API live. §6.3 and §12 deal with it explicitly; it is
the largest honest cost in this plan and the place where a rewrite could quietly become a rewrite of
everything.

## 3. Why the current path stops here

Two pieces of evidence, both from this branch's own documents rather than from argument.

**The cell-diet wall.** [`rfc-serverless-control-cell-diet.md`](rfc-serverless-control-cell-diet.md) §2, after
adversarial review, concludes that making Class C metadata non-resident requires `Metadata`'s
per-index storage to stop being `Map<String, IndexMetadata>` — across all of `server/`, on the hot
request path, on any coordinating node — while remaining invisible to every caller that didn't opt in.
It names the resulting shape "closer to a lazily-resolving proxy than a plain sum type," and says
plainly that it is "not fully designed here."

That difficulty is **entirely an artifact of where the work is being done**. In a shell that never
publishes a full `Metadata` to every node, there is no residency problem to solve: no
`IndexMetadataOrStub`, no diff-basis retention rule, no hot-path materialization contract. The design
is hard because it is being performed inside a system whose type-level assumption is the thing being
removed, under a constraint (constraint 1: zero change for non-opted-in indices) that exists only
because classic and serverless share one process.

**"This seam fails by succeeding."** [`HANDOFF.md`](HANDOFF.md) records eight occasions where a computed
index made an operation return *a confident empty answer rather than an error*: refresh reaching no
shards, field mappings reporting no fields, stats/segments/recovery/force-merge reporting nothing, cat
listing no shard, file-cache capacity undercounting. Force merge "accepted an instruction, reported
success, and did no work at all."

This is the structural signature of a guest plugin whose indices the host control plane cannot see. It
is not eight bugs; it is one bug with eight instances, and the count grows with every admin API
touched. In a shell where these indices are the *only* kind that exists, the failure mode has no
source: there is no second, authoritative view of the world to disagree with.

Neither of these says the seam work was wasted — §15 argues the opposite. They say it has reached the
point where each further seam costs more than the last and buys back a property the new shell has for free.

## 4. Architecture

Three planes, with a hard rule about which may depend on which.

```
┌─────────────────────────────────────────────────────────────┐
│  SHELL          ServerlessNode: main, bootstrap, wiring,    │  new
│                 lifecycle, node roles, REST binding         │  ~5k lines
├─────────────────────────────────────────────────────────────┤
│  CONTROL PLANE  object-store truth: descriptors, shard-     │  new + ported
│                 heads (CAS), node leases; membership,        │  from serverless-storage
│                 gossip, reconcilers, LocalViewProjector      │
├─────────────────────────────────────────────────────────────┤
│  DATA PLANE     IndicesService, IndexService, IndexShard,   │  REUSED
│                 Engine, MapperService, SearchService,        │  ~416k lines
│                 TransportService, RestController             │  unmodified
└─────────────────────────────────────────────────────────────┘
```

- The shell depends on both lower planes.
- The control plane depends on the data plane's **value types** (`ShardRouting`, `IndexMetadata`,
  `DiscoveryNodes`, `ClusterState`) and on `ClusterApplier`. Nothing else.
- The data plane depends on **neither**, and must never learn that a new shell exists.

Per D4, that last rule is about **direction and shape**, not about never editing `server/`:

1. **Direction is absolute.** No class in `server/` may reference `org.opensearch.serverless.*`. This is
   Gradle-enforced and CI-failing (§13.2), and it is what keeps upstream merges routine.
2. **Shape is preferred, not mandated.** Coupling should go through a narrow shared interface — either
   `server` declares it and `serverless` implements it (an extension point, the shape today's plugin
   SPIs already have), or a `server` class implements an interface the shell consumes. Reaching into a
   concrete `server` class is permitted but is the exception, and each instance is a standing candidate
   for promotion to an interface.
3. **Every interface added to `server` must be one we would upstream on its own merits.** If it only
   makes sense because serverless exists, it is the wrong interface and the coupling belongs in the
   shell.

Rule 3 is the practical test that keeps rule 1 from being eroded a commit at a time.

## 5. The central move: `ClusterState` as a node-local materialized view

Today: one `ClusterState` per cluster, agreed by consensus, published in full-then-diffs to every
node, containing every index. Its cost is O(all indices) on every node — which is precisely the
ceiling `plan-area-h-metadata-off-cluster-state.md` measured and the cell-diet RFC failed to get under.

Proposed: one `ClusterState` per **node**, computed locally, containing only what that node needs —
the indices whose shards it hosts, plus the indices it is currently coordinating requests for. It is
a cache, not a truth. It is never published, never diffed, never agreed.

```
object store (truth)                      shell                       data plane
─────────────────────                     ─────                       ──────────
index descriptors (blob + CAS)  ──┐
shard-heads (term, lease, node) ──┤
node leases                     ──┼──►  LocalViewProjector  ──►  ClusterApplierService
manifest / segment metadata     ──┘      builds ClusterState        .onNewClusterState(...)
                                         for THIS node only               │
routing hints (gossip / cache)  ─────►   soft hints, never truth           ▼
                                                              IndicesClusterStateService
                                                              → IndexShard.updateShardState
```

Consequences, in order of how much they matter:

1. **The cell-diet problem dissolves.** A node materializes an `IndexMetadata` when it starts hosting
   or coordinating for that index, and drops it when it stops. Residency becomes proportional to
   *working set*, not to cluster population. No new type, no proxy, no diff-basis rule.
2. **`Metadata.Builder.build()`'s O(N) sweep stops mattering.** N is now this node's working set. The
   superlinear creation cost measured in `plan-area-h` §H.1 is a property of a map that no longer
   contains every index.
3. **The data plane is unmodified.** `IndicesClusterStateService`, `IndexService`, `IndexShard` all
   receive exactly the `ClusterState` shape they expect.
4. **`ClusterService` is constructed for real**, not stubbed — settings bus, local node, applier — and
   the ~400 files that reference it keep compiling and working, including plugins.

### 5.1 The precise wiring

```java
// shell startup
ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool);
clusterService.setNodeConnectionsService(membershipDrivenConnections);   // F1: required to start
clusterService.getClusterManagerService().setClusterStatePublisher(localOnlyPublisher);
clusterService.getClusterManagerService().setClusterStateSupplier(applier::state);
clusterService.getClusterApplierService().setInitialState(emptyLocalState(localNode));
clusterService.start();

// steady state — driven by the metadata plane, not by a Coordinator
projector.onTruthChanged(delta ->
    clusterService.getClusterApplierService()
        .onNewClusterState("local-view", () -> projector.project(delta), listener));
```

`localOnlyPublisher` applies locally and completes — there are no peers to publish to. Every node
computes its own view from the same object-store truth; they are not required to agree, and
divergence is resolved by CAS on shard-heads, exactly as
[`rfc-serverless-metadata-plane.md`](rfc-serverless-metadata-plane.md) §6 already specifies.

### 5.2 The risk this creates, named up front

`IndicesClusterStateService` computes shard *removals* by diffing the applied state against local
shards. A projected view that omits an index the node is still hosting will be read as "this shard
was removed" and the shard will be closed. **The projector's correctness obligation is therefore
one-directional and absolute: it may omit an index the node does not host; it must never omit one it
does.** This is the single highest-risk element of the design and it is the primary thing spike S0
(§11) must falsify. **Update after §10.5:** the alternative — replacing `IndicesClusterStateService` with a shell-owned
reconciler driving `updateShardState` directly (§2.2 shows the interface supports it) — is now the
*expected* path rather than the fallback, because that class turns out to be the only place in the
reused tree that assumes an elected cluster-manager.

**Resolved by S1.** Measured, not argued: applying a view that mentions no indices at all left the
shard `STARTED` and serving, while the close-set a diff-based reconciler *would* have computed was
exactly the set of locally-open shards. So the fear above was mis-located — projection is harmless;
the hazard is entirely a property of the reconciler. What was R1 is now an invariant:

> **Absence from a projected view is never a removal signal.** A shard closes when the shard-head
> register says this node no longer owns it, and never because a locally-computed view failed to
> mention it.

**Phase 2 carries it as structure, not only as a test.** `ShardReconciler.ensureOpen` has no code path
that closes anything, and `releaseShard` takes a shard id the caller must have obtained from truth.
There is deliberately no method that accepts a view and closes what is missing from it. The test that
guards this was validated by planting the diff-based rule and confirming it failed with
`expected:<STARTED> but was:<CLOSED>`.

### 5.3 `ClusterState.version()` stops being globally meaningful — and one reused class depends on it

If each node computes its own view, per-node state versions are monotonic locally but **not comparable
across nodes**. That is fine almost everywhere, and fatal in one place, found by reading
[`ReplicationTracker`](server/src/main/java/org/opensearch/index/seqno/ReplicationTracker.java) rather than
by assuming:

- [:1461](server/src/main/java/org/opensearch/index/seqno/ReplicationTracker.java#L1461) gates every update on
  `applyingClusterStateVersion > appliedClusterStateVersion`.
- [:1792](server/src/main/java/org/opensearch/index/seqno/ReplicationTracker.java#L1792) ships that value inside
  `PrimaryContext` from the relocation **source** node to the **target**, and
  [:1840](server/src/main/java/org/opensearch/index/seqno/ReplicationTracker.java#L1840) assigns it straight into
  the target's own `appliedClusterStateVersion`.

So the version genuinely crosses a node boundary and is then compared with `>`. Feeding it a per-node
projection counter would make primary relocation either silently drop legitimate updates or accept
stale ones — a class of bug that produces no exception, which is exactly the failure mode `HANDOFF.md`
warns about.

**Fix, and it is a better fit than what it replaces:** feed the **shard-head register generation** as
`applyingClusterStateVersion`. It is monotonic, globally agreed (it is the CAS generation of the single
object that arbitrates that shard), and per-shard — which is the granularity `ReplicationTracker`
actually cares about. The global cluster-state version was always a coarser proxy for it.

This is the only instance found so far. It is unlikely to be the only one that exists; enumerating the
rest is spike S0's Q4 — which **S0 could not answer**, being single-node, and which therefore moves to
the two-node probe in phase 2.

**S0 finding F4 makes the fix mandatory rather than preferable.** `ReplicationTracker.activatePrimaryMode`
refuses to activate a primary at term 0 (*"primary term must be positive"*), so the data plane will not
start a primary without a term supplied from outside. Today the elected manager bumps it on allocation.
In this design the only remaining source is the shard-head register — so feeding its CAS generation is
load-bearing, not an optimisation.

## 6. What the shell builds, and what it never builds

### 6.1 Constructed
`ThreadPool`, `NodeEnvironment`, `PluginsService`, `SettingsModule`, `CircuitBreakerService`,
`BigArrays`/`PageCacheRecycler`, `ScriptService`, `AnalysisRegistry`, `MapperRegistry`,
`NamedXContentRegistry`/`NamedWriteableRegistry`, `IndicesService`, `IndexingPressureService`,
`SearchService`, `SearchModule`, `TransportService`, `NetworkModule`, `RestController`,
`RepositoriesService`, `ClusterService` (per §5.1), `NodeClient`.

### 6.2 Never constructed
`Coordinator` and all of `cluster/coordination/` (11,940 lines), `GatewayMetaState` and
`gateway/local` (the object-store parts of `gateway/remote` are ported, not deleted — §15),
`AllocationService` and `cluster/routing/allocation/`, `discovery/`, `NodeJoinController`,
`PersistedClusterStateService`, `MetaStateService` (a no-op implementation satisfies
`IndicesService`'s constructor), `Node`, `NodeService`, and the Guice `Injector` in its entirety.

### 6.3 The `action/` question — decided, because it decides the size of this project

213 of 1,028 files in `action/` touch cluster state. They are not one population:

- **Data-plane actions** (`TransportShardBulkAction`, the search phases, get/mget, TransportReplicationAction
  family). These need routing and `IndexMetadata` — both of which the projected local view supplies.
  **Reused unmodified.** This is the bulk of the request path and the reason this plan is affordable.
- **Cluster-manager admin actions** (`TransportClusterManagerNodeAction` subclasses — create index, put
  mapping, settings update, cluster health, reroute). These submit `ClusterStateUpdateTask`s to an
  elected manager that does not exist. **Not reused.** The shell re-implements the subset it needs
  against the metadata plane (descriptor CAS), and the rest **do not exist at all** rather than
  existing and answering emptily. §3's "fails by succeeding" is the entire reason for that emphasis:
  *an absent API is a better failure than a confidently empty one.*
- **Stats/cat/monitoring actions.** Case by case. Default to absent until re-implemented.

The commitment, confirmed as **D2**: the shell's REST surface is an **explicit allowlist**, not whatever
happens to route. An unimplemented endpoint returns 501 with the reason. Nothing silently returns an
empty answer. This is not a drop-in replacement for existing OpenSearch clients, and Dashboards is not
a target — if either becomes one later, it arrives as demand-driven additions to the allowlist, not as
a change of posture.

**Sequence numbers, and what they cost (M48).** M47 deferred `_seq_no`/`_primary_term`/`_version` as
"a real feature, not a shape fix", on the grounds that this shell had no version model. That was wrong in
an instructive way: core's own engine had been assigning all three on every write all along, and
`BackgroundReconciler` had depended on the sequence number since M10. The one field with a hard
cluster-manager dependency in classic OpenSearch — the primary term — this design had already replaced with
the shard-head's compare-and-swap generation, which is exactly the monotonic fencing token core requires.
The real gap was narrower: the log recorded only an id and a source, so a successor renumbered everything
it replayed. It now records each operation's sequence identity and replays through core's own
`Engine#engineRecoveryOperations()` seam under a recovery origin, which preserves it — and conditional
writes follow for free, because the comparison is core's own. See
[`m48-sequence-numbers-notes.md`](m48-sequence-numbers-notes.md).

**The two limits M48 disclosed, taken (M49).** One of them was not real. M48 said sequence gaps go
unfilled because `fillSeqNoGaps` is unreachable from outside `IndexShard`'s promotion path — true of the
method, false of the conclusion: `StoreRecovery#internalRecoverFromStore` calls it inside
`recoverFromStore`, which is the only way this shell opens a writer, so gaps have always been filled. That
claim was reasoned from an API surface without checking the call graph, and is now pinned by a test. The
other limit was real and is closed in the way the architecture already suggested: a successor **seals** the
log at takeover, durably, so nothing a not-yet-aware predecessor appends afterwards is replayed by it or by
any node after it — the seal has to outlive the node that took it, because the case that matters is that
node dying before it publishes. A per-write check of the node's own lease, costing no I/O, bounds how long
a partitioned writer can go on appending at all; it bounds, and the seal fences. See
[`m49-fencing-notes.md`](m49-fencing-notes.md) for the one window that remains, between winning the
shard-head and taking the seal.

**The allowlist, kept honest (M50).** A re-audit after M49 — reading every handler's routes, then driving a
running node over HTTP, because half of what it found is invisible from the source — showed the surface had
drifted off its own posture in three ways. One endpoint gave a *wrong answer* rather than a refusal:
`PUT /{index}` read the shard count from a query parameter and the whole body as the mapping, so a client
sending OpenSearch's create envelope got a one-shard index whose mapping was the envelope, and was told
`acknowledged` and `shards_acknowledged` for it. Several refusals had stopped being true when M48 shipped
conditional writes — `_bulk` still told callers the system had none. And the promised 501-with-a-reason
covered ten paths; everything else fell through to core's default 400, which reads like a typo. All three are
closed: the envelope is honoured (and a replica count refused with its real reason rather than dropped),
`_bulk` does `create` and per-item conditions, and the endpoints a client actually tries each refuse with the
reason they are refused for. See [`m50-api-compatibility-notes.md`](m50-api-compatibility-notes.md), including
the gap this leaves: `update` inside `_bulk`, which needs a read per item and so changes what a batch is.

**What D2 does not license (M47).** "Not a drop-in" was never permission to be gratuitously different —
a real audit (reading actual handler source, not assuming from names) found a mix of two very different
things wearing the same "incompatible" label: the deliberate, load-bearing refusals D2 exists for
(conditional writes, non-prefix wildcards, a cluster-wide surface — each needs a real feature this
architecture does not have), and shell-specific inventions with no reason to differ at all — `_shards`
field names, a bare-string error where every uncaught exception on the same surface already rendered the
real object, a missing `took`. The second kind cost nothing architecturally to fix and is fixed
([`m47-api-compatibility-notes.md`](m47-api-compatibility-notes.md)); the first kind is untouched, because
closing it for real (a version model behind `_seq_no`, a settings service behind the classic index-create
envelope) is a real feature with its own scope, not a response-shape change.

## 7. Module layout

```
libs/                       unchanged
server/                     becomes a library dependency; may gain the §8.1
  └─ shared interfaces, and nothing else

serverless/
  ├─ shell/                 ServerlessNode, bootstrap, wiring, roles, CLI
  ├─ control/               descriptors, shard-heads, leases, CAS, reconcilers,
  │                         LocalViewProjector, membership, gossip
  ├─ actions/               the allowlisted REST/transport surface
  ├─ engine/                ported from plugins/serverless-storage
  └─ testkit/               ServerlessTestCluster (§13)

distribution/serverless/    its own tarball/docker image, own main class
```

`server/` is consumed with `implementation project(':server')`. The **build enforces §4's rule 1**:
`serverless/*` may depend on `server`, and nothing in `server` may reference `org.opensearch.serverless.*`.
That is a Gradle constraint, not a convention, and it is what prevents drift into a fork. Per D4,
`server/` may still change — it may gain the shared interfaces of §8.1 — it simply may never point back.

## 8. The narrow interfaces

Five, total. This is the whole contract between the new control plane and the reused data plane.

| Interface | Shape | Who implements |
|---|---|---|
| `ClusterApplier` | exists today, unmodified ([:65](server/src/main/java/org/opensearch/cluster/service/ClusterApplier.java#L65)) | `ClusterApplierService` (reused) |
| `ClusterStatePublisher` | exists today | shell's `LocalOnlyPublisher` |
| `ShardStateStore` | `get`/`compareAndSet`/`renewLease` over `BlobContainer` registers | ported (`BlobContainerShardStateStore`) |
| `DescriptorStore`-equivalent | blob GET + register CAS, per `plan-area-h` | ported (`BlobDescriptorBackend`) |
| `MembershipSource` | `current()` + `subscribe()`; §10.2 | new — blob-lease default, K8s, static |

Both of the latter two sit on `BlobContainer.readRegister`/`compareAndSwapRegister`, which already exists
in `server/` and is already implemented against S3, GCS, Azure and Fs (§9.2). No new storage primitive
is introduced by this RFC — the register is the whole interface to truth.

Note what is *not* on this list: no new SPI in `server/`, no new extension point, no core seam. All
five live in `serverless/`. If a sixth appears, it is a signal that something is being done in the
wrong plane.

### 8.1 The shared interfaces `server` gains (D4)

The god-object coupling §2.1 measured is, capability by capability, tiny. Extracting it gives four
narrow interfaces that `ClusterService`/`ClusterApplierService` implement unchanged, and that the
reused data plane consumes instead of the concrete classes:

| Interface | Capability | Current concrete source | Consumers found in §2.1 |
|---|---|---|---|
| `ClusterSettingsAccessor` | `getSettings()`, `getClusterSettings()` | `ClusterService`, `ClusterApplierService` | `IndicesService` (all 14 uses), `SearchService` (:536–:607), `IndexShard` (:559) |
| `LocalNodeProvider` | `localNode()` | `ClusterService` | `SearchService` (:777, :1371, and four more) |
| `ClusterStateListenerRegistry` | `addListener(ClusterStateListener)` | `ClusterApplierService` | `IngestionEngine` (:173), via `EngineConfig` |
| `MinNodeVersionSupplier` | `state().nodes().getMinNodeVersion()` | `ClusterService` | `SearchService` (:1386) |

Two things worth noticing about that table.

**It is an improvement to OpenSearch independent of serverless.** "`IndicesService` needs a settings
bus, not a cluster service" is true today, on `main`, with no serverless in the picture — which is
exactly rule 3's test, and why these are upstreamable rather than a fork tax.

**It shrinks R6.** Upstream drift against four interfaces is a much smaller surface than drift against
`ClusterService`, `ClusterApplierService` and `EngineConfig`'s concrete shapes.

The one deliberate non-extraction: `EngineConfig` currently carries a whole `ClusterApplierService`
([:126](server/src/main/java/org/opensearch/index/engine/EngineConfig.java#L126)) whose only real consumer is
`IngestionEngine`'s `addListener(streamPoller)`. Narrowing that field to `ClusterStateListenerRegistry`
is the single highest-value item in the table, because it is the only one reaching into the *engine*
layer — but it changes a public builder signature, so it is sequenced after S0 rather than assumed.

## 9. Control plane: cluster state as CAS registers

The truth layer is not a replicated state machine. It is a set of small object-store blobs, each
mutated by compare-and-swap, and **there is no consensus process anywhere in the system**.

This is a deliberate strengthening of [`rfc-serverless-metadata-plane.md`](rfc-serverless-metadata-plane.md) §8,
which retained a 3–5 node "control cell" for cluster config, membership arbitration and directory
partition assignment. §9.4 argues that all three are single-object CAS problems, so the control cell
is dissolved rather than shrunk.

### 9.1 Why CAS is sufficient, and what it actually costs

A linearizable compare-and-swap register has consensus number ∞: it can implement consensus among any
number of processes. Dropping Raft/Zen2 in favour of CAS is therefore **not a weakening of the
consistency model** — it is the same power obtained from the storage layer instead of from a quorum
of our own processes. What is given up is not safety. It is three specific affordances:

| Given up | Consequence | Handling |
|---|---|---|
| **Change notification** | Object stores have no watch. Nodes must poll. | §9.5 — this is the real cost, not a footnote |
| **Multi-object atomicity** | Only single-register linearizability | Intent-object pattern (metadata-plane RFC §7), used only for rare operations |
| **Cheap linearizable reads** | A truth read is a GET (~10–100 ms) | Never read truth on a request path; §9.3's rule |

### 9.2 The primitive and the codecs both already exist

Neither half of this needs to be invented, which is most of why this section is short.

**The CAS primitive is production code on every real backend.**
[`BlobContainer.readRegister`/`compareAndSwapRegister`](server/src/main/java/org/opensearch/common/blobstore/BlobContainer.java#L376)
give generation-versioned register semantics, defaulting to `UnsupportedOperationException` so no
existing implementer breaks — with real implementations backed by each provider's native conditional
write:

| Backend | Mechanism |
|---|---|
| [S3](plugins/repository-s3/src/main/java/org/opensearch/repositories/s3/S3BlobContainer.java#L1093) | `If-Match` / `If-None-Match` on `PutObject` |
| [GCS](plugins/repository-gcs/src/main/java/org/opensearch/repositories/gcs/GoogleCloudStorageBlobContainer.java#L136) | generation preconditions |
| [Azure](plugins/repository-azure/src/main/java/org/opensearch/repositories/azure/AzureBlobStore.java#L436) | `BlobRequestConditions` ETag `If-Match` |
| [Fs](server/src/main/java/org/opensearch/common/blobstore/fs/FsBlobContainer.java) | filesystem atomicity |

`createRegisterIfAbsent` exists as a distinct one-round-trip path because index creation is
put-if-absent and is the operation expected to run at 10⁸.

**The serialization already exists too.** `gateway/remote/model/` already decomposes `ClusterState`
into exactly the per-entity blobs this design wants as registers — `RemotePersistentSettingsMetadata`,
`RemoteTransientSettingsMetadata`, `RemoteCoordinationMetadata`, `RemoteTemplatesMetadata`,
`RemoteClusterBlocks`, `RemoteDiscoveryNodes`, `RemoteCustomMetadata`, `RemoteIndexMetadata`,
`RemoteRoutingTableBlobStore`, `RemoteHashesOfConsistentSettings`. Today these are **write-behind of a
state consensus already decided**, tied together by a manifest. The change is to make each one
**CAS-arbitrated truth in its own right** and delete the manifest that made them a single logical
object. The codecs, the blob layout and the round-trip tests carry over.

### 9.3 The register map, and the trap it avoids

The trap is making cluster state *one* register. A single global object would serialize every mutation
in the system through one optimistic-concurrency point — retry storms under load, and it silently
reinstates the O(all indices) cost that §5 exists to remove. Partition by natural CAS granularity
instead, so that **each register has a bounded, disjoint writer population**:

| Register | Contents | Writers | Write rate |
|---|---|---|---|
| `/cluster/config` | persistent + transient settings, templates, blocks | operators | human-scale |
| `/cluster/members/{nodeId}` | node lease: identity, roles, heartbeat, TTL | that node only | 1 per TTL, **zero contention by construction** |
| `/indices/{name}` | index descriptor: settings, mappings ref, partition assignment | index lifecycle ops | per-index, rare |
| `/shards/{index}/{id}/head` | term, lease ref, current owner | activation/failover | per-shard, rare |

Note what has **no register at all**: the routing table and the membership list. Both are *derived* —
membership by listing live leases, routing by reading shard-heads plus directory hints. Nothing agrees
on them, and nothing needs to, because safety comes from shard-head CAS rather than from a shared view.

Two rules that follow, and that a design review should treat as rejection criteria:

1. **No CAS on a request path.** Term bumps and lease acquisitions happen at activation and failover,
   not per request. Any design putting a register write in a bulk or search path is wrong.
2. **A register's write rate must be bounded by design, not by hope.** If a proposed register can be
   written by an unbounded set of nodes at an unbounded rate, it is the wrong granularity.

### 9.4 Dissolving the control cell

The three duties §8 of the metadata-plane RFC retained consensus for:

- **Cluster config** — one register (`/cluster/config`), operator write rate, CAS on conflict. Consensus
  buys nothing here; the write rate is human.
- **Membership arbitration** — dissolved rather than moved. Membership is not a decision, it is a
  derived view over live leases. Two nodes disagreeing about the member list cannot cause harm, because
  no safety property depends on that list — shard ownership is arbitrated per-shard by CAS.
- **Routing hint distribution** — soft state, where a wrong hint costs a retry rather than
  correctness. §10.3 removes the dedicated tier this duty was attached to; the hints spread by gossip
  or are simply recomputed.

Result: **zero consensus processes.** Nothing to bootstrap, no quorum to lose, no split-brain, no
minimum cluster size, no seed hosts. The `control` role disappears from §10. This is a real
simplification and not merely a relocation of the problem — but it rests entirely on the provider's
conditional write being genuinely linearizable, which is R11.

### 9.5 The honest cost: no watch

This is the one place where CAS-on-blobs is *worse* than consensus, and it should not be glossed.

A consensus system pushes changes. An object store must be polled. Naively, N nodes polling K
registers at interval T is `N·K/T` GETs per second forever — at 10⁴ nodes and a 1 s interval, that is
10⁴ GET/s to learn that nothing changed.

Three mitigations, in the order they should be applied:

1. **Piggyback an epoch.** Every transport response already flowing between nodes carries the sender's
   observed `/cluster/config` generation. A node learns it is stale for free, on traffic it was already
   sending, and only then does it GET. Polling becomes the fallback for idle nodes, not the mechanism.
2. **Set the interval by what the register is.** Config staleness of seconds is harmless. Shard-head
   staleness matters only at activation, which reads the head anyway. There is no register that needs
   sub-second universal propagation, and if one appears it is a design smell.
3. **Spread it epidemically.** Gossip (§10.3) generalizes mitigation 1 from "rides on traffic that
   happened to flow" to "reaches the fleet in O(log N) rounds," and on Kubernetes the membership half
   of the problem has a real push channel already (§10.2).

### 9.6 Fencing: where this design loses data if it is wrong

CAS on the shard-head establishes *who owns* a shard. It does not by itself stop a **zombie** — a
writer whose JVM paused past its lease TTL, that does not yet know it lost ownership — from continuing
to write segments. Ownership arbitration and byte-level fencing are different problems, and object
store systems lose data at exactly this seam.

**Rule: the term must be enforced where bytes are written, not only where ownership is claimed.**

The preferred mechanism is term-scoped key paths: a writer holding term `T` writes under
`.../t={T}/...`, and the head names the live term. A zombie at term `T-1` writes to a prefix that no
reader ever consults, so its writes are inert rather than corrupting, and GC reclaims them as orphans.
This costs nothing on the hot path and needs no conditional-write support for data objects — only for
the head. Conditional writes on a per-shard manifest are the alternative, and are strictly more
expensive.

## 10. Membership, discovery, and roles

Membership is not cluster state, discovery is a plugin, propagation is gossip, and every node runs the
same binary. Each of those is safe here for the same reason: **§9 moved safety into per-shard CAS**, so
nothing in this section can cause harm by being wrong — only by being slow.

That is the whole argument, and it does not transfer. In classic OpenSearch, membership *is* the safety
boundary — quorum is computed from it — so an eventually-consistent member list would be a correctness
bug. Here no safety property reads the member list at all.

### 10.1 Membership is a derived view, not a decision

§9.3 already gives membership no register of its own. It is a **LIST over `/cluster/members/`, filtered
by lease expiry**. Two nodes holding different member lists forever is not a fault to be repaired; it is
the normal state of the system, and it is harmless because shard ownership is arbitrated per-shard by
CAS rather than by agreement about who exists.

One distinction to hold onto, because conflating these is how object-store systems lose data:

> **A node existing is not a node owning anything.** Kubernetes readiness, gossip liveness and a live
> lease answer three different questions. Only the lease — and, for a specific shard, only that shard's
> head — is authoritative about ownership.

### 10.2 Discovery as a plugin — but not `DiscoveryPlugin`

The instinct is right; the existing SPI is the wrong shape. All three of
[`DiscoveryPlugin`](server/src/main/java/org/opensearch/plugins/DiscoveryPlugin.java)'s hooks are
Coordinator concepts: `getSeedHostProviders` seeds unicast pinging for Zen2, `getJoinValidator` returns
a `BiConsumer<DiscoveryNode, ClusterState>` run at join, and `getElectionStrategies` configures an
election that no longer happens. `discovery-ec2`, `discovery-gce` and `discovery-azure-classic` all
implement exactly one of them — `SeedHostsProvider` — which answers "who might I ping to find a
cluster to join," a question the new shell never asks.

The replacement is smaller, and it is the fifth and last interface in §8:

```java
interface MembershipSource {
    Collection<NodeInfo> current();              // who is alive now
    void subscribe(Consumer<MembershipDelta> l); // push, if this source can push
}
```

| Implementation | Mechanism | Notes |
|---|---|---|
| `BlobLeaseMembership` (default) | LIST `/cluster/members/`, filter by TTL | Zero new infrastructure; fate-shared with the data; works on any backend. Poll-only. |
| `KubernetesMembership` | EndpointSlice / Pod watch | **Push.** See below. |
| `StaticMembership` | configured list | tests, single-node dev |

The Kubernetes case is more interesting than "another cloud discovery plugin," and it is worth naming
why: **it supplies the watch primitive the object store lacks.** §9.5 records no-watch as the real cost
of CAS-on-blobs, mitigated by epoch piggybacking and polling. A K8s informer is a genuine push channel
for the membership half of that problem, and it costs the operator nothing they are not already
running. It converts R10 from "design around polling" to "design around polling, except where the
platform already solved it."

The boundary from §10.1 still applies with full force: K8s tells you a pod is Ready. It does not tell
you that pod still holds shard `s` — a Ready pod whose lease expired owns nothing, and a node that
treats readiness as ownership is R9 with extra steps.

### 10.3 Gossip: what it may carry, and what it must never

Yes to gossip, with one boundary stated as a rule:

> **Gossip carries hints and epochs. CAS carries truth.**

| May carry | Why it is safe |
|---|---|
| Config/register epochs | A stale epoch means one extra GET, never a wrong action |
| Routing hints, cache invalidation | A wrong hint costs a retry; the shard-head is consulted before anything is written |
| Load and capacity for placement candidate selection | Placement *quality* is gossip's job; placement *safety* is CAS's (metadata-plane RFC §6) |
| Liveness suspicion | A suspicion triggers a lease read, it does not evict anyone |

The test for whether a proposed use is legitimate: **if two nodes hold different values for this
forever, what breaks?** If the answer is worse than a retry or a stale read, it does not belong in
gossip. Shard ownership, term numbers and index existence all fail that test.

Given that, gossip becomes the general form of §9.5's epoch piggybacking — an epoch reaches the fleet in
O(log N) rounds instead of riding only on traffic that happened to be flowing — and **it dissolves the
directory tier.** §9.3's routing hints and the `directory` role existed to fan out soft state from a
dedicated tier; gossip does that with no tier at all. The role is deleted below.

Two costs, neither of which should be waved through:

1. **There is no gossip implementation in this repo.** Verified, not assumed: zero hits across
   `server/`, `modules/` and `plugins/`. SWIM/Lifeguard-style membership and failure detection has to
   be built or vendored, and failure-detector tuning at 10⁴ nodes is real work with a long tail of
   false-positive behaviour.
2. **It is the only new distributed protocol this design introduces**, having just removed one. It
   deserves the same skepticism consensus got — including the question of whether phases 1–7 need it
   at all, or whether polling plus §10.2's K8s push carries the system until phase 8.

### 10.4 One binary, equal capability, roles as lease attributes

"All nodes equal" is right about deployment and wrong about scaling, and the two can be separated.

Taken literally, homogeneous roles would delete independent ingest/search scaling and per-index
search scale-to-zero — goals 2 and 3 of `rfc-serverless-opensearch.md` §2, and a large part of why any
of this is being built. Taken as a deployment property, it is exactly right: one image, no special
node, any pod substitutable for any other.

The resolution is that **role is a dynamic attribute a node advertises in its lease, not a topology
decision baked into a cluster**. No new mechanism is needed: `DiscoveryNode` already carries
`getAttributes()`/`getRoles()`, and `isRemoteStoreNode()`
([:550](server/src/main/java/org/opensearch/cluster/node/DiscoveryNode.java#L550)) is already derived from
attributes rather than from topology.

| Role | Advertised in lease as | Notes |
|---|---|---|
| `ingest` | accepts writer activation | CAS-acquired shard ownership |
| `search` | accepts reader activation | scale-to-zero; no local durable state |

Both from one binary. An operator gets asymmetric scaling with two Deployments differing by one
environment variable, or a single Deployment whose nodes switch roles under load — and that becomes a
policy question rather than an architectural one. The `directory` role from the previous draft is gone
(§10.3).

### 10.5 The one thing that must still be synthesized — and what it settles

The data plane does need a `DiscoveryNodes`: `IndexShard`
[:651](server/src/main/java/org/opensearch/index/shard/IndexShard.java#L651) calls
`discoveryNodes.get(nodeId).isRemoteStoreNode()`, and `SearchService` needs `localNode()`. The projector
builds one from the membership view — straightforward.

Except that `DiscoveryNodes` also carries a `clusterManagerNodeId`
([:81](server/src/main/java/org/opensearch/cluster/node/DiscoveryNodes.java#L81)), and
`isLocalNodeElectedClusterManager()` is defined as `localNodeId.equals(clusterManagerNodeId)`. With no
election, neither available answer is obviously safe: `null` risks NPEs in reused code, and naming the
local node makes **every** node believe it was elected, potentially arming cluster-manager-only paths
everywhere at once.

The measurement settles it. Across `index/`, `search/` and `indices/`, every reference to
`getClusterManagerNode()` or `isLocalNodeElectedClusterManager()` — all six — is in
`IndicesClusterStateService` ([:404](server/src/main/java/org/opensearch/indices/cluster/IndicesClusterStateService.java#L404),
:890, :944, :1108–1116), where they exist to report shard-failed/shard-started **back to the elected
manager**. In this design those reports are CAS writes to the shard-head instead. `index/` and
`search/` contain **zero** such references.

So: `clusterManagerNodeId` is `null`, and `IndicesClusterStateService` is replaced rather than reused.
That **promotes §5.2's fallback to the expected path** — a shell-owned reconciler driving
`updateShardState` directly, which §2.2 already showed the interface supports. The class was the one
place the data plane assumed an elected manager; not reusing it removes that assumption entirely rather
than papering over it. S0's Q2 now asks how big that reconciler is, not whether it is needed.

## 11. Spike S0 — the acceptance test, before anything else

> **RESULT (2026-08-27): PASSED — 2 tests, 0 failures.** A shard was opened, recovered, started,
> written to and searched with no `Coordinator`, `AllocationService`, `GatewayMetaState` or `Node`;
> the object-graph walk visited 12,728 objects and found none of them. Full report, including four
> findings that correct this document and one question S0 could not answer, in
> [`s0-findings.md`](s0-findings.md). §5 is not falsified; phase 1 is unblocked.
>
> **Phase 1 is complete (2026-08-27).** `ServerlessNode` boots with transport and HTTP bound, serves
> its allowlisted surface, and shuts down cleanly — 20 tests, `check` green. The control-plane absence
> assertion now scans the whole node (11,984 objects) rather than named services, so it covers the
> transport and HTTP layers too.
>
> **Where this stands overall:** phases 0–9, **M10 (durability)**, **M11 (the data path)** and the
> autonomic half of **M13** are delivered — a document can be written to any node and found from any
> node in a multi-node deployment, it survives its node dying, and a node started as a process renews,
> publishes, acquires and shuts down cleanly without anything driving it. **§12.1 lists what remains**;
> **the shell now runs on an object store**: a node boots against a bucket, publishes, renews and serves
> searches, and a successor rebuilds from the bucket alone. Doing that found a real bug — segment upload
> was impossible on S3 and worked perfectly on disk. MinIO is not AWS S3, nothing is measured, and
> contention and object storage have not yet been put together. The green table below is not
> the whole picture.
>
> **Phase 9 is complete (2026-08-27).** Creation cost is flat in the population — one object-store write
> per create at every population measured — and node residency tracks the working set. The phase also
> found what it was for: `truthFor` was doing an O(population) sweep on the steady-state path, 2N+1
> operations per tick per node, invisible to eight phases of tests that each had a handful of indices.
> A per-node assignment listing takes it to 3 operations regardless of population, and the listing is a
> hint that the shard-head still overrules. See [`phase9-notes.md`](phase9-notes.md).
>
> **Phase 7 is complete for the admin surface (2026-08-27), built after phase 8.** Index lifecycle and
> catalog endpoints served from the metadata plane, with `never_activated` distinguished from `unowned`
> and `/_serverless/nodes` labelled as this node's observation rather than a cluster fact. The data
> surface — `_doc`, `_bulk`, `_search` — is still absent. See [`phase7-notes.md`](phase7-notes.md),
> whose addendum settles the evidence gap phase 8's gossip gate flagged.
>
> **Phase 8 is complete (2026-08-27); gossip deliberately not built.** A reconciliation tick releases a
> lost shard and re-acquires it once the holder's lease lapses, with no privileged node involved. GC
> keeps files a live commit inherited from older terms — the mistake it is shaped to prevent — and both
> its rules were verified by breaking them. The §10.3 gate is answered with 3.0 object-store ops per
> tick per shard; the write is per-shard lease renewal, which §7's batching removes and which is not yet
> implemented. **Phase 7 was skipped**, so the evidence §10.3 expected from it is absent. See
> [`phase8-notes.md`](phase8-notes.md).
>
> **Phase 6 is complete (2026-08-27).** Failure detection runs on the node that might be failing: stop
> heartbeating and the lease lapses. The zombie test — a writer alive, paused past its lease, still
> believing it owns the shard — confirms its local writes succeed, reach nobody, and vanish, which
> separates R9's fence from phase 4's WAL gap. See [`phase6-notes.md`](phase6-notes.md).
>
> **Phase 5 is complete (2026-08-27).** A search node serves a shard by reading its manifest, touching
> no register — the head's owner and term are unchanged before and after. The role selects
> `ReadOnlyEngine`, so reader safety is structural rather than conventional. Scale-to-zero was verified
> by closing every search node and serving the same data from a fresh one. See
> [`phase5-notes.md`](phase5-notes.md).
>
> **Phase 4 is partially complete (2026-08-27).** A document indexed on one node is served by another
> after failover, restored from the object store with no peer recovery — the gap phase 3 named. Fencing
> is half-built and both halves are canary-verified. WAL, bulk-action wiring and automatic publication
> are deferred and listed in [`phase4-notes.md`](phase4-notes.md).
>
> **Phase 3 is complete (2026-08-27).** An index is created in the object store with one put-if-absent,
> a shard is activated with one compare-and-swap, and `node.syncFrom(plane)` makes a node work out what
> to serve with nothing telling it. 40 tests. Both safety claims — exactly one winner under contention,
> and a live lease is never stolen — were verified by planting canaries and watching them fail. Scope
> limits and what this does *not* establish are in [`phase3-notes.md`](phase3-notes.md).
>
> **Phase 2 is complete (2026-08-27).** `node.applyTruth(descriptors, assignments)` projects a
> node-local view, applies it, and opens every owned shard, which then serves a search. §5.2's
> invariant lives in `ShardReconciler`'s shape — there is no method that closes what a view omits — and
> a planted diff-based canary was confirmed to fail the invariant test before being removed. 26 tests.
>
> **S1 (two-node probe) RESULT: PASSED — 5 tests total, 0 failures.** Two nodes holding disjoint
> node-local views both serve; a projected view omitting a hosted index leaves the shard STARTED and
> serving; and the §5.3 version hazard reproduces exactly (version 4 silently ignored after 500) with
> its fix verified. Q4 answered, R1 resolved into a reconciler contract — see
> [`s1-findings.md`](s1-findings.md).

**Nothing in §12 starts until S0 answers.** S0 is disposable code on a throwaway branch, and its
purpose is to falsify §5, not to demonstrate it.

> Stand up a process that opens an `IndexShard` against a local directory, applies a hand-built
> `ClusterState` through `ClusterApplierService.onNewClusterState`, indexes a document, and answers a
> search for it — with no `Coordinator`, no `GatewayMetaState`, no `AllocationService`, no discovery,
> and no `Node`.

Pass criteria, stated as numbers that are zero when it is broken — per `HANDOFF.md`'s own rule, since
this spike is exactly the kind of seam that fails by succeeding:

1. `getSuccessfulShards() == 1` on the search response. Not "did not throw."
2. Hit count equals the indexed document count.
3. The retrieved `_source` matches what was indexed, field for field.
4. A second document, indexed after the first search, is visible to a second search after refresh.
5. The process contains no instance of `Coordinator`, `AllocationService`, `GatewayMetaState` or
   `Node` — asserted by reflection over the constructed object graph, not by inspection.

Deliverable is a written answer to three questions:

- **Q1.** What is the minimum `ClusterState` a shard will accept? (Determines the projector's schema.)
- **Q2.** How large is the shell-owned reconciler that replaces `IndicesClusterStateService`? §10.5
  settles *that* it is replaced; S0 measures the cost. Build the smallest one that passes the five
  criteria above and report its size.
- **Q3.** What is the actual constructor closure of `IndicesService` + `SearchService` — how many of
  its 38 parameters have no sensible serverless value?
- **Q4.** Beyond `ReplicationTracker` (§5.3), what else in the reused data plane compares a
  `ClusterState` version, or any other globally-agreed number, **across nodes**? Enumerate by grepping
  for cross-node value objects that carry a version, not by reasoning about which ones "should" matter
  — §3's record is that every hypothesis argued from reading code was wrong and every probe was right.

Estimated: days, not weeks. If S0 comes back "short list, concentrated in `IndicesService`," proceed.
If it comes back "the data plane assumes a live control plane in forty unexpected places," **this RFC
is wrong** and the seam path on `feature/serverless` resumes — including approving the cell-diet
design it currently blocks.

## 12. Phasing

Each phase ends in something runnable. No phase is a refactor with no observable result.

| # | Phase | Ends when |
|---|---|---|
| 0 | **S0 spike** (§11) | Q1–Q3 answered in writing |
| 1 | **Shell skeleton** | ✅ **COMPLETE.** `ServerlessNode` boots, binds netty4 transport + HTTP on real ports, serves `GET /` and `GET /_serverless/health`, and releases its port on shutdown. `MembershipSource`/`BlobLeaseMembership` landed and read by the health endpoint. D2 enforced with explicit 501s. 20 tests; `check` green on both projects including the §13.2 direction rule. |
| 2 | **Local view** | ✅ **COMPLETE.** `IndexDescriptor`/`ShardAssignment` truth records, `LocalViewProjector`, and `ShardReconciler` — the replacement for `IndicesClusterStateService`. A shard opens from a descriptor and serves a search through production code. §5.2's invariant is structural and verified by a planted canary. 26 tests. |
| 3 | **Metadata plane** | ✅ **COMPLETE.** `DescriptorStore`, `ShardHeadStore`, `MetadataPlane`, and the §9.3 register map. Create-index is one put-if-absent; activation is one CAS; `node.syncFrom(plane)` replaces cluster-state publication. 40 tests; both CAS safety claims verified by planted canaries. Per **D5**, `FsBlobContainer` only — no S3/GCS durability claim. See [`phase3-notes.md`](phase3-notes.md). |
| 4 | **Write path** | ⚠️ **PARTIAL.** Segment publication and restore through the object store: failover now moves *data*, not just ownership. Term-scoped containers + manifest CAS fence a zombie (R9 partially closed, both halves canary-verified). Roles are lease attributes. **Deferred:** WAL, `TransportShardBulkAction` wiring, automatic publication — see [`phase4-notes.md`](phase4-notes.md). 45 tests. |
| 5 | **Search path** | ✅ **COMPLETE.** Reader shards open from a manifest with no CAS and no shard-head entry; the `search` role selects `ReadOnlyEngine`, so a reader is structurally unable to write. Scale-to-zero verified by closing every search node and serving from a fresh one. 51 tests; both claims canary-verified. See [`phase5-notes.md`](phase5-notes.md). |
| 6 | **Activation & failover** | ✅ **COMPLETE.** `activateWriter` (CAS, losing returns empty not an error) and `heartbeat` (renew what is held, release what is lost). kill-9 loses no published data. The paused-JVM zombie test walks the full sequence and separates fencing from the WAL gap. 56 tests; all three claims canary-verified. See [`phase6-notes.md`](phase6-notes.md). **Fencing later sharpened:** it has two halves and only the term check had a test — the manifest compare-and-swap, which catches a writer that read *before* its successor published, could be destroyed with 148 of 149 tests still green. Now covered on a filesystem and on a bucket, with the race window held open deliberately rather than raced for, and asserting which half did the refusing ([`m18-fencing-notes.md`](m18-fencing-notes.md)). |
| 7 | **Surface** | ✅ **COMPLETE (admin only).** `PUT/GET/DELETE /{index}` plus `/_serverless/{indices,shards,nodes}`, each one or two object-store reads. Absent, duplicate and unconfigured are 404/400/503 — never an empty 200; classic endpoints still 501. Both D2 properties canary-verified, and wildcard shadowing tested. **No document APIs** (`_doc`, `_bulk`, `_search`). 65 tests. See [`phase7-notes.md`](phase7-notes.md). |
| 8 | **Gossip & reconcilers** | ✅ **COMPLETE (gossip not built, by the gate).** `BackgroundReconciler` closes phase 6's gap: a tick releases what was lost and re-acquires automatically. `GarbageCollector` collects a zombie's orphans while keeping inherited files — both rules canary-verified. `RoutingHints` are soft state that stay wrong until refreshed. Gossip gate answered with a measurement: **3.0 ops/tick/shard**, the write being per-shard lease renewal. **§7 batching then built and re-measured: writes fell from one-per-shard to one-per-node** (4→1 at 4 shards). Deployment sweep and lease tidying added. 70 tests. See [`phase8-notes.md`](phase8-notes.md). |
| 9 | **Scale validation** | ✅ **COMPLETE.** Creation measured flat: **1 op/create, times unchanged from 0 to 1,500 indices** (classic: 7.4→98.8 ms). Residency tracks the working set. **Found `truthFor` was O(population)** — 401 ops at 200 indices, 2,001 at 1,000, on every tick — and fixed it with a per-node assignment listing: **3 ops at both**. Listing is a hint; the head still decides, canary-verified. Then removed index enumeration from the serving path entirely (501, "maintenance operation, not a serving one"); `listPage` survives only for offline sweeps. Lifecycle is now CAS throughout — delete was an unconditional removal and is now a tombstone swap at an expected generation. `MAX_SHARDS = 4096` enforces that an index's shard list stays self-contained in its descriptor. 79 tests. See [`phase9-notes.md`](phase9-notes.md). |

Phases 1–2 are the ones that decide whether this is a two-quarter project or a two-year one. Treat
their estimates as unknown until phase 2 lands.

### 12.1 Remaining milestones

Phases 0–9 are done and the table above is almost entirely green, which is misleading on its own: a
reader would conclude the project is finished and would not learn that **no endpoint touches a
document**. Every deferred item was recorded honestly, but as prose inside seven separate
"what this does NOT establish" sections — good provenance, useless for planning. This is that inventory
consolidated, with nothing new added.

| # | Milestone | Contents | Blocked on |
|---|---|---|---|
| **M10** | ✅ **DONE.** Publication happens in the reconciler tick (and not on an idle shard); a document-level WAL makes a write durable before it is acknowledged. End-to-end: 5 published, 3 not, writer killed, successor serves 8. Both halves canary-isolated. All three items this row once listed as remaining are done and superseded elsewhere: batching (M16, see M11's row), logged deletes (M11's row), and `if_seq_no` — refused explicitly rather than silently accepted (M15 part 1). See [`m10-notes.md`](m10-notes.md). | — |
| **M11** | ✅ **DONE.** Documents written and searched over HTTP across a multi-node deployment. Writes forward to the shard owner; searches fan out by **reader placement** (rendezvous hashing over live `search` leases) rather than to the writer, with placement a hint a node can always override by opening the shard itself. Peers resolved from leases. `_shards` coverage always reported. **Deletes now exist** and are logged, replayed in order, and cannot be resurrected by a failover ([`m15-delete-notes.md`](m15-delete-notes.md)). **`_bulk` is done** ([`m16-bulk-notes.md`](m16-bulk-notes.md)): the write path is now priced per request rather than per document — 50 documents cost 1 object-store write against 50, on a bucket as on a filesystem — with the batching in the log, the node and the wire, so a batch that crosses the network stays a batch. `create` and `update` are refused per item rather than approximated as `index`. **`GET _doc/{id}` is done** ([`m17-get-notes.md`](m17-get-notes.md)), routed to the shard's *owner* rather than fanned out to readers — a write is acknowledged before it is searchable and long before it is published, so only the owner can answer without appearing to lose data. An unowned shard is served from its published commit (a get works after scale-to-zero); an owner that cannot be reached is refused rather than answered from a staler copy. Doing it found that `ShardRouter.peer()` never checked lease expiry despite documenting that it did. **The query DSL is in** ([`m19-query-notes.md`](m19-query-notes.md)): a search body is parsed with `SearchModule`'s registry, so `bool`/`range`/`match_all` and the rest work — and the merge that had never worked now does. Hits used to be concatenated in shard order and returned in full (thirty hits for `size=10` over three shards), which no test could notice, and which could not be fixed until two bugs under it were: a score never crossed the network, and fetched hits arrive unscored. What cannot be merged — `collapse`, `suggest`, `profile` — is refused with a 501 rather than silently ignored. **`search_after` works** ([`m36-search-after-notes.md`](m36-search-after-notes.md)), which is what makes deep paging cost the same at page one thousand as at page one; it is refused without a sort, beside `from`, or carrying the wrong number of values, because each of those is a request that means two things at once. **Aggregations are reduced now** ([`m31-aggregations-notes.md`](m31-aggregations-notes.md)) and cost nothing extra at the object store — 3 requests for a search over three locally-held shards, and 3 for the same search carrying two aggregations, identical on a filesystem and on a bucket — through core's own `InternalAggregations#topLevelReduce` rather than a second implementation of bucket merging and error bounds; doing it uncovered four placeholders that had been correct only while no aggregation could reach a shard — a null `ValuesSourceRegistry`, a `BigArrays` that cannot allocate because its breaker service is null, a second such one inside `SearchService`, and an `OriginalIndices.NONE` that trips an assertion the moment the shard request cache tries to key on it. **`sort` no longer is** ([`m30-sort-notes.md`](m30-sort-notes.md)): every shard could already sort itself, since its query goes through the same `SearchService` a classic node uses, and the missing half was a coordinating node that dropped the sort keys and merged by score. Shards now attach their keys to their hits, which travel inside a serialised `SearchHit`, and the merge uses them — proved over three shards whose documents interleave, with paging asserted after the merge and one case split across two nodes so the keys have to survive the wire. **Both fan-outs are now concurrent** and a query's cost is finally counted ([`m20-fanout-notes.md`](m20-fanout-notes.md)): warm 3 requests, forwarded 10, and **41 for the first query on a node holding nothing with nobody owning the shards** — the scale-to-zero number, identical on a filesystem and on a bucket. Making the shards run together exposed a check-then-act race in `ShardReconciler` that surfaced as unreachable shards, and the concurrency is asserted by a rendezvous rather than a stopwatch. **Deleting an index now deletes it** ([`m32-index-deletion-notes.md`](m32-index-deletion-notes.md)): storage is keyed by index name, so creating an index whose name had been used before opened the old commit and returned the old documents — a caller creating an empty index and being shown data. Fixed in the path: a shard's storage is keyed by the index's uuid as well as its name, and a uuid is minted per create, so two indices sharing a name cannot share a path however the earlier one ended. Deletion also purges the bytes, which nothing did before, so a deleted index stops costing storage. The first version of the fix cleared the path on create instead, which worked and cost a second object-store operation on the cheapest operation in the system — `testCreationCostIsFlatInThePopulation` reported that within the hour, which is what a cost test is for; keying by uuid gives the same guarantee for nothing. **Multi-index search is in** ([`m33-multi-index-search-notes.md`](m33-multi-index-search-notes.md)): a comma-separated list is resolved by name — one register read each, no listing — and every shard of every index goes out in one fan-out with the window cut once over the whole set. **Prefix patterns now work** ([`m42-index-patterns-notes.md`](m42-index-patterns-notes.md)): `logs-*` is one bounded `ListObjectsV2` with a maximum key count, measured at 1 object-store request against 3 indices and 1 against 63, so the cost is set by the cap rather than by the population — which is what §6.3's refusal of enumeration was actually protecting, since the rule was never that listings are forbidden but that a request must not cost the size of the deployment nor answer from a subset while looking complete. A pattern matching more than the cap is refused rather than truncated, a pattern matching nothing is an error rather than an empty answer, and a pattern that is not a prefix stays refused because it cannot be answered by a listing at all. An index in the list that does not exist is refused rather than dropped. Doing it found that a hit did not know which index it came from — `SearchHit`'s index field is transient and set from a shard target the shell never set — so every hit would have claimed to come from the whole list. **`_mget` is in and `_source` filtering turned out never to have been missing** ([`m35-mget-and-source-notes.md`](m35-mget-and-source-notes.md)): source filtering comes free from running a shard's search through the same `SearchService` a classic node uses, and needed a test rather than an implementation; `_mget` fans its documents out concurrently, answers per item so one bad entry cannot lose the good ones, and distinguishes an index that does not exist from a document that is not in one. **Aliases, `ignore_unavailable` and point-in-time readers are all in.** An alias lives in the same register namespace as an index descriptor, so the two cannot take one name and resolving either is one read; `ignore_unavailable` turns an unreachable named index into a `skipped` entry, and only when the caller asked for that. **A point in time** ([`m37-point-in-time-notes.md`](m37-point-in-time-notes.md)) copies each shard's commit into a record rather than pointing at one — a manifest register holds only the current commit, so "the commit at 10:03" cannot be referred to afterwards — and the garbage collector treats a live view's blobs as referenced, without which the sweep would collect a caller's commit halfway through their paging. Building it found that a view carries the name of the index it is a view of, and that seven places on the write path identified a shard by name and number alone: a write could match the view, find a reader, and answer `503 activation_in_progress` for a shard that was open, owned and healthy — sometimes, depending on set iteration order. `openShards()` now means the shards a node serves as itself, which fixes all seven and the eighth nobody has written yet. **Circuit breakers are real now** ([`m34-circuit-breakers-notes.md`](m34-circuit-breakers-notes.md)) — every component had been given a `NoneCircuitBreakerService`, so a single aggregation could exhaust a node's heap; the node now builds core's own hierarchy with core's own defaults, and an aggregation past the limit is refused while the node keeps serving. Wiring it exposed that a search **no** shard could answer returned 200 with zero hits and a `complete:false` flag — a confident empty answer wearing a disclaimer — so the failure that stopped every shard is now the response, as itself, keeping a breaker at 429 and an authorization refusal at 403; and no sweep for shard containers orphaned by the residual publish race. **A frozen search now fans out by placement too** ([`m44-frozen-search-placement-notes.md`](m44-frozen-search-placement-notes.md)): every shard of a point-in-time view used to be opened unconditionally on whichever node coordinated the request, so a wide view's whole cost landed on one node and a second coordinator (the next page, no session affinity) paid it again. It now tries the shard locally, then the placement-preferred peer over the wire, then opens it here as the guaranteed fallback — `askOneShard`'s own shape, extended rather than reinvented, and still only a hint: nothing durable records which node holds which view. See [`m11-notes.md`](m11-notes.md). | M10 |
| **M12** | ✅ **RUN ON A PROVIDER.** The whole suite (7 conformance tests, plus 2 self-checks) passes against **MinIO**, through OpenSearch's own `S3BlobContainer` — the container the shell would use, not a lookalike. The recorded blocker (a package-private constructor) was never the real one; three things were: `S3Service` NPEs when `opensearch.path.conf` is unset, `deleteBlobsIgnoringIfNotExists` secretly delegates to the async client, and `serverSideEncryptionType` is compared with `String.equals` so null is an NPE. **Checked on this transport, not just on a filesystem:** a lying compare-and-swap produced 8 winners and a range-ignoring read returned 4096 bytes for a 64-byte request, so the green run discriminates. `./gradlew :serverless:testkit:s3Test`; skips loudly with no endpoint. **Remaining:** MinIO is not AWS S3 — conditional-write linearizability under real contention, listing bounds and clock skew on S3/GCS/R2 are still unproven. See [`r11-conformance.md`](r11-conformance.md). | — |
| **M13** | ✅ **DONE.** A node runs by itself, as a process. **Three drivers, not one timer** — lease renewal on a jittered clock at `ttl/3` (the only genuinely clock-bound job; ±20%, 159 distinct delays in 200 draws, so a co-started fleet does not renew in lockstep), publication **edge-triggered by a write and coalesced** (200 writes → 1 publish), activation **triggered by failure** (a fenced publish, an unreachable owner, a write to an unowned shard). A slow backstop runs the full `tick` regardless, so the edges are accelerators and the timer is the guarantee. `ServerlessBootstrap` is a real `main()`: it starts, serves, takes shards on demand up to a cap, renews, publishes and releases its lease on shutdown, with nothing driving it. **Demand-driven activation** answers what a fresh node serves — off by default in the library, on by default in the daemon, both visible. Twenty canaries planted and caught; one found a vacuous assertion that in turn found `release()` was never called in production. **Liveness is now batched and there is only one mode** — per-head expiry was measured at 34 implied S3 requests per idle tick at 8 shards against 18 batched (it compare-and-swaps once per shard), made the default, then removed outright rather than left as a switch nobody should choose; `ShardHeadStore` refuses to construct without an oracle. Flipping the default exposed two real properties that per-head had been hiding: a node must publish its lease *before* acquiring a head, and a head's stamp is a floor rather than the whole answer. **A node now lets go of shards nobody is asking about** ([`m21-idle-release-notes.md`](m21-idle-release-notes.md)), which is what makes scale-to-zero a property of the system rather than an operator killing the process — before it, a shard was released only when ownership was taken away, and `maxShardsHeld` was a refusal rather than an eviction. The threshold is derived from M20's two measured numbers (holding costs reads per tick; re-opening costs ~41 requests) rather than picked, and a writer publishes, releases its head, then closes — in that order, because closing while the head still names you is volunteering to be the thing failover routes around. **Eviction now has hysteresis and a per-index pin** ([`m13-hysteresis-notes.md`](m13-hysteresis-notes.md)) — a full node clears down to a floor below the cap on the first eviction pass a burst needs, rather than one slot per arrival, so the O(open shards) victim search runs once per burst instead of once per arrival; a pinned index is exempt from both cap-pressure eviction and idle release, a node-local runtime setting by design, not a durable per-index record. Intervals and caps are otherwise reasoned, not measured; single process, filesystem-backed (D5). See [`m13-notes.md`](m13-notes.md). | M11 |
| **M14** | ✅ **DONE.** **The shell runs on an object store** (`serverless.store.type: s3`, reached through `S3RepositoryPlugin#getRepositories` rather than package-private internals): a node boots against a bucket, publishes its commit, renews its lease and answers searches, and a successor with its own `path.home` rebuilds from the bucket alone. Doing it found a bug no filesystem test could — `IndexInputStream` had no mark support, which the S3 client requires to retry, so **publishing a segment was impossible on S3 and worked perfectly on disk**. Lazy block-range reads are done. See [`m14-object-store-notes.md`](m14-object-store-notes.md). **Costs now counted, not argued** ([`m14-cost-notes.md`](m14-cost-notes.md)): §7's batched leases hold exactly on the write side — compare-and-swaps per tick are constant at 1.0 from 1 to 8 shards — but each held head is still *read* every tick, so an idle tick is O(shards) either way and batching improves it by a constant rather than an order. One document is exactly one object-store write. An idle node costs ~1,800 requests/hour. This row's own "Remaining" list had drifted: name index / prefix search shipped at M42; `ServerlessCostTests` already measures S3, not only the filesystem; and `ServerlessBucketContentionTests` already combines contention with object storage. **Shard-count scaling and per-index memory are now measured** ([`m14-shard-memory-notes.md`](m14-shard-memory-notes.md)): an open writer shard costs ~67 KB of heap and exactly 3 file descriptors, a reader shard ~50 KB — about 27% lighter, with no translog or indexing buffers — and the marginal cost of the next 30 open shards is not larger than the first 30, in two independent measurements. Notably lighter than core's own measured 150,888 B for one *gated* (shard-closed) index, because this design carries no `ClusterState`/`IndexMetadata` object graph behind an open shard at all — the 67 KB is the whole cost, not an addition on top of a heavier resident structure. **Remaining:** the whole-deployment orphan sweep is still a single unbounded listing, not paged (out of scope while `server/` may not change — `BlobContainer#children` has no bounded variant). | M11 |
| **M15** | ✅ **DONE.** Production surface. Auth and multi-tenancy landed at M24-M28, not after this row was written. The classic create-body envelope was never an open question — D2 already answers it: not a drop-in for existing clients. "Delete draining a live writer" is done, just not by draining: the head is cleared before the bytes, so a writer loses ownership on its next tick, and the residual publish race is bounded and self-healing, cleared by the next index of the same name ([`m15-delete-notes.md`](m15-delete-notes.md)). Bulk index and delete are both done — the row's own next sentence contradicted the sentence before it. **Conditional writes are refused rather than silently dropped now** ([`m15-production-surface-notes.md`](m15-production-surface-notes.md)): `if_seq_no`/`if_primary_term`/`version` used to be read nowhere on either the single-document or the bulk path, so a caller asking for a compare-and-swap got an unconditional overwrite with nothing to say the request was answered differently than it asked — worse than every other refusal on this surface, because it looks like success. Both paths now refuse with a 501 naming why. Finding the fix's own negative case surfaced a second, unrelated defect: `GET` on a document in an index that had never had a writer activate threw an internal "cannot serve as a reader" exception that surfaced as a 500; fixed by checking directly rather than catching. **An update API is in**: `POST /{index}/_update/{id}` merges a partial document using core's own `XContentHelper.update` — the same utility core's own `_update` uses, recursing into nested objects rather than overwriting them whole — with `doc_as_upsert`, `upsert` and `detect_noop`. Not a transaction: the read and the write are two separate calls with nothing holding the document still between them, which is the honest shape of the feature without the version model above. Built as one gated operation under `indices:data/write/update` rather than composing the already-gated `get`/`index` primitives, which would have run a filter chain twice under the wrong names and never once under its own. **The `cluster/config` register is built**: `GET/PUT /_cluster/settings`, the one piece of `/_cluster/*` not refused, because it genuinely is one CAS register rather than genuinely cluster-wide state — persistent settings only, scalar values only (a list cannot be told apart from an explicit removal through `Settings`'s public surface without risking silent data loss on the next unrelated update), and no validation against a settings registry: a store, not a settings service. The write path found a bug in itself before it shipped — the filter-surface sweep tripped core's own "must not be a transport thread" assertion, because the PUT branch ran its object-store CAS inline on the transport thread instead of dispatching to a worker like every other write handler already does. **`_delete_by_query` completes M15.** Matching is evaluated once against a frozen point-in-time view — the same mechanism `search_after` paging uses, and for the same reason: a query re-evaluated against a moving index could match a document twice or never depending on when a write landed relative to which page was read. It walks one shard at a time in native Lucene `_doc` order rather than through the cross-shard `search_after` merge `_search` uses, because that merge exists to produce one globally ordered page across shards — machinery this does not need, since visiting every match exactly once needs no order that is meaningful *across* shards, only one that is exhaustive *within* one. Deletes go to live state, not the frozen view. Gated once, under `indices:data/write/delete/byquery`, by moving the whole operation onto `ShardOperations` rather than composing the already-gated `get`/`delete` primitives from the REST layer — the same fix `_update` needed, and `BulkHandler`'s own stated reason for gating a whole batch once ("a privilege evaluator... would otherwise never see it"). Two defects found by its own tests before either shipped: deletes defaulted to `refresh=false`, correctly, but the endpoint offered no way to ask for a refresh at all, so a caller verifying via search immediately after saw stale results — fixed by piggybacking a refresh onto the last delete of each shard rather than one per document. And extending the class's own tests to actually assert something happened after a `_search`-based check (rather than only the response's own `matched`/`deleted` counters) surfaced that gap directly. | M11 |

**Ordering.** M10 before M11, and the reason is not preference: shipping a data path first would give
people a way to write to a system that silently drops recent writes on failover, which is worse than
having no data path. M12 has no dependencies and gates every durability claim the project makes —
three phases have now ended pointing at it, and phase 9 *grew* its surface from "is CAS linearizable"
to also "does a listing actually bound", which `FsBlobContainer` cannot demonstrate at all.

**What is deliberately absent rather than pending:** gossip (§10.3's gate, unmet by measurement),
drop-in client compatibility (D2), and Azure support (D3). Those are decisions, not debt.

## 13. Testing

### 13.1 The harness does not come for free
[`InternalTestCluster`](test/framework/src/main/java/org/opensearch/test/InternalTestCluster.java) is 2,790 lines
built on `MockNode extends Node`. It does not follow us. `serverless/testkit` needs a
`ServerlessTestCluster` that starts N `ServerlessNode`s over a shared `FsBlobContainer` acting as the
object store.

**This is the largest single cost in the plan — larger than the shell itself.** It is also the thing
most likely to be underestimated, because it produces no user-visible capability. Budget it explicitly
in phase 1, not opportunistically.

Partial mitigation: `plugins/serverless-storage/src/internalClusterTest/` already contains a substantial
IT suite against object-store-backed behaviour. Those tests encode the behaviours we care about even
though their harness changes.

### 13.2 The dependency rule is a test — **implemented**
`:serverless:shell:checkServerDoesNotReferenceServerless`, wired into `check`. Asserts that no file
under `server/` references `org.opensearch.serverless.*`. §4's rule is worth nothing as a convention.

It deliberately lives in `serverless/shell/build.gradle` rather than `server/build.gradle`: the rule is
ours to enforce, and putting it in `server/` would itself be a change upstream never asked for.

Verified in both directions — a planted canary class under `server/` fails the build, and removing it
passes. A check that has never been seen to fail is not a check.

### 13.3 Inherited discipline
`HANDOFF.md`'s finding applies with more force here than where it was written: a shell with a partial
control plane is *made of* seams that can return confident empty answers. Every IT asserts a number
that is zero when broken. No test asserts only the absence of an exception.

## 14. Risks and open questions

| # | Risk | Severity | Handling |
|---|---|---|---|
| R1 | Projected partial `ClusterState` closes live shards (§5.2) | **Resolved (S1)** | Measured: projection alone is harmless — the shard stayed STARTED and serving. The hazard lives in the reconciler, and becomes the §5.2 contract below. Phase 2 must carry it as a test |
| R2 | Test harness cost dominates (§13.1) | High | Budgeted in phase 1; measured, not estimated |
| R3 | `action/` re-implementation is larger than §6.3 assumes | High | Allowlist + 501 caps the surface; scope grows only by explicit decision |
| R4 | Plugins assume Guice `createComponents` | Medium→**largely handled** | Shell implements the plugin contract without an `Injector`; the ecosystem we must support is small. **The `Client` a plugin calls now exists** ([`m22-plugin-client-notes.md`](m22-plugin-client-notes.md)): `AbstractClient` funnels everything through one `doExecute`, so a working client is a dispatch table rather than the `action/` package §6.3 declined to build. Index, get, delete and create-index are served; everything else is refused by name. **The lifecycle is now built too** ([`m23-plugin-host-notes.md`](m23-plugin-host-notes.md)): a plugin is loaded, `createComponents` runs without an injector, its REST routes are served, and its request wrapper sees every request — the seam authentication belongs on and the one the security plugin uses. A failing plugin stops the node rather than being skipped, and two request wrappers are refused rather than ordered. **System indices are now honoured too** ([`m24-auth-notes.md`](m24-auth-notes.md)): a plugin declares them through `SystemIndexPlugin#getSystemIndexDescriptors` and no REST request reaches one, whoever sent it — refused entirely rather than per caller, because a shell with no authorization layer cannot enforce "only administrators may read this" but can enforce "nothing routes here". The owning plugin still reaches it through the `Client`. Writing real authentication against this host also found three defects a test plugin could not: the `NodeClient` given to every REST handler was wired to nothing, no plugin was ever closed, and the client's failures were untyped across the plugin boundary. **Disk loading now works** ([`m25-installed-plugins-notes.md`](m25-installed-plugins-notes.md)): a directory under `plugins/` with a descriptor and a jar is loaded through core's own `PluginsService` — the class the shell was already constructing with empty arguments purely because `IndicesService` demands one — so descriptor validation, version compatibility, jar-hell checks, `extended.plugins` ordering and classloader construction are core's behaviour rather than a second implementation of it. Modules are still deliberately not loaded. Proved by a test that compiles its own plugin into a jar, because a class already on the test classpath would be found whether or not a classloader was built. `serverless.plugins` remains as the classpath route for a plugin a distribution ships. **And what a plugin declares is now honoured** ([`m26-extension-points-notes.md`](m26-extension-points-notes.md)): the analysis registry, mapper registry and search module were built from empty lists, so an installed `AnalysisPlugin` loaded and contributed nothing — and the hand-built registry was also missing core's own built-in analyzers. Proved end to end by installing `analysis-icu` as the build assembles it, with its 14MB `icu4j`, and asserting that a document only matches a query because the plugin's token filter ran. Three of the four null collaborators are now real; the fourth, `IndexNameExpressionResolver`, cannot be — a node's `ClusterState` holds only the indices that node has open, so resolving against it answers a wildcard with a subset, which for a plugin evaluating privileges fails open — and so it refuses by name, with a reflection test that fails the build if core adds a method it does not override. Doing this found that `IndexDescriptor` could not carry a list-valued setting, which ruled out configuring any analysis plugin at all. All three extension points have an end-to-end test with a control that fails without the plugin: `analysis-icu` folding a search term, `mapper-murmur3` providing a field type, and a compiled `SearchPlugin` whose query the shell parses, builds and runs. **`ActionFilter`s now run** ([`m27-authorization-notes.md`](m27-authorization-notes.md)), which closes the last structural gap: a filter needs an action name and a request, and neither requires the `action/` package §6.3 declined to build. `ActionGate` drives the chain with the operation as its terminal step, at every endpoint that reads or changes data, with a coverage test that names each one. Proved by a filter that refuses a reader the write an administrator is allowed, reading the authenticated principal out of the thread context — authentication and authorization together. Writing that coverage table found that the REST write and search paths both went round `ShardOperations`, so the first version guarded a plugin's own reads and none of the user's traffic. **Audited against the real security plugin** ([`m28-security-plugin-audit.md`](m28-security-plugin-audit.md)): of the seventeen hooks it declares, the shell carries roughly two thirds, including everything that decides whether a request is allowed. `onNodeStarted` was missing and is now called — Security reads its configuration index there, so without it the plugin would have been loaded, constructed and never started. What is missing is not authorization but transport: `NetworkPlugin` is not consulted, so the plugin cannot install its TLS transport, and `getTransportInterceptors` is not consulted, so identity does not propagate between nodes. **`NetworkPlugin`s are now consulted** ([`m29-network-plugins-notes.md`](m29-network-plugins-notes.md)): core's `NetworkModule` already resolved transports by name and composed interceptors, and was simply being given netty4 and nothing else. A plugin's transport interceptor now sees the shell's own node-to-node forwarding, and a plugin's `additionalSettings` reaches the node — layered under an operator's settings and over the shell's defaults, where the shell's transport choice used to sit on top of everything. Doing that exposed a general split: every extension point was filtered through `PluginsService`, which knows only disk-loaded plugins, so a plugin handed to the constructor was honoured differently from an installed one. One list now, `onIndexModule` excepted. **Remaining:** no `Transport` has actually been substituted, so TLS through a plugin is enabled rather than demonstrated; the shell still does not propagate identity between nodes, deliberately, because a header travels to anything that can reach the transport port; filters see requests, not responses, so document-level security and field redaction cannot work; no action registry, so its config-update API would not work; and most of what a `SearchPlugin` can offer stays unreachable while aggregations, suggesters, highlighting and rescoring are 501 |
| R5 | Two shells to maintain until parity | Medium | `server/` stays untouched, so the cost is ours alone, not upstream's |
| R6 | Upstream drift in the data plane's internal APIs | Medium→Low | §8.1's extraction narrows the coupling surface to five shell interfaces plus four shared ones. Merges stay routine; D4 rule 1 is CI-enforced |
| R7 | Snapshot/restore, security, ISM assume the old shell | Medium→**partly answered for authentication and snapshot/restore** | ISM remains out of scope (§16). **Snapshot/restore is no longer absent** ([`m45-snapshot-notes.md`](m45-snapshot-notes.md)): a repository is a namespace within this deployment's own object store rather than a distinct storage backend — this shell has exactly one object store (D3), and a snapshot's data already lives durably in it, so registering one records a name and nothing to configure. Taking a snapshot costs no data movement: it reads each named index's currently published manifests and writes one record referencing blobs that already exist, the same shallow shape a point in time already has, and `indices` must be named explicitly — this surface still does not enumerate a deployment's population. Restoring is not free, and the reason is a real invariant rather than an oversight: shard storage is keyed by an index's own uuid, so a restore (always a new index, a fresh uuid, for the same reason M32 keys storage by uuid rather than name) has to copy the referenced blobs into the new index's own storage. The garbage collector now also protects a live snapshot's blobs, keyed by uuid rather than by index name — tighter than the point-in-time check, which a name reused by an unrelated later index could otherwise fool. None of this touches `RepositoriesService`: the classic repository-plugin API surface still yields null to a plugin, unchanged, because this is a shell-native surface built the same way `_pit` was, not a workaround for the missing service. **Both shallow and standard snapshot exist now, API-compatible with real OpenSearch** ([`m46-shallow-standard-snapshot-notes.md`](m46-shallow-standard-snapshot-notes.md)): `remote_store_index_shallow_copy` is a repository setting, not a per-snapshot choice, verified against real OpenSearch's own source rather than assumed — standard (copying each shard's blobs into the repository's own storage at capture time, real cost, complete independence from the index it was taken from) is the default, matching real OpenSearch's own default. Scoping standard mode surfaced a real gap in shallow mode itself: index deletion purged a shard's storage unconditionally, silently destroying a shallow snapshot taken from it — the ordinary shape a backup is used for. Fixed by extending the collector's own uuid-keyed pin check into index deletion. Extending the authorization sweep to the new endpoints found a second real gap: `handleCreate` and `handleRestore` ran their read and validation work *before* gating rather than inside it, so a caller refused early (a missing repository, an unpublished shard) never reached a filter at all — fixed by moving the whole operation inside the gate, matching `update`'s own shape. Request and response bodies now match real OpenSearch's `_snapshot` API — `rename_pattern`/`rename_replacement` is the real Java-regex mechanism rather than a literal name map, a restore's target collisions refuse the whole request rather than one item of it, repository `PUT` is an upsert, and `GET /_snapshot` (no name) is allowed the same way `/_cluster/settings` is: a repository population is operator-scale, not deployment-scale. **Authentication is no longer absent** ([`m24-auth-notes.md`](m24-auth-notes.md)): the shell's own authentication is written as a plugin in `serverless/auth`, which depends on `:server` and not on the shell, so everything it uses is API a third-party plugin has. HTTP Basic at the request wrapper, accounts in an index reached through the `Client`, salted PBKDF2 records, one configured account in the keystore checked before the index so an unreadable store is not a locked door, and `IdentityPlugin` so a plugin gets the real caller. **It authenticates; a separate plugin now authorizes** — M27 made `ActionFilter`s run, so privilege evaluation keyed on action names is possible, though the authentication plugin itself still ships no policy beyond one rule (only the configured account may manage accounts). **The wire is protected and the peer can be authenticated** ([`m39-tls-notes.md`](m39-tls-notes.md)): the shell was building its network module with an empty collection of secure-settings factories, so core never called `getSecureTransports` and a plugin naming the secure transport was told the type did not exist — TLS through a plugin was impossible rather than undemonstrated. It is collected now, and mutual TLS works: a node presenting a certificate is served and one presenting none is refused. **A filter can also rewrite what comes back** ([`m40-response-filtering-notes.md`](m40-response-filtering-notes.md)) — a search and a get are shown their real responses, so field redaction and document-level filtering both work, while coverage stays the shell's so a redaction cannot masquerade as a partial answer. **And a plugin's own transport actions run** ([`m41-plugin-actions-notes.md`](m41-plugin-actions-notes.md)), built by resolving each constructor against what the node can supply, since Guice is not on offer. What remains: filters run once, on the node the request reached, and not again on the node a write is forwarded to — a caller's identity does travel in a thread-context header, so the mechanism is there and the second enforcement point is a design choice not to have |
| R8 | Object-store list consistency is still unmeasured | Open | Pre-existing open item, carried in `plan-100m-index-implementation.md`; unchanged by this RFC |
| R9 | Zombie writer corrupts data past lease expiry (§9.6) | **Closed for published data (phase 6)** | Term-scoped containers + manifest CAS, both canary-verified. The paused-JVM test confirms a zombie's writes succeed locally, cannot be published, cannot overwrite a live writer's blobs, and vanish on release. Residual: unpublished writes are lost, which is the WAL gap, not a fence failure |
| R10 | No watch primitive; polling cost at fleet scale (§9.5) | High | Epoch piggybacked on existing transport traffic; poll is the idle fallback. Measure GET/s at target fleet size in phase 9 |
| R11 | Provider conditional writes are not as linearizable as assumed | **Critical — deferred by D5** | Unchanged in severity; only its timing moved. The entire safety argument still rests on it. Phase 3 may proceed on `FsBlobContainer` alone, clearly labelled as such; no durability claim against S3 or GCS is permitted until the conformance suite runs |
| R12 | Cross-node version comparisons beyond `ReplicationTracker` (§5.3) | **Confirmed and fixed in design (S1)** | Hazard reproduced (silent ignore) and the shard-head-generation fix verified. Residual exposure: any *other* cross-node number, still unenumerated |
| R13 | Gossip is a new distributed protocol, hand-built, tuned at 10⁴ nodes (§10.3) | High | Deferred to phase 8 and gated on measurement — the design must work without it first. Vendor rather than invent if it is needed |
| R14 | Readiness mistaken for ownership on Kubernetes (§10.1, §10.2) | **Critical** | Ownership reads the shard-head. A conformance test asserts a Ready pod with an expired lease serves nothing — this is R9 wearing a different hat |

Open questions this document does **not** answer, and should not be read as answering:

- What the wire-compatibility story is with classic OpenSearch clusters (CCS/CCR). Currently: none.
- Whether §9.4's dissolution of the control cell survives review. It is the most aggressive claim in
  this document, and the fallback — a small consensus group for `/cluster/config` only — is cheap
  enough that being wrong here costs a phase, not the design.
- How index-level security and multi-tenancy land in a shell with no security plugin.

## 15. What carries over from `feature/serverless`

The seam work was not wasted, and this is not a restart. It is the thing that makes S0 answerable in
days rather than months.

- **Carries over unchanged**: the object-store engine, WAL, segment bundles, block cache; descriptor
  CAS and `BlobContainer.readRegister`/`compareAndSwapRegister`; manifest sharding; computed placement;
  name index tier; write-partition routing; every design document.
- **Carries over as design**: `rfc-serverless-metadata-plane.md` §5–§9 is the control plane, adopted
  wholesale (§9).
- **Becomes unnecessary**: the pluggability seams themselves — `EnginePlugin` per-shard-role dispatch,
  the lifecycle SPI, the descriptor-write seams, `ShardRecoveryStrategy` validation. Not because they
  were wrong, but because the shell chooses its implementations directly instead of negotiating for
  the right to substitute them.
- **Becomes moot**: `rfc-serverless-control-cell-diet.md`. §5 dissolves the problem it addresses.
  **It should not be approved while this RFC is open** — approving `IndexMetadataOrStub` commits
  `server/` to a pervasive type change for years, and it is the wrong thing to build if §5 holds.

`feature/serverless` stays alive and unmerged until S0 answers. If S0 falsifies §5, that branch is the
plan of record and this one is deleted.

## 16. Non-goals

- Replacing Lucene, the document model, the mapping system, or the query DSL.
- Reimplementing the REST API surface in full. §6.3 is an allowlist by design.
- Wire or index-format compatibility with classic OpenSearch beyond what the reused data plane
  provides for free.
- Running classic and serverless indices in one process. That constraint is what this RFC removes;
  reintroducing it defeats the purpose.
- Forking `server/`. Every temptation to edit it is a §4 violation until proven otherwise.
- Depending on Kubernetes. It is one `MembershipSource` among three (§10.2); the blob-lease default
  must remain fully supported, or the system has acquired an orchestrator dependency it does not need.
- Azure Blob support (D3). The in-tree register implementation is left in place, untested and
  unsupported by this work; adding it later is a conformance-suite run, not a design change.

## 17. Summary

416k lines of data plane are almost entirely independent of the 119k lines of control plane that this
architecture needs to replace — `IndexShard` makes zero `clusterService` calls, `IndicesService` uses
it only as a settings bus, and the shard lifecycle interface takes value objects. `ClusterService` is
382 lines that start without a coordinator, and `ClusterApplier.onNewClusterState` is public.

And the truth those views are computed from is itself just blobs: a handful of small registers —
config, node leases, index descriptors, shard-heads — each mutated by a compare-and-swap that S3, GCS,
Azure and Fs already implement in this repo today, with the codecs to serialize every cluster-state
component into them already written in `gateway/remote/model/`. A CAS register has consensus number ∞,
so this gives up no safety relative to Raft; it gives up change notification, multi-object atomicity,
and cheap linearizable reads, which §9.5 and §9.6 cost out rather than wave away. What it buys is
**zero consensus processes**: nothing to elect, nothing to bootstrap, no quorum to lose.

So: keep the engine, delete the consensus, and let `ClusterState` become what it should have been for
this workload all along — a node's local view of the shards it happens to be serving, computed from
object-store truth, cheap in proportion to the working set rather than the world.

S0 decides whether the first half of that is true. R9 and R11 decide whether the second half is.
