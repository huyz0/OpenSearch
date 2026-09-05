# Deep review of the serverless-storage plugin — implementation against goal

September 2026. Seven code-grounded reviews ran in parallel over
`plugins/serverless-storage` (427 production files, 58.5k lines; 373 test files, 68.8k lines), each
reading the RFC sections that govern its area and then the source: goal match, storage format and
durability, the reader path and caches, the descriptor control plane, lifecycle and resharding,
garbage collection and snapshots, and security. Every headline finding below was re-verified by
reading the cited lines. The seven full reports are in the session scratchpad
(`plugin-A-goals.md` … `plugin-G-security.md`, 4,787 lines).

> **Status:** every finding below has been addressed; what each fix is and how it was verified is in
> [`serverless-storage-review-fixes-2026-09.md`](serverless-storage-review-fixes-2026-09.md). The text
> below is left as written so the fixes can be read against the findings.

**Baseline before any of this:** the plugin's 1,486 unit tests all pass. Nothing below is a broken
build. What follows is what the tests do not cover.

**The short version.** This is a much more ambitious codebase than the shell, and its hardest parts —
the compare-and-swap head protocol, the bundle and log formats, the clone pin protocol — are careful
work with TLA+ models behind them. The problems are of three kinds. First, five paths can lose or
corrupt acknowledged data, and each survives because the test that would catch it uses a fixture too
simple to trigger it. Second, the defaults ship the goals off: the object store is not the durable
home of a write unless an operator turns write-ahead-log mirroring on. Third, the RFC's central
affordability claim — that this is delivered predominantly as a plugin with a small set of core seams
— is no longer true, and the gap is large enough that the document misleads.

---

## 1. Does the implementation match the goal?

| Goal or principle | Status | Evidence |
|---|---|---|
| §2.1 the object store is the sole durable home of segments and write-ahead data | **Contradicted by default** | `wal_mirroring.enabled` defaults false, so the writer engine falls back to a plain local translog. Unflushed acknowledged writes die with the node, with no error. |
| §2.2 two asymmetric shard roles, reader shards never holding a full local copy | **Partial** | The default reader path is the eager commit materializer, a full local copy by its own javadoc. The lazy block-cached directory is double-gated off: node cache size zero and a per-index flag that is false and final. |
| §2.5 delivered predominantly as a plugin, core changes minimal and confined to §15's list | **Contradicted** | §15 lists eight seams. Core actually gained 63 new production files, including a roughly ten-interface gated-metadata plane (`IndexCatalog`, `IndexCreationStrategy`, `ClaimedIndexLifecycle`, `IndexResidencyPolicy` and the absent-descriptor suppliers threaded through 20 core files), a new shard-recovery strategy SPI with its own index setting, `BlobRegister`, and in-place split actions. I verified all six named classes exist under `server/`. |
| §2.6 classic mode remains the default and untouched | **Partial** | Engine dispatch is clean, but an index is opted in **by name**: the `serverless_` prefix is unreserved and ungated, so any user creating `serverless_foo` gets the object-store engine. Eleven cluster-wide hooks install unconditionally. |
| §5.2 batch writes, stream reads | **Contradicted** | Log batching defaults off, so the legacy path is about three object-store requests per document. Streaming reads are the off-by-default lazy path. |
| §5.4 fencing by term, not by lock | **Done, and the strongest work here** | Compare-and-swap head protocol with TLA+ models and runtime fuzzing — except for gated indices, see C2 below. |
| §7.4 compaction, §6.5 garbage collection | **Structurally unreachable** | Both schedulers are constructed inside the search-only shard branch. I confirmed the compaction config sits inside `if (isReaderShard)`. An index with no search replicas never compacts and never collects. |
| §16 phases 4 and 4.5 marked done | **Contradicted** | Phase 4's cold-start measurement classes were deleted; phase 4.5's schedulers are the unreachable ones above. Risks 2, 8 and 9 cite benchmarks that no longer exist. |

---

## 2. Critical: five ways to lose or corrupt acknowledged data

**C1. Garbage collection can delete the live commit.** The collector never reads the shard head — I
confirmed there is no reference to a head store anywhere in it — and takes "latest" from a blob
listing. The publisher writes the manifest *before* the head compare-and-swap. A crash in that gap
leaves an orphan manifest strictly newer than the head, so the next sweep classifies the real head
manifest as superseded and, after the retention window, deletes it and its exclusive bundles. The
shard is unrecoverable. The existing dual-writer test only covers the safe older-term orphan.

**C2. Gated indices have no writer fencing at all.** Every gated shard's primary term is pinned at
one, and the publisher's refusal branch only fires when the lease term exceeds the primary term, so
that branch is dead code, and the lease holder's node id is never compared to the publishing node.
Two nodes that disagree across a membership epoch both acquire the lease and both publish, and the
head alternates between divergent manifest lineages. The RFC's "losers see a precondition failure"
does not hold on this path.

**C3. Compaction output is invisible to a reader, then corrupts it.** Compaction merges into an empty
in-memory directory, so every compaction commits a file named `segments_1` — verified against Lucene
10.5.0 as the same name and the same 155-byte length as the writer's own, with different bytes, and
segment names recycle across compactions. The reader picks the highest generation in an additive
directory and so serves pre-compaction data while reporting the compacted generation; the
materializer's equal-length check then skips the fetch; and when lengths do differ it rewrites files
the open commit references, producing a corrupt index on the next restart.

**C4. The lazy block cache can serve the wrong bytes.** Blocks are keyed by Lucene file name and block
number alone, with no bundle, no generation and no checksum, in a directory that persists across
restarts, and core's transfer manager trusts an existing on-disk block without validating it. This
triggers on compaction and on any plain shard reopen.

**C5. A synchronous write is acknowledged before its log chunk is uploaded.** The mirroring translog
assigns the pending future *after* enqueuing, so two concurrent appends can leave the field holding
the older future, and the sync path waits on that one while ignoring the location it was asked about.

Beside these, the review found that nested child documents are silently hidden by every split (their
identifier is not stored, and the filtering reader excludes documents without one) and then
physically deleted on the next sibling merge, with nothing rejecting a nested mapping.

---

## 3. High findings by area

**Durability.** The replay term floor is computed as the current term minus one, so two term bumps
without an intervening publish silently drop the still-unpublished records of the original term. A
fenced writer keeps acknowledging writes until its next flush because the lease-renewal failure is
discarded and the write path never consults the lease. A failed overflow of a shard's log records
drops them instead of restoring them, and does its upload under a monitor that blocks every other
shard. The commit manifest is the only on-disk format with no magic number, no version and no
checksum. There is no retry or backoff anywhere on the publish path, so one transient error fails the
engine, and a brownout fails every writer shard within one refresh interval. Publication uploads the
whole commit rather than the new files, which caps a shard at about two gigabytes and spikes heap by
twice the shard size on every flush.

**Readers.** The refresh-deferral budget reads total cache usage, which an LRU keeps near capacity by
design, so once warm every reader open throws and every refresh is deferred forever. A failed engine
bypasses close, leaking its admission permit and continuing to poll, compact and collect. Generation
pinning, which §7.2 requires, does not exist: the collector is passed an empty pin set and relies on a
thirty-minute window, so on the lazy path a collected bundle kills a live query. Commits can be
materialised in an order that writes the segments file first, and the refresh path is unsynchronised
against the poll.

**Control plane.** Every node applying a cluster-state diff writes the descriptor, so one metadata
change becomes N nodes times several compare-and-swaps on one key. A cache invalidation is lost
against a load already in flight, so a deleted index resolves as live and takes writes for the cache
window. A mapping compare-and-swap never checks the descriptor's identifier, so after a delete and
recreate a stale writer's dynamic field lands on the new incarnation. A put at the absent generation
resurrects a deleted descriptor. The directory tier has no production caller at all, and
prune-before-activate does not exist.

**Lifecycle.** The default orchestrated split fences the source unconditionally and then returns
early without enabling write routing, and no unfence primitive exists — I verified both. Writes to
that index are stuck permanently and the only escape is deleting it; the integration test always
passes the non-default flag. The in-place split filter is installed as a reader wrapper, so its scan
over every document runs per search request. Two builders omit the soft-deletes field, so a split of
any index that ever took a delete throws. A shard activity registry with no unregister lets a stale
entry suspend a live busy shard. There are three ways a node stays "warming" forever, after which an
allocation decider blocks every reader shard.

**Security.** Not one of the plugin's 38 transport actions implements the request interface that
index-level authorization keys off, so per-index privileges and document or field security
structurally cannot apply to any of them; clone, shrink and split then become a cross-index data-copy
primitive behind a single cluster-wide grant. Seven actions pass a caller-supplied index identifier
into blob paths with no validation, and `..` escapes the base path. The cipher never authenticates
associated data and the block header is unauthenticated, so blocks are swappable between blobs under
one key. Per-index keys exist as a class that is never constructed outside tests. Two handlers accept
unbounded lists that are written into cluster state.

**Cost at scale.** The change-log design costs roughly 37 listings per node per five-second pass,
which is about 74,000 listings per second at ten thousand nodes while completely idle. Leases
heartbeat per shard rather than per node, which is 400,000 operations per second at two million active
shards — precisely the per-shard heartbeating the RFC says must be batched. The directory rebuild
walks every index and every shard serially, which does not finish at the target scale.

---

## 4. What holds up

The head compare-and-swap protocol and its term fencing, with TLA+ models and fuzzing, on the
non-gated path. The bundle and log readers, which bound counts before allocating and reject duplicate
names and out-of-range entries. The log garbage collector's conservative all-or-nothing bail-out. The
pre-commit log-position snapshot, whose reasoning is correct and closes a real silent-loss window. The
disk bundle cache, which is content-addressed and re-verifies its checksum on every hit, so it cannot
serve wrong bytes. The compaction rebase protocol, which cannot clobber a writer's manifest. The clone
pin-before-read ordering. Settings hygiene: 83 defined, 83 registered, enforced by a test. And the
encryption seam does reach the clone, shrink and snapshot paths through one resolver.

---

## 5. Fix order

1. **The three deletion and corruption paths that need no concurrency to trigger**: the collector must
   read the head and anchor "latest" to it, failing closed; compaction must not emit a colliding
   commit name; the lazy cache key must carry the bundle and generation and be validated.
2. **Fencing for gated indices** — a real primary term, and compare the lease holder against the
   publishing node.
3. **The acknowledgement path** — the pending future must be per-location, and the lease-renewal
   failure must fence the writer rather than being discarded.
4. **The default split** — either fence only when write routing is enabled, or add the unfence
   primitive. This one is a permanent outage from a documented, default invocation.
5. **Nested documents in splits** — reject a nested mapping, or store the identifier. Add one deleted
   document and one nested document to the resharding fixtures; that single fixture change catches
   this and the soft-deletes bug, and the correct implementation already exists in the same package.
6. **Garbage collection and compaction must be node-level tasks**, not reader-engine tasks. Until then
   a common index never reclaims anything, and index deletion leaks every object-store byte.
7. **Security**: make the requests index-aware so authorization can apply, validate the identifier
   before it reaches a path, authenticate the block header, bound the list parameters.
8. **The defaults that contradict the goals**: log mirroring and batching should follow the index
   opt-in or fail loudly, and the refresh budget should measure active usage.
9. **The cost model**: batch leases per node, stop broadcasting descriptor writes from every node, and
   give the change log a bounded fan-in.
10. **The documents**: correct §15 to state the real core footprint, mark the phases that are not
    done, and reopen the risks whose evidence was deleted.

The cheapest high-value change in the whole list is the last one in item 5, and one integration test
that runs scale-to-zero, an in-place split and a drain against the same index. Every cross-cutting
finding here is invisible only because each subsystem is tested alone.
