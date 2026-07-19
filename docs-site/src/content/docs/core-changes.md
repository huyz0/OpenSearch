---
title: Core Changes
description: Changes made to OpenSearch core (outside plugins/serverless-storage) to make a pluggable, per-shard-role storage engine possible.
---

Everything on this page is a change to OpenSearch **core** (`server/`, `modules/`, blob-store repository plugins) — not to `plugins/serverless-storage` itself. These are the extension points and seams core needed before an external plugin could implement a writer/reader-split, object-store-native engine at all. For the plugin's own design and its internal correctness fixes, see [Architecture](/design/architecture/) and [Plugin Fixes](/plugin-fixes/).

`rfc-serverless-opensearch.md` and `rfc-serverless-metadata-plane.md` (repo root) are the design-level proposals behind this work — this page is the concrete, as-built list of what actually changed in core to realize them.

## 1. Per-shard-role engine and directory selection

Before this change, a plugin's `EngineFactory` and `Directory` were chosen once per index, at index-service construction — every shard copy of an index got the same engine. That makes a writer/reader split impossible: there was no hook to say "this specific shard copy is a search-only reader, give it a different engine than the primary."

**`EnginePlugin.java`** — before, a plugin only got the index's settings, never which shard copy was being created:

```diff
 public interface EnginePlugin {
+    /**
+     * Unlike the index-level method, this overload also receives the shard's ShardRouting,
+     * so a plugin can select a different EngineFactory depending on the role of the shard
+     * copy being created (for example, a promotable/writable copy versus a search-only copy
+     * that will never be promoted to primary). Defaults to delegating to the existing
+     * single-argument overload, so existing plugins are unaffected.
+     */
+    default Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings, @Nullable ShardRouting shardRouting) {
+        return getEngineFactory(indexSettings);
+    }

     /**
      * EXPERT: When an index is created this method is invoked for each engine plugin. ...
      */
     Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings);
 }
```

**`IndexStorePlugin.java`** — the matching overload on the directory side:

```diff
     Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException;

+    /**
+     * Same as newDirectory(IndexSettings, ShardPath), but also given the ShardRouting of the
+     * specific shard copy being created, so an implementation can pick a different directory
+     * depending on the copy's role. Defaults to delegating to the two-argument overload.
+     */
+    default Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath, @Nullable ShardRouting shardRouting)
+        throws IOException {
+        return newDirectory(indexSettings, shardPath);
+    }
```

**`Node.java`** — the provider collection threaded through plugin wiring changes shape to carry the routing:

```diff
-final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders = enginePlugins.stream()
-    .map(plugin -> (Function<IndexSettings, Optional<EngineFactory>>) plugin::getEngineFactory)
-    .collect(Collectors.toList());
+final Collection<BiFunction<IndexSettings, ShardRouting, Optional<EngineFactory>>> engineFactoryProviders = enginePlugins
+    .stream()
+    .map(plugin -> (BiFunction<IndexSettings, ShardRouting, Optional<EngineFactory>>) plugin::getEngineFactory)
+    .collect(Collectors.toList());
```

`IndexModule`/`IndexService` carry the same shape change: a `BiFunction<IndexSettings, ShardRouting, IndexerFactory> indexerFactoryProvider` resolved per-shard inside `IndexService#createShard`, instead of a single `Function<IndexSettings, IndexerFactory>` fixed once at index-service construction.

This is the seam the whole writer/reader split is built on: the plugin's `getEngineFactory` inspects the incoming `ShardRouting` and returns `WriterEngineFactory` or `ReaderEngineFactory` accordingly.

## 2. Engine abstraction: hooks for a non-local-durable, remote-materialized engine

A classic `Engine` assumes local Lucene commits plus a local translog are the durability source of truth, and that a shard with no local commit is either brand new or corrupt. An object-store-backed engine breaks both assumptions — durability lives in a remote WAL, and a shard can legitimately have no local commit yet still hold valid remote state (a reactivating scaled-to-zero shard, or a freshly cloned split child).

**`EngineFactory.java`** — before, the interface had exactly two methods (`newReadWriteEngine`, `newReadOnlyEngine`) and no recovery or durability-ownership hooks at all:

```diff
 public interface EngineFactory {
     Engine newReadOnlyEngine(EngineConfig config);
     Engine newReadWriteEngine(EngineConfig config);
+
+    /**
+     * Called by StoreRecovery#internalRecoverFromStore exactly once, only when local recovery
+     * expected an existing commit but found none on disk — the point where core would otherwise
+     * fail the shard outright. Default false: this engine has nothing else to try. An engine whose
+     * durability doesn't depend on this node's own local disk survival can override this to
+     * materialize a local Lucene commit and translog from wherever its durable copy lives, and
+     * return true so recovery proceeds normally instead of failing.
+     */
+    default boolean recoverMissingLocalStore(IndexShard indexShard, Store store) throws IOException {
+        return false;
+    }
+
+    /** Same shape as recoverMissingLocalStore, for a child shard of an in-place split. */
+    default boolean recoverInPlaceSplitLocalStore(IndexShard indexShard, Store store) throws IOException {
+        return false;
+    }
+
+    /** Same shape, the reverse case: a parent shard revived by an in-place merge. */
+    default boolean recoverInPlaceMergeLocalStore(IndexShard indexShard, Store store) throws IOException {
+        return false;
+    }
+
+    /**
+     * Whether this engine already provides its own durable, remote copy of every segment it
+     * writes, independent of core's own remote-store upload path. Default false: core keeps
+     * uploading via RemoteStoreRefreshListener exactly as it always has. An engine that already
+     * publishes its own remote manifest can opt out here to avoid duplicate upload cost.
+     */
+    default boolean ownsRemoteSegmentDurability() {
+        return false;
+    }
 }
```

**`Engine.java`** gained, alongside the existing abstract engine surface: `engineRecoveryOperations()` (replays ops from a non-local-translog durability source through the same `applyTranslogOperation` path core already uses for ordinary translog replay — called from `IndexShard.recoverAdditionalEngineOperations()` right after normal translog recovery), `onPrimaryTermBumped(long)` (fired atomically inside `IndexShard.bumpPrimaryTerm`, propagated through `Indexer`/`EngineBackedIndexer`, so an engine can capture activation-time state such as a WAL replay-fencing position at the exact moment a term bump happens instead of racing to observe it separately), and `globalCheckpointSupplierForCombinedDeletionPolicy()` (lets an engine widen local-commit retention when a remote store, not core's usual global-checkpoint/retention-lease chain, is the actual retention authority).

## 3. Shard-level write guards and recovery-source dispatch

**`IndexShard.java`** — a new pre-write check, called from the same place ordinary shard-state checks already run before a primary operation is accepted:

```diff
+                ensureNotInProgressSplitParent();
+                ensureNotInProgressMergeChild();
             }
         }
     }

+    /**
+     * Rejects a new primary write while this shard is the parent of an in-progress in-place split.
+     * Any document acknowledged after a child had already cloned but before the split committed
+     * would be written only into a later parent manifest generation no child ever references —
+     * silent data loss once the parent is retired at commit. The thrown
+     * IllegalIndexShardStateException is a shard-not-available exception, so it is retriable the
+     * same way a relocating/closing shard's rejection is.
+     */
+    private void ensureNotInProgressSplitParent() throws IllegalIndexShardStateException {
+        final IndexMetadata indexMetadata = indexSettings.getIndexMetadata();
+        if (indexMetadata != null
+            && indexMetadata.getSplitShardsMetadata() != null
+            && indexMetadata.getSplitShardsMetadata().isSplitOfShardInProgress(shardId.id())) {
+            throw new IllegalIndexShardStateException(
+                shardId,
+                state,
+                "operation rejected while an in-place split of this shard is in progress; retry once the split completes"
+            );
+        }
+    }
+
+    /** The exact mirror of ensureNotInProgressSplitParent(), for a live child of an in-progress merge. */
+    private void ensureNotInProgressMergeChild() throws IllegalIndexShardStateException {
+        // ... same isChildOfInProgressMerge(shardId.id()) check, same exception shape
+    }
```

`StoreRecovery`'s dispatch now routes `RecoverySource.Type.IN_PLACE_SPLIT_SHARD` / `IN_PLACE_MERGE_SHARD` through the normal `recoverFromStore` path, treating an empty local store on these recovery sources as "legitimately empty, to be materialized via the `EngineFactory` hooks above" rather than corrupt.

## 4. Node wiring: activating dormant split/merge services

`MetadataInPlaceSplitShardService` existed in core before this work — but nothing ever constructed one, so `TransportInPlaceSplitShardAction` had no service to call and the feature was unreachable from any API. `Node.java` now constructs and Guice-binds it, plus three new services:

```diff
+// Finalizes or aborts in-place shard splits once their child shards converge.
+new MetadataInPlaceSplitShardCommitService(settings, clusterService);
+
+// Surfaces a revived in-place-merge parent that exhausted its allocation retries.
+new MetadataInPlaceMergeShardCommitService(settings, clusterService);
+
+// The service TransportInPlaceSplitShardAction actually calls to trigger a split —
+// previously constructed nowhere, making the whole in-place split feature unreachable
+// from any user-facing API despite MetadataInPlaceSplitShardService itself existing.
+final MetadataInPlaceSplitShardService metadataInPlaceSplitShardService =
+    new MetadataInPlaceSplitShardService(clusterService, clusterModule.getAllocationService());
+
+// The service TransportInPlaceMergeShardAction calls to reverse a split in place —
+// the merge counterpart above, likewise previously constructed nowhere.
+final MetadataInPlaceMergeShardService metadataInPlaceMergeShardService =
+    new MetadataInPlaceMergeShardService(clusterService, clusterModule.getAllocationService());
```

```diff
 b.bind(AwarenessReplicaBalance.class).toInstance(awarenessReplicaBalance);
+b.bind(MetadataInPlaceSplitShardService.class).toInstance(metadataInPlaceSplitShardService);
+b.bind(MetadataInPlaceMergeShardService.class).toInstance(metadataInPlaceMergeShardService);
```

## 5. Blob-store compare-and-swap primitive

The plugin's `ShardHead` coordination (see [Architecture](/design/architecture/)) needs an atomic, fencable compare-and-swap over a small blob — something no existing `BlobContainer` method provided.

**`BlobContainer.java`** — two new default methods, `UnsupportedOperationException` unless a backend implements them:

```diff
 public interface BlobContainer {
     // ... existing get/write/delete/list methods ...

+    /**
+     * Reads the current value of a "register" blob — a small blob whose writes are arbitrated
+     * by compareAndSwapRegister, giving callers a generic optimistic-concurrency primitive
+     * independent of which repository backend they run against.
+     * @return empty if the register has never been written.
+     */
+    default Optional<BlobRegister> readRegister(String blobName) throws IOException {
+        throw new UnsupportedOperationException(getClass() + " does not support readRegister");
+    }
+
+    /**
+     * Atomically replaces a register blob's value, succeeding only if its current generation
+     * equals expectedGeneration. This is the seam production serverless implementations use to
+     * back a shard-head CAS protocol: a native-object-store-backed container implements this with
+     * its provider's conditional write (S3 If-Match, GCS generation preconditions, Azure ETag
+     * If-Match); FsBlobContainer implements it with real local-filesystem atomicity.
+     * @return whether the write applied, and either way the generation now actually stored.
+     */
+    default BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
+        throws IOException {
+        throw new UnsupportedOperationException(getClass() + " does not support compareAndSwapRegister");
+    }
 }
```

New `BlobRegister` (generation + value pair returned by `readRegister`) and `BlobRegisterCasResult` (applied/generation pair returned by `compareAndSwapRegister`) types back the interface. `FsBlobContainer` implements it with a generation-tracked file plus an intra-JVM `ReentrantLock` map — `FileChannel.lock()` alone is inter-process only, not sufficient for concurrent callers inside the same JVM. S3, Azure, and GCS repository code gained matching support so each backend's native conditional-write primitive can back the same interface.

This generalizes CAS-over-blob as a repository capability, rather than something specific to this plugin's manifest store.

## 6. REST handler serverless-scope annotation

**`RestHandler.java`** — a new default method and its enum:

```diff
 public interface RestHandler {
     // ... existing routes()/handleRequest()/etc ...

+    /**
+     * Declares this handler's availability on a node running in serverless mode. Defaults to
+     * UNAVAILABLE deliberately: new APIs must opt in consciously rather than being silently
+     * exposed by omission. This default has no behavioral effect today — nothing in
+     * RestController reads this method yet, since enforcement needs a node-level "is this node
+     * in serverless mode" flag that does not yet exist as a wired setting.
+     */
+    default ServerlessScope serverlessScope() {
+        return ServerlessScope.UNAVAILABLE;
+    }
+
+    enum ServerlessScope {
+        /** Available to ordinary callers on a node running in serverless mode. */
+        AVAILABLE,
+        /** Available only to internal/system callers, never external clients. */
+        INTERNAL_ONLY,
+        /** Not available at all — the default for any unannotated handler. */
+        UNAVAILABLE
+    }
 }
```

Roughly fifteen existing `Rest*Action` classes (e.g. `RestGetAction`, `RestBulkAction`, `RestSearchAction`) then override `serverlessScope()` to return `AVAILABLE`. Nothing in `RestController` enforces this yet — it's a forward-declaring seam so handlers record their intended availability incrementally, ahead of a future gate that would reject `UNAVAILABLE` handlers in a serverless deployment. Treat this as scaffolding, not an active restriction.

## 7. Routing and recovery-source changes for split/merge

**`ShardRoutingState.java`** — the transient in-progress-split marker was removed outright, superseded by the two-phase commit-service model in item 4 (a split child is now a first-class routing table entry from the start, not an ephemeral flag):

```diff
 public enum ShardRoutingState {
     // ... UNASSIGNED, INITIALIZING, STARTED ...
-    RELOCATING((byte) 4),
-    /** The shard is in the process of being split in-place. */
-    SPLITTING((byte) 5);
+    RELOCATING((byte) 4);
```

- **`RecoverySource` gained `InPlaceMergeShardRecoverySource`**, carrying the retired children's `ShardRange`s directly. Unlike the split case, this can't be read back from `SplitShardsMetadata` at parent-recovery time, because the merge's cluster-state update de-commits the split metadata in the same step that revives the parent.
- **`RecoverySource` gained `InPlaceMergeShardRecoverySource`**, carrying the retired children's `ShardRange`s directly. Unlike the split case, this can't be read back from `SplitShardsMetadata` at parent-recovery time, because the merge's cluster-state update de-commits the split metadata in the same step that revives the parent.
- **`SplitShardsMetadata`** grew substantially to track in-progress merges alongside splits (`isSplitOfShardInProgress`, `isChildOfInProgressMerge`), and `OperationRouting`/`IndexRoutingTable` route based on that instead of the old `includeInProgressChild` parameter.
- **`IndexMetadata`** relaxes `inSyncAllocationIds`/`primaryTermsMap` invariants to allow entries for shard IDs at or above `numberOfShards` (reserved for in-place-split children) without growing `numberOfShards` itself, since that value stays the hash-routing denominator.

## 8. New in-place merge transport actions

`InPlaceMergeShardAction` / `TransportInPlaceMergeShardAction` / `RestInPlaceMergeShardAction`, mirroring the pre-existing split actions. Combined with item 4's wiring fix and item 7's metadata support, this completes what had been a split-only, partially-wired feature into a full two-phase split-and-merge capability in core, which the plugin's resharding feature (see [Flows](/design/flows/)) builds on directly rather than reimplementing.

:::note
This list reflects a diff against the OpenSearch upstream merge-base, filtered to exclude `plugins/serverless-storage` and this docs site. Class and method names are transcribed from that diff — confirm against the current source before relying on exact signatures.
:::
