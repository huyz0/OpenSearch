/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.serverless.shard.ShardReconciler;

import java.util.Map;

/**
 * Builds {@link ServerlessNode}'s storage: the engine each shard role runs, and the directories its shards open on
 * the object store and the block cache.
 */
final class NodeStorage {

    private final ServerlessNode node;
    private volatile org.opensearch.serverless.store.LocalSegmentTier localSegments;

    NodeStorage(ServerlessNode node) {
        this.node = node;
    }

    /** A shard's local directory, with the node's memory tier for freshly written files in front of it. */
    org.apache.lucene.store.Directory localDirectory(org.apache.lucene.store.Directory local) {
        return new org.opensearch.serverless.store.TieredSegmentDirectory(local, localSegments());
    }

    /**
     * The node's memory tier for freshly written segment files, built from its settings on first use (this object is
     * built before the node's settings are settled). Eviction runs on GENERIC.
     */
    org.opensearch.serverless.store.LocalSegmentTier localSegments() {
        org.opensearch.serverless.store.LocalSegmentTier tier = localSegments;
        if (tier == null) {
            synchronized (this) {
                tier = localSegments;
                if (tier == null) {
                    tier = org.opensearch.serverless.store.LocalSegmentTier.fromSettings(node.settings(), () -> {
                        final org.opensearch.threadpool.ThreadPool pool = node.threadPool();
                        return pool == null ? null : pool.generic();
                    });
                    localSegments = tier;
                }
            }
        }
        return tier;
    }

    /**
     * The node's role decides its engine, which is what makes the two shard roles asymmetric rather
     * than merely differently labelled.
     *
     * <p>A node that does not accept writer activation runs {@link org.opensearch.index.engine.ReadOnlyEngine},
     * so a reader shard is <em>structurally</em> unable to write — not merely expected not to. That
     * matters because a reader opens a shard without acquiring its head: there is no compare-and-swap
     * behind it, and safety comes from the engine being incapable rather than from a convention.
     *
     * <p>Readers needing no coordination is the point, not a shortcut.
     * Readers are interchangeable caches, so any search
     * node can serve any shard by reading its manifest, and the directory tier just learns about it.
     */
    java.util.Collection<
        java.util.function.Function<org.opensearch.index.IndexSettings, java.util.Optional<org.opensearch.index.engine.EngineFactory>>>
        engineFactoryProviders() {
        if (node.roles.contains(ServerlessNode.ROLE_INGEST)) {
            // Core's own writer engine, with one thing added: it knows this shard's history may continue
            // in an object-store log rather than a local translog, and replays it during recovery so the
            // sequence numbers survive. See ServerlessWriterEngine for why replay cannot happen later.
            final org.opensearch.index.engine.EngineFactory writer = config -> new org.opensearch.serverless.engine.ServerlessWriterEngine(
                config,
                new org.opensearch.serverless.engine.ServerlessWriterEngine.ReplayLog() {
                    @Override
                    public java.util.List<org.opensearch.serverless.store.WalRecord> recordsFor(
                        org.opensearch.core.index.shard.ShardId shardId
                    ) throws java.io.IOException {
                        return node.reconciler == null ? java.util.List.of() : node.reconciler.replayRecordsFor(shardId);
                    }

                    @Override
                    public void forEachRecord(
                        org.opensearch.core.index.shard.ShardId shardId,
                        org.opensearch.common.CheckedConsumer<org.opensearch.serverless.store.WalRecord, java.io.IOException> consumer
                    ) throws java.io.IOException {
                        if (node.reconciler != null) {
                            node.reconciler.forEachReplayRecord(shardId, consumer);
                        }
                    }

                    @Override
                    public boolean replayed(
                        org.opensearch.core.index.shard.ShardId shardId,
                        org.opensearch.serverless.engine.ReplaySpool spool
                    ) throws java.io.IOException {
                        return node.reconciler != null && node.reconciler.retainReplay(shardId, spool);
                    }
                }
            );
            return java.util.List.of(indexSettings -> java.util.Optional.of(writer));
        }
        // The commit, and on top of it the acknowledged writes its writer pushes: see OverlayReadOnlyEngine. Its overlay stays
        // empty, and the engine is the commit alone, for an index whose writes are not pushed.
        final org.opensearch.index.engine.EngineFactory readOnly = config -> new org.opensearch.serverless.engine.OverlayReadOnlyEngine(
            config,
            node.router == null ? null : node.router.tailOverlays(),
            record -> parsePushed(config.getShardId(), record)
        );
        return java.util.List.of(indexSettings -> java.util.Optional.of(readOnly));
    }

    /**
     * Parses a pushed record with the index's mapping as this node holds it; null when the document needs a field this
     * node's mapping does not have yet, which then waits for the commit.
     */
    private org.opensearch.index.mapper.ParsedDocument parsePushed(
        org.opensearch.core.index.shard.ShardId shardId,
        org.opensearch.serverless.store.WalRecord record
    ) {
        final org.opensearch.index.IndexService service = node.indicesService.indexService(shardId.getIndex());
        final org.opensearch.index.mapper.DocumentMapper mapper = service == null ? null : service.mapperService().documentMapper();
        if (mapper == null) {
            return null;
        }
        final org.opensearch.index.mapper.ParsedDocument parsed = mapper.parse(
            new org.opensearch.index.mapper.SourceToParse(
                shardId.getIndexName(),
                record.id(),
                new org.opensearch.core.common.bytes.BytesArray(record.source()),
                org.opensearch.common.xcontent.XContentType.JSON,
                record.routing()
            )
        );
        return parsed.dynamicMappingsUpdate() == null ? parsed : null;
    }

    Map<String, org.opensearch.plugins.IndexStorePlugin.DirectoryFactory> directoryFactories() {
        final org.opensearch.plugins.IndexStorePlugin.DirectoryFactory factory =
            new org.opensearch.plugins.IndexStorePlugin.DirectoryFactory() {
                @Override
                public org.apache.lucene.store.Directory newDirectory(
                    org.opensearch.index.IndexSettings indexSettings,
                    org.opensearch.index.shard.ShardPath shardPath
                ) throws java.io.IOException {
                    return open(
                        localDirectory(org.apache.lucene.store.FSDirectory.open(shardPath.resolveIndex())),
                        indexSettings,
                        shardPath.getShardId().id()
                    );
                }

                @Override
                public org.apache.lucene.store.Directory newFSDirectory(
                    java.nio.file.Path location,
                    org.apache.lucene.store.LockFactory lockFactory,
                    org.opensearch.index.IndexSettings indexSettings
                ) throws java.io.IOException {
                    // This hands over a location rather than a ShardPath, so the shard number has to be
                    // recovered from the directory the location sits in
                    // ({data}/indices/{uuid}/{shard}/index).
                    final org.apache.lucene.store.Directory local = localDirectory(
                        org.apache.lucene.store.FSDirectory.open(location, lockFactory)
                    );
                    final int shardNumber;
                    try {
                        shardNumber = Integer.parseInt(location.getParent().getFileName().toString());
                    } catch (RuntimeException e) {
                        // An unexpected layout means we cannot say which shard this is, and guessing would
                        // attach one shard's directory to another's data. Local-only is the safe answer.
                        ServerlessNode.logger.warn(
                            "could not derive a shard number from " + location + "; serving it from local disk only",
                            e
                        );
                        return local;
                    }
                    return open(local, indexSettings, shardNumber);
                }
            };
        return Map.of(ServerlessNode.BLOCK_CACHE_STORE_TYPE, factory);
    }

    /**
     * Builds one shard's directory, whichever way the store asked for it.
     *
     * <p><b>Both entry points come here, and that is the point.</b> They were written separately, and the
     * one core happened to call for a live shard grew the logic while the other kept deriving the storage
     * path from the shard's own uuid. That is right for every ordinary shard and silently wrong for a
     * frozen view, whose uuid is the view's and under which nothing was ever written — the view opened
     * onto an empty directory and reported an index with no segments, which reads like a corrupted shard
     * rather than a shell bug. Whatever decides which method core calls, it cannot decide correctness.
     *
     * @param local the shard's local directory
     * @param indexSettings the shard's settings, which say whose bytes it reads
     * @param shardNumber the shard
     * @return the directory to serve it from
     * @throws java.io.IOException if the manifest or its term containers cannot be read
     */
    org.apache.lucene.store.Directory open(
        org.apache.lucene.store.Directory local,
        org.opensearch.index.IndexSettings indexSettings,
        int shardNumber
    ) throws java.io.IOException {
        final var plane = node.metadataPlane;
        if (plane == null) {
            // No metadata plane means no manifest to read; a plain local directory is the truthful
            // fallback rather than a half-built remote one.
            return local;
        }
        final String indexName = indexSettings.getIndex().getName();
        final var shardBase = org.opensearch.serverless.metadata.RegisterMap.shardData(
            plane.basePath(),
            indexName,
            ServerlessNode.storageUuidOf(indexSettings),
            shardNumber
        );
        final String viewId = ServerlessNode.frozenViewIdOf(indexSettings);
        // The commit the reconciler is opening this shard at, if it is opening one right now. It read the
        // manifest -- or the view's record -- to decide how to open the shard, and arms it for exactly the
        // window in which this factory runs; taking it here is what makes a reader open one register read
        // rather than one per layer that needs the same answer.
        final ShardReconciler opening = node.reconciler;
        final java.util.Optional<org.opensearch.serverless.store.CommitManifest> armed = opening == null
            ? java.util.Optional.empty()
            : opening.pendingManifest(indexSettings.getIndex(), shardNumber);
        if (armed.isPresent()) {
            final String scope = (viewId != null ? viewId : ServerlessNode.storageUuidOf(indexSettings)) + "#" + shardNumber;
            return org.opensearch.serverless.store.BlockCacheDirectory.create(
                local,
                plane.blobStore(),
                shardBase,
                node.blockCache,
                scope,
                armed.get()
            );
        }
        // A frozen view opens the commit it froze, which is not the one the manifest register holds now --
        // that is the entire point of it. The view's identifier is this index's uuid, so the record is one
        // lookup away.
        if (viewId != null) {
            final var pit = plane.pointInTime(viewId);
            if (pit.isEmpty()) {
                // Released or expired between the search resolving it and the shard opening. An empty
                // directory here would look like an empty index; refusing says what happened.
                throw new java.io.IOException("the point in time [" + viewId + "] is no longer held");
            }
            final var frozenCommit = pit.get().shards().get(shardNumber);
            if (frozenCommit == null) {
                // The same refusal as above, for the other way this commit can be absent. A null manifest
                // is a legitimate "nothing published yet" for a writer opening a fresh shard, and it is
                // exactly the wrong reading here: a view that does not cover this shard would open an
                // empty directory, the shard would answer nothing, and the search would count it as having
                // answered -- a hole in the coverage reported as a complete result. No view reaches this
                // holding no commit for the shard being opened, and if one ever does it says so.
                throw new java.io.IOException(
                    "the point in time [" + viewId + "] holds no commit for shard " + shardNumber + " of " + shardBase.buildAsString()
                );
            }
            return org.opensearch.serverless.store.BlockCacheDirectory.create(
                local,
                plane.blobStore(),
                shardBase,
                node.blockCache,
                viewId + "#" + shardNumber,
                frozenCommit
            );
        }
        // Through the plane, not a publisher of its own over the path: a bucket member's commit is its entry in the
        // bucket's manifest, and the shard's own register is never written.
        final String storageUuid = ServerlessNode.storageUuidOf(indexSettings);
        return org.opensearch.serverless.store.BlockCacheDirectory.create(
            local,
            plane.blobStore(),
            shardBase,
            node.blockCache,
            // The uuid, not the name: a recreated index reuses its name and none of its bytes.
            storageUuid + "#" + shardNumber,
            plane.segmentPublisher(indexName, storageUuid, shardNumber).readManifest().orElse(null)
        );
    }
}
