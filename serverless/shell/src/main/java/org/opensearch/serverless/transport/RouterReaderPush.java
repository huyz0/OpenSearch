/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.transport;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.engine.OverlayReadOnlyEngine;
import org.opensearch.serverless.node.ReaderPush;
import org.opensearch.serverless.nodelog.WalRecord;
import org.opensearch.serverless.store.CommitManifest;

import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** {@link ReaderPush} over the router's commit push, tail push, tail overlays and refresh visibility. */
final class RouterReaderPush implements ReaderPush {

    private final ShardRouter router;

    RouterReaderPush(ShardRouter router) {
        this.router = router;
    }

    @Override
    public void applyCluster(Settings cluster) {
        router.tailOverlays().applyCluster(cluster);
        // And how far a refreshing write is made visible before it is answered.
        router.refreshVisibility().applyCluster(cluster);
    }

    @Override
    public void published(ShardId shardId, CommitManifest commit, LongSupplier committedCheckpoint) {
        router.commitPush().published(shardId, commit);
        if (router.tailPush().keeps(shardId)) {
            router.tailPush().published(shardId, committedCheckpoint.getAsLong());
        }
    }

    @Override
    public void acked(ShardId shardId, List<WalRecord> records, boolean push) {
        router.tailPush().acked(shardId, records, push);
    }

    @Override
    public boolean awaitVisible(ShardId shardId, BooleanSupplier searchedOnlyHere) {
        if (router.refreshVisibility().waitsForSearchNodes() == false) {
            return true;
        }
        if (router.tailOverlays().onFor(shardId) == false) {
            if (searchedOnlyHere.getAsBoolean() == false) {
                return false;
            }
            // A search node that opens the shard later compares its commit with what this answer claims.
            router.tailPush().claim(shardId);
            return true;
        }
        return router.tailPush().awaitSearchNodes(shardId, RefreshVisibility.WAIT_MILLIS);
    }

    @Override
    public boolean grantsFreshness() {
        return router.refreshVisibility().grants();
    }

    @Override
    public boolean ensureFresh(ShardId shardId, String writer, long commitCheckpoint, Runnable applyPending) {
        return router.tailOverlays().ensureFresh(shardId, writer, commitCheckpoint, applyPending);
    }

    @Override
    public Optional<CommitManifest> takePushed(String indexName, String indexUuid, int shard) {
        return router.commitPush().takePushed(indexName, indexUuid, shard);
    }

    @Override
    public Optional<CommitManifest> newerThan(Optional<CommitManifest> register, String indexName, String indexUuid, int shard) {
        return new NewestCommits(router).newerThan(register, indexName, indexUuid, shard);
    }

    @Override
    public void opened(ShardId shardId, String writer, long localCheckpoint) {
        router.tailOverlays().opened(shardId, writer, localCheckpoint);
    }

    @Override
    public OverlayReadOnlyEngine.Tail tail() {
        return router.tailOverlays();
    }
}
