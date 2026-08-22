/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.snapshots.blobstore.EngineNativeShardSnapshot;
import org.opensearch.index.store.Store;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link FilterRepository} forwards every {@link Repository} method to its delegate verbatim --
 * proves the two new engine-native-snapshot methods weren't missed the way an easy-to-overlook
 * forwarding class like this one always risks.
 */
public class FilterRepositoryTests extends OpenSearchTestCase {

    public void testSnapshotEngineNativeForwardsToDelegate() {
        final Repository delegate = mock(Repository.class);
        final Repository filter = new FilterRepository(delegate);

        final Store store = mock(Store.class);
        final SnapshotId snapshotId = new SnapshotId("test-snap", "test-snap-uuid");
        final IndexId indexId = new IndexId("test-index", "test-index-uuid");
        final IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(ShardGenerations.NEW_SHARD_GEN);
        final byte[] payload = new byte[] { 1, 2, 3 };
        @SuppressWarnings("unchecked")
        final ActionListener<String> listener = mock(ActionListener.class);

        filter.snapshotEngineNative(store, snapshotId, indexId, snapshotStatus, 42L, "test-engine/v1", payload, listener);

        verify(delegate).snapshotEngineNative(store, snapshotId, indexId, snapshotStatus, 42L, "test-engine/v1", payload, listener);
    }

    public void testGetEngineNativeShardSnapshotMetadataForwardsToDelegate() {
        final Repository delegate = mock(Repository.class);
        final Repository filter = new FilterRepository(delegate);

        final SnapshotId snapshotId = new SnapshotId("test-snap", "test-snap-uuid");
        final IndexId indexId = new IndexId("test-index", "test-index-uuid");
        final ShardId shardId = new ShardId("test-index", "test-index-uuid", 0);
        final EngineNativeShardSnapshot expected = new EngineNativeShardSnapshot(
            "test-snap",
            "test-engine/v1",
            0L,
            0L,
            new byte[] { 4, 5, 6 }
        );
        when(delegate.getEngineNativeShardSnapshotMetadata(snapshotId, indexId, shardId)).thenReturn(Optional.of(expected));

        assertEquals(Optional.of(expected), filter.getEngineNativeShardSnapshotMetadata(snapshotId, indexId, shardId));
    }
}
