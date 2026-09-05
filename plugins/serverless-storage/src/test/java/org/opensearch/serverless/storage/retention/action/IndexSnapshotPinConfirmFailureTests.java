/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * The second phase of an index-wide pin, whose failure direction was documented backwards.
 *
 * <h2>What actually happened on a failure</h2>
 *
 * Phase one puts a short-lived pin on every shard; phase two walks the shards again and re-stamps each pin
 * as permanent. The javadoc claimed a failure there was "the safe direction -- they lapse". That is true
 * only of the shards the loop had not reached yet. On a 200-shard index whose object store returns 503 for
 * shard 120, shards 0..119 already hold permanent pins that nothing will ever release, garbage collection on
 * those 120 shards is blocked at whatever generation was current, and the caller has been told the pin did
 * not take -- so it has no reason to release anything. The only thing that would have noticed is a sweeper
 * that releases nothing and is off by default.
 */
public class IndexSnapshotPinConfirmFailureTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx-uuid";
    private static final String SNAPSHOT_ID = "nightly";

    /**
     * A transport action with the two object-store-touching steps replaced: one confirm is made to fail, and
     * the compensating releases are recorded instead of dispatched. Everything between them -- which shards
     * the failure compensates over, and what the caller is told -- is the real code.
     */
    private static final class RecordingAction extends TransportIndexSnapshotPinAction {

        private final int failAtShard;
        private final List<Integer> confirmed = new ArrayList<>();
        private final List<Integer> released = new ArrayList<>();

        RecordingAction(int failAtShard) {
            super(
                mock(TransportService.class),
                new ActionFilters(Set.of()),
                mock(ClusterService.class),
                mock(NodeClient.class),
                mock(ServerlessStoragePlugin.class)
            );
            this.failAtShard = failAtShard;
        }

        @Override
        void confirmShardPin(String indexUuid, int shardId, String snapshotId) throws Exception {
            if (shardId == failAtShard) {
                throw new IOException("simulated 503 from the object store confirming shard " + shardId);
            }
            confirmed.add(shardId);
        }

        @Override
        void releaseShardPin(String indexUuid, int shardId, String snapshotId, ActionListener<SnapshotReleaseResponse> listener) {
            released.add(shardId);
            listener.onResponse(new SnapshotReleaseResponse(true));
        }
    }

    public void testAFailedConfirmReleasesEveryShardRatherThanLeavingHalfOfThemPinnedForever() {
        RecordingAction action = new RecordingAction(3);
        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicReference<IndexSnapshotPinResponse> response = new AtomicReference<>();

        action.confirmPins(INDEX_UUID, 8, SNAPSHOT_ID, ActionListener.wrap(response::set, failure::set));

        assertNull("the request must not report success", response.get());
        assertNotNull("and must report the original failure", failure.get());
        assertTrue(failure.get().getMessage(), failure.get().getMessage().contains("simulated 503"));
        assertEquals(
            "shards 0..2 were already made permanent when shard 3 failed, so leaving them pinned is a permanent "
                + "leak the caller has no reason to clean up -- every shard must be compensated",
            List.of(0, 1, 2, 3, 4, 5, 6, 7),
            action.released
        );
        assertEquals("precondition: the confirm really did get partway through before failing", List.of(0, 1, 2), action.confirmed);
    }

    public void testASuccessfulConfirmReleasesNothing() {
        RecordingAction action = new RecordingAction(-1);
        AtomicReference<IndexSnapshotPinResponse> response = new AtomicReference<>();

        action.confirmPins(INDEX_UUID, 4, SNAPSHOT_ID, ActionListener.wrap(response::set, e -> fail(e.toString())));

        assertNotNull(response.get());
        assertEquals(4, response.get().shardCount());
        assertTrue("a clean confirm must not compensate anything", action.released.isEmpty());
    }
}
