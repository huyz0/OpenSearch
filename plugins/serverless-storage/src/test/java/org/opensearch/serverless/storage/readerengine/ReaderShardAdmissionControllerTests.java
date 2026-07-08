/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

public class ReaderShardAdmissionControllerTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("idx", "idx-uuid"), 0);

    public void testConstructorRejectsNonPositiveLimits() {
        expectThrows(IllegalArgumentException.class, () -> new ReaderShardAdmissionController(0));
        expectThrows(IllegalArgumentException.class, () -> new ReaderShardAdmissionController(-1));
    }

    public void testAcquireSucceedsUpToTheLimit() {
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(2);
        controller.acquire(SHARD_ID);
        controller.acquire(SHARD_ID);
        assertEquals(0, controller.availablePermits());
    }

    public void testAcquireBeyondTheLimitThrows() {
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(1);
        controller.acquire(SHARD_ID);
        expectThrows(IllegalStateException.class, () -> controller.acquire(SHARD_ID));
    }

    public void testReleaseFreesAPermitForTheNextAcquire() {
        ReaderShardAdmissionController controller = new ReaderShardAdmissionController(1);
        controller.acquire(SHARD_ID);
        controller.release();
        // Must not throw -- the released permit is available again.
        controller.acquire(SHARD_ID);
        assertEquals(0, controller.availablePermits());
    }
}
