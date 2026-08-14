/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ManifestShardFunctionTests extends OpenSearchTestCase {

    public void testResultIsAlwaysInRange() {
        int shardCount = randomIntBetween(1, 4096);
        for (int i = 0; i < 500; i++) {
            int shardId = ManifestShardFunction.shardFor(UUID.randomUUID().toString(), shardCount);
            assertTrue("shard id must be >= 0, got " + shardId, shardId >= 0);
            assertTrue("shard id must be < shardCount (" + shardCount + "), got " + shardId, shardId < shardCount);
        }
    }

    public void testDeterministic() {
        String indexUUID = UUID.randomUUID().toString();
        int shardCount = randomIntBetween(1, 4096);
        int first = ManifestShardFunction.shardFor(indexUUID, shardCount);
        int second = ManifestShardFunction.shardFor(indexUUID, shardCount);
        assertEquals("the same (uuid, shardCount) must always resolve to the same shard", first, second);
    }

    public void testDistributesAcrossShardsRatherThanCollapsingToOne() {
        // Not a statistical rigor test -- just confirms the function isn't accidentally constant.
        // 5000 random UUIDs into 16 shards should hit every shard at least once with overwhelming
        // probability; a real bug (e.g. always returning 0) would fail this immediately.
        int shardCount = 16;
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 5000; i++) {
            int shardId = ManifestShardFunction.shardFor(UUID.randomUUID().toString(), shardCount);
            counts.merge(shardId, 1, Integer::sum);
        }
        assertEquals("expected every one of " + shardCount + " shards to be hit at least once", shardCount, counts.size());
    }

    public void testRejectsNonPositiveShardCount() {
        String indexUUID = UUID.randomUUID().toString();
        expectThrows(IllegalArgumentException.class, () -> ManifestShardFunction.shardFor(indexUUID, 0));
        expectThrows(IllegalArgumentException.class, () -> ManifestShardFunction.shardFor(indexUUID, -1));
    }

    public void testRejectsNullUUID() {
        expectThrows(NullPointerException.class, () -> ManifestShardFunction.shardFor(null, 16));
    }

    public void testSingleShardAlwaysResolvesToZero() {
        for (int i = 0; i < 50; i++) {
            assertEquals(0, ManifestShardFunction.shardFor(UUID.randomUUID().toString(), 1));
        }
    }
}
