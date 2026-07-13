/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ShardCacheStatsEntryTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ShardCacheStatsEntry original = new ShardCacheStatsEntry("idx-a", 3, 10L, 4L, 12L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardCacheStatsEntry deserialized = new ShardCacheStatsEntry(out.bytes().streamInput());

        assertEquals(original, deserialized);
    }

    public void testEqualsAndHashCode() {
        ShardCacheStatsEntry a = new ShardCacheStatsEntry("idx-a", 0, 1L, 2L, 3L);
        ShardCacheStatsEntry sameFields = new ShardCacheStatsEntry("idx-a", 0, 1L, 2L, 3L);
        ShardCacheStatsEntry differentLatency = new ShardCacheStatsEntry("idx-a", 0, 1L, 2L, 99L);

        assertEquals(a, sameFields);
        assertEquals(a.hashCode(), sameFields.hashCode());
        assertNotEquals(a, differentLatency);
    }
}
