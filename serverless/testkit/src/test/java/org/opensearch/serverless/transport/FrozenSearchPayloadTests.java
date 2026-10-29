/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.transport;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

public class FrozenSearchPayloadTests extends OpenSearchTestCase {
    public void testForwardCarriesOneFileMapWithoutLosingOtherShardTerms() throws Exception {
        final Map<Integer, CommitManifest> manifests = new LinkedHashMap<>();
        for (int shard = 0; shard < 32; shard++) {
            final Map<String, String> files = new LinkedHashMap<>();
            final Map<String, Long> lengths = new LinkedHashMap<>();
            for (int file = 0; file < 64; file++) {
                final String name = "segment_" + file + ".cfs";
                files.put(name, "t=" + (shard + 1));
                lengths.put(name, 10_000L + file);
            }
            manifests.put(shard, new CommitManifest(shard + 1L, files, "writer", lengths));
        }
        final PointInTime original = new PointInTime("view", "index", "uuid", 100_000L, manifests);
        final var request = new ForwardedFrozenSearchRequest(original, 7, new SearchSourceBuilder().size(1), 1_000L);
        try (BytesStreamOutput full = new BytesStreamOutput(); BytesStreamOutput wire = new BytesStreamOutput()) {
            original.writeTo(full);
            request.writeTo(wire);
            assertTrue("a shard forward must not repeat every shard's files", wire.bytes().length() < full.bytes().length() / 4);
            try (var in = wire.bytes().streamInput()) {
                final var copy = new ForwardedFrozenSearchRequest(in);
                assertEquals(7, copy.shard());
                assertEquals(original.id(), copy.pit().id());
                assertEquals(original.indexUuid(), copy.pit().indexUuid());
                assertEquals(original.expiresAtMillis(), copy.pit().expiresAtMillis());
                assertEquals(1_000L, copy.nowInMillis());
                for (int shard = 0; shard < 32; shard++) {
                    assertEquals(manifests.get(shard).term(), copy.pit().shards().get(shard).term());
                    if (shard == 7) {
                        assertEquals(manifests.get(shard).files(), copy.pit().shards().get(shard).files());
                        assertEquals(
                            manifests.get(shard).lengthOf("segment_0.cfs"),
                            copy.pit().shards().get(shard).lengthOf("segment_0.cfs")
                        );
                    } else {
                        assertTrue(copy.pit().shards().get(shard).files().isEmpty());
                    }
                }
            }
        }
        assertEquals("projection must not mutate the stored view", 64, original.shards().get(0).files().size());
    }
}
