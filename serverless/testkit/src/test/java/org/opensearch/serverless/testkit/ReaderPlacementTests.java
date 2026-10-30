/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/** Reader selection preserves rendezvous placement while retaining only the requested candidates. */
public class ReaderPlacementTests extends OpenSearchTestCase {
    public void testBoundedSelectionMatchesCompleteFleetRanking() {
        final List<NodeLease> members = new ArrayList<>();
        final List<String> readers = new ArrayList<>();
        for (int i = 0; i < 4096; i++) {
            final String id = "node-" + i;
            final boolean search = i % 3 != 0;
            members.add(new NodeLease(id, "process", "127.0.0.1:9300", Set.of(search ? "search" : "ingest"), Long.MAX_VALUE));
            if (search) {
                readers.add(id);
            }
        }
        for (int shard = 0; shard < 8; shard++) {
            final String key = "index#" + shard;
            readers.sort(
                Comparator.<String>comparingInt(id -> Murmur3HashFunction.hash(id + "/" + key)).reversed().thenComparing(id -> id)
            );
            Collections.shuffle(members, random());
            for (int limit : new int[] { 0, 1, 2, 7, 4096, Integer.MAX_VALUE }) {
                assertEquals(
                    readers.subList(0, Math.min(limit, readers.size())),
                    ReaderPlacement.candidatesFor("index", shard, members, "search", limit)
                );
            }
        }
    }

    public void testHashCollisionsKeepNodeIdTieBreak() {
        final String first = "node-9892";
        final String second = "node-27597";
        assertEquals(Murmur3HashFunction.hash(first + "/index#0"), Murmur3HashFunction.hash(second + "/index#0"));
        final var a = new NodeLease(first, "p", "127.0.0.1:9300", Set.of("search"), Long.MAX_VALUE);
        final var b = new NodeLease(second, "p", "127.0.0.1:9300", Set.of("search"), Long.MAX_VALUE);
        final String preferred = first.compareTo(second) < 0 ? first : second;
        assertEquals(List.of(preferred), ReaderPlacement.candidatesFor("index", 0, List.of(a, b), "search", 1));
        assertEquals(List.of(preferred), ReaderPlacement.candidatesFor("index", 0, List.of(b, a), "search", 1));
    }
}
