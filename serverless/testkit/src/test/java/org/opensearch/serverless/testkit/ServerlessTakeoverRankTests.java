/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The order survivors take a dead node's shards in: each member is the preferred taker of its own share, every member
 * agrees on whose share a shard is, and the shares are even.
 *
 * <p>Survivors used to queue every orphaned shard in the order writes for it arrived -- the same order on every node --
 * and race on each at the same moment: one of five won and four spent an activation on nothing.
 */
public class ServerlessTakeoverRankTests extends OpenSearchTestCase {

    private static final List<String> MEMBERS = List.of("n0", "n1", "n3", "n4", "n5");

    public void testEachShardHasExactlyOnePreferredTakerAndAFullRanking() {
        for (int i = 0; i < 1_000; i++) {
            final String index = "logs-" + i;
            final Set<Integer> ranks = new HashSet<>();
            for (String member : MEMBERS) {
                ranks.add(BackgroundReconciler.takeoverRank(member, MEMBERS, index, 0));
            }
            assertEquals("every member a different place for " + index, Set.of(0, 1, 2, 3, 4), ranks);
        }
    }

    public void testSharesAreEven() {
        final int shards = 5_000;
        for (String member : MEMBERS) {
            int first = 0;
            for (int i = 0; i < shards; i++) {
                if (BackgroundReconciler.takeoverRank(member, MEMBERS, "logs-" + i, 0) == 0) {
                    first++;
                }
            }
            final int even = shards / MEMBERS.size();
            assertTrue(member + " is first for " + first + " of " + shards, Math.abs(first - even) < even / 5);
        }
    }

    public void testAMemberLeavingMovesOnlyItsOwnShare() {
        final List<String> after = List.of("n0", "n1", "n3", "n5");
        for (int i = 0; i < 2_000; i++) {
            final String index = "logs-" + i;
            for (String member : after) {
                if (BackgroundReconciler.takeoverRank(member, MEMBERS, index, 0) == 0) {
                    assertEquals(
                        "a shard " + member + " was first for stays its own",
                        0,
                        BackgroundReconciler.takeoverRank(member, after, index, 0)
                    );
                }
            }
        }
    }
}
