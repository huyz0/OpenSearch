/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.broadcast.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexCatalogRegistry;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Notices when a broadcast reached no shards at all for an index that has some.
 *
 * <p>This exists because of how one particular bug keeps recurring. A broadcast subclass reads the
 * routing table directly, finds nothing for an index whose placement is computed rather than published,
 * and the request then <em>succeeds</em> having touched nothing. A refresh reported success having
 * refreshed nothing, and a force merge reported success having merged nothing. Seven instances of that
 * shape have been found in this area, and not one of them threw. Each was found by a person eventually
 * noticing a zero, which is not a detection strategy.
 *
 * <p><b>Why this is not inside the resolver the fixed callers use.</b> That was the obvious home and it
 * would have been useless. A check there only runs for a caller that already resolves correctly, and the
 * bug is by definition a caller that does not: an unconverted subclass never reaches the resolver, so the
 * resolver can never notice it. The guard has to sit where the subclass cannot route around it, which is
 * the shared base, after the subclass has already produced its answer.
 *
 * <p><b>Assert and warn rather than fail.</b> Failing the request would turn a degraded read into an
 * outage and reverse the design choice made deliberately when absent-routing degradation was
 * introduced: an unresolvable placement degrades rather than throws. The assertion makes this loud in CI, which is where the mistake is
 * introduced; the warning makes it findable in production, where the alternative is an operator
 * concluding their index is empty.
 *
 * <p><b>Only when a supplier is installed.</b> Without one an open index always has a published entry
 * carrying at least unassigned shards, so this condition would mean something else entirely, and a guard
 * written for this feature has no business failing requests on clusters that do not use it.
 */
public final class BroadcastEmptiness {

    private static final Logger logger = LogManager.getLogger(BroadcastEmptiness.class);

    private BroadcastEmptiness() {}

    /**
     * @param indicesWithShards the index names that actually contributed at least one shard
     * @return the open indices that contributed nothing, empty when all is well, for testing
     */
    public static List<String> check(
        String actionName,
        ClusterState clusterState,
        String[] concreteIndices,
        Set<String> indicesWithShards
    ) {
        // IndexCatalog#isActive(), not "is a catalog registered" (an earlier attempt swapped this for
        // checking the attached resolver of the day and a real test caught why that's wrong):
        // registration is a node-lifetime-scoped fact, true for as long as a plugin implements the SPI at
        // all, while this guard needs "is the underlying feature *currently active*" -- a dynamically
        // toggled fact. The SPI now models exactly that, so this no longer has to reach past it into the
        // static registry; SupplierBackedIndexCatalog#isActive reads that live registry on every call, so
        // the answer here is the same one this line always gave.
        if (IndexCatalogRegistry.isActive() == false) {
            return List.of();
        }
        List<String> missing = new ArrayList<>();
        for (String index : concreteIndices) {
            if (indicesWithShards.contains(index)) {
                continue;
            }
            IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (indexMetadata == null || indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                // Deleted from under the request, or closed and legitimately contributing nothing.
                continue;
            }
            missing.add(index);
        }
        if (missing.isEmpty() == false) {
            logger.warn(
                "[{}] resolved no shards for open indices {}, so it will report success having done nothing to them",
                actionName,
                missing
            );
        }
        return missing;
    }

    /** Same check, with the assertion that makes it fail a test rather than only log. */
    public static void assertEveryOpenIndexContributedShards(
        String actionName,
        ClusterState clusterState,
        String[] concreteIndices,
        Set<String> indicesWithShards
    ) {
        List<String> missing = check(actionName, clusterState, concreteIndices, indicesWithShards);
        assert missing.isEmpty() : "[" + actionName + "] resolved no shards for open indices " + missing;
    }
}
