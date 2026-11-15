/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.warm;

import org.opensearch.serverless.cluster.HotShardSpread;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.node.NodeContext;
import org.opensearch.serverless.transport.CacheWarmCopy;
import org.opensearch.serverless.transport.HandOffResponse;
import org.opensearch.serverless.transport.HotSetResponse;

import java.util.Collection;
import java.util.List;

/** What a search node answers its peers with when one drains or joins: warmup and the hot set, over the transport. */
public final class WarmAnswers implements CacheWarmCopy.Answers {

    private final NodeContext node;

    /**
     * Creates the answers for a node.
     *
     * @param node the node
     */
    public WarmAnswers(NodeContext node) {
        this.node = node;
    }

    @Override
    public HandOffResponse takeOverFrom(MetadataPlane plane, String drainingId) {
        final SearchWarmup.Outcome outcome = SearchWarmup.takeOverFrom(node, plane, drainingId);
        return new HandOffResponse(outcome.opened(), outcome.copy().blocksCopied());
    }

    @Override
    public List<HotSetResponse.HotShard> hotSet(Collection<NodeLease> members, String joinerId, long readSinceMillis, long maxBytes) {
        return CacheHotSet.forJoiner(node, members, joinerId, HotShardSpread.of(node, node.settings()), readSinceMillis, maxBytes);
    }
}
