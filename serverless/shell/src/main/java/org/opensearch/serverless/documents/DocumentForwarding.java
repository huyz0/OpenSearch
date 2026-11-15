/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.documents;

import org.opensearch.serverless.documents.DocumentWriteOutcomes.ForwardFailed;
import org.opensearch.serverless.node.NodeContext;

import java.util.Optional;

/**
 * Sends a single-document write to the node the shard-head names, as a forwarded index request. Split out of
 * the REST document handler.
 */
final class DocumentForwarding {

    private DocumentForwarding() {}

    static org.opensearch.serverless.transport.ForwardedIndexResponse forwardTo(
        NodeContext serving,
        String owner,
        String writtenIndex,
        String indexUuid,
        int shard,
        String id,
        String routing,
        String shapedSource,
        boolean refresh,
        boolean deletion,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent
    ) throws Exception {
        return forwardTo(
            serving,
            owner,
            writtenIndex,
            indexUuid,
            shard,
            id,
            routing,
            shapedSource,
            refresh,
            deletion,
            ifSeqNo,
            ifPrimaryTerm,
            requireAbsent,
            false
        );
    }

    static org.opensearch.serverless.transport.ForwardedIndexResponse forwardTo(
        NodeContext serving,
        String owner,
        String writtenIndex,
        String indexUuid,
        int shard,
        String id,
        String routing,
        String shapedSource,
        boolean refresh,
        boolean deletion,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean steered
    ) throws Exception {
        final Optional<org.opensearch.cluster.node.DiscoveryNode> peer;
        try {
            peer = org.opensearch.serverless.transport.ShardRouter.of(serving).peer(owner);
        } catch (Exception e) {
            // Resolving the peer connects to it, so this fails as readily as the forward itself does, and
            // means the same thing.
            throw new ForwardFailed(owner, e);
        }
        if (peer.isEmpty()) {
            // The head names an owner that holds no live lease. That is precisely a dead writer nobody
            // has noticed yet, and this request is the first thing in the system to prove it.
            throw new ForwardFailed(
                owner,
                new org.opensearch.serverless.write.OwnerUnreachableException(
                    "shard " + shard + " is owned by " + owner + ", which has no reachable lease"
                )
            );
        }
        try {
            final var request = new org.opensearch.serverless.transport.ForwardedIndexRequest(
                writtenIndex,
                indexUuid,
                shard,
                id,
                shapedSource == null ? "" : shapedSource,
                refresh,
                deletion,
                ifSeqNo,
                ifPrimaryTerm,
                requireAbsent
            ).routing(routing);
            return org.opensearch.serverless.transport.ShardRouter.of(serving)
                .forwardIndex(peer.get(), steered ? request.steered() : request);
        } catch (Exception e) {
            throw new ForwardFailed(owner, e);
        }
    }
}
