/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;

import java.io.IOException;
import java.util.Optional;

/**
 * Finds a shard's current commit for a reader: the register, or, when the register's writer stages commits, the newer
 * one that writer holds.
 *
 * <p>A writer that stages commits writes the register only as a checkpoint, so the register can be a few minutes behind
 * what the writer has published. The register says so: it carries a stage sequence. A reader that sees one asks the
 * writer, over transport, for its newest commit, and takes it only if it is a later commit of the same writer and term.
 * A writer that cannot be reached, or answers with nothing newer, leaves the register as the answer: older, never
 * wrong. No object-store request is added: the register read is the one a reader made before staging existed.
 */
public final class NewestCommits {

    private static final Logger LOG = LogManager.getLogger(NewestCommits.class);

    /** How long a reader waits for a writer's answer before serving the register. */
    static final TimeValue ASK_TIMEOUT = TimeValue.timeValueMillis(500);

    private final ShardRouter router;

    private NewestCommits(ShardRouter router) {
        this.router = router;
    }

    /**
     * Returns the lookup for a node. The node must have started, since its router is built then.
     *
     * @param node the node
     * @return the lookup
     */
    public static NewestCommits of(org.opensearch.serverless.shell.ServerlessNode node) {
        return new NewestCommits(node.router());
    }

    /** Registers the handler that answers readers. Called once, at startup, after the router's own. */
    public void registerHandlers() {
        router.transportService.registerRequestHandler(
            NewestCommitRequest.ACTION,
            ThreadPool.Names.MANAGEMENT,
            NewestCommitRequest::new,
            this::handle
        );
    }

    void handle(NewestCommitRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        router.requireMac(NewestCommitRequest.ACTION, request);
        channel.sendResponse(new NewestCommitResponse(local(request.indexName(), request.indexUuid(), request.shard())));
    }

    private CommitManifest local(String indexName, String indexUuid, int shard) {
        return router.node.reconciler().newestCommit(new ShardId(new Index(indexName, indexUuid), shard));
    }

    /**
     * Returns a shard's current commit: the register, or the newer commit its writer staged after it.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param indexUuid the index's uuid
     * @param shard the shard number
     * @return the commit, or empty if nothing has been published
     * @throws IOException if the register cannot be read
     */
    public Optional<CommitManifest> current(MetadataPlane plane, String indexName, String indexUuid, int shard) throws IOException {
        return newerThan(plane.segmentPublisher(indexName, indexUuid, shard).readManifest(), indexName, indexUuid, shard);
    }

    /**
     * Returns the newer commit a register's writer staged after it, or the register itself.
     *
     * @param register what the register holds
     * @param indexName the index
     * @param indexUuid the index's uuid
     * @param shard the shard number
     * @return the commit
     */
    public Optional<CommitManifest> newerThan(Optional<CommitManifest> register, String indexName, String indexUuid, int shard) {
        if (register.isEmpty() || register.get().stageSequence() < 0L || register.get().writer() == null) {
            return register;
        }
        final String writer = register.get().writer();
        try {
            final CommitManifest newest;
            if (writer.equals(router.node.localNode().getId())) {
                newest = local(indexName, indexUuid, shard);
            } else {
                final Optional<DiscoveryNode> peer = router.peer(writer, ASK_TIMEOUT);
                if (peer.isEmpty()) {
                    return register;
                }
                newest = router.send(
                    peer.get(),
                    NewestCommitRequest.ACTION,
                    new NewestCommitRequest(indexName, indexUuid, shard),
                    NewestCommitResponse::new,
                    ASK_TIMEOUT
                ).commit();
            }
            return newest != null && newest.stagedAfter(register.get()) ? Optional.of(newest) : register;
        } catch (Exception e) {
            LOG.debug("could not ask [{}] for the newest commit of [{}][{}]; serving the register", writer, indexName, shard, e);
            return register;
        }
    }
}
