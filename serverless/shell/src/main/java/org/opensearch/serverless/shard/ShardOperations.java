/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;
import java.util.Optional;

/**
 * Where a document lives, who may serve it, and the four things you can do to it.
 *
 * <p><b>This exists because the REST handlers were about to be forked.</b> Routing a single document —
 * find the descriptor, hash to a shard, serve it here or forward it to the owner, and say something
 * useful when nobody can — was written inside {@code DocumentHandler} and again inside {@code GetHandler},
 * and a {@code Client} façade for plugins would have written it a third time. Three copies of a routing
 * rule is three chances for them to disagree about who owns a shard, which is the one thing this system
 * must not be vague about.
 *
 * <p><b>Failures are typed, not rendered.</b> Each caller says what a lost race means in its own
 * vocabulary: REST turns "not the writer" into a 421 naming the owner, and a plugin's {@code Client} turns
 * the same thing into an exception it can retry. Neither meaning belongs down here.
 */
public final class ShardOperations {

    private final ServerlessNode node;
    private final MetadataPlane plane;

    /**
     * Creates the operations view.
     *
     * @param node the node serving requests
     * @param plane the metadata plane
     */
    public ShardOperations(ServerlessNode node, MetadataPlane plane) {
        this.node = node;
        this.plane = plane;
    }

    /** The index named does not exist. */
    public static final class NoSuchIndexException extends IOException {
        /**
         * Creates the exception.
         *
         * @param index the index that does not exist
         */
        public NoSuchIndexException(String index) {
            super("no such index: " + index);
        }
    }

    /**
     * This node cannot serve the shard, and says what it knows about who can.
     *
     * <p>Carrying the owner is the point: a caller that is told only "wrong node" has to guess, and
     * guessing at ownership is how two writers come to believe the same thing.
     */
    public static final class NotHereException extends IOException {

        private final String owner;
        private final boolean retryable;

        NotHereException(String message, String owner, boolean retryable) {
            super(message);
            this.owner = owner;
            this.retryable = retryable;
        }

        /**
         * Returns the node the shard-head names, if any.
         *
         * @return the owner, or null
         */
        public String owner() {
            return owner;
        }

        /**
         * Reports whether asking again shortly is likely to work.
         *
         * <p>True for a shard mid-activation or an owner that has gone quiet — states that resolve
         * themselves. False when no node owns the shard at all, which needs someone to activate it.
         *
         * @return true when the caller should retry
         */
        public boolean retryable() {
            return retryable;
        }
    }

    /** Which shard a document belongs to, and what this node can do about it. */
    public static final class Placement {

        private final ShardId local;
        private final int shard;
        private final String owner;

        Placement(ShardId local, int shard, String owner) {
            this.local = local;
            this.shard = shard;
            this.owner = owner;
        }

        /**
         * Returns the shard if it is open here as a writer, or null.
         *
         * @return the local shard, or null
         */
        public ShardId local() {
            return local;
        }

        /**
         * Returns the shard number the document hashes to.
         *
         * @return the shard number
         */
        public int shard() {
            return shard;
        }

        /**
         * Returns the node the shard-head names, or null if nobody owns it.
         *
         * @return the owner, or null
         */
        public String owner() {
            return owner;
        }
    }

    /**
     * Works out where a document belongs and who can serve it.
     *
     * @param index the index
     * @param id the document id
     * @return the placement
     * @throws IOException if the metadata plane cannot be read
     * @throws NoSuchIndexException if the index does not exist
     */
    public Placement place(String index, String id) throws IOException {
        final Optional<IndexDescriptor> descriptor = plane.describe(index);
        if (descriptor.isEmpty()) {
            throw new NoSuchIndexException(index);
        }
        final int shard = org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor.get(), id);
        final ShardId local = node.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(index) && s.id() == shard)
            .filter(s -> node.reconciler().readerShards().contains(s) == false)
            .findFirst()
            .orElse(null);
        final String owner = plane.heads().read(index, shard).map(h -> h.ownerNodeId()).orElse(null);
        return new Placement(local, shard, owner);
    }

    /**
     * Explains why this node cannot serve a placement, in the terms every caller needs.
     *
     * <p>Split out because the three "not here" cases are genuinely different and collapsing them is how
     * a brief, self-healing state gets reported as a permanent failure. A head naming <em>this</em> node
     * with no open shard is the activation window; a head naming somebody else is routing; no head at all
     * means nothing will change until a node activates the shard, and the arrival of this request is the
     * only reason one would.
     */
    private NotHereException notHere(String index, Placement placement) {
        if (placement.owner() == null) {
            node.signals().ownershipDoubted(index, placement.shard());
            return new NotHereException("no node currently owns shard " + placement.shard() + " of " + index, null, false);
        }
        if (placement.owner().equals(node.localNode().getId())) {
            return new NotHereException(
                "this node is acquiring shard " + placement.shard() + " of " + index + "; retry",
                placement.owner(),
                true
            );
        }
        return new NotHereException(
            "shard " + placement.shard() + " of " + index + " is owned by " + placement.owner(),
            placement.owner(),
            true
        );
    }

    /** A document, and which copy answered for it. */
    public static final class Read {

        private final ServerlessNode.Document document;
        private final String servedBy;
        private final boolean realtime;

        Read(ServerlessNode.Document document, String servedBy, boolean realtime) {
            this.document = document;
            this.servedBy = servedBy;
            this.realtime = realtime;
        }

        /**
         * Returns what was found, which may be nothing.
         *
         * @return the document
         */
        public ServerlessNode.Document document() {
            return document;
        }

        /**
         * Returns the node that actually read it.
         *
         * @return the node id
         */
        public String servedBy() {
            return servedBy;
        }

        /**
         * Reports whether the answer includes writes that are acknowledged but not yet published.
         *
         * @return true when the owner answered
         */
        public boolean realtime() {
            return realtime;
        }
    }

    /**
     * Reads one document by id, from the copy that has everything acknowledged.
     *
     * <p>Answered by the shard's owner, or from a published commit when nobody owns it. See
     * {@code GetHandler} for why a get is routed to the writer and a search is not.
     *
     * <p>Returns who answered rather than only what they said, because the caller cannot work that out
     * afterwards: a forwarded read is served by a node this one merely asked. A first version had the
     * handler re-derive it from the placement and quietly name the local node for every forwarded get.
     *
     * @param index the index
     * @param id the document id
     * @return what was found and who found it
     * @throws IOException if the read fails or no copy can answer
     */
    public Read get(String index, String id) throws IOException {
        final Placement placement = place(index, id);
        if (placement.local() != null) {
            return new Read(node.get(placement.local(), id), node.localNode().getId(), true);
        }
        if (placement.owner() == null) {
            // Nobody owns it, so there is no writer holding unpublished writes and the commit is the
            // current state.
            try {
                final var shardId = node.serveAsReader(plane, index, placement.shard());
                return new Read(node.get(shardId, id), node.localNode().getId(), false);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                // serveAsReader opens a shard from the object store, which can fail for reasons that are
                // not IO. Wrapped rather than swallowed so the caller still sees one exception type.
                throw new IOException("could not open shard " + placement.shard() + " of " + index + " as a reader", e);
            }
        }
        if (placement.owner().equals(node.localNode().getId())) {
            throw notHere(index, placement);
        }
        // Resolving the peer connects to it, so this can throw as readily as the forward itself can --
        // and both mean the same thing. Leaving the connect outside the catch let a transport failure
        // escape as a 500 for what is a routing problem and a retry.
        try {
            final var peer = node.router().peer(placement.owner());
            if (peer.isEmpty()) {
                node.signals().ownershipDoubted(index, placement.shard());
                throw new NotHereException(
                    "shard " + placement.shard() + " of " + index + " is owned by " + placement.owner() + ", which has no reachable lease",
                    placement.owner(),
                    true
                );
            }
            final var response = node.router()
                .forwardGet(peer.get(), new org.opensearch.serverless.transport.ForwardedGetRequest(index, placement.shard(), id));
            return new Read(response.document(), response.ownerNodeId(), true);
        } catch (NotHereException e) {
            throw e;
        } catch (Exception e) {
            node.signals().ownershipDoubted(index, placement.shard());
            throw new NotHereException(
                "could not forward to " + placement.owner() + ", which the shard-head named as owner: " + e.getMessage(),
                placement.owner(),
                true
            );
        }
    }

    /**
     * Writes one document, durably, wherever it belongs.
     *
     * @param index the index
     * @param id the document id
     * @param source the document body
     * @param refresh whether to make it visible to search before returning
     * @return the node that performed the write
     * @throws IOException if the write fails or no node can take it
     */
    public String index(String index, String id, String source, boolean refresh) throws IOException {
        return write(index, id, source, refresh, false);
    }

    /**
     * Removes one document, wherever it lives.
     *
     * @param index the index
     * @param id the document id
     * @param refresh whether to make the removal visible before returning
     * @return true if the document was there
     * @throws IOException if the delete fails or no node can take it
     */
    public boolean delete(String index, String id, boolean refresh) throws IOException {
        final Placement placement = place(index, id);
        if (placement.local() != null) {
            final boolean found = node.delete(placement.local(), id);
            if (refresh) {
                node.reconciler().shard(placement.local()).refresh("serverless-ops-refresh");
            }
            return found;
        }
        forward(index, id, "", refresh, true, placement);
        // A forwarded acknowledgement does not carry found-ness; the single-document REST path has always
        // reported a forwarded delete as found, and this keeps that rather than inventing a new answer.
        return true;
    }

    private String write(String index, String id, String source, boolean refresh, boolean deletion) throws IOException {
        final Placement placement = place(index, id);
        if (placement.local() != null) {
            node.index(placement.local(), id, source);
            if (refresh) {
                node.reconciler().shard(placement.local()).refresh("serverless-ops-refresh");
            }
            return node.localNode().getId();
        }
        return forward(index, id, source, refresh, deletion, placement);
    }

    private String forward(String index, String id, String source, boolean refresh, boolean deletion, Placement placement)
        throws IOException {
        if (placement.owner() == null || placement.owner().equals(node.localNode().getId())) {
            throw notHere(index, placement);
        }
        try {
            final var peer = node.router().peer(placement.owner());
            if (peer.isEmpty()) {
                node.signals().ownershipDoubted(index, placement.shard());
                throw new NotHereException(
                    "shard " + placement.shard() + " of " + index + " is owned by " + placement.owner() + ", which has no reachable lease",
                    placement.owner(),
                    true
                );
            }
            return node.router()
                .forwardIndex(
                    peer.get(),
                    new org.opensearch.serverless.transport.ForwardedIndexRequest(index, placement.shard(), id, source, refresh, deletion)
                )
                .ownerNodeId();
        } catch (NotHereException e) {
            throw e;
        } catch (Exception e) {
            // Stale routing is a retry, not a failure of the write itself.
            node.signals().ownershipDoubted(index, placement.shard());
            throw new NotHereException(
                "could not forward to " + placement.owner() + ", which the shard-head named as owner: " + e.getMessage(),
                placement.owner(),
                true
            );
        }
    }
}
