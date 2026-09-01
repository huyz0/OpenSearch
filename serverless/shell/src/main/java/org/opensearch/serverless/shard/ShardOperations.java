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

    /**
     * What this request has already looked up.
     *
     * <p><b>Scoped to the request because this object is.</b> One of these is built per REST request and per
     * client call, so memoising inside it is memoising for exactly as long as it is safe to: a multi-get of
     * ten documents in one index read that index's descriptor ten times, and each shard's head once per
     * document that landed on it. Measurement made that visible — ten documents fetched singly cost thirty
     * object-store requests and the same ten as one multi-get cost twenty, which is batching that does not
     * batch the part that matters.
     *
     * <p><b>What it costs is freshness within one request, and that changes nothing.</b> A shard-head read
     * at the start of a request can be stale by the end of it whether or not it was read again — routing is
     * a moment in time, and every caller of this already handles being wrong about the owner. Reading it
     * twice narrows no window; it only pays twice for the same answer.
     *
     * <p>{@code computeIfAbsent} rather than check-then-put, so two items for the same index cannot both
     * pay: the second blocks on the first rather than racing it, which makes the saving a number rather
     * than a likelihood.
     */
    private final java.util.concurrent.ConcurrentHashMap<String, Optional<IndexDescriptor>> described =
        new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.ConcurrentHashMap<String, Optional<String>> owners = new java.util.concurrent.ConcurrentHashMap<>();

    private Optional<IndexDescriptor> describeOnce(String index) throws IOException {
        return once(described, index, () -> plane.describe(index));
    }

    private Optional<String> ownerOnce(String index, int shard) throws IOException {
        return once(owners, index + "#" + shard, () -> plane.heads().read(index, shard).map(head -> head.ownerNodeId()));
    }

    private static <T> T once(
        java.util.concurrent.ConcurrentHashMap<String, T> cache,
        String key,
        org.opensearch.common.CheckedSupplier<T, IOException> read
    ) throws IOException {
        try {
            return cache.computeIfAbsent(key, ignored -> {
                try {
                    return read.get();
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        } catch (java.io.UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /** The index named does not exist. */
    public static final class NoSuchIndexException extends IOException {

        private final String index;

        /**
         * Creates the exception.
         *
         * @param index the index that does not exist
         */
        public NoSuchIndexException(String index) {
            super("no such index: " + index);
            this.index = index;
        }

        /**
         * Returns the index that does not exist.
         *
         * <p>Carried rather than left in the message so that a caller translating this into another
         * vocabulary -- {@code ServerlessClient} turns it into core's {@code IndexNotFoundException} --
         * does not have to parse prose to name the index.
         *
         * @return the index name
         */
        public String index() {
            return index;
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
            this(message, owner, retryable, null);
        }

        NotHereException(String message, String owner, boolean retryable, Throwable cause) {
            super(message, cause);
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
        final Optional<IndexDescriptor> descriptor = describeOnce(index);
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
        final String owner = ownerOnce(index, shard).orElse(null);
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
        return gated(
            org.opensearch.action.get.GetAction.NAME,
            new org.opensearch.action.get.GetRequest(index, id),
            () -> doGet(index, id),
            getView(index)
        );
    }

    /**
     * How a get is shown to a filter, and how the filter's answer is read back.
     *
     * <p><b>A redaction that covered only search would not be one.</b> A filter that removes a field from
     * every hit and cannot touch a get leaves the field one request away, under a URL any caller can
     * guess — which is worse than no redaction, because it looks like protection. So a get is shown its
     * real {@code GetResponse} for the same reason a search is.
     *
     * <p>Who served the read and whether it was realtime are kept from the original: they are facts about
     * this node's routing, not about the document, and a filter has no business restating them.
     *
     * @param index the index, which a {@code GetResponse} has to carry
     * @return the view
     */
    private static org.opensearch.serverless.shell.ActionGate.ResponseView<Read> getView(String index) {
        return new org.opensearch.serverless.shell.ActionGate.ResponseView<>() {

            @Override
            public org.opensearch.core.action.ActionResponse show(Read read) {
                final var document = read.document();
                final org.opensearch.index.get.GetResult result = new org.opensearch.index.get.GetResult(
                    index,
                    document.id(),
                    org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0L,
                    1L,
                    document.found(),
                    document.source() == null
                        ? null
                        : new org.opensearch.core.common.bytes.BytesArray(
                            document.source().getBytes(java.nio.charset.StandardCharsets.UTF_8)
                        ),
                    java.util.Map.of(),
                    java.util.Map.of()
                );
                return new org.opensearch.action.get.GetResponse(result);
            }

            @Override
            public Read read(org.opensearch.core.action.ActionResponse response, Read original) {
                if (response instanceof org.opensearch.action.get.GetResponse answered) {
                    return new Read(
                        new org.opensearch.serverless.shell.ServerlessNode.Document(
                            answered.getId(),
                            answered.isExists(),
                            answered.isSourceEmpty() ? null : answered.getSourceAsString()
                        ),
                        original.servedBy(),
                        original.realtime()
                    );
                }
                throw new IllegalStateException(
                    "an action filter answered a get with " + response.getClass().getName() + ", which is not a get response"
                );
            }
        };
    }

    /**
     * Runs one operation through the plugins' action filters.
     *
     * <p>Wrapped here rather than at each REST handler because this is the funnel: a get through the REST
     * surface, a get through a plugin's {@code Client} and a get from the shell's own code are all this
     * method. A guard placed in a handler is a guard the next handler forgets.
     *
     * <p>The checked-exception dance exists because these operations declare {@code IOException} and the
     * gate declares {@code Exception}; a filter's own refusal is a runtime exception and passes through
     * unchanged, carrying the plugin's status rather than one invented here.
     */
    private <T> T gated(
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws IOException {
        return gated(action, request, work, org.opensearch.serverless.shell.ActionGate.opaque());
    }

    private <T> T gated(
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work,
        org.opensearch.serverless.shell.ActionGate.ResponseView<T> view
    ) throws IOException {
        try {
            return node.actionGate().run(action, request, work, view);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private Read doGet(String index, String id) throws IOException {
        final Placement placement = place(index, id);
        if (placement.local() != null) {
            return new Read(node.get(placement.local(), id), node.localNode().getId(), true);
        }
        if (placement.owner() == null) {
            // Nobody owns it, so there is no writer holding unpublished writes and the commit is the
            // current state -- except when nothing has ever been published at all. Then there is no
            // commit to open a reader onto, and with no owner there is no writer about to make one either.
            // That is not a failure to report, it is a document that has never existed: a fresh index,
            // read before its first write ever lands. Checked here rather than left to openReader's own
            // refusal, which exists for a different reader -- the search fan-out, where "unpublished"
            // must not collapse into "empty" because a live writer really might be mid-first-publish.
            // Here the owner check already ruled that out.
            if (plane.segmentPublisher(index, placement.shard()).readManifest().isEmpty()) {
                return new Read(org.opensearch.serverless.shell.ServerlessNode.Document.absent(id), node.localNode().getId(), false);
            }
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
            // Carrying the cause, because the message on its own is often a single word. A TLS handshake
            // that fails arrives here as "connect_exception" and nothing else, which says a connection did
            // not happen and not one thing about why.
            throw new NotHereException(
                "could not forward to " + placement.owner() + ", which the shard-head named as owner: " + e.getMessage(),
                placement.owner(),
                true,
                e
            );
        }
    }

    /** What a whole search found, and how much of the index it managed to look at. */
    public static final class SearchOutcome {

        private final long total;
        private final java.util.List<org.opensearch.search.SearchHit> hits;
        private final int shards;
        private final int answered;
        private final org.opensearch.search.aggregations.InternalAggregations aggregations;

        /**
         * Creates the outcome.
         *
         * @param total how many matched
         * @param hits the merged page
         * @param shards how many shards the index has
         * @param answered how many answered
         */
        public SearchOutcome(long total, java.util.List<org.opensearch.search.SearchHit> hits, int shards, int answered) {
            this(total, hits, shards, answered, null);
        }

        /**
         * Creates the outcome, with the reduced aggregations.
         *
         * @param total how many matched
         * @param hits the merged page
         * @param shards how many shards the index has
         * @param answered how many answered
         * @param aggregations the combined aggregations, or null if none were asked for
         */
        public SearchOutcome(
            long total,
            java.util.List<org.opensearch.search.SearchHit> hits,
            int shards,
            int answered,
            org.opensearch.search.aggregations.InternalAggregations aggregations
        ) {
            this.total = total;
            this.hits = hits;
            this.shards = shards;
            this.answered = answered;
            this.aggregations = aggregations;
        }

        /**
         * Returns the aggregations, already combined across shards.
         *
         * @return the aggregations, or null
         */
        public org.opensearch.search.aggregations.InternalAggregations aggregations() {
            return aggregations;
        }

        /**
         * Returns how many documents matched, summed over the shards that answered.
         *
         * @return the total
         */
        public long total() {
            return total;
        }

        /**
         * Returns the page of hits, merged across shards and cut to the requested window.
         *
         * @return the hits
         */
        public java.util.List<org.opensearch.search.SearchHit> hits() {
            return hits;
        }

        /**
         * Returns how many shards the index has.
         *
         * @return the shard count
         */
        public int shards() {
            return shards;
        }

        /**
         * Returns how many of them answered.
         *
         * @return the answered count
         */
        public int answered() {
            return answered;
        }

        /**
         * Reports whether every shard answered.
         *
         * <p>Stated rather than implied: a caller reading only {@link #total()} has no way to tell a
         * complete answer from one computed over a fraction of the index.
         *
         * @return true when the whole index was searched
         */
        public boolean complete() {
            return answered == shards;
        }
    }

    /**
     * Runs a search across every shard of an index and merges the answers.
     *
     * <p>Lifted out of {@code SearchHandler} unchanged so that the REST layer and a plugin's
     * {@code Client} run the same fan-out, the same merge and the same window. A search that behaved one
     * way for a user and another for a plugin would be two search engines wearing one name.
     *
     * @param index the index
     * @param source the query, with its own from and size
     * @return what was found and how completely
     * @throws IOException if the search fails outright
     */
    public SearchOutcome search(String index, org.opensearch.search.builder.SearchSourceBuilder source) throws IOException {
        return gated(
            org.opensearch.action.search.SearchAction.NAME,
            new org.opensearch.action.search.SearchRequest(new String[] { index }, source),
            () -> {
                final IndexDescriptor descriptor = describeOnce(index).orElseThrow(() -> new NoSuchIndexException(index));
                return org.opensearch.serverless.rest.SearchFanout.run(node, plane, index, descriptor.numberOfShards(), source);
            }
        );
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
        return gated(
            org.opensearch.action.index.IndexAction.NAME,
            new org.opensearch.action.index.IndexRequest(index).id(id).source(source, org.opensearch.common.xcontent.XContentType.JSON),
            () -> write(index, id, source, refresh, false)
        );
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
        return gated(
            org.opensearch.action.delete.DeleteAction.NAME,
            new org.opensearch.action.delete.DeleteRequest(index, id),
            () -> doDelete(index, id, refresh)
        );
    }

    /** A document does not exist, and there was no {@code upsert} or {@code doc_as_upsert} to fall back to. */
    public static final class DocumentMissingException extends IOException {

        private final String index;
        private final String id;

        /**
         * Creates the exception.
         *
         * @param index the index
         * @param id the document that is not there
         */
        public DocumentMissingException(String index, String id) {
            super("document missing: [" + index + "]/[" + id + "]");
            this.index = index;
            this.id = id;
        }

        /**
         * Returns the index.
         *
         * @return the index name
         */
        public String index() {
            return index;
        }

        /**
         * Returns the document id.
         *
         * @return the id
         */
        public String id() {
            return id;
        }
    }

    /** What an update did. */
    public static final class UpdateOutcome {

        private final String result;
        private final String servedBy;

        UpdateOutcome(String result, String servedBy) {
            this.result = result;
            this.servedBy = servedBy;
        }

        /**
         * Returns what happened: {@code created}, {@code updated} or {@code noop}.
         *
         * @return the result
         */
        public String result() {
            return result;
        }

        /**
         * Returns the node that performed the write, or answered the read for a noop.
         *
         * @return the node id
         */
        public String servedBy() {
            return servedBy;
        }
    }

    /**
     * Reads a document, merges a partial update into it, and writes the result back.
     *
     * <p><b>This is a merge, not a transaction.</b> The read and the write are two separate calls to two
     * separate primitives — {@link #get} and {@link #index}'s underlying machinery — with nothing holding
     * the document still in between. A write that lands in that window is overwritten by this one, silently,
     * exactly as two plain {@code index} calls racing each other would be. Classic OpenSearch's
     * {@code _update} avoids that by retrying under {@code if_seq_no}/{@code if_primary_term}, which is
     * exactly the version model {@code WalRecord} does not have. This is the honest version of the feature
     * without one: best-effort, not compare-and-swap, and it does not pretend otherwise.
     *
     * <p><b>The merge itself is core's own</b> ({@link org.opensearch.common.xcontent.XContentHelper#update}),
     * used by core's own {@code _update} internally — recursing into nested objects, overwriting everything
     * else, and reporting whether anything actually changed. Reusing it means the merge semantics are
     * exactly what a classic client already expects, and {@code detect_noop} falls out of it for free rather
     * than needing its own comparison.
     *
     * @param index the index
     * @param id the document id
     * @param doc the partial document to merge in
     * @param upsert the document to write if none exists, or null
     * @param docAsUpsert if true, write {@code doc} itself when none exists
     * @param detectNoop if true, a merge that changed nothing is not written
     * @param refresh whether to make the result visible to search before returning
     * @return what happened
     * @throws DocumentMissingException if nothing exists and neither {@code upsert} nor {@code docAsUpsert}
     *     was given
     * @throws IOException if the read or the write fails
     */
    public UpdateOutcome update(
        String index,
        String id,
        java.util.Map<String, Object> doc,
        java.util.Map<String, Object> upsert,
        boolean docAsUpsert,
        boolean detectNoop,
        boolean refresh
    ) throws IOException {
        return gated(org.opensearch.action.update.UpdateAction.NAME, new org.opensearch.action.update.UpdateRequest(index, id), () -> {
            // doGet, not get: this whole method is one gated operation under UpdateAction's own name, and
            // calling the public get() here would run the filter chain a second time under GetAction's --
            // which is not what a filter checking "may this caller write" expects to see for an update.
            final Read current = doGet(index, id);
            final ServerlessNode.Document existing = current.document();

            final java.util.Map<String, Object> merged;
            final String result;
            if (existing.found() == false) {
                if (docAsUpsert) {
                    merged = doc;
                    result = "created";
                } else if (upsert != null) {
                    merged = upsert;
                    result = "created";
                } else {
                    throw new DocumentMissingException(index, id);
                }
            } else {
                merged = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                    new org.opensearch.core.common.bytes.BytesArray(existing.source()),
                    false,
                    org.opensearch.common.xcontent.XContentType.JSON
                ).v2();
                final boolean changed = org.opensearch.common.xcontent.XContentHelper.update(merged, doc, detectNoop);
                result = (detectNoop && changed == false) ? "noop" : "updated";
            }

            if ("noop".equals(result)) {
                return new UpdateOutcome(result, current.servedBy());
            }
            final String mergedJson;
            try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.map(merged);
                mergedJson = org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString();
            }
            final String servedBy = write(index, id, mergedJson, refresh, false);
            return new UpdateOutcome(result, servedBy);
        });
    }

    private boolean doDelete(String index, String id, boolean refresh) throws IOException {
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
            // Carrying the cause, because the message on its own is often a single word. A TLS handshake
            // that fails arrives here as "connect_exception" and nothing else, which says a connection did
            // not happen and not one thing about why.
            throw new NotHereException(
                "could not forward to " + placement.owner() + ", which the shard-head named as owner: " + e.getMessage(),
                placement.owner(),
                true,
                e
            );
        }
    }
}
