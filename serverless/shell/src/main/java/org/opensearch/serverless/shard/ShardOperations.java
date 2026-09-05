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
    private final boolean refuseSystemIndices;

    /**
     * Creates the operations view for a caller inside the node -- a plugin's {@code Client}, which may
     * read and write its own system index.
     *
     * @param node the node serving requests
     * @param plane the metadata plane
     */
    public ShardOperations(ServerlessNode node, MetadataPlane plane) {
        this(node, plane, false);
    }

    /**
     * Creates the operations view.
     *
     * <p><b>The system-index rule lives here, once, for every request that arrives over REST.</b> The
     * registration-time guard reads only the index in the request path, and a body-addressed request --
     * {@code _mget} naming {@code .serverless_auth} in {@code docs} -- walked straight past it to the
     * credential records. Placing the refusal in {@link #place} means the next handler that resolves an
     * index from a body cannot miss it, because every operation here places before it does anything.
     *
     * @param node the node serving requests
     * @param plane the metadata plane
     * @param refuseSystemIndices true for a REST caller, which may not touch a plugin's index by any spelling
     */
    public ShardOperations(ServerlessNode node, MetadataPlane plane, boolean refuseSystemIndices) {
        this.node = node;
        this.plane = plane;
        this.refuseSystemIndices = refuseSystemIndices;
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
        // The owner as this node last saw it, before the register: a wrong hint is refused by the node it
        // names and corrected by one read then, see freshOwner.
        return once(owners, index + "#" + shard, () -> {
            final Optional<String> hinted = node.ownerHint(index, shard);
            if (hinted.isPresent()) {
                return hinted;
            }
            return freshOwner(index, shard);
        });
    }

    /** Reads the head and notes it for the node. Called from inside the cache's compute, so it does not touch the cache. */
    private Optional<String> freshOwner(String index, int shard) throws IOException {
        final var head = plane.heads().read(index, shard);
        node.noteHead(index, shard, head.orElse(null));
        return head.map(h -> h.ownerNodeId());
    }

    /**
     * Seeds this instance with a descriptor the caller has already read, so it is not read again.
     *
     * @param descriptor the descriptor
     */
    public void assumeDescribed(IndexDescriptor descriptor) {
        described.put(descriptor.name(), Optional.of(descriptor));
    }

    /**
     * What to do after a forward failed, whatever the failure was.
     *
     * <p><b>The hint is forgotten first, unconditionally.</b> It used to survive everything except the one
     * refusal whose message said "does not own", so a hint naming a node that had died, or that held the
     * shard only as a reader, or that was shedding load, was consulted again on every request and every
     * request failed the same way -- indefinitely, on that coordinator, while the same write through
     * {@code _bulk} succeeded because the batch path reads the register every time.
     *
     * <p>Then the failure is sorted by {@link org.opensearch.serverless.transport.ForwardFailure}. A refusal or an unreachable owner means the
     * request was never applied: the register is read once and, if it names somebody new, the caller
     * forwards there. A deadline or a dropped connection means it may have been applied, so it is not sent
     * again; the caller is told to read before retrying. An answer from the owner -- a lost condition, a
     * 429, a mapper's 400 -- is the owner's answer and is thrown as itself.
     *
     * @return the placement to forward to next
     * @throws IOException when there is nothing to retry, worded for the caller
     */
    private Placement afterForwardFailure(String index, Placement stale, Exception failure, boolean mayRetry) throws IOException {
        node.forgetOwner(index, stale.shard());
        owners.remove(index + "#" + stale.shard());
        final org.opensearch.serverless.transport.ForwardFailure kind = org.opensearch.serverless.transport.ForwardFailure.classify(
            failure
        );
        switch (kind) {
            case CONFLICT:
                // A lost compare-and-swap came back from a healthy owner that answered correctly. It is
                // not stale routing, must not cast doubt on ownership, and must not be retried blindly --
                // so it is rethrown as itself rather than dressed as a routing failure.
                throw (org.opensearch.index.engine.VersionConflictEngineException) org.opensearch.ExceptionsHelper.unwrap(
                    failure,
                    org.opensearch.index.engine.VersionConflictEngineException.class
                );
            case REJECTED:
            case REMOTE:
                // The owner answered. A client-side status -- its 429, a mapper's 400 -- is the owner's
                // answer and reaches the caller as it is, with the status the owner gave it. A server-side
                // one means the operation did not land there, and the caller retries against a re-read head.
                if (org.opensearch.serverless.transport.ForwardFailure.isClientStatus(failure)) {
                    if (failure instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    throw new IOException(failure);
                }
                throw new NotHereException(
                    org.opensearch.serverless.transport.ForwardFailure.describe(stale.owner(), failure),
                    stale.owner(),
                    true,
                    failure
                );
            case DEADLINE:
            case INTERRUPTED:
                // May have been applied: not sent again, and worded so the caller reads before it retries.
                node.signals().ownershipDoubted(index, stale.shard());
                throw new NotHereException(
                    org.opensearch.serverless.transport.ForwardFailure.describe(stale.owner(), failure),
                    stale.owner(),
                    true,
                    failure
                );
            case NOT_OWNER:
            case UNREACHABLE:
            default:
                break;
        }
        // The owner never applied it. One register read, and one more forward to whoever it names now.
        final Optional<String> fresh = mayRetry ? freshOwner(index, stale.shard()) : Optional.empty();
        if (mayRetry) {
            owners.put(index + "#" + stale.shard(), fresh);
            if (fresh.isPresent() && fresh.get().equals(stale.owner()) == false && fresh.get().equals(node.localNode().getId()) == false) {
                return new Placement(null, stale.shard(), fresh.get(), stale.uuid());
            }
        }
        node.signals().ownershipDoubted(index, stale.shard());
        if (mayRetry && fresh.isEmpty()) {
            // Whoever the hint named is gone and nobody has taken over: the same state, and the same
            // answer, as a register that never named anyone.
            throw new NotHereException(
                "no node currently owns shard " + stale.shard() + " of " + index + "; activate it before writing",
                null,
                false,
                failure
            );
        }
        if (mayRetry && fresh.get().equals(node.localNode().getId())) {
            throw new NotHereException(
                "this node is acquiring shard " + stale.shard() + " of " + index + "; retry",
                fresh.get(),
                true,
                failure
            );
        }
        // An owner with no live lease keeps its own words, which name the missing lease; anything else is
        // worded as the forward that failed.
        throw new NotHereException(
            failure instanceof org.opensearch.serverless.transport.OwnerUnreachableException
                ? failure.getMessage()
                : org.opensearch.serverless.transport.ForwardFailure.describe(stale.owner(), failure),
            stale.owner(),
            true,
            failure
        );
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

        /**
         * The HTTP status this refusal renders as, the same on every endpoint.
         *
         * <p>"Nobody owns the shard" used to be a 421 from {@code PUT} and {@code _bulk} and a 503
         * {@code owner_unreachable} from {@code _update}, {@code _get} and {@code _mget} -- a state the code
         * itself classifies as not retryable, reported to the client as "retry". The one place that knows
         * what was thrown decides what it is.
         *
         * @return 421 when no node owns the shard, 503 for every self-resolving state
         */
        public org.opensearch.core.rest.RestStatus restStatus() {
            return owner == null
                ? org.opensearch.core.rest.RestStatus.MISDIRECTED_REQUEST
                : org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;
        }

        /**
         * The error type this refusal renders as, in the vocabulary the single-document endpoints use.
         *
         * @param localNodeId this node's id, to recognise the activation window
         * @return the type
         */
        public String restType(String localNodeId) {
            if (owner == null) {
                return "not_the_writer";
            }
            if (owner.equals(localNodeId)) {
                return "activation_in_progress";
            }
            final String message = getMessage() == null ? "" : getMessage();
            return message.contains("could not forward") || message.contains("may or may not have been applied")
                ? "forward_failed"
                : "owner_unreachable";
        }
    }

    /** The index named belongs to a plugin and is not reachable through the request path. */
    public static final class SystemIndexException extends IOException {

        private final String index;

        /**
         * Creates the refusal.
         *
         * @param index the system index that was named
         */
        public SystemIndexException(String index) {
            super("[" + index + "] belongs to a plugin and is not reachable through the request path");
            this.index = index;
        }

        /**
         * Returns the index that was refused.
         *
         * @return the index name
         */
        public String index() {
            return index;
        }
    }

    /** Which shard a document belongs to, and what this node can do about it. */
    public static final class Placement {

        private final ShardId local;
        private final int shard;
        private final String owner;
        private final String uuid;

        Placement(ShardId local, int shard, String owner, String uuid) {
            this.local = local;
            this.shard = shard;
            this.owner = owner;
            this.uuid = uuid;
        }

        /**
         * Returns the uuid of the index incarnation this placement was resolved against.
         *
         * <p>Travels with every forward, so the owner can refuse a request for an index that has since
         * been deleted and recreated under the same name.
         *
         * @return the uuid
         */
        public String uuid() {
            return uuid;
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
        if (refuseSystemIndices && node.isSystemIndex(index)) {
            // Before the descriptor is read: a refusal that first confirmed the index exists would say so.
            throw new SystemIndexException(index);
        }
        final Optional<IndexDescriptor> descriptor = describeOnce(index);
        if (descriptor.isEmpty()) {
            throw new NoSuchIndexException(index);
        }
        final int shard = org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor.get(), id);
        final ShardId local = node.reconciler()
            .openShards()
            .stream()
            // Matched by uuid as well as name: a deleted and recreated index keeps its name and changes
            // its uuid, and a writer that has not yet noticed the delete still holds the old one open under
            // the same name. Matching by name alone acknowledged writes into that shard.
            .filter(s -> s.getIndexName().equals(index) && s.id() == shard && s.getIndex().getUUID().equals(descriptor.get().uuid()))
            .filter(s -> node.reconciler().readerShards().contains(s) == false)
            .findFirst()
            .orElse(null);
        final String owner = ownerOnce(index, shard).orElse(null);
        return new Placement(local, shard, owner, descriptor.get().uuid());
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
                    // Real now, not placeholders. A filter deciding what a caller may see should be shown
                    // the document as it actually is; a redaction policy keyed on a version it was told
                    // was always 1 would be keyed on nothing.
                    document.seqNo(),
                    document.primaryTerm(),
                    document.version(),
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
                    // The sequence identity travels with the document. This used to rebuild it with the
                    // three-argument constructor, whose defaults are the unassigned sentinels -- so with any
                    // action filter installed, every get reported _seq_no -2 and _version -1, and the
                    // conditional write a caller built on those tokens lost every time.
                    return new Read(
                        new org.opensearch.serverless.shell.ServerlessNode.Document(
                            answered.getId(),
                            answered.isExists(),
                            answered.isSourceEmpty() ? null : answered.getSourceAsString(),
                            answered.getSeqNo(),
                            answered.getPrimaryTerm(),
                            answered.getVersion()
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
        // Local only while the head agrees. A writer this node still holds after the head moved to another
        // node is a writer that has not noticed it lost the shard, and its documents are behind the real
        // owner's: the forward below asks the node the head names, which is the one answer that is fresh.
        if (placement.local() != null && (placement.owner() == null || placement.owner().equals(node.localNode().getId()))) {
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
        return forwardGet(index, id, placement, true);
    }

    /**
     * Asks the owner for a document, and once -- after a refusal or an unreachable owner -- whoever the
     * register names instead.
     */
    private Read forwardGet(String index, String id, Placement placement, boolean mayRetry) throws IOException {
        // Resolving the peer connects to it, so this can throw as readily as the forward itself can --
        // and both mean the same thing. Leaving the connect outside the catch let a transport failure
        // escape as a 500 for what is a routing problem and a retry.
        try {
            final var peer = node.router().peer(placement.owner());
            if (peer.isEmpty()) {
                throw new org.opensearch.serverless.transport.OwnerUnreachableException(
                    "shard " + placement.shard() + " of " + index + " is owned by " + placement.owner() + ", which has no reachable lease"
                );
            }
            final var response = node.router()
                .forwardGet(peer.get(), new org.opensearch.serverless.transport.ForwardedGetRequest(index, placement.shard(), id));
            return new Read(response.document(), response.ownerNodeId(), true);
        } catch (NotHereException e) {
            throw e;
        } catch (Exception e) {
            return forwardGet(index, id, afterForwardFailure(index, placement, e, mayRetry), false);
        }
    }

    /** An explanation, and which copy produced it. */
    public record Explained(boolean exists, org.apache.lucene.search.Explanation explanation, String servedBy, boolean realtime) {

        /**
         * Reports whether the document matched.
         *
         * @return true when it exists and scored a match
         */
        public boolean matched() {
            return exists && explanation != null && explanation.isMatch();
        }
    }

    /**
     * Explains why a document does or does not match a query.
     *
     * <p>Routed exactly as a get is, and for the same reason: an explain names one document, and the copy
     * that has every acknowledged write is the owner. Explaining from a published commit while a writer holds
     * newer segments would produce a plausible number computed from the wrong statistics — a score is a
     * function of the whole shard's term and document frequencies, not just of the document being scored.
     *
     * <p>Which copy answered is returned rather than inferred, for the reason {@link Read} returns it: when
     * nobody owns the shard this reads a published commit, and a caller comparing this score against a search
     * result needs to know the two were computed over the same segments.
     *
     * @param index the index
     * @param id the document id
     * @param query the query to score against
     * @return the explanation and who produced it
     * @throws IOException if the explain fails or no copy can answer
     */
    public Explained explain(String index, String id, org.opensearch.index.query.QueryBuilder query) throws IOException {
        return gated(
            org.opensearch.action.explain.ExplainAction.NAME,
            new org.opensearch.action.explain.ExplainRequest(index, id),
            () -> doExplain(index, id, query)
        );
    }

    private Explained doExplain(String index, String id, org.opensearch.index.query.QueryBuilder query) throws IOException {
        final Placement placement = place(index, id);
        if (placement.local() != null) {
            final ShardExplain.Outcome outcome = ShardExplain.execute(node.searchService(), placement.local(), id, query);
            return new Explained(outcome.exists(), outcome.explanation(), node.localNode().getId(), true);
        }
        if (placement.owner() == null) {
            // Nobody owns it, so the published commit is the current state -- except before the first publish,
            // where there is no commit to open and no writer about to make one. The same case doGet handles,
            // and the same answer: a document that has never existed, rather than a failure.
            if (plane.segmentPublisher(index, placement.shard()).readManifest().isEmpty()) {
                return new Explained(false, null, node.localNode().getId(), false);
            }
            try {
                final var shardId = node.serveAsReader(plane, index, placement.shard());
                final ShardExplain.Outcome outcome = ShardExplain.execute(node.searchService(), shardId, id, query);
                return new Explained(outcome.exists(), outcome.explanation(), node.localNode().getId(), false);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("could not open shard " + placement.shard() + " of " + index + " as a reader", e);
            }
        }
        if (placement.owner().equals(node.localNode().getId())) {
            throw notHere(index, placement);
        }
        return forwardExplain(index, id, query, placement, true);
    }

    private Explained forwardExplain(
        String index,
        String id,
        org.opensearch.index.query.QueryBuilder query,
        Placement placement,
        boolean mayRetry
    ) throws IOException {
        try {
            final var peer = node.router().peer(placement.owner());
            if (peer.isEmpty()) {
                throw new org.opensearch.serverless.transport.OwnerUnreachableException(
                    "shard " + placement.shard() + " of " + index + " is owned by " + placement.owner() + ", which has no reachable lease"
                );
            }
            final var response = node.router()
                .forwardExplain(
                    peer.get(),
                    new org.opensearch.serverless.transport.ForwardedExplainRequest(index, placement.shard(), id, query)
                );
            return new Explained(response.exists(), response.explanation(), response.ownerNodeId(), true);
        } catch (NotHereException e) {
            throw e;
        } catch (Exception e) {
            return forwardExplain(index, id, query, afterForwardFailure(index, placement, e, mayRetry), false);
        }
    }

    /** What a whole search found, and how much of the index it managed to look at. */
    public static final class SearchOutcome {

        private final long total;
        private final java.util.List<org.opensearch.search.SearchHit> hits;
        private final int shards;
        private final int answered;
        private final org.opensearch.search.aggregations.InternalAggregations aggregations;
        private final org.apache.lucene.search.TotalHits.Relation relation;
        private final float maxScore;
        private final boolean timedOut;
        private final Boolean terminatedEarly;
        private final java.util.List<org.opensearch.action.search.ShardSearchFailure> failures;

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
            this(
                total,
                hits,
                shards,
                answered,
                aggregations,
                org.apache.lucene.search.TotalHits.Relation.EQUAL_TO,
                Float.NaN,
                false,
                null,
                java.util.List.of()
            );
        }

        /**
         * Creates the outcome with everything a real search response reports about how it went.
         *
         * <p>The first five are what the answer is; the rest are how much to trust it. A shard that
         * timed out, or stopped counting past a ceiling, or could not be reached and why, all used to
         * vanish between the fan-out and the response, which then stated {@code timed_out: false} and
         * {@code relation: eq} as constants and reported a failed shard as a count with no cause.
         *
         * @param total how many matched, summed over the shards that answered
         * @param hits the merged page
         * @param shards how many shards were asked
         * @param answered how many answered
         * @param aggregations the combined aggregations, or null if none were asked for
         * @param relation whether {@code total} is exact or a lower bound
         * @param maxScore the best score over every shard's top docs, or NaN when unscored
         * @param timedOut whether any shard hit the search timeout
         * @param terminatedEarly whether {@code terminate_after} stopped any shard, or null if not asked
         * @param failures why each unanswered shard did not answer
         */
        public SearchOutcome(
            long total,
            java.util.List<org.opensearch.search.SearchHit> hits,
            int shards,
            int answered,
            org.opensearch.search.aggregations.InternalAggregations aggregations,
            org.apache.lucene.search.TotalHits.Relation relation,
            float maxScore,
            boolean timedOut,
            Boolean terminatedEarly,
            java.util.List<org.opensearch.action.search.ShardSearchFailure> failures
        ) {
            this.total = total;
            this.hits = hits;
            this.shards = shards;
            this.answered = answered;
            this.aggregations = aggregations;
            this.relation = relation;
            this.maxScore = maxScore;
            this.timedOut = timedOut;
            this.terminatedEarly = terminatedEarly;
            this.failures = java.util.List.copyOf(failures);
        }

        /**
         * Returns whether {@link #total()} is exact or a lower bound.
         *
         * @return the relation
         */
        public org.apache.lucene.search.TotalHits.Relation relation() {
            return relation;
        }

        /**
         * Returns the best score over every shard that answered.
         *
         * @return the score, or NaN when the query was not scored
         */
        public float maxScore() {
            return maxScore;
        }

        /**
         * Returns whether any shard hit the search timeout.
         *
         * @return true if one did
         */
        public boolean timedOut() {
            return timedOut;
        }

        /**
         * Returns whether {@code terminate_after} stopped any shard.
         *
         * @return true or false when it was asked for, null when it was not
         */
        public Boolean terminatedEarly() {
            return terminatedEarly;
        }

        /**
         * Returns why each shard that did not answer did not answer.
         *
         * @return one failure per unanswered shard, in shard order
         */
        public java.util.List<org.opensearch.action.search.ShardSearchFailure> failures() {
            return failures;
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
                return org.opensearch.serverless.rest.SearchFanout.run(node, plane, descriptor, source);
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

    /** How many a delete-by-query operation visited and how many it actually removed. */
    public static final class DeleteByQueryOutcome {

        private final long matched;
        private final long deleted;
        private final long versionConflicts;
        private final String abortedOn;

        DeleteByQueryOutcome(long matched, long deleted) {
            this(matched, deleted, 0L, null);
        }

        DeleteByQueryOutcome(long matched, long deleted, long versionConflicts, String abortedOn) {
            this.versionConflicts = versionConflicts;
            this.abortedOn = abortedOn;
            this.matched = matched;
            this.deleted = deleted;
        }

        /**
         * Returns how many documents the frozen view matched.
         *
         * @return the count
         */
        public long matched() {
            return matched;
        }

        /**
         * Returns how many matched documents had changed since the view was taken and were left alone.
         *
         * @return the count
         */
        public long versionConflicts() {
            return versionConflicts;
        }

        /**
         * Returns the id of the conflict the operation stopped at, or null if it did not stop.
         *
         * @return the id, or null
         */
        public String abortedOn() {
            return abortedOn;
        }

        /**
         * Returns how many were actually removed.
         *
         * <p>Can differ from {@link #matched()}: a document deleted between the query freezing and this
         * operation reaching it (by an unrelated, ordinary delete) was already gone, and removing something
         * already gone is not counted twice.
         *
         * @return the count
         */
        public long deleted() {
            return deleted;
        }
    }

    /** How many matches are fetched per page, per shard, while walking a frozen view. */
    static final int DELETE_BY_QUERY_BATCH_SIZE = 1000;

    /** How long the frozen view a delete-by-query takes is held. Bounded, not indefinite. */
    static final long DELETE_BY_QUERY_KEEP_ALIVE_MILLIS = 3_600_000L;

    /**
     * Deletes every document a query matches.
     *
     * <p><b>Evaluated against a frozen view, not live state.</b> The set of documents to delete is decided
     * once, against the last published commit as of when the operation starts — the same point-in-time
     * mechanism {@code search_after} paging uses, and for the same reason: a query re-evaluated against a
     * moving index could match a document twice, or never, depending on when a write happened to land
     * relative to which page was being read. A write acknowledged after the operation starts, published or
     * not, is not touched by it — the same boundary a plain search_after export of matching ids would have.
     *
     * <p><b>Walked one shard at a time, sorted by {@code _doc}</b> — native Lucene document order, always
     * available with no fielddata, and exhaustive within one shard's frozen reader — rather than through
     * the cross-shard {@code search_after} merge a multi-index search uses. That merge exists to produce
     * one globally ordered page across shards, which needs a sort whose values are comparable across
     * shards; this needs only that every match is visited exactly once, which a per-shard order already
     * gives without needing to be meaningful across shards at all.
     *
     * <p><b>Deletes go to live state, not the frozen view.</b> Matching is frozen; deleting is not — each
     * matched id is removed through the ordinary write path. A document rewritten between the query
     * freezing and its delete reaching the shard is still deleted: there is no version check to make that a
     * conflict, the same as everywhere else on this surface.
     *
     * <p><b>Gated once, under {@code indices:data/write/delete/byquery}</b>, not once per document under
     * {@code indices:data/write/delete} — the same reasoning {@code BulkHandler} gives for gating a whole
     * batch once: a privilege evaluator registered for the by-query action would otherwise never see it, and
     * one registered for plain deletes would see thousands of calls a caller never made individually.
     *
     * @param index the index
     * @param query the query; required, there is no implicit match_all
     * @param maxDocs the most documents to visit, or {@code Long.MAX_VALUE} for no cap
     * @param refresh whether to make the deletions visible to search before returning
     * @return how many matched and how many were removed
     * @throws NoSuchIndexException if the index does not exist
     * @throws IOException if the query, a delete, or the point-in-time machinery fails
     */
    public DeleteByQueryOutcome deleteByQuery(String index, org.opensearch.index.query.QueryBuilder query, long maxDocs, boolean refresh)
        throws IOException {
        return deleteByQuery(index, query, maxDocs, refresh, true);
    }

    /**
     * Deletes every document a query matches, leaving alone any that changed after the query was frozen.
     *
     * <p>Each delete is conditional on the sequence number the frozen view saw, so a document rewritten
     * between the view and the delete is a version conflict rather than a silent loss of the rewrite --
     * which is what core's {@code conflicts=abort} and {@code conflicts=proceed} are about.
     *
     * @param index the index
     * @param query the query
     * @param maxDocs the most documents to visit
     * @param refresh whether to make the deletions visible before returning
     * @param abortOnConflict whether the first conflict stops the operation, as core does by default
     * @return the outcome
     * @throws NoSuchIndexException if the index does not exist
     * @throws IOException if the query, a delete, or the point-in-time machinery fails
     */
    public DeleteByQueryOutcome deleteByQuery(
        String index,
        org.opensearch.index.query.QueryBuilder query,
        long maxDocs,
        boolean refresh,
        boolean abortOnConflict
    ) throws IOException {
        return gated(
            org.opensearch.index.reindex.DeleteByQueryAction.NAME,
            new org.opensearch.index.reindex.DeleteByQueryRequest(index),
            () -> doDeleteByQuery(index, query, maxDocs, refresh, abortOnConflict)
        );
    }

    private DeleteByQueryOutcome doDeleteByQuery(
        String index,
        org.opensearch.index.query.QueryBuilder query,
        long maxDocs,
        boolean refresh,
        boolean abortOnConflict
    ) throws Exception {
        final IndexDescriptor descriptor = describeOnce(index).orElseThrow(() -> new NoSuchIndexException(index));

        // A frozen view, exactly as _pit takes one -- see the class javadoc for why the query is decided
        // once rather than re-evaluated as the walk goes.
        final java.util.Map<Integer, org.opensearch.serverless.store.CommitManifest> shards = new java.util.LinkedHashMap<>();
        for (int shard = 0; shard < descriptor.numberOfShards(); shard++) {
            final var manifest = plane.segmentPublisher(index, descriptor.uuid(), shard).readManifest();
            if (manifest.isEmpty()) {
                // Nothing published on this shard: nothing to match, and nothing silently skipped either
                // -- an unpublished shard has no documents a search could have found.
                continue;
            }
            shards.put(shard, manifest.get());
        }
        if (shards.isEmpty()) {
            return new DeleteByQueryOutcome(0, 0);
        }

        final var pit = new org.opensearch.serverless.metadata.PointInTime(
            org.opensearch.common.UUIDs.randomBase64UUID(),
            index,
            plane.clock().getAsLong() + DELETE_BY_QUERY_KEEP_ALIVE_MILLIS,
            shards
        );
        plane.createPointInTime(pit);
        try {
            long visited = 0;
            long deleted = 0;
            long conflicts = 0;
            for (Integer shard : shards.keySet()) {
                final var frozenShardId = node.openFrozenView(plane, pit, shard);
                Object[] cursor = null;
                while (visited < maxDocs) {
                    final var page = new org.opensearch.search.builder.SearchSourceBuilder().query(query)
                        .sort(new org.opensearch.search.sort.FieldSortBuilder(org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME))
                        .size((int) Math.min(DELETE_BY_QUERY_BATCH_SIZE, maxDocs - visited))
                        .trackTotalHits(false)
                        // The sequence number each delete is conditional on.
                        .seqNoAndPrimaryTerm(true)
                        .fetchSource(false);
                    if (cursor != null) {
                        page.searchAfter(cursor);
                    }
                    final var result = org.opensearch.serverless.shard.ShardQuery.execute(node.searchService(), frozenShardId, page);
                    final var hits = result.hits();
                    if (hits.isEmpty()) {
                        break;
                    }
                    final boolean lastPageOfShard = hits.size() < page.size() || visited + hits.size() >= maxDocs;
                    long pageBytes = 0L;
                    for (var hit : hits) {
                        pageBytes += 64L + (hit.getId() == null ? 0 : hit.getId().length());
                    }
                    // A page of deletes is a page of writes, and accounted as one: this operation used to
                    // be the only writer on the node that indexing pressure could not see.
                    try (
                        org.opensearch.common.lease.Releasable inFlight = node.indexingPressure()
                            .markCoordinatingOperationStarted(pageBytes, false)
                    ) {
                        for (int i = 0; i < hits.size(); i++) {
                            final var hit = hits.get(i);
                            visited++;
                            // The refresh -- when asked for -- rides the last delete of the shard, not every
                            // one: refreshing per document would refresh the shard once per document instead
                            // of once for the whole operation.
                            final boolean isLastOfShard = refresh && lastPageOfShard && i == hits.size() - 1;
                            try {
                                if (doDeleteIfUnchanged(index, hit.getId(), isLastOfShard, hit.getSeqNo(), hit.getPrimaryTerm())) {
                                    deleted++;
                                }
                            } catch (org.opensearch.index.engine.VersionConflictEngineException e) {
                                // Rewritten -- or already removed -- after the view was taken. Left alone,
                                // and counted, rather than deleted regardless.
                                conflicts++;
                                if (abortOnConflict) {
                                    return new DeleteByQueryOutcome(visited, deleted, conflicts, hit.getId());
                                }
                            }
                        }
                    }
                    cursor = hits.get(hits.size() - 1).getSortValues();
                    if (lastPageOfShard) {
                        // Fewer than a full page, or the cap was reached: this shard has nothing left to
                        // give (or nothing more is wanted), and asking again would cost a round trip to
                        // learn what this already knows.
                        break;
                    }
                }
                // A shard with no match never entered the loop above, so it never issued a delete and
                // never needed a refresh -- correctly, since nothing changed on it to make visible.
            }
            return new DeleteByQueryOutcome(visited, deleted, conflicts, null);
        } finally {
            // Local shards first, then the record -- the same order _pit's own release does, so this node
            // is never left holding open frozen shards for a view no longer recorded anywhere.
            node.reconciler().closeFrozenReader(pit.id());
            plane.releasePointInTime(pit.id());
        }
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
        private final long seqNo;
        private final long primaryTerm;
        private final long version;

        UpdateOutcome(String result, String servedBy, long seqNo, long primaryTerm, long version) {
            this.result = result;
            this.servedBy = servedBy;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
        }

        /**
         * Returns the sequence number the engine assigned the write this update performed.
         *
         * <p>For a noop it is the sequence number the document already had: nothing was written, so the
         * document's identity is unchanged, and a caller conditioning a later write on it is right to.
         *
         * @return the sequence number
         */
        public long seqNo() {
            return seqNo;
        }

        /**
         * Returns the primary term that operation ran at.
         *
         * @return the primary term
         */
        public long primaryTerm() {
            return primaryTerm;
        }

        /**
         * Returns the document's version after this update.
         *
         * @return the version
         */
        public long version() {
            return version;
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
        boolean refresh,
        long ifSeqNo,
        long ifPrimaryTerm
    ) throws IOException {
        return update(index, id, doc, upsert, docAsUpsert, detectNoop, refresh, ifSeqNo, ifPrimaryTerm, null, false);
    }

    /**
     * Updates a document, optionally by running a script over it.
     *
     * <p><b>The script decides, and core runs it.</b> A scripted update compiles through the node's own
     * {@code ScriptService} under core's {@code UpdateScript} context and is handed the same {@code ctx} map
     * classic OpenSearch hands it — {@code ctx._source}, {@code ctx.op}, {@code ctx._index}, {@code ctx._id}.
     * Nothing here interprets the script or re-implements the context; a script that works against a classic
     * node works here because it is the same context object and the same engine.
     *
     * <p>{@code ctx.op} is honoured: a script setting it to {@code "noop"} writes nothing and a script
     * setting it to {@code "delete"} removes the document, which is the whole reason a caller reaches for a
     * scripted update rather than a partial document.
     *
     * @param index the index
     * @param id the document id
     * @param doc the partial document, or null for a scripted update
     * @param upsert what to write when the document is absent, or null
     * @param docAsUpsert whether the partial document is also the upsert
     * @param detectNoop whether an update that changes nothing is reported as a noop
     * @param refresh whether to make the write visible before answering
     * @param ifSeqNo the sequence number the document must be at, or unassigned
     * @param ifPrimaryTerm the primary term the document must be at, or 0
     * @param script the script to run, or null
     * @param scriptedUpsert whether the script also runs when the document is absent
     * @return what happened
     * @throws IOException if the read or the write fails
     */
    public UpdateOutcome update(
        String index,
        String id,
        java.util.Map<String, Object> doc,
        java.util.Map<String, Object> upsert,
        boolean docAsUpsert,
        boolean detectNoop,
        boolean refresh,
        long ifSeqNo,
        long ifPrimaryTerm,
        org.opensearch.script.Script script,
        boolean scriptedUpsert
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
                if (script != null) {
                    result = "updated";
                } else {
                    final boolean changed = org.opensearch.common.xcontent.XContentHelper.update(merged, doc, detectNoop);
                    result = (detectNoop && changed == false) ? "noop" : "updated";
                }
            }

            String operation = result;
            if (script != null && (existing.found() || scriptedUpsert)) {
                // ctx as classic OpenSearch builds it, so a script written against a classic node behaves
                // the same here. The map is mutable and the script edits it in place, which is the contract
                // UpdateScript defines rather than one chosen here.
                final java.util.Map<String, Object> ctx = new java.util.HashMap<>();
                ctx.put("_source", merged);
                ctx.put("_index", index);
                ctx.put("_id", id);
                ctx.put("_version", existing.found() ? existing.version() : 0L);
                ctx.put("op", "index");
                final var factory = node.scriptService().compile(script, org.opensearch.script.UpdateScript.CONTEXT);
                factory.newInstance(script.getParams(), ctx).execute();
                operation = String.valueOf(ctx.getOrDefault("op", "index"));
                @SuppressWarnings("unchecked")
                final java.util.Map<String, Object> edited = (java.util.Map<String, Object>) ctx.get("_source");
                // A script usually mutates ctx._source in place, in which case it *is* this map and there is
                // nothing to copy. It may also replace it wholesale. Clearing first and copying second
                // handles the second case and silently empties the document in the first, because the source
                // and the destination are the same object -- which is what the assertion on the document's
                // contents, rather than on the response, caught.
                if (edited != null && edited != merged) {
                    final java.util.Map<String, Object> replacement = new java.util.LinkedHashMap<>(edited);
                    merged.clear();
                    merged.putAll(replacement);
                }
            }

            if ("noop".equals(operation) || "none".equals(operation)) {
                return new UpdateOutcome("noop", current.servedBy(), existing.seqNo(), existing.primaryTerm(), existing.version());
            }
            if ("delete".equals(operation)) {
                // A script may remove the document, and that is the point of one: the alternative is a read,
                // a decision in the client, and a second request that races with anyone else writing.
                // doDelete, not delete: this is already inside the update's own gate, and running the
                // delete filter chain here would show a filter a DeleteAction it did not see the caller ask
                // for -- the same reason the read above is doGet rather than get.
                doDelete(index, id, refresh);
                return new UpdateOutcome("deleted", current.servedBy(), existing.seqNo(), existing.primaryTerm(), existing.version());
            }

            if ("noop".equals(result)) {
                // Nothing written, so the document keeps the identity the read just observed. Reporting
                // that identity rather than an unassigned one is what lets a caller chain a conditional
                // write after a noop without having to re-read.
                return new UpdateOutcome(result, current.servedBy(), existing.seqNo(), existing.primaryTerm(), existing.version());
            }
            final String mergedJson;
            try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.map(merged);
                mergedJson = org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString();
            }
            // The compare-and-swap an update is: against what was just read, unless the caller brought a
            // condition of their own. This used to pass the caller's (unassigned) condition, so two
            // concurrent partial updates each read, each wrote, and one merge was lost -- and the
            // retry_on_conflict loop above this could never fire. A missing document is written as a
            // create, so two concurrent upserts cannot both "create".
            final boolean unconditional = ifSeqNo == org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
            final Written written = written(
                index,
                id,
                mergedJson,
                refresh,
                false,
                unconditional && existing.found() ? existing.seqNo() : ifSeqNo,
                unconditional && existing.found() ? existing.primaryTerm() : ifPrimaryTerm,
                unconditional && existing.found() == false
            );
            return new UpdateOutcome(
                result,
                written.servedBy(),
                written.outcome().seqNo(),
                written.outcome().primaryTerm(),
                written.outcome().version()
            );
        });
    }

    private boolean doDelete(String index, String id, boolean refresh) throws IOException {
        final Placement placement = place(index, id);
        if (placement.local() != null) {
            final boolean found = node.delete(placement.local(), id).found();
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

    /**
     * Deletes a document only if it is still at the sequence number a frozen view saw it at.
     *
     * <p>A hit that carries no sequence number -- a shard that did not report one -- falls back to the
     * unconditional delete, which is the behaviour this had for every document before.
     */
    private boolean doDeleteIfUnchanged(String index, String id, boolean refresh, long seqNo, long primaryTerm) throws IOException {
        if (seqNo < 0 || primaryTerm <= 0) {
            return doDelete(index, id, refresh);
        }
        final Placement placement = place(index, id);
        if (placement.local() != null) {
            final boolean found = node.delete(placement.local(), id, seqNo, primaryTerm).found();
            if (refresh) {
                node.reconciler().shard(placement.local()).refresh("serverless-ops-refresh");
            }
            return found;
        }
        return forwardWritten(index, id, "", refresh, true, placement, seqNo, primaryTerm).outcome().found();
    }

    private String write(String index, String id, String source, boolean refresh, boolean deletion) throws IOException {
        return written(index, id, source, refresh, deletion, org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, 0L).servedBy();
    }

    /** Who served a write, and what the engine assigned it. */
    private static final class Written {
        private final String servedBy;
        private final org.opensearch.serverless.shell.ServerlessNode.WriteOutcome outcome;

        Written(String servedBy, org.opensearch.serverless.shell.ServerlessNode.WriteOutcome outcome) {
            this.servedBy = servedBy;
            this.outcome = outcome;
        }

        String servedBy() {
            return servedBy;
        }

        org.opensearch.serverless.shell.ServerlessNode.WriteOutcome outcome() {
            return outcome;
        }
    }

    private Written written(String index, String id, String source, boolean refresh, boolean deletion, long ifSeqNo, long ifPrimaryTerm)
        throws IOException {
        return written(index, id, source, refresh, deletion, ifSeqNo, ifPrimaryTerm, false);
    }

    private Written written(
        String index,
        String id,
        String source,
        boolean refresh,
        boolean deletion,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent
    ) throws IOException {
        final Placement placement = place(index, id);
        if (placement.local() != null) {
            final var outcome = node.index(placement.local(), id, source, ifSeqNo, ifPrimaryTerm, requireAbsent);
            if (refresh) {
                node.reconciler().shard(placement.local()).refresh("serverless-ops-refresh");
            }
            return new Written(node.localNode().getId(), outcome);
        }
        return forwardWritten(index, id, source, refresh, deletion, placement, ifSeqNo, ifPrimaryTerm, requireAbsent);
    }

    private String forward(String index, String id, String source, boolean refresh, boolean deletion, Placement placement)
        throws IOException {
        return forwardWritten(
            index,
            id,
            source,
            refresh,
            deletion,
            placement,
            org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
            0L
        ).servedBy();
    }

    private Written forwardWritten(
        String index,
        String id,
        String source,
        boolean refresh,
        boolean deletion,
        Placement placement,
        long ifSeqNo,
        long ifPrimaryTerm
    ) throws IOException {
        return forwardWritten(index, id, source, refresh, deletion, placement, ifSeqNo, ifPrimaryTerm, false);
    }

    private Written forwardWritten(
        String index,
        String id,
        String source,
        boolean refresh,
        boolean deletion,
        Placement placement,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent
    ) throws IOException {
        return forwardWritten(index, id, source, refresh, deletion, placement, ifSeqNo, ifPrimaryTerm, requireAbsent, true);
    }

    private Written forwardWritten(
        String index,
        String id,
        String source,
        boolean refresh,
        boolean deletion,
        Placement placement,
        long ifSeqNo,
        long ifPrimaryTerm,
        boolean requireAbsent,
        boolean mayRetry
    ) throws IOException {
        if (placement.owner() == null || placement.owner().equals(node.localNode().getId())) {
            throw notHere(index, placement);
        }
        try {
            final var peer = node.router().peer(placement.owner());
            if (peer.isEmpty()) {
                throw new org.opensearch.serverless.transport.OwnerUnreachableException(
                    "shard " + placement.shard() + " of " + index + " is owned by " + placement.owner() + ", which has no reachable lease"
                );
            }
            final var ack = node.router()
                .forwardIndex(
                    peer.get(),
                    new org.opensearch.serverless.transport.ForwardedIndexRequest(
                        index,
                        placement.uuid(),
                        placement.shard(),
                        id,
                        source,
                        refresh,
                        deletion,
                        ifSeqNo,
                        ifPrimaryTerm,
                        requireAbsent
                    )
                );
            return new Written(
                ack.ownerNodeId(),
                new org.opensearch.serverless.shell.ServerlessNode.WriteOutcome(
                    ack.seqNo(),
                    ack.primaryTerm(),
                    ack.version(),
                    ack.created(),
                    ack.found()
                )
            );
        } catch (NotHereException e) {
            throw e;
        } catch (Exception e) {
            // Sorted by afterForwardFailure: a lost condition is rethrown as itself, an owner's own answer
            // keeps its status, a deadline is not retried, and a refusal or an unreachable owner costs one
            // register read and one more forward.
            return forwardWritten(
                index,
                id,
                source,
                refresh,
                deletion,
                afterForwardFailure(index, placement, e, mayRetry),
                ifSeqNo,
                ifPrimaryTerm,
                requireAbsent,
                false
            );
        }
    }
}
