/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.Document;
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;

/**
 * Single-document reads for {@link ShardOperations}: a get or an explain served here, or forwarded to the node that
 * owns the shard.
 */
final class DocumentReads {

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private final RequestRouting routing;

    DocumentReads(ServerlessNode node, MetadataPlane plane, RequestRouting routing) {
        this.node = node;
        this.plane = plane;
        this.routing = routing;
    }

    Read readAcceptedGet(org.opensearch.action.get.GetRequest request) throws Exception {
        final var validation = request.validate();
        if (validation != null) {
            throw validation;
        }
        if (request.routing() != null) {
            throw new UnsupportedRequestOptionException("routing is not supported: documents are placed by their id");
        }
        if (request.storedFields() != null) {
            throw new UnsupportedRequestOptionException("stored_fields is not supported");
        }
        final Read read = doGet(request.index(), request.id());
        final var document = read.document();
        if (document.found() && request.versionType().isVersionConflictForReads(document.version(), request.version())) {
            throw new org.opensearch.index.engine.VersionConflictEngineException(
                read.shardId(),
                request.id(),
                request.versionType().explainConflictForReads(document.version(), request.version())
            );
        }
        if (request.fetchSourceContext() == null) {
            return read;
        }
        return new Read(
            new Document(
                document.id(),
                document.found(),
                org.opensearch.serverless.rest.SourceFiltering.apply(document.source(), request.fetchSourceContext()),
                document.seqNo(),
                document.primaryTerm(),
                document.version()
            ),
            read.servedBy(),
            read.realtime(),
            read.shardId()
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
    static org.opensearch.serverless.shell.ActionGate.ResponseView<Read> getView(String index) {
        return new org.opensearch.serverless.shell.ActionGate.ResponseView<>() {

            @Override
            public org.opensearch.core.action.ActionResponse show(Read read) {
                final var document = read.document();
                final org.opensearch.index.get.GetResult result = new org.opensearch.index.get.GetResult(
                    read.shardId() == null ? index : read.shardId().getIndexName(),
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
                        new org.opensearch.serverless.shell.Document(
                            answered.getId(),
                            answered.isExists(),
                            answered.isSourceEmpty() ? null : answered.getSourceAsString(),
                            answered.getSeqNo(),
                            answered.getPrimaryTerm(),
                            answered.getVersion()
                        ),
                        original == null ? null : original.servedBy(),
                        original != null && original.realtime(),
                        original == null ? null : original.shardId()
                    );
                }
                throw new IllegalStateException(
                    "an action filter answered a get with " + response.getClass().getName() + ", which is not a get response"
                );
            }
        };
    }

    /** How long a get of a shard whose commit is behind its log waits for this node to take the shard. */
    static final long BEHIND_LOG_WAIT_MILLIS = 10_000L;

    /**
     * Answers a get whose reader was behind the log by taking the shard here and reading from the writer.
     *
     * <p>The refusal already asked for the takeover; this waits for it, the way a write to a shard nobody holds
     * does, so the caller is answered from the log rather than told to come back. The writer's answer is the
     * realtime one. If the shard is not taken in time -- another node won it, or this one is full -- the caller is
     * told to retry, which is true: the takeover is under way.
     */
    Read readOnceTaken(
        String index,
        String id,
        int shard,
        String expectedUuid,
        org.opensearch.serverless.shell.ShardBehindLogException behind
    ) throws IOException {
        if (node.nodeCapacity().takeoverBelongsElsewhere(plane, index, shard)) {
            // A dead member's shard another survivor is named to take: asking for it here queued it on every survivor
            // a get reached. The caller retries, and finds it taken there.
            throw NotHereException.behindLog(behind);
        }
        java.util.Optional<org.opensearch.core.index.shard.ShardId> taken = java.util.Optional.empty();
        try {
            taken = node.signals().activate(index, shard).get(BEHIND_LOG_WAIT_MILLIS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
            behind.addSuppressed(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            behind.addSuppressed(e);
        }
        if (taken.isPresent() && node.reconciler().isReader(taken.get()) == false) {
            if (expectedUuid.equals(taken.get().getIndex().getUUID()) == false) {
                throw new org.opensearch.serverless.shell.StaleIncarnationException(new ShardId(index, expectedUuid, shard));
            }
            return new Read(node.reads().get(taken.get(), id), node.localNode().getId(), true, taken.get());
        }
        throw NotHereException.behindLog(behind);
    }

    Read doGet(String index, String id) throws IOException {
        final Placement placement = routing.place(index, id);
        // Local only while the head agrees. A writer this node still holds after the head moved to another
        // node is a writer that has not noticed it lost the shard, and its documents are behind the real
        // owner's: the forward below asks the node the head names, which is the one answer that is fresh.
        if (placement.local() != null && (placement.owner() == null || placement.owner().equals(node.localNode().getId()))) {
            return new Read(node.reads().get(placement.local(), id), node.localNode().getId(), true, placement.local());
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
            if (plane.segmentPublisher(index, placement.uuid(), placement.shard()).readManifest().isEmpty()) {
                return new Read(
                    org.opensearch.serverless.shell.Document.absent(id),
                    node.localNode().getId(),
                    false,
                    new ShardId(new org.opensearch.core.index.Index(index, placement.uuid()), placement.shard())
                );
            }
            try {
                final var shardId = node.readers().serveAsReader(plane, index, placement.shard(), placement.uuid());
                return new Read(node.reads().get(shardId, id), node.localNode().getId(), false, shardId);
            } catch (org.opensearch.serverless.shell.ShardBehindLogException behind) {
                return readOnceTaken(index, id, placement.shard(), placement.uuid(), behind);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                // serveAsReader opens a shard from the object store, which can fail for reasons that are
                // not IO. Wrapped rather than swallowed so the caller still sees one exception type.
                throw new IOException("could not open shard " + placement.shard() + " of " + index + " as a reader", e);
            }
        }
        if (placement.owner().equals(node.localNode().getId())) {
            throw routing.notHere(index, placement);
        }
        return forwardGet(index, id, placement, true);
    }

    /**
     * Asks the owner for a document, and once -- after a refusal or an unreachable owner -- whoever the
     * register names instead.
     */
    Read forwardGet(String index, String id, Placement placement, boolean mayRetry) throws IOException {
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
                .forwardGet(
                    peer.get(),
                    new org.opensearch.serverless.transport.ForwardedGetRequest(index, placement.uuid(), placement.shard(), id)
                );
            return new Read(
                response.document(),
                response.ownerNodeId(),
                true,
                new ShardId(new org.opensearch.core.index.Index(index, placement.uuid()), placement.shard())
            );
        } catch (NotHereException e) {
            throw e;
        } catch (Exception e) {
            return forwardGet(index, id, routing.afterForwardFailure(index, placement, e, mayRetry), false);
        }
    }

    Explained doExplain(String index, String id, org.opensearch.index.query.QueryBuilder query) throws IOException {
        final Placement placement = routing.place(index, id);
        // A retained writer may lag the successor's acknowledged writes, just as in doGet.
        if (placement.local() != null && (placement.owner() == null || placement.owner().equals(node.localNode().getId()))) {
            final ShardExplain.Outcome outcome = node.reads().explain(placement.local(), id, query);
            return new Explained(outcome.exists(), outcome.explanation(), node.localNode().getId(), true);
        }
        if (placement.owner() == null) {
            // Nobody owns it, so the published commit is the current state -- except before the first publish,
            // where there is no commit to open and no writer about to make one. The same case doGet handles,
            // and the same answer: a document that has never existed, rather than a failure.
            if (plane.segmentPublisher(index, placement.uuid(), placement.shard()).readManifest().isEmpty()) {
                return new Explained(false, null, node.localNode().getId(), false);
            }
            try {
                final var shardId = node.readers().serveAsReader(plane, index, placement.shard(), placement.uuid());
                final ShardExplain.Outcome outcome = node.reads().explain(shardId, id, query);
                return new Explained(outcome.exists(), outcome.explanation(), node.localNode().getId(), false);
            } catch (org.opensearch.serverless.shell.ShardBehindLogException behind) {
                throw NotHereException.behindLog(behind);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("could not open shard " + placement.shard() + " of " + index + " as a reader", e);
            }
        }
        if (placement.owner().equals(node.localNode().getId())) {
            throw routing.notHere(index, placement);
        }
        return forwardExplain(index, id, query, placement, true);
    }

    Explained forwardExplain(String index, String id, org.opensearch.index.query.QueryBuilder query, Placement placement, boolean mayRetry)
        throws IOException {
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
                    new org.opensearch.serverless.transport.ForwardedExplainRequest(index, placement.uuid(), placement.shard(), id, query)
                );
            return new Explained(response.exists(), response.explanation(), response.ownerNodeId(), true);
        } catch (NotHereException e) {
            throw e;
        } catch (Exception e) {
            return forwardExplain(index, id, query, routing.afterForwardFailure(index, placement, e, mayRetry), false);
        }
    }
}
