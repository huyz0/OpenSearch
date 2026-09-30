/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code GET /_list/indices/{pattern}} — the indices matching a prefix, a bounded page at a time.
 *
 * <p><b>Why this endpoint and not {@code _cat/indices}.</b> OpenSearch reached the same conclusion about its
 * own {@code _cat} APIs: they do not scale, because their contract is "return everything". Rather than
 * quietly truncating them, OpenSearch 2.18 left them alone and added {@code _list/indices}, whose contract
 * <em>is</em> a page at a time. Serving that contract involves no lying; serving {@code _cat/indices} at this
 * design's scale would.
 *
 * <p><b>{@code _list/indices} pages; {@code _cat/indices} refuses.</b> A prefix pattern, or the bare form
 * meaning every index, is walked with core's contract: {@code size} rows per page (500 by default, 5000 at
 * most) and a {@code next_token} to resume, null on the last page. The walk resumes in the store's own
 * listing ({@code StartAfter} on S3), so page N costs what page 1 did: one listing per thousand names plus
 * one descriptor read per row. {@code _cat/indices}, whose contract is the whole answer, still resolves
 * under the pattern cap and refuses past it rather than truncating.
 *
 * <p><b>What a walk promises, and what it does not.</b> It is not a snapshot. An index created or deleted
 * behind the cursor is not seen by the rest of the walk; one created or deleted ahead of it is seen or not
 * depending on when its page is read. Every index that exists for the whole walk appears exactly once, in
 * name order. Core's {@code _list/indices} sorts by creation time; there is no creation-ordered index over
 * a hundred million names here, so {@code sort} is accepted and the order is by name.
 *
 * <p><b>The token.</b> Opaque to a caller: the prefix and the last name, base64url-encoded. A token for one
 * prefix presented with another, or one that does not decode, is refused as tainted -- core's wording --
 * rather than restarting the walk, which for a caller paginating a large deployment would mean never
 * finishing and never being told why.
 *
 * <p><b>What is reported, and what is not.</b> {@code _list/indices} carries {@code docs.count} and
 * {@code store.size} in OpenSearch. Those are shard-level facts and this reads only descriptors, so they are
 * omitted rather than reported as zero — zero is a claim, and the true answer is that they were not measured.
 */
public final class ListIndicesHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public ListIndicesHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_list_indices_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_list/indices/{index}"),
            // The spelling both AWS OpenSearch Serverless and Elastic Cloud Serverless keep of the _cat
            // family, served under the same bounded-prefix rule: a name or a prefix pattern answers, and
            // the bare /_cat/indices stays refused as the enumeration it is.
            new Route(RestRequest.Method.GET, "/_cat/indices/{index}"),
            // The bare forms, which used to be refused outright as enumeration. They are the same
            // bounded listing with an empty prefix: one listBlobsByPrefixInSortedOrder capped at the
            // pattern cap, refused past it rather than truncated. That refusal is what makes serving
            // them honest -- a deployment small enough to answer gets its answer, and one too large is
            // told to narrow rather than handed a page that looks complete and is not.
            new Route(RestRequest.Method.GET, "/_list/indices"),
            new Route(RestRequest.Method.GET, "/_cat/indices")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // An absent {index} is the bare form, which means every index -- expressed as the prefix
        // pattern that already has a bound, so it travels the identical path rather than a second one.
        final String index = request.param("index", "*");
        final String nextToken = request.param("next_token");
        final String unsupportedCat = CatTable.unsupported(request);
        request.param("format");
        request.paramAsBoolean("v", false);
        final String size = request.param("size");
        // _cat/indices hints: the columns are fixed here, there is no closed state or health to filter
        // on, and sizes are not reported. Consumed so a client sending them is not turned away.
        for (String hint : new String[] {
            "bytes",
            "time",
            "help",
            "health",
            "pri",
            "expand_wildcards",
            "local",
            "master_timeout",
            "cluster_manager_timeout",
            "sort" }) {
            request.param(hint);
        }

        if (unsupportedCat != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_cat_parameter", unsupportedCat)
            );
        }
        final boolean listing = request.path().startsWith("/_list/");
        if (listing == false && (size != null || nextToken != null)) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "_cat/indices is not paginated; _list/indices takes size and next_token"
                )
            );
        }
        final Paging paging;
        try {
            paging = listing ? Paging.of(index, size, nextToken) : null;
        } catch (IllegalArgumentException e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "illegal_argument_exception", e.getMessage())
            );
        }

        final MetadataPlane metadata = plane.get();
        final var serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                IndexAdminHandler.gate(
                    serving,
                    org.opensearch.action.admin.indices.get.GetIndexAction.NAME,
                    new org.opensearch.action.admin.indices.get.GetIndexRequest().indices(index),
                    () -> {
                        if (paging != null && paging.prefix() != null) {
                            page(channel, request, metadata, serving, paging);
                        } else {
                            answer(channel, request, metadata, serving, index, paging != null);
                        }
                        return null;
                    }
                );
            } catch (org.opensearch.serverless.metadata.DescriptorStore.TooManyMatchesException e) {
                // The refusal that makes this endpoint honest. An answer cut off at a limit looks exactly
                // like a complete one, so the caller is told to narrow the prefix instead.
                sendQuietly(channel, RestStatus.BAD_REQUEST, "too_many_indices", e.getMessage());
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a list failure", nested);
                }
            }
        });
    }

    private void answer(
        org.opensearch.rest.RestChannel channel,
        RestRequest request,
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        String index,
        boolean asPage
    ) throws Exception {
        final List<String> names = new java.util.ArrayList<>(IndexPatterns.expand(metadata, index, serving.patternCap()));
        names.removeIf(serving::isSystemIndex);
        final CatTable table = new CatTable("index", "uuid", "pri", "rep", "status", "mapping_version");
        for (String name : names) {
            final Optional<IndexDescriptor> descriptor = metadata.describe(name);
            if (descriptor.isEmpty()) {
                if (IndexPatterns.isPrefixPattern(index)) {
                    continue;
                }
                sendQuietly(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + name);
                return;
            }
            // "open" always: there is no closed state here, so reporting the column at all is reporting the
            // only value it can take rather than implying a state machine that does not exist.
            table.row(
                descriptor.get().name(),
                descriptor.get().uuid(),
                descriptor.get().numberOfShards(),
                0,
                "open",
                descriptor.get().mappingVersion()
            );
        }
        if (asPage) {
            // Named indices are one page that is the whole answer.
            table.sendPage(channel, request, "indices", null);
        } else {
            table.send(channel, request);
        }
    }

    /** One page of a prefix walk. */
    private void page(
        org.opensearch.rest.RestChannel channel,
        RestRequest request,
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        Paging paging
    ) throws Exception {
        final java.util.concurrent.Executor pool = serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC);
        final var page = metadata.descriptors()
            .listPage(paging.prefix(), paging.after(), paging.size(), new org.opensearch.serverless.metadata.DescriptorStore.Reads() {
                @Override
                public <T> List<T> runAll(List<java.util.concurrent.Callable<T>> tasks) throws InterruptedException {
                    return Fanout.run(pool, serving.searchFanoutConcurrency(), tasks);
                }
            });
        final CatTable table = new CatTable("index", "uuid", "pri", "rep", "status", "mapping_version");
        for (IndexDescriptor descriptor : page.descriptors().values()) {
            if (serving.isSystemIndex(descriptor.name())) {
                continue;
            }
            table.row(descriptor.name(), descriptor.uuid(), descriptor.numberOfShards(), 0, "open", descriptor.mappingVersion());
        }
        table.sendPage(channel, request, "indices", page.hasMore() ? Paging.token(paging.prefix(), page.nextAfter()) : null);
    }

    /**
     * What a {@code _list/indices} request asks for: a prefix walk when the expression is one prefix pattern,
     * otherwise named indices answered in one page.
     *
     * @param prefix the prefix walked, or null for named indices
     * @param after the name to resume after, or null for the first page
     * @param size rows per page
     */
    record Paging(String prefix, String after, int size) {

        static final int DEFAULT_SIZE = 500;
        static final int MAX_SIZE = 5000;

        static Paging of(String index, String size, String nextToken) {
            final int pageSize;
            try {
                pageSize = size == null ? DEFAULT_SIZE : Integer.parseInt(size);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("size must be a number, got [" + size + "]");
            }
            if (pageSize < 1 || pageSize > MAX_SIZE) {
                throw new IllegalArgumentException("size must be between 1 and [" + MAX_SIZE + "], got [" + pageSize + "]");
            }
            final String expression = index.trim();
            final boolean onePrefix = expression.indexOf(',') < 0 && IndexPatterns.isPrefixPattern(expression);
            if (onePrefix == false) {
                if (nextToken != null) {
                    throw new IllegalArgumentException("next_token resumes a walk over one prefix pattern, and [" + index + "] is not one");
                }
                return new Paging(null, null, pageSize);
            }
            final String prefix = expression.substring(0, expression.length() - 1);
            if (nextToken == null) {
                return new Paging(prefix, null, pageSize);
            }
            final String decoded;
            try {
                decoded = new String(java.util.Base64.getUrlDecoder().decode(nextToken), java.nio.charset.StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                throw tainted();
            }
            final int split = decoded.startsWith(TOKEN_VERSION) ? decoded.indexOf('\n', TOKEN_VERSION.length()) : -1;
            if (split < 0 || decoded.substring(TOKEN_VERSION.length(), split).equals(prefix) == false) {
                throw tainted();
            }
            return new Paging(prefix, decoded.substring(split + 1), pageSize);
        }

        private static final String TOKEN_VERSION = "1\n";

        static String token(String prefix, String after) {
            return java.util.Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString((TOKEN_VERSION + prefix + "\n" + after).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        private static IllegalArgumentException tainted() {
            return new IllegalArgumentException(
                "Parameter [next_token] has been tainted and is incorrect. Please provide a valid [next_token]."
            );
        }
    }

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a list refusal", e);
        }
    }
}
