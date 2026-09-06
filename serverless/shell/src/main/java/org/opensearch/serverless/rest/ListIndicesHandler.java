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
 * <p><b>Why a pattern is required.</b> {@code namesWithPrefix} resolves through one
 * {@code listBlobsByPrefixInSortedOrder} call with a cap — genuinely bounded, and it <em>refuses</em> past the
 * cap rather than truncating, which is the rule search wildcards already follow. The unscoped form has no such
 * bound. {@code DescriptorStore#listPage} looks like it provides one and does not: it lists every blob, sorts
 * in memory, and slices, so walking N indices costs N full listings. A cursor whose every page is a full scan
 * is a cursor in shape only, and offering it here would be selling that shape to a caller.
 *
 * <p><b>What a real cursor needs, stated so it is not mistaken for a design choice.</b> S3 supports
 * {@code start-after} natively. Core's {@code BlobContainer} exposes {@code listBlobs},
 * {@code listBlobsByPrefix} and {@code listBlobsByPrefixInSortedOrder} — none of them resumable. Until that
 * interface grows a cursor, {@code next_token} here is refused when given, and never rendered: the answer is
 * a table, and a table with no cursor row is one page that is the whole answer.
 *
 * <p><b>What is reported, and what is not.</b> {@code _list/indices} carries {@code docs.count} and
 * {@code store.size} in OpenSearch. Those are shard-level facts and this reads only descriptors, so they are
 * omitted rather than reported as zero — zero is a claim, and the true answer is that they were not measured.
 * {@code complete} says which, borrowing the field search responses already use.
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
        if (size != null) {
            // Refused for the reason next_token is. A page size is the other half of a paginated walk, and
            // this used to consume it and answer with everything -- so a caller asking for ten and getting
            // four hundred could not tell whether the parameter was honoured, ignored or misspelt.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_parameter",
                    "size is not supported: this listing is not paginated. A prefix narrow enough to fit under "
                        + "the pattern cap returns in one page, and one too broad is refused rather than truncated"
                )
            );
        }
        if (nextToken != null) {
            // Refused rather than ignored. A caller passing a token is resuming a walk, and answering from
            // the beginning while accepting the token would silently restart it -- which for a caller
            // paginating through a large deployment means never finishing and never being told why.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_parameter",
                    "next_token is not supported: resuming a listing needs a start-after cursor, which core's "
                        + "BlobContainer does not expose. A prefix narrow enough to fit under the pattern cap "
                        + "returns in one page, and one too broad is refused rather than truncated"
                )
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
                        answer(channel, request, metadata, serving, index);
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
        String index
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
        table.send(channel, request);
    }

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a list refusal", e);
        }
    }
}
