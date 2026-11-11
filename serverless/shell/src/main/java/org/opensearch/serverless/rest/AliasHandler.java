/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code /_alias/{name}} — naming a set of indices.
 *
 * <p>An alias is one register holding a list of index names, in the same namespace as the descriptors, so
 * resolving one costs a single read and a name cannot be both an index and an alias. See
 * {@link AliasRecord} for why that namespace is shared rather than separate.
 *
 * <p><b>There is no {@code GET /_alias} that lists them all</b>, and there will not be one for the same
 * reason there is no way to list indices: enumerating a deployment is not an operation this system offers
 * on a request path. An alias is looked up by name, like everything else here.
 *
 * <p><b>Replacing an alias wholesale is a delete and a create; changing its membership is a
 * compare-and-swap.</b> Every add or remove reads the record and the register generation it sits at in one
 * read, and writes back under that generation, so two callers changing one alias at the same time both land
 * or one is told to retry -- never one silently overwriting the other. The generation and the value it
 * protects come from the same read on purpose: read separately, a writer landing between the two hands
 * this handler a fresh token for a stale value, and its swap succeeds over the other caller's change.
 *
 * <p><b>A data stream is not an alias here, even though it is stored as one.</b> Every mutation on this
 * surface refuses a name that resolves to a data stream, in core's words, because rebuilding the record
 * as a plain alias would silently take the stream's generation and timestamp field with it.
 */
public final class AliasHandler extends BaseRestHandler {

    final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     */
    public AliasHandler(Supplier<MetadataPlane> plane) {
        this(plane, () -> null);
    }

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the node, for its action gate and system-index list
     */
    public AliasHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;
    /** Applies the alias changes this handler parsed. */
    final AliasUpdates updates = new AliasUpdates(this);

    private void executeAuthorized(
        org.opensearch.rest.RestChannel channel,
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedBiConsumer<org.opensearch.action.ActionRequest, org.opensearch.rest.RestChannel, Exception> work
    ) {
        final var serving = node.get();
        final Runnable execute = () -> {
            try {
                final org.opensearch.common.CheckedFunction<
                    org.opensearch.action.ActionRequest,
                    org.opensearch.rest.RestResponse,
                    Exception> capture = admitted -> {
                        final var pending = new java.util.concurrent.atomic.AtomicReference<org.opensearch.rest.RestResponse>();
                        final var buffered = new org.opensearch.rest.AbstractRestChannel(
                            channel.request(),
                            channel.detailedErrorsEnabled()
                        ) {
                            @Override
                            public void sendResponse(org.opensearch.rest.RestResponse response) {
                                if (pending.compareAndSet(null, response) == false) {
                                    throw new IllegalStateException("an alias operation produced more than one response");
                                }
                            }
                        };
                        work.accept(admitted, buffered);
                        if (pending.get() == null) {
                            throw new IllegalStateException("an alias operation did not produce a response");
                        }
                        return pending.get();
                    };
                final var response = serving == null
                    ? capture.apply(request)
                    : serving.services().actionGate().run(action, request, capture, org.opensearch.serverless.shell.ActionGate.opaque());
                if (response == null) {
                    throw new UnsupportedOperationException("a cached alias action response is not supported");
                }
                // A listener filter may still refuse after the work. Nothing reaches HTTP before it completes.
                channel.sendResponse(response);
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report an alias operation failure", nested);
                }
            }
        };
        if (serving == null) {
            execute.run();
        } else {
            serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(execute);
        }
    }

    private static org.opensearch.action.admin.indices.alias.get.GetAliasesRequest acceptedRead(
        org.opensearch.action.ActionRequest request
    ) {
        if (request instanceof org.opensearch.action.admin.indices.alias.get.GetAliasesRequest accepted) {
            return accepted;
        }
        throw new IllegalArgumentException("an alias read filter must provide GetAliasesRequest");
    }

    @Override
    public String getName() {
        return "serverless_alias_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_alias/{name}"),
            new Route(RestRequest.Method.POST, "/_alias/{name}"),
            new Route(RestRequest.Method.PUT, "/_aliases/{name}"),
            new Route(RestRequest.Method.POST, "/_aliases/{name}"),
            new Route(RestRequest.Method.GET, "/_alias/{name}"),
            new Route(RestRequest.Method.HEAD, "/_alias/{name}"),
            new Route(RestRequest.Method.DELETE, "/_alias/{name}"),
            // OpenSearch's own spellings. This shell shipped a name-scoped API of its own invention and
            // refused these, which was the last shape on this surface that differed from classic for no
            // architectural reason.
            new Route(RestRequest.Method.PUT, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.POST, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.GET, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.HEAD, "/{index}/_alias/{name}"),
            new Route(RestRequest.Method.DELETE, "/{index}/_alias/{name}"),
            // The _aliases spelling of the same thing, and the form that names the alias in its body.
            new Route(RestRequest.Method.PUT, "/{index}/_aliases/{name}"),
            new Route(RestRequest.Method.POST, "/{index}/_aliases/{name}"),
            new Route(RestRequest.Method.DELETE, "/{index}/_aliases/{name}"),
            new Route(RestRequest.Method.PUT, "/{index}/_alias"),
            new Route(RestRequest.Method.PUT, "/{index}/_aliases"),
            // GET /{index}/_alias: which aliases name an index, from the descriptor's verified hint.
            new Route(RestRequest.Method.GET, "/{index}/_alias"),
            // The bulk action API, for the one case it exists to serve. See updates.actions().
            new Route(RestRequest.Method.POST, "/_aliases")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String name = request.param("name");
        final String indexParam = request.param("index");
        final RestRequest.Method method = request.method();
        final boolean bulk = request.path().equals("/_aliases");
        // Hints: there is no cluster manager to time out against or be local to.
        for (String hint : new String[] {
            "timeout",
            "master_timeout",
            "cluster_manager_timeout",
            "local",
            "ignore_unavailable",
            "allow_no_indices",
            "expand_wildcards" }) {
            request.param(hint);
        }
        if (name == null && indexParam != null && method == RestRequest.Method.GET) {
            // The reverse question, answered from the hint each descriptor carries and verified against the
            // alias records it names -- bounded by the aliases on this index, not by the aliases that exist.
            final MetadataPlane reverse = plane.get();
            if (reverse == null) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
                );
            }
            return channel -> executeAuthorized(
                channel,
                org.opensearch.action.admin.indices.alias.get.GetAliasesAction.NAME,
                new org.opensearch.action.admin.indices.alias.get.GetAliasesRequest().indices(indexParam),
                (admitted, buffered) -> {
                    final var selected = acceptedRead(admitted);
                    if (selected.indices() == null || selected.indices().length != 1) {
                        throw new UnsupportedOperationException("a reverse alias lookup requires one concrete index");
                    }
                    AliasReads.aliasesOf(buffered, reverse, selected.indices()[0], selected.aliases());
                }
            );
        }
        if (name == null && bulk == false && request.hasContentOrSourceParam()) {
            // PUT /{index}/_alias with the alias named in the body, which core accepts as "alias" or "name".
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                final var body = parser.map();
                final Object named = body.get("alias") == null ? body.get("name") : body.get("alias");
                if (named instanceof String one && one.isBlank() == false) {
                    name = one;
                }
            }
        }
        if (name == null && bulk == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "missing_alias",
                    "name the alias, in the path or as 'alias' in the body"
                )
            );
        }
        final String aliasName = name;
        final List<String> indices = new ArrayList<>();
        if (indexParam != null) {
            // The index-scoped spellings name their index in the path rather than in a body.
            for (String each : indexParam.split(",")) {
                if (each.isBlank() == false) {
                    indices.add(each.trim());
                }
            }
        }
        // Alias options, refused on every spelling rather than dropped. A filter, a routing value or a
        // write-index flag used to be read into the body map and never looked at again, so a filtered
        // alias was created unfiltered with {"acknowledged": true} -- and the index-scoped spelling never
        // parsed its body at all.
        if ((method == RestRequest.Method.PUT || method == RestRequest.Method.POST) && bulk == false && request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                final String option = unsupportedAliasOption(parser.map());
                if (option != null) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_alias_option", option)
                    );
                }
            }
        }
        if ((method == RestRequest.Method.PUT || method == RestRequest.Method.POST) && indexParam == null && bulk == false) {
            if (request.hasContentOrSourceParam() == false) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "an alias needs a body naming its indices")
                );
            }
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                final var body = parser.map();
                if (body.get("indices") instanceof List<?> listed) {
                    for (Object index : listed) {
                        indices.add(String.valueOf(index));
                    }
                }
            }
            if (indices.isEmpty()) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.BAD_REQUEST,
                        "no_indices",
                        "an alias must name at least one index; an alias that stands for nothing would resolve to nothing"
                    )
                );
            }
        }

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        if (bulk) {
            final String body;
            try {
                body = request.hasContent() ? request.content().utf8ToString() : null;
            } catch (Exception e) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "malformed_body", "could not read the actions")
                );
            }
            return channel -> executeAuthorized(
                channel,
                org.opensearch.action.admin.indices.alias.IndicesAliasesAction.NAME,
                new org.opensearch.action.admin.indices.alias.IndicesAliasesRequest(),
                (admitted, buffered) -> updates.actions(buffered, metadata, body)
            );
        }

        final String gateAction = method == RestRequest.Method.GET || method == RestRequest.Method.HEAD
            ? org.opensearch.action.admin.indices.alias.get.GetAliasesAction.NAME
            : org.opensearch.action.admin.indices.alias.IndicesAliasesAction.NAME;
        final org.opensearch.action.ActionRequest gateRequest = method == RestRequest.Method.GET || method == RestRequest.Method.HEAD
            ? new org.opensearch.action.admin.indices.alias.get.GetAliasesRequest(aliasName).indices(indices.toArray(new String[0]))
            : new org.opensearch.action.admin.indices.alias.IndicesAliasesRequest();
        return channel -> executeAuthorized(channel, gateAction, gateRequest, (admitted, buffered) -> {
            if (method == RestRequest.Method.GET || method == RestRequest.Method.HEAD) {
                final var selected = acceptedRead(admitted);
                if (selected.aliases() == null || selected.aliases().length != 1) {
                    throw new UnsupportedOperationException("this alias lookup requires one named alias");
                }
                final List<String> selectedIndices = selected.indices() == null ? List.of() : java.util.Arrays.asList(selected.indices());
                updates.dispatch(
                    buffered,
                    metadata,
                    method,
                    selectedIndices.isEmpty() ? null : String.join(",", selectedIndices),
                    selected.aliases()[0],
                    selectedIndices
                );
            } else {
                updates.dispatch(buffered, metadata, method, indexParam, aliasName, indices);
            }
        });
    }

    /** Whether an index is a plugin's, which no alias may name: an alias over one is a read of it by another name. */
    boolean isSystemIndex(String index) {
        final var serving = node.get();
        return serving != null && serving.isSystemIndex(index);
    }

    /** One name or a list of them, from whichever of the two spellings the body used. */
    static List<String> namesOf(Object single, Object many) {
        final List<String> names = new ArrayList<>();
        if (single instanceof String one && one.isBlank() == false) {
            names.add(one);
        } else if (single instanceof List<?> list) {
            for (Object each : list) {
                names.add(String.valueOf(each));
            }
        }
        if (many instanceof List<?> list) {
            for (Object each : list) {
                names.add(String.valueOf(each));
            }
        } else if (many instanceof String one && one.isBlank() == false) {
            names.add(one);
        }
        return names;
    }

    /**
     * Names the first alias option a body carries that this design cannot honour, or null.
     *
     * <p>An alias here is a name for a set of indices and nothing else. A filter would make a search
     * through the alias narrower than a search of its indices, routing would place documents by a value
     * this system does not route on, and is_write_index would make one of several indices the target of a
     * write through the alias -- each a promise that would be accepted and not kept.
     */
    static String unsupportedAliasOption(java.util.Map<?, ?> body) {
        if (body.get("filter") != null) {
            return "filter is not supported: an alias here names indices and does not narrow them";
        }
        if (body.get("routing") != null || body.get("index_routing") != null || body.get("search_routing") != null) {
            return "routing on an alias is not supported: a document is placed by its id alone";
        }
        if (Boolean.TRUE.equals(body.get("is_write_index"))) {
            return "is_write_index is not supported: writes here name an index, and an alias resolves on read";
        }
        if (Boolean.TRUE.equals(body.get("is_hidden"))) {
            return "is_hidden is not supported: aliases are found by name and never listed, so there is nothing to hide from";
        }
        return null;
    }

}
