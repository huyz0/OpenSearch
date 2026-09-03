/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code GET|POST /{index}/_analyze} — what the analyzer does to a string, before a query depends on it.
 *
 * <p>Both compared serverless products ship it, and the reason a client wants it is diagnostic: a query that
 * matches nothing is usually an analysis disagreement, and this is the only way to see one. The refusal this
 * replaces claimed that "a node that does not hold a shard of this index cannot answer for it", which was
 * never true — analysis needs the mapping, and any node can read that.
 *
 * <p><b>It needs no shard.</b> The analyzer comes from a mapper service built from the descriptor, the same
 * way {@code _field_caps} gets field types. So an index whose shards are all dormant still answers, which is
 * the normal resting state here.
 *
 * <p><b>What is supported, and what is refused rather than approximated.</b> A named analyzer, a field's
 * analyzer, and the index default. Building an analyzer out of a tokenizer and a filter list on the fly is
 * <em>not</em> supported: it needs an analysis registry assembled per request, and answering with the default
 * analyzer while accepting the parameters would be the confident wrong answer this surface refuses — a caller
 * debugging an analysis problem would be shown the analysis of something else.
 */
public final class AnalyzeHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public AnalyzeHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_analyze_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/{index}/_analyze"), new Route(RestRequest.Method.POST, "/{index}/_analyze"));
    }

    /** What an analyze request asked for. */
    private record Ask(List<String> text, String analyzer, String field, String refusal) {
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        final Ask ask = read(request);

        if (ask.refusal() != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_parameter", ask.refusal())
            );
        }
        if (ask.text().isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_text", "give some text to analyze")
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
                answer(channel, metadata, serving, index, ask);
            } catch (IllegalArgumentException e) {
                try {
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "illegal_argument_exception",
                            e.getMessage() == null ? e.toString() : e.getMessage()
                        )
                    );
                } catch (IOException nested) {
                    logger.error("failed to report an analyze refusal", nested);
                }
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report an analyze failure", nested);
                }
            }
        });
    }

    private static Ask read(RestRequest request) throws IOException {
        final List<String> text = new ArrayList<>();
        String analyzer = request.param("analyzer");
        String field = request.param("field");
        // Read so the request is fully consumed whichever branch answers.
        final String tokenizer = request.param("tokenizer");
        final String filters = request.param("filter");
        final String explain = request.param("explain");
        if (request.param("text") != null) {
            text.add(request.param("text"));
        }

        if (request.hasContent()) {
            final Map<String, Object> body = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                request.content(),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
            final Object given = body.get("text");
            if (given instanceof List<?> many) {
                for (Object each : many) {
                    text.add(String.valueOf(each));
                }
            } else if (given != null) {
                text.add(String.valueOf(given));
            }
            if (body.get("analyzer") != null) {
                analyzer = String.valueOf(body.get("analyzer"));
            }
            if (body.get("field") != null) {
                field = String.valueOf(body.get("field"));
            }
            if (body.get("tokenizer") != null || body.get("filter") != null || body.get("char_filter") != null) {
                return new Ask(text, analyzer, field, custom());
            }
            if (body.get("explain") != null && Boolean.parseBoolean(String.valueOf(body.get("explain")))) {
                return new Ask(text, analyzer, field, explanation());
            }
        }

        if (tokenizer != null || filters != null) {
            return new Ask(text, analyzer, field, custom());
        }
        if (explain != null && Boolean.parseBoolean(explain)) {
            return new Ask(text, analyzer, field, explanation());
        }
        return new Ask(text, analyzer, field, null);
    }

    private static String custom() {
        return "building an analyzer from a tokenizer and filters per request is not supported: it needs an "
            + "analysis registry assembled for the request, and answering with the index's analyzer while "
            + "accepting these would show the analysis of something other than what was asked for. Name an "
            + "analyzer or a field instead";
    }

    private static String explanation() {
        return "explain reports each analysis step's intermediate tokens, which this implementation does not "
            + "collect; the token list it does return is the final one";
    }

    private void answer(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        String index,
        Ask ask
    ) throws Exception {
        final Optional<IndexDescriptor> descriptor = metadata.describe(index);
        if (descriptor.isEmpty()) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
            return;
        }

        final MapperService mapperService = serving.indicesService().createIndexMapperService(descriptor.get().toIndexMetadata(Map.of()));
        try {
            if (descriptor.get().mapping() != null) {
                // As in _field_caps: the mapper service is built empty, and a field's analyzer is only
                // knowable once the mapping is in it.
                mapperService.merge(
                    MapperService.SINGLE_MAPPING_NAME,
                    new org.opensearch.common.compress.CompressedXContent(descriptor.get().mapping()),
                    MapperService.MergeReason.MAPPING_RECOVERY
                );
            }

            final Analyzer chosen;
            final String using;
            if (ask.analyzer() != null) {
                final var named = mapperService.getIndexAnalyzers().get(ask.analyzer());
                if (named == null) {
                    throw new IllegalArgumentException("failed to find analyzer [" + ask.analyzer() + "]");
                }
                chosen = named;
                using = ask.analyzer();
            } else if (ask.field() != null) {
                if (mapperService.fieldType(ask.field()) == null) {
                    throw new IllegalArgumentException("failed to find field [" + ask.field() + "] in the mapping");
                }
                // The index-level analyzer delegates per field, so asking it for this field's tokens is
                // asking the same object indexing would have asked.
                chosen = mapperService.indexAnalyzer();
                using = ask.field();
            } else {
                chosen = mapperService.indexAnalyzer();
                using = "default";
            }

            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.startArray("tokens");
                int carried = 0;
                for (String each : ask.text()) {
                    carried = tokens(builder, chosen, ask.field() == null ? "text" : ask.field(), each, carried);
                }
                builder.endArray();
                // Additive, and useful precisely because this endpoint is used to settle disagreements: a
                // caller seeing tokens it did not expect wants to know which analyzer produced them.
                builder.field("analyzer_used", using);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        } finally {
            mapperService.close();
        }
    }

    /**
     * Runs one string through an analyzer and writes its tokens.
     *
     * @param builder the response being built
     * @param analyzer the analyzer to use
     * @param field the field name to analyze as
     * @param text the string
     * @param positionOffset the position the previous string ended at
     * @return the position the next string should start at
     * @throws IOException if analysis or writing fails
     */
    private static int tokens(XContentBuilder builder, Analyzer analyzer, String field, String text, int positionOffset)
        throws IOException {
        int position = positionOffset;
        try (TokenStream stream = analyzer.tokenStream(field, text)) {
            final CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            final OffsetAttribute offset = stream.addAttribute(OffsetAttribute.class);
            final TypeAttribute type = stream.addAttribute(TypeAttribute.class);
            final PositionIncrementAttribute increment = stream.addAttribute(PositionIncrementAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                position += increment.getPositionIncrement();
                builder.startObject();
                builder.field("token", term.toString());
                builder.field("start_offset", offset.startOffset());
                builder.field("end_offset", offset.endOffset());
                builder.field("type", type.type());
                // Positions are one sequence across several strings, which is what makes a phrase query
                // spanning them behave the way this says it will.
                builder.field("position", position - 1);
                builder.endObject();
            }
            stream.end();
        }
        return position;
    }
}
