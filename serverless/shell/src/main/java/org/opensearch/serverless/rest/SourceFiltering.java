/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Returning only the fields a caller asked for, on a document read by id.
 *
 * <p><b>What this does and does not save.</b> It shapes the response; it does not make the read cheaper.
 * The document was fetched whole — a get is answered by the shard's owner, and for a forwarded get the
 * whole source has already crossed the internal network by the time this runs. What the caller stops
 * paying for is the bytes back to them and the parsing at their end, which for a document with one large
 * field and two small ones is most of the cost they see. Saying so here rather than letting somebody infer
 * that {@code _source=name} makes a get cheap for the deployment.
 *
 * <p>A search is different: there, filtering happens inside the shard's fetch phase, before a hit is ever
 * returned, so it saves the transfer too. That asymmetry is real and is not something this can fix without
 * pushing the context through the forwarded-get request.
 *
 * <p><b>Core's own filter.</b> {@code XContentMapValues.filter} is what applies includes and excludes
 * everywhere else in OpenSearch, wildcards and dotted paths included. A second implementation would agree
 * with it until somebody used {@code obj.*.name}.
 */
public final class SourceFiltering {

    private SourceFiltering() {}

    /**
     * Applies a fetch-source context to a document's source.
     *
     * @param source the document's source, as stored
     * @param context what the caller asked for, or null for everything
     * @return the source to return, or null when the caller asked for none
     * @throws IOException if the source cannot be parsed or rewritten
     */
    public static String apply(String source, FetchSourceContext context) throws IOException {
        if (source == null || context == null) {
            return source;
        }
        if (context.fetchSource() == false) {
            // No source at all, which is a different answer from an empty one: the field is absent rather
            // than present and empty.
            return null;
        }
        if (context.includes().length == 0 && context.excludes().length == 0) {
            return source;
        }
        final Map<String, Object> parsed;
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8))
                )
        ) {
            parsed = parser.map();
        }
        final Map<String, Object> filtered = XContentMapValues.filter(parsed, context.includes(), context.excludes());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(filtered);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
