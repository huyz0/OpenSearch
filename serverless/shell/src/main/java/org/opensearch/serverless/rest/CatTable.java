/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The two shapes a {@code _cat} endpoint answers in.
 *
 * <p><b>Why plain text and not only JSON.</b> {@code _cat} exists for a person at a terminal. Answering a
 * {@code curl /_cat/nodes} with JSON is not a lie, but it is not the answer the endpoint is for, and a
 * caller who wanted JSON has always been able to ask for it with {@code ?format=json}. Both are here so
 * that neither audience has to know this is a different implementation.
 *
 * <p><b>Column selection is refused rather than ignored.</b> Classic {@code _cat} takes {@code h=} to pick
 * columns and {@code s=} to sort. Accepting either and returning the default columns anyway would hand a
 * caller a table that is not the one they asked for, which on this surface is the failure that matters more
 * than the missing feature. So they are refused, by name, and the default column set is what there is.
 */
final class CatTable {

    private final List<String> headers;
    private final List<List<String>> rows = new ArrayList<>();

    CatTable(String... headers) {
        this.headers = List.of(headers);
    }

    void row(Object... cells) {
        final List<String> row = new ArrayList<>(cells.length);
        for (Object cell : cells) {
            row.add(cell == null ? "null" : String.valueOf(cell));
        }
        rows.add(row);
    }

    /**
     * Whether this request asks for something a {@code _cat} endpoint here cannot give.
     *
     * @param request the request
     * @return the refusal reason, or null when the request is answerable
     */
    static String unsupported(RestRequest request) {
        // Consumed, not merely inspected. BaseRestHandler rejects a request whose parameters were not all
        // read, and it does so after this handler has already decided to refuse -- so a refusal that only
        // looked at the parameter came back as "unrecognized parameter: [h]" instead of as itself.
        final String columns = request.param("h");
        final String sort = request.param("s");
        if (columns != null) {
            return "'h' selects columns, which this implementation does not support; it answers with a fixed "
                + "column set, and returning that while accepting 'h' would hand back a table that is not the "
                + "one asked for";
        }
        if (sort != null) {
            return "'s' sorts, which this implementation does not support; rows come back in a fixed order";
        }
        return null;
    }

    /**
     * Sends this table, as text or as JSON depending on {@code format}.
     *
     * @param channel the channel to answer on
     * @param request the request, read for {@code format} and {@code v}
     * @throws IOException if writing fails
     */
    void send(RestChannel channel, RestRequest request) throws IOException {
        // Any structured format the channel can render -- json, yaml, cbor, smile -- comes out of the
        // same builder, since the channel picks the content type from the format parameter itself. Only
        // text is drawn by hand.
        final String format = request.param("format");
        if (format != null && "text".equals(format) == false) {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startArray();
                for (List<String> row : rows) {
                    builder.startObject();
                    for (int i = 0; i < headers.size(); i++) {
                        builder.field(headers.get(i), row.get(i));
                    }
                    builder.endObject();
                }
                builder.endArray();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
            return;
        }

        // Widths from the content, so columns line up whatever is in them. Headers are off unless v=true,
        // which is classic's own default and the reason `_cat/nodes?v` is the form everyone types.
        final boolean withHeaders = request.paramAsBoolean("v", false);
        final int[] widths = new int[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
            widths[i] = withHeaders ? headers.get(i).length() : 0;
        }
        for (List<String> row : rows) {
            for (int i = 0; i < headers.size(); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }

        final StringBuilder text = new StringBuilder();
        if (withHeaders) {
            appendRow(text, headers, widths);
        }
        for (List<String> row : rows) {
            appendRow(text, row, widths);
        }
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, "text/plain; charset=UTF-8", text.toString()));
    }

    private static void appendRow(StringBuilder text, List<String> cells, int[] widths) {
        for (int i = 0; i < cells.size(); i++) {
            final String cell = cells.get(i);
            text.append(cell);
            if (i < cells.size() - 1) {
                text.append(" ".repeat(widths[i] - cell.length() + 1));
            }
        }
        text.append('\n');
    }
}
