/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.routing;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * How a {@link UnitLayout} is written in an index descriptor, and read back.
 *
 * <p>Hashes are written as unsigned hex, so a layout read in an object store console shows its cut points as
 * the numbers they are rather than as negative longs. Unknown fields are skipped, so a newer node may add
 * some; a layout that does not cover the hash space or breaks the shape rule is refused as malformed.
 */
public final class UnitLayoutFormat {

    private UnitLayoutFormat() {}

    /**
     * Writes a layout as an object's fields. Hashes are written as unsigned hex, so a layout read in an
     * object store console shows its cut points as the numbers they are.
     *
     * @param layout the layout
     * @param builder a builder positioned inside an object
     * @throws IOException if writing fails
     */
    public static void toXContent(UnitLayout layout, XContentBuilder builder) throws IOException {
        builder.field("mode", layout.mode().settingValue());
        builder.field("declared_shards", layout.declaredShards());
        builder.field("next_unit_id", layout.nextUnitId());
        builder.startArray("units");
        for (UnitLayout.Unit unit : layout.units()) {
            builder.startObject();
            builder.field("id", unit.id());
            builder.field("first", Long.toHexString(unit.first()));
            builder.field("last", Long.toHexString(unit.last()));
            builder.endObject();
        }
        builder.endArray();
    }

    /**
     * Parses a layout written by {@link #toXContent}.
     *
     * @param parser a parser positioned on the layout object's start
     * @return the layout
     * @throws IOException if the layout is malformed
     */
    public static UnitLayout fromXContent(XContentParser parser) throws IOException {
        ShardMode mode = null;
        int declared = -1;
        int nextId = -1;
        final List<UnitLayout.Unit> units = new ArrayList<>();
        String field = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY && "units".equals(field)) {
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    units.add(parseUnit(parser));
                }
            } else if (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT) {
                parser.skipChildren();
            } else if (token.isValue()) {
                switch (field == null ? "" : field) {
                    case "mode" -> mode = ShardMode.parse(parser.text());
                    case "declared_shards" -> declared = parser.intValue();
                    case "next_unit_id" -> nextId = parser.intValue();
                    default -> {
                        // forward compatibility: a newer node may write fields we do not know
                    }
                }
            }
        }
        if (mode == null || declared < 1 || nextId < 1 || units.isEmpty()) {
            throw new IOException("malformed unit layout: missing a required field");
        }
        try {
            return new UnitLayout(mode, declared, units, nextId);
        } catch (IllegalArgumentException e) {
            throw new IOException("malformed unit layout: " + e.getMessage(), e);
        }
    }

    /**
     * Returns a layout as a JSON object, for carrying it where only a string fits.
     *
     * @param layout the layout
     * @return the JSON text
     * @throws IOException if writing fails
     */
    public static String toJson(UnitLayout layout) throws IOException {
        try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.startObject();
            toXContent(layout, builder);
            builder.endObject();
            return builder.toString();
        }
    }

    /**
     * Parses a layout written by {@link #toJson}.
     *
     * @param json the JSON text
     * @return the layout
     * @throws IOException if it is malformed
     */
    public static UnitLayout fromJson(String json) throws IOException {
        try (
            XContentParser parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    json
                )
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IOException("malformed unit layout: not an object");
            }
            return fromXContent(parser);
        }
    }

    private static UnitLayout.Unit parseUnit(XContentParser parser) throws IOException {
        int id = -1;
        String first = null;
        String last = null;
        String field = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT) {
                parser.skipChildren();
            } else if (token.isValue()) {
                switch (field == null ? "" : field) {
                    case "id" -> id = parser.intValue();
                    case "first" -> first = parser.text();
                    case "last" -> last = parser.text();
                    default -> {
                        // forward compatibility
                    }
                }
            }
        }
        if (id < 0 || first == null || last == null) {
            throw new IOException("malformed unit in layout: missing a required field");
        }
        try {
            return new UnitLayout.Unit(id, Long.parseUnsignedLong(first, 16), Long.parseUnsignedLong(last, 16));
        } catch (NumberFormatException e) {
            throw new IOException("malformed unit in layout: " + e.getMessage(), e);
        }
    }
}
