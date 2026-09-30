/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PointValues;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateUtils;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * What a published commit's point fields range over: per field, the smallest and largest value any document
 * holds. Written into the manifest at publish, so a coordinator can tell -- from one register read, without
 * opening the shard -- that a query cannot match anything in it.
 *
 * <p><b>Conservative is the whole contract.</b> {@link #canMatch} answers false only when no document in the
 * commit can match; anything it does not understand, cannot parse, or finds borderline is a "maybe", and the
 * shard is searched. A wrong "maybe" costs an activation; a wrong "no" is a silently missing hit, so every
 * rule here errs one way:
 * <ul>
 *   <li>Only range and term queries on a digested field are ever judged, and only a {@code bool}'s
 *   {@code must} and {@code filter} clauses (and a {@code constant_score}'s filter) carry a "no" upward: one
 *   clause that cannot match makes the conjunction unmatchable. {@code should} and {@code must_not} never
 *   prune.</li>
 *   <li>A range is pruned only when it lies strictly outside {@code [min, max]}: both ends are treated as
 *   inclusive whatever the query says, so an exclusive bound or a rounding difference can only keep a
 *   shard.</li>
 *   <li>A field with no entry -- no points in this commit, not a numeric or date type, indexed without
 *   points -- is a "maybe". A field without points can still match through doc values.</li>
 *   <li>Dates are parsed the way the field would parse them: the query's format or the mapping's, the
 *   query's time zone, and the request's single {@code now}. Any parse failure is a "maybe".</li>
 * </ul>
 *
 * <p><b>What the digest describes is the published commit</b>, which is exactly what a reader serves. A
 * writer can hold refreshed documents newer than it; the coordinator therefore prunes only shards with no
 * owner, whose published commit is the whole of what they hold.
 *
 * <p>No Lucene change is needed: the per-field bounds are the BKD tree's own root bounds
 * ({@link PointValues#getMinPackedValue}), which every segment keeps in its points metadata.
 */
public final class PruningDigest {

    /** A digest that describes nothing, so every query may match. */
    public static final PruningDigest EMPTY = new PruningDigest(Map.of());

    /**
     * One field's bounds.
     *
     * @param type the mapping type: long, integer, short, byte, double, float, date or date_nanos
     * @param min the smallest value, as stored: epoch millis for date, epoch nanos for date_nanos
     * @param max the largest value
     * @param format for a date, the mapping's format; null otherwise
     * @param locale for a date, the mapping's locale tag; null otherwise
     */
    public record FieldRange(String type, Number min, Number max, String format, String locale) {
        /** Bounds held as Long, or Double for a floating type, however they were read. */
        public FieldRange {
            final boolean floating = "double".equals(type) || "float".equals(type);
            min = floating ? Double.valueOf(min.doubleValue()) : Long.valueOf(min.longValue());
            max = floating ? Double.valueOf(max.doubleValue()) : Long.valueOf(max.longValue());
        }
    }

    private final Map<String, FieldRange> fields;

    /**
     * Creates a digest.
     *
     * @param fields field name to bounds
     */
    public PruningDigest(Map<String, FieldRange> fields) {
        this.fields = Map.copyOf(fields);
    }

    /**
     * Returns the digested fields.
     *
     * @return field name to bounds
     */
    public Map<String, FieldRange> fields() {
        return fields;
    }

    /**
     * Whether this digest describes nothing.
     *
     * @return true when no field was digested
     */
    public boolean isEmpty() {
        return fields.isEmpty();
    }

    /**
     * Computes the digest of a reader's point fields.
     *
     * @param reader the commit's reader
     * @param types the mapping, by full field name; fields it does not know are skipped
     * @return the digest
     * @throws IOException if the points cannot be read
     */
    public static PruningDigest compute(IndexReader reader, Function<String, MappedFieldType> types) throws IOException {
        final Map<String, FieldRange> fields = new LinkedHashMap<>();
        for (FieldInfo info : FieldInfos.getMergedFieldInfos(reader)) {
            if (info.getPointDimensionCount() != 1) {
                continue;
            }
            final MappedFieldType type = types.apply(info.getName());
            if (type == null) {
                continue;
            }
            final byte[] min = PointValues.getMinPackedValue(reader, info.getName());
            final byte[] max = PointValues.getMaxPackedValue(reader, info.getName());
            if (min == null || max == null) {
                continue;
            }
            final FieldRange range = decode(type, info.getPointNumBytes(), min, max);
            if (range != null) {
                fields.put(info.getName(), range);
            }
        }
        return new PruningDigest(fields);
    }

    private static FieldRange decode(MappedFieldType type, int bytes, byte[] min, byte[] max) {
        if (type instanceof DateFieldMapper.DateFieldType date && bytes == Long.BYTES) {
            final DateFormatter formatter = date.dateTimeFormatter();
            return new FieldRange(
                date.typeName(),
                LongPoint.decodeDimension(min, 0),
                LongPoint.decodeDimension(max, 0),
                formatter.pattern(),
                formatter.locale().toLanguageTag()
            );
        }
        if (type instanceof NumberFieldMapper.NumberFieldType number) {
            final String name = number.typeName();
            switch (name) {
                case "long":
                    return bytes == Long.BYTES
                        ? new FieldRange(name, LongPoint.decodeDimension(min, 0), LongPoint.decodeDimension(max, 0), null, null)
                        : null;
                case "integer":
                case "short":
                case "byte":
                    return bytes == Integer.BYTES
                        ? new FieldRange(name, IntPoint.decodeDimension(min, 0), IntPoint.decodeDimension(max, 0), null, null)
                        : null;
                case "double":
                    return bytes == Long.BYTES
                        ? new FieldRange(name, DoublePoint.decodeDimension(min, 0), DoublePoint.decodeDimension(max, 0), null, null)
                        : null;
                case "float":
                    return bytes == Integer.BYTES
                        ? new FieldRange(name, FloatPoint.decodeDimension(min, 0), FloatPoint.decodeDimension(max, 0), null, null)
                        : null;
                default:
                    // half_float, scaled_float and unsigned_long encode differently; not digested, so a maybe.
                    return null;
            }
        }
        return null;
    }

    /**
     * Whether a query has anything a digest could judge: a range, term or {@code match_none} reachable through
     * {@code bool} {@code must} and {@code filter} clauses and {@code constant_score}. A query with nothing of
     * the kind cannot be pruned by any digest, so there is no point reading one.
     *
     * @param query the query; null means match all
     * @return true when some digest could rule it out
     */
    public static boolean mayPrune(QueryBuilder query) {
        if (query == null) {
            return false;
        }
        if (query instanceof MatchNoneQueryBuilder || query instanceof RangeQueryBuilder || query instanceof TermQueryBuilder) {
            return true;
        }
        if (query instanceof ConstantScoreQueryBuilder constant) {
            return mayPrune(constant.innerQuery());
        }
        if (query instanceof BoolQueryBuilder bool) {
            for (QueryBuilder clause : bool.must()) {
                if (mayPrune(clause)) {
                    return true;
                }
            }
            for (QueryBuilder clause : bool.filter()) {
                if (mayPrune(clause)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether a query may match any document this digest describes.
     *
     * @param query the query; null means match all
     * @param nowMillis the request's single {@code now}, for date math
     * @return false only when no document can match
     */
    public boolean canMatch(QueryBuilder query, long nowMillis) {
        return cannotMatch(query, nowMillis) == false;
    }

    private boolean cannotMatch(QueryBuilder query, long nowMillis) {
        if (query == null) {
            return false;
        }
        if (query instanceof MatchNoneQueryBuilder) {
            return true;
        }
        if (query instanceof ConstantScoreQueryBuilder constant) {
            return cannotMatch(constant.innerQuery(), nowMillis);
        }
        if (query instanceof BoolQueryBuilder bool) {
            for (QueryBuilder clause : bool.must()) {
                if (cannotMatch(clause, nowMillis)) {
                    return true;
                }
            }
            for (QueryBuilder clause : bool.filter()) {
                if (cannotMatch(clause, nowMillis)) {
                    return true;
                }
            }
            return false;
        }
        if (query instanceof RangeQueryBuilder range) {
            final FieldRange digested = fields.get(range.fieldName());
            if (digested == null
                || range.relation() != null && "intersects".equalsIgnoreCase(range.relation().getRelationName()) == false) {
                return false;
            }
            final BigDecimal lower = bound(
                digested,
                range.from(),
                range.includeLower() == false,
                range.format(),
                range.timeZone(),
                nowMillis
            );
            final BigDecimal upper = bound(digested, range.to(), range.includeUpper(), range.format(), range.timeZone(), nowMillis);
            if ((range.from() != null && lower == null) || (range.to() != null && upper == null)) {
                return false;
            }
            return disjoint(digested, lower, upper);
        }
        if (query instanceof TermQueryBuilder term) {
            final FieldRange digested = fields.get(term.fieldName());
            if (digested == null || term.value() == null) {
                return false;
            }
            final BigDecimal lower = bound(digested, term.value(), false, null, null, nowMillis);
            final BigDecimal upper = bound(digested, term.value(), true, null, null, nowMillis);
            if (lower == null || upper == null) {
                return false;
            }
            return disjoint(digested, lower, upper);
        }
        return false;
    }

    /** Strictly outside {@code [min, max]}, with both of the query's ends taken as inclusive. */
    private static boolean disjoint(FieldRange digested, BigDecimal lower, BigDecimal upper) {
        final BigDecimal min = exact(digested.min());
        final BigDecimal max = exact(digested.max());
        if (min == null || max == null) {
            return false;
        }
        return (lower != null && max.compareTo(lower) < 0) || (upper != null && min.compareTo(upper) > 0);
    }

    /** A query bound in the digest's units, or null when it cannot be read with certainty. */
    private static BigDecimal bound(FieldRange digested, Object value, boolean roundUp, String format, String timeZone, long nowMillis) {
        if (value == null) {
            return null;
        }
        try {
            final String type = digested.type();
            if ("date".equals(type) || "date_nanos".equals(type)) {
                final String pattern = format != null ? format : digested.format();
                DateFormatter formatter = DateFormatter.forPattern(pattern);
                if (digested.locale() != null) {
                    formatter = formatter.withLocale(Locale.forLanguageTag(digested.locale()));
                }
                final ZoneId zone = timeZone == null ? ZoneOffset.UTC : ZoneId.of(timeZone);
                final Instant instant = formatter.toDateMathParser().parse(String.valueOf(value), () -> nowMillis, roundUp, zone);
                return "date".equals(type)
                    ? BigDecimal.valueOf(instant.toEpochMilli())
                    : BigDecimal.valueOf(DateUtils.toLong(DateUtils.clampToNanosRange(instant)));
            }
            if (value instanceof Number number) {
                return exact(number);
            }
            return new BigDecimal(String.valueOf(value).trim());
        } catch (RuntimeException e) {
            // A bound this cannot read is a bound it cannot judge.
            return null;
        }
    }

    private static BigDecimal exact(Number number) {
        if (number instanceof Double || number instanceof Float) {
            final double d = number.doubleValue();
            return Double.isFinite(d) ? new BigDecimal(d) : null;
        }
        if (number instanceof BigDecimal big) {
            return big;
        }
        try {
            return new BigDecimal(number.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Writes this digest as the value of the current field.
     *
     * @param builder the builder, positioned where the value goes
     * @throws IOException if writing fails
     */
    public void toXContent(XContentBuilder builder) throws IOException {
        builder.startObject();
        for (Map.Entry<String, FieldRange> field : fields.entrySet()) {
            final FieldRange range = field.getValue();
            builder.startObject(field.getKey());
            builder.field("type", range.type());
            builder.field("min", range.min());
            builder.field("max", range.max());
            if (range.format() != null) {
                builder.field("format", range.format());
            }
            if (range.locale() != null) {
                builder.field("locale", range.locale());
            }
            builder.endObject();
        }
        builder.endObject();
    }

    /**
     * Reads a digest written by {@link #toXContent}, the parser positioned on its opening brace. Anything it
     * does not recognise is skipped, so a later version can add to it.
     *
     * @param parser the parser
     * @return the digest
     * @throws IOException if the bytes are malformed
     */
    public static PruningDigest parse(XContentParser parser) throws IOException {
        final Map<String, FieldRange> fields = new LinkedHashMap<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                continue;
            }
            final String name = parser.currentName();
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                parser.skipChildren();
                continue;
            }
            String type = null;
            Number min = null;
            Number max = null;
            String format = null;
            String locale = null;
            String key = null;
            XContentParser.Token inner;
            while ((inner = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (inner == XContentParser.Token.FIELD_NAME) {
                    key = parser.currentName();
                } else if (inner == XContentParser.Token.START_OBJECT || inner == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                } else if ("type".equals(key)) {
                    type = parser.text();
                } else if ("min".equals(key)) {
                    min = parser.numberValue();
                } else if ("max".equals(key)) {
                    max = parser.numberValue();
                } else if ("format".equals(key)) {
                    format = parser.text();
                } else if ("locale".equals(key)) {
                    locale = parser.text();
                }
            }
            if (type != null && min != null && max != null) {
                fields.put(name, new FieldRange(type, min, max, format, locale));
            }
        }
        return new PruningDigest(fields);
    }

    @Override
    public String toString() {
        return "PruningDigest" + fields;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PruningDigest that && fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }
}
