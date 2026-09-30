/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.PruningDigest;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/** The digest's rules, one at a time: what it may rule out, and everything it must not. */
public class PruningDigestTests extends OpenSearchTestCase {

    private static final long DAY3 = Instant.parse("2026-09-03T00:00:00Z").toEpochMilli();
    private static final long DAY3_END = Instant.parse("2026-09-03T23:00:00Z").toEpochMilli();

    private static PruningDigest digest() {
        return new PruningDigest(
            Map.of(
                "@timestamp",
                new PruningDigest.FieldRange("date", DAY3, DAY3_END, "strict_date_optional_time||epoch_millis", "und"),
                "n",
                new PruningDigest.FieldRange("long", 10L, 20L, null, null),
                "score",
                new PruningDigest.FieldRange("double", 0.5d, 1.5d, null, null)
            )
        );
    }

    private static final long NOW = Instant.parse("2026-09-10T12:00:00Z").toEpochMilli();

    public void testARangeOutsideTheBoundsIsRuledOut() {
        final PruningDigest d = digest();
        assertFalse(d.canMatch(QueryBuilders.rangeQuery("n").gte(21), NOW));
        assertFalse(d.canMatch(QueryBuilders.rangeQuery("n").lte(9), NOW));
        assertFalse(d.canMatch(QueryBuilders.rangeQuery("@timestamp").gte("2026-09-04"), NOW));
        assertFalse(d.canMatch(QueryBuilders.rangeQuery("@timestamp").lt("2026-09-02T23:59:59Z"), NOW));
        assertFalse("date math against the request's now", d.canMatch(QueryBuilders.rangeQuery("@timestamp").gte("now-1d"), NOW));
        assertFalse(d.canMatch(QueryBuilders.rangeQuery("score").gt(1.6), NOW));
        assertFalse(d.canMatch(QueryBuilders.termQuery("n", 25), NOW));
    }

    public void testARangeThatTouchesTheBoundsIsKept() {
        final PruningDigest d = digest();
        assertTrue(d.canMatch(QueryBuilders.rangeQuery("n").gte(20), NOW));
        assertTrue(d.canMatch(QueryBuilders.rangeQuery("n").lte(10), NOW));
        // Exclusive bounds are taken as inclusive: a shard is kept at the boundary whatever the query says.
        assertTrue(d.canMatch(QueryBuilders.rangeQuery("n").gt(20), NOW));
        assertTrue(d.canMatch(QueryBuilders.rangeQuery("n").lt(10), NOW));
        assertTrue(d.canMatch(QueryBuilders.rangeQuery("@timestamp").gte("2026-09-03T23:00:00Z"), NOW));
        assertTrue(d.canMatch(QueryBuilders.rangeQuery("@timestamp").lt("2026-09-03"), NOW));
        assertTrue(d.canMatch(QueryBuilders.termQuery("n", 15), NOW));
        assertTrue(
            "a date rounded down to the day includes it",
            d.canMatch(QueryBuilders.rangeQuery("@timestamp").gte("2026-09-03||/d"), NOW)
        );
    }

    public void testWhatItCannotJudgeIsAMaybe() {
        final PruningDigest d = digest();
        assertTrue("an undigested field", d.canMatch(QueryBuilders.rangeQuery("other").gte(1000), NOW));
        assertTrue("an unparsable bound", d.canMatch(QueryBuilders.rangeQuery("@timestamp").gte("not a date"), NOW));
        assertTrue("a text query", d.canMatch(QueryBuilders.matchQuery("msg", "hello"), NOW));
        assertTrue("match all", d.canMatch(null, NOW));
        assertTrue(
            "a should clause never prunes",
            d.canMatch(QueryBuilders.boolQuery().should(QueryBuilders.rangeQuery("n").gte(99)), NOW)
        );
        assertTrue("nor must_not", d.canMatch(QueryBuilders.boolQuery().mustNot(QueryBuilders.rangeQuery("n").gte(0)), NOW));
        assertTrue("an empty digest rules nothing out", PruningDigest.EMPTY.canMatch(QueryBuilders.rangeQuery("n").gte(99), NOW));
    }

    public void testAConjunctionIsRuledOutByAnyOfItsClauses() {
        final PruningDigest d = digest();
        assertFalse(
            d.canMatch(
                QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("msg", "x")).filter(QueryBuilders.rangeQuery("n").gte(99)),
                NOW
            )
        );
        assertFalse(d.canMatch(QueryBuilders.constantScoreQuery(QueryBuilders.rangeQuery("n").lte(1)), NOW));
        assertFalse(d.canMatch(QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("n", 5))), NOW));
        assertTrue(d.canMatch(QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("n").gte(15)), NOW));
        assertFalse(d.canMatch(new org.opensearch.index.query.MatchNoneQueryBuilder(), NOW));
    }

    public void testMayPruneSeesOnlyWhatADigestCouldJudge() {
        assertFalse(PruningDigest.mayPrune(null));
        assertFalse(PruningDigest.mayPrune(QueryBuilders.matchQuery("msg", "x")));
        assertFalse(PruningDigest.mayPrune(QueryBuilders.boolQuery().should(QueryBuilders.rangeQuery("n").gte(1))));
        assertTrue(PruningDigest.mayPrune(QueryBuilders.boolQuery().filter(QueryBuilders.rangeQuery("n").gte(1))));
        assertTrue(PruningDigest.mayPrune(QueryBuilders.termQuery("n", 1)));
    }

    /** Round trip through the manifest, and a manifest a later version extended still parses. */
    public void testTheDigestRidesInTheManifestAndUnknownFieldsAreSkipped() throws Exception {
        final CommitManifest manifest = new CommitManifest(3L, Map.of("_0.cfs", "t=3"), "node-a", Map.of("_0.cfs", 42L), digest());
        final CommitManifest read = CommitManifest.fromStream(manifest.toBytes().streamInput());
        assertEquals(digest(), read.digest());
        assertEquals(Map.of("_0.cfs", "t=3"), read.files());

        final String extended = "{\"term\":4,\"files\":{\"a\":\"t=4\"},\"future\":{\"nested\":{\"term\":99}},\"lengths\":{\"a\":7},"
            + "\"digest\":{\"n\":{\"type\":\"long\",\"min\":1,\"max\":2,\"later\":{\"x\":1}},\"odd\":[1,2]},\"after\":[{\"term\":5}]}";
        final CommitManifest parsed = CommitManifest.fromStream(
            new java.io.ByteArrayInputStream(extended.getBytes(StandardCharsets.UTF_8))
        );
        assertEquals(4L, parsed.term());
        assertEquals(Map.of("a", "t=4"), parsed.files());
        assertEquals(Map.of("a", 7L), parsed.lengths());
        assertEquals(Map.of("n", new PruningDigest.FieldRange("long", 1, 2, null, null)), parsed.digest().fields());
    }

    /** What a node from before digests does with a manifest that has one: it reads everything it knows. */
    public void testAnOldParserStillReadsTermFilesAndLengths() throws Exception {
        final BytesReference bytes = new CommitManifest(3L, Map.of("_0.cfs", "t=3"), "node-a", Map.of("_0.cfs", 42L), digest()).toBytes();
        // The old loop, as it was: files and lengths consumed, anything else read token by token, so it stops
        // at the first END_OBJECT of an object it does not know.
        long term = -1;
        final Map<String, Long> lengths = new java.util.HashMap<>();
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, bytes.streamInput())
        ) {
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null && token != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && "files".equals(field)) {
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        // consumed, as the old parser consumed it
                    }
                } else if (token == XContentParser.Token.START_OBJECT && "lengths".equals(field)) {
                    String name = null;
                    XContentParser.Token inner;
                    while ((inner = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (inner == XContentParser.Token.FIELD_NAME) {
                            name = parser.currentName();
                        } else if (inner.isValue()) {
                            lengths.put(name, parser.longValue());
                        }
                    }
                } else if (token.isValue() && "term".equals(field)) {
                    term = parser.longValue();
                }
            }
        }
        assertEquals(3L, term);
        assertEquals(Map.of("_0.cfs", 42L), lengths);
    }
}
