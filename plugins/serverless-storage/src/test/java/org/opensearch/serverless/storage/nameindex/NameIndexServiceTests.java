/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * A12, A13 and A14. The wire format, the checkpoint, and expression resolution.
 *
 * <p>The resolver is the part with real risk. The structure underneath was measurable and testable in
 * isolation; matching core's {@link IndicesOptions} semantics is neither, and getting it wrong is worse
 * than not having it, because a query that silently selects a different set of indices returns wrong
 * answers rather than errors.
 */
public class NameIndexServiceTests extends OpenSearchTestCase {

    // ------------------------------------------------------------ A13 wire

    public void testEntryRoundTripsOverTheWire() throws IOException {
        IndexNameEntry plain = new IndexNameEntry("plain", uuid(1), IndexNameEntry.STATUS_OPEN);
        IndexNameEntry alias = new IndexNameEntry("alias", uuid(2), IndexNameEntry.STATUS_ALIAS, List.of("a", "b"));
        IndexNameEntry closed = new IndexNameEntry("closed", uuid(3), IndexNameEntry.STATUS_CLOSED);

        for (IndexNameEntry original : List.of(plain, alias, closed)) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    assertEquals(original, new IndexNameEntry(in));
                }
            }
        }
    }

    // ---------------------------------------------------- A12 checkpoint

    public void testCheckpointRoundTripsTheWholeStructure() throws IOException {
        CompactNameIndex original = new CompactNameIndexBuilder().add("logs-a", uuid(1), IndexNameEntry.STATUS_OPEN)
            .add("logs-b", uuid(2), IndexNameEntry.STATUS_CLOSED)
            .addAlias("logs", uuid(3), List.of("logs-a", "logs-b"))
            .build();

        CompactNameIndex restored = roundTrip(original);

        assertEquals(original.size(), restored.size());
        for (int i = 0; i < original.size(); i++) {
            assertEquals(original.entryAt(i), restored.entryAt(i));
        }
        assertEquals(List.of("logs-a", "logs-b"), restored.aliasTargetsAt(restored.ordinalOf("logs")));
    }

    public void testCheckpointOfAnEmptyIndex() throws IOException {
        assertEquals(0, roundTrip(CompactNameIndex.empty()).size());
    }

    /**
     * A version a reader does not know must be refused rather than guessed at. A half-understood name
     * index is a cluster that cannot find its own indices.
     */
    public void testUnknownCheckpointVersionIsRejected() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(NameIndexCheckpoint.VERSION + 1);
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                IOException e = expectThrows(IOException.class, () -> NameIndexCheckpoint.read(in));
                assertTrue(e.getMessage(), e.getMessage().contains("unsupported name index checkpoint version"));
            }
        }
    }

    /**
     * The count is written up front so a truncated stream fails rather than loading as a smaller but
     * perfectly valid index, which is the failure where a cluster silently loses indices instead of
     * refusing to start.
     */
    public void testTruncatedCheckpointFailsRatherThanLoadingPartially() throws IOException {
        CompactNameIndex original = new CompactNameIndexBuilder().add("a", uuid(1), IndexNameEntry.STATUS_OPEN)
            .add("b", uuid(2), IndexNameEntry.STATUS_OPEN)
            .build();

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            NameIndexCheckpoint.write(original, out);
            byte[] full = org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes());
            byte[] truncated = new byte[full.length - 6];
            System.arraycopy(full, 0, truncated, 0, truncated.length);

            try (StreamInput in = StreamInput.wrap(truncated)) {
                expectThrows(Exception.class, () -> NameIndexCheckpoint.read(in));
            }
        }
    }

    // ------------------------------------------------------- A14 resolver

    public void testResolveConcreteNamesKeepsRequestOrder() {
        NameIndexResolver resolver = resolver("alpha", "beta", "gamma");

        assertEquals(List.of("gamma", "alpha"), resolver.resolve(IndicesOptions.strictExpandOpen(), "gamma", "alpha"));
    }

    public void testResolveWildcardReturnsByteOrder() {
        NameIndexResolver resolver = resolver("logs-c", "logs-a", "logs-b", "metrics");

        assertEquals(List.of("logs-a", "logs-b", "logs-c"), resolver.resolve(IndicesOptions.strictExpandOpen(), "logs-*"));
    }

    public void testNoExpressionMeansEverything() {
        NameIndexResolver resolver = resolver("a", "b");

        assertEquals(List.of("a", "b"), resolver.resolve(IndicesOptions.strictExpandOpen()));
    }

    /**
     * The distinction most easily got wrong: a wildcard matching nothing is governed by
     * allowNoIndices, while a concrete name that is missing is governed by ignoreUnavailable. They are
     * different flags for different situations and are frequently conflated.
     */
    public void testWildcardMatchingNothingIsGovernedByAllowNoIndices() {
        NameIndexResolver resolver = resolver("alpha");

        // strictExpandOpen() has allowNoIndices=true, so the flag has to be set explicitly rather than
        // assumed from the name of a preset -- "strict" refers to unavailable concrete names, not to
        // empty wildcards, which is the same conflation this test exists to pin down.
        IndicesOptions forbidEmptyWildcard = IndicesOptions.fromOptions(false, false, true, false);

        assertEquals(List.of(), resolver.resolve(IndicesOptions.lenientExpandOpen(), "nope-*"));
        assertEquals(List.of(), resolver.resolve(IndicesOptions.strictExpandOpen(), "nope-*"));
        expectThrows(IndexNotFoundException.class, () -> resolver.resolve(forbidEmptyWildcard, "nope-*"));
    }

    public void testMissingConcreteNameIsGovernedByIgnoreUnavailable() {
        NameIndexResolver resolver = resolver("alpha");

        assertEquals(List.of(), resolver.resolve(IndicesOptions.lenientExpandOpen(), "missing"));
        expectThrows(IndexNotFoundException.class, () -> resolver.resolve(IndicesOptions.strictExpandOpen(), "missing"));
    }

    /** Closed indices are a status rather than an absence precisely so the expansion flags can select them. */
    public void testClosedIndicesFollowTheExpansionFlags() {
        CompactNameIndex base = new CompactNameIndexBuilder().add("open-one", uuid(1), IndexNameEntry.STATUS_OPEN)
            .add("shut-one", uuid(2), IndexNameEntry.STATUS_CLOSED)
            .build();
        NameIndexResolver resolver = new NameIndexResolver(new NameIndex(base));

        assertEquals(List.of("open-one"), resolver.resolve(IndicesOptions.strictExpandOpen(), "*"));
        assertEquals(List.of("open-one", "shut-one"), resolver.resolve(IndicesOptions.strictExpand(), "*"));
    }

    public void testAliasExpandsToItsTargetsAndNotToItself() {
        CompactNameIndex base = new CompactNameIndexBuilder().add("logs-a", uuid(1), IndexNameEntry.STATUS_OPEN)
            .add("logs-b", uuid(2), IndexNameEntry.STATUS_OPEN)
            .addAlias("all-logs", uuid(3), List.of("logs-a", "logs-b"))
            .build();
        NameIndexResolver resolver = new NameIndexResolver(new NameIndex(base));

        assertEquals(List.of("logs-a", "logs-b"), resolver.resolve(IndicesOptions.strictExpandOpen(), "all-logs"));
        // Matched by a wildcard, the alias still contributes its targets rather than its own name.
        assertFalse(resolver.resolve(IndicesOptions.strictExpandOpen(), "*").contains("all-logs"));
    }

    /** An alias target that is closed is re-checked against the flags rather than trusted. */
    public void testAliasTargetsAreFilteredByStatus() {
        CompactNameIndex base = new CompactNameIndexBuilder().add("open-one", uuid(1), IndexNameEntry.STATUS_OPEN)
            .add("shut-one", uuid(2), IndexNameEntry.STATUS_CLOSED)
            .addAlias("both", uuid(3), List.of("open-one", "shut-one"))
            .build();
        NameIndexResolver resolver = new NameIndexResolver(new NameIndex(base));

        assertEquals(List.of("open-one"), resolver.resolve(IndicesOptions.strictExpandOpen(), "both"));
        assertEquals(List.of("open-one", "shut-one"), resolver.resolve(IndicesOptions.strictExpand(), "both"));
    }

    public void testDuplicateExpressionsCollapse() {
        NameIndexResolver resolver = resolver("alpha", "beta");

        assertEquals(List.of("alpha", "beta"), resolver.resolve(IndicesOptions.strictExpandOpen(), "alpha", "*", "beta"));
    }

    public void testResolutionSeesOverlayWritesImmediately() {
        NameIndex index = new NameIndex(new CompactNameIndexBuilder().add("alpha", uuid(1), IndexNameEntry.STATUS_OPEN).build());
        NameIndexResolver resolver = new NameIndexResolver(index);

        index.create("beta", uuid(2), IndexNameEntry.STATUS_OPEN);
        assertEquals(List.of("alpha", "beta"), resolver.resolve(IndicesOptions.strictExpandOpen(), "*"));

        index.delete("alpha");
        assertEquals(List.of("beta"), resolver.resolve(IndicesOptions.strictExpandOpen(), "*"));
    }

    // --------------------------------------------------------------- helpers

    private static CompactNameIndex roundTrip(CompactNameIndex index) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            NameIndexCheckpoint.write(index, out);
            try (StreamInput in = out.bytes().streamInput()) {
                return NameIndexCheckpoint.read(in);
            }
        }
    }

    private static NameIndexResolver resolver(String... names) {
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder();
        for (int i = 0; i < names.length; i++) {
            builder.add(names[i], uuid(i), IndexNameEntry.STATUS_OPEN);
        }
        return new NameIndexResolver(new NameIndex(builder.build()));
    }

    private static byte[] uuid(int seed) {
        byte[] uuid = new byte[CompactNameIndex.UUID_LENGTH];
        for (int i = 0; i < uuid.length; i++) {
            uuid[i] = (byte) (seed + i);
        }
        return uuid;
    }
}
