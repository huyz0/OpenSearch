/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class CommitManifestTests extends OpenSearchTestCase {

    private CommitManifest randomManifest(long primaryTerm, long generation) {
        Map<String, FileReference> files = Map.of(
            "segments_3",
            new FileReference("bundle-1-1", 0, 100, 42L),
            "_0.si",
            new FileReference("bundle-1-1", 100, 200, 43L)
        );
        return new CommitManifest(
            "index-uuid-abc",
            randomIntBetween(0, 10),
            primaryTerm,
            generation,
            "segments_3",
            files,
            randomLongBetween(0, 1_000_000),
            randomLongBetween(0, 1_000_000),
            randomBoolean() ? new WalPosition("epoch-1", randomLongBetween(0, 1000)) : null,
            randomLongBetween(0, 100),
            new PruningStats(
                randomLongBetween(0, 1000),
                1_700_000_000_000L,
                1_700_000_100_000L,
                Map.of("price", new PruningStats.FieldRange(10, 500))
            ),
            System.currentTimeMillis()
        );
    }

    public void testSerializationRoundTrip() throws Exception {
        CommitManifest original = randomManifest(1, 5);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        CommitManifest deserialized = new CommitManifest(out.bytes().streamInput());

        assertEquals(original, deserialized);
        assertEquals(original.indexUuid(), deserialized.indexUuid());
        assertEquals(original.files(), deserialized.files());
        assertEquals(original.pruningStats(), deserialized.pruningStats());
        assertEquals(original.walPosition(), deserialized.walPosition());
    }

    public void testSerializationRoundTripWithNullWalPosition() throws Exception {
        CommitManifest original = new CommitManifest(
            "index-uuid",
            0,
            1,
            0,
            "segments_1",
            Map.of("segments_1", new FileReference("bundle-1-0", 0, 10, 1L)),
            -1,
            -1,
            null,
            0,
            PruningStats.empty(),
            0L
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        CommitManifest deserialized = new CommitManifest(out.bytes().streamInput());

        assertNull(deserialized.walPosition());
        assertEquals(original, deserialized);
    }

    public void testQuiescentDefaultsToFalse() {
        assertFalse(randomManifest(1, 5).quiescent());
    }

    public void testQuiescentFlagRoundTripsThroughSerialization() throws Exception {
        CommitManifest original = new CommitManifest(
            "idx",
            0,
            1,
            0,
            "segments_1",
            Map.of("segments_1", new FileReference("bundle-A", 0, 10, 1L)),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            0L,
            true
        );
        assertTrue(original.quiescent());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        CommitManifest deserialized = new CommitManifest(out.bytes().streamInput());

        assertTrue(deserialized.quiescent());
        assertEquals(original, deserialized);
    }

    public void testQuiescentParticipatesInEquality() {
        Map<String, FileReference> files = Map.of("segments_1", new FileReference("bundle-A", 0, 10, 1L));
        CommitManifest notQuiescent = new CommitManifest(
            "idx",
            0,
            1,
            0,
            "segments_1",
            files,
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            0L,
            false
        );
        CommitManifest quiescent = new CommitManifest("idx", 0, 1, 0, "segments_1", files, 0, 0, null, 0, PruningStats.empty(), 0L, true);

        assertNotEquals(notQuiescent, quiescent);
    }

    public void testManifestNameEncodesTermAndGeneration() {
        assertEquals("manifest-7-42", CommitManifest.manifestName(7, 42));
        assertEquals("manifest-7-42", randomManifest(7, 42).manifestName());
    }

    public void testIsNewerThanOrdersByTermFirstThenGeneration() {
        CommitManifest term1Gen5 = randomManifest(1, 5);
        CommitManifest term1Gen10 = randomManifest(1, 10);
        CommitManifest term2Gen0 = randomManifest(2, 0);

        assertTrue(term1Gen10.isNewerThan(term1Gen5));
        assertFalse(term1Gen5.isNewerThan(term1Gen10));

        // A higher generation under an older (stale) term must still lose to a newer term,
        // even at generation 0 -- this is the split-brain fencing property from
        // rfc-serverless-opensearch.md section 6.3.
        assertTrue(term2Gen0.isNewerThan(term1Gen10));
        assertFalse(term1Gen10.isNewerThan(term2Gen0));

        assertFalse(term1Gen5.isNewerThan(term1Gen5));
    }

    public void testReferencedBundlesDeduplicatesAcrossFiles() {
        Map<String, FileReference> files = Map.of(
            "segments_1",
            new FileReference("bundle-A", 0, 10, 1L),
            "_0.si",
            new FileReference("bundle-A", 10, 20, 2L),
            "_1.si",
            new FileReference("bundle-B", 0, 5, 3L)
        );
        CommitManifest manifest = new CommitManifest("idx", 0, 1, 0, "segments_1", files, 0, 0, null, 0, PruningStats.empty(), 0L);

        assertEquals(java.util.Set.of("bundle-A", "bundle-B"), manifest.referencedBundles());
    }

    public void testConstructorRejectsMissingSegmentsFileInFileMap() {
        Map<String, FileReference> files = Map.of("_0.si", new FileReference("bundle-A", 0, 10, 1L));
        expectThrows(
            IllegalArgumentException.class,
            () -> new CommitManifest("idx", 0, 1, 0, "segments_1", files, 0, 0, null, 0, PruningStats.empty(), 0L)
        );
    }

    public void testConstructorRejectsInvalidPrimaryTerm() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new CommitManifest(
                "idx",
                0,
                0, // primary terms are 1-based, like the rest of OpenSearch
                0,
                "segments_1",
                Map.of("segments_1", new FileReference("bundle-A", 0, 10, 1L)),
                0,
                0,
                null,
                0,
                PruningStats.empty(),
                0L
            )
        );
    }
}
