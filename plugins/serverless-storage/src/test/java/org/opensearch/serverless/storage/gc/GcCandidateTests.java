/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class GcCandidateTests extends OpenSearchTestCase {

    public void testRoundTripsThroughAStream() throws Exception {
        GcCandidate candidate = new GcCandidate("idx", 3, 2, 7);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            candidate.writeTo(out);
            try (StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes, 0, out.bytes().length())) {
                assertEquals(candidate, new GcCandidate(in));
            }
        }
    }

    public void testEntryNameIsStableForTheSameIdentity() {
        GcCandidate a = new GcCandidate("idx", 3, 2, 7);
        GcCandidate b = new GcCandidate("idx", 3, 2, 7);
        assertEquals(
            "two appends of the same fact must land on the same entry name, or a retry duplicates the entry",
            a.entryName(),
            b.entryName()
        );
    }

    public void testEntryNameDiffersOnEveryField() {
        GcCandidate base = new GcCandidate("idx", 0, 1, 1);
        assertNotEquals(base.entryName(), new GcCandidate("other-idx", 0, 1, 1).entryName());
        assertNotEquals(base.entryName(), new GcCandidate("idx", 1, 1, 1).entryName());
        assertNotEquals(base.entryName(), new GcCandidate("idx", 0, 2, 1).entryName());
        assertNotEquals(base.entryName(), new GcCandidate("idx", 0, 1, 2).entryName());
    }

    public void testManifestIdCarriesTermAndGenerationOnly() {
        GcCandidate candidate = new GcCandidate("idx", 0, 4, 9);
        ManifestId id = candidate.manifestId();
        assertEquals(4, id.primaryTerm());
        assertEquals(9, id.generation());
    }

    public void testRejectsANullIndexUuid() {
        expectThrows(NullPointerException.class, () -> new GcCandidate(null, 0, 1, 1));
    }

    public void testRejectsANegativeShardId() {
        expectThrows(IllegalArgumentException.class, () -> new GcCandidate("idx", -1, 1, 1));
    }

    public void testRejectsAPrimaryTermBelowOne() {
        expectThrows(IllegalArgumentException.class, () -> new GcCandidate("idx", 0, 0, 1));
    }

    public void testRejectsANegativeGeneration() {
        expectThrows(IllegalArgumentException.class, () -> new GcCandidate("idx", 0, 1, -1));
    }
}
