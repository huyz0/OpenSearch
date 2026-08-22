/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.serverless.storage.retention.PitrRetentionPolicy;
import org.opensearch.test.OpenSearchTestCase;

public class IndexSnapshotReleaseRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        IndexSnapshotReleaseRequest original = new IndexSnapshotReleaseRequest("my-index", "snap-1");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        IndexSnapshotReleaseRequest deserialized = new IndexSnapshotReleaseRequest(out.bytes().streamInput());

        assertEquals(original.indexName(), deserialized.indexName());
        assertEquals(original.snapshotId(), deserialized.snapshotId());
    }

    public void testValidateRejectsAMissingIndexName() {
        ActionRequestValidationException e = new IndexSnapshotReleaseRequest("", "snap-1").validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("indexName"));
    }

    public void testValidateRejectsAMissingSnapshotId() {
        ActionRequestValidationException e = new IndexSnapshotReleaseRequest("my-index", "").validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("snapshotId"));
    }

    public void testValidateAcceptsAWellFormedRequest() {
        assertNull(new IndexSnapshotReleaseRequest("my-index", "snap-1").validate());
    }

    /** See SnapshotReleaseRequestTests' own equivalent test for why this exact name is reserved. */
    public void testValidateRejectsTheReservedPitrSnapshotId() {
        ActionRequestValidationException e = new IndexSnapshotReleaseRequest("my-index", PitrRetentionPolicy.PITR_PIN_ID).validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("reserved"));
    }
}
