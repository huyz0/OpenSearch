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
import org.opensearch.test.OpenSearchTestCase;

public class IndexSnapshotRestoreRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        IndexSnapshotRestoreRequest original = new IndexSnapshotRestoreRequest("my-index", "snap-1");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        IndexSnapshotRestoreRequest deserialized = new IndexSnapshotRestoreRequest(out.bytes().streamInput());

        assertEquals(original.indexName(), deserialized.indexName());
        assertEquals(original.snapshotId(), deserialized.snapshotId());
    }

    public void testValidateRejectsAMissingIndexName() {
        ActionRequestValidationException e = new IndexSnapshotRestoreRequest("", "snap-1").validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("indexName"));
    }

    public void testValidateRejectsAMissingSnapshotId() {
        ActionRequestValidationException e = new IndexSnapshotRestoreRequest("my-index", "").validate();
        assertNotNull(e);
        assertTrue(e.getMessage().contains("snapshotId"));
    }

    public void testValidateAcceptsAWellFormedRequest() {
        assertNull(new IndexSnapshotRestoreRequest("my-index", "snap-1").validate());
    }
}
