/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class InPlaceMergeShardActionRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        InPlaceMergeShardAction.Request request = new InPlaceMergeShardAction.Request("my-index", 3);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InPlaceMergeShardAction.Request deserialized = new InPlaceMergeShardAction.Request(in);
        assertEquals("my-index", deserialized.index());
        assertEquals(3, deserialized.parentShardId());
    }

    public void testValidateAcceptsAWellFormedRequest() {
        assertNull(new InPlaceMergeShardAction.Request("idx", 0).validate());
    }

    public void testValidateRejectsAMissingIndex() {
        ActionRequestValidationException e = new InPlaceMergeShardAction.Request(null, 0).validate();
        assertNotNull(e);
        assertTrue(e.validationErrors().toString().contains("index is missing"));
    }

    public void testValidateRejectsANegativeParentShardId() {
        ActionRequestValidationException e = new InPlaceMergeShardAction.Request("idx", -1).validate();
        assertNotNull(e);
        assertTrue(e.validationErrors().toString().contains("parentShardId must be >= 0"));
    }
}
