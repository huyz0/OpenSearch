/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class CompactionTriggerRequestTests extends OpenSearchTestCase {

    public void testValidateAcceptsAWellFormedRequest() {
        CompactionTriggerRequest request = new CompactionTriggerRequest("idx", 0);
        assertNull(request.validate());
    }

    public void testValidateRejectsAMissingIndexUuid() {
        CompactionTriggerRequest request = new CompactionTriggerRequest("", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("indexUuid")));
    }

    public void testValidateRejectsANullIndexUuid() {
        CompactionTriggerRequest request = new CompactionTriggerRequest(null, 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("indexUuid")));
    }

    public void testValidateRejectsANegativeShardId() {
        CompactionTriggerRequest request = new CompactionTriggerRequest("idx", -1);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("shardId")));
    }

    public void testValidateReportsEveryViolationTogether() {
        CompactionTriggerRequest request = new CompactionTriggerRequest(null, -1);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertEquals(2, validation.validationErrors().size());
    }

    public void testSerializationRoundTrip() throws Exception {
        CompactionTriggerRequest original = new CompactionTriggerRequest("idx", 3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        CompactionTriggerRequest deserialized = new CompactionTriggerRequest(out.bytes().streamInput());

        assertEquals(original.indexUuid(), deserialized.indexUuid());
        assertEquals(original.shardId(), deserialized.shardId());
    }

    /**
     * The indexUuid becomes a blob-path segment ({@code BlobPath.cleanPath().add(indexUuid)}) that
     * the underlying store resolves without normalising, so a {@code ..} or {@code /} in it used to
     * be a path the caller chose rather than a shard the caller owns. The authoritative control is
     * the transport action's cluster-metadata resolution; this boundary check is the cheap half
     * that refuses the obviously hostile shapes outright.
     */
    public void testValidateRejectsATraversalShapedIndexUuid() {
        ActionRequestValidationException e = new CompactionTriggerRequest("../../../etc", 0).validate();
        assertNotNull(e);
        assertTrue(e.getMessage(), e.getMessage().contains("indexUuid must contain only"));
    }

    /** Every character a real generated uuid can carry must still be accepted. */
    public void testValidateAcceptsTheFullRealIndexUuidAlphabet() {
        assertNull(new CompactionTriggerRequest("Ab0-_zZ9", 0).validate());
    }
}
