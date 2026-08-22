/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ShardCloneRequestTests extends OpenSearchTestCase {

    public void testValidateAcceptsAWellFormedRequest() {
        ShardCloneRequest request = new ShardCloneRequest("source-idx", 0, "target-idx", 1);
        assertNull(request.validate());
    }

    public void testValidateRejectsAMissingSourceIndexUuid() {
        ShardCloneRequest request = new ShardCloneRequest("", 0, "target-idx", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("source indexUuid")));
    }

    public void testValidateRejectsANullSourceIndexUuid() {
        ShardCloneRequest request = new ShardCloneRequest(null, 0, "target-idx", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("source indexUuid")));
    }

    public void testValidateRejectsAMissingTargetIndexUuid() {
        ShardCloneRequest request = new ShardCloneRequest("source-idx", 0, "", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("target indexUuid")));
    }

    public void testValidateRejectsANegativeSourceShardId() {
        ShardCloneRequest request = new ShardCloneRequest("source-idx", -1, "target-idx", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("source shardId")));
    }

    public void testValidateRejectsANegativeTargetShardId() {
        ShardCloneRequest request = new ShardCloneRequest("source-idx", 0, "target-idx", -2);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("target shardId")));
    }

    public void testValidateRejectsSourceAndTargetNamingTheSameShard() {
        ShardCloneRequest request = new ShardCloneRequest("same-idx", 2, "same-idx", 2);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("must not name the same shard")));
    }

    public void testValidateAllowsSameIndexDifferentShardId() {
        ShardCloneRequest request = new ShardCloneRequest("same-idx", 0, "same-idx", 1);
        assertNull(request.validate());
    }

    public void testValidateReportsEveryViolationTogether() {
        ShardCloneRequest request = new ShardCloneRequest(null, -1, null, -1);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertEquals(4, validation.validationErrors().size());
    }

    public void testSerializationRoundTrip() throws Exception {
        ShardCloneRequest original = new ShardCloneRequest("source-idx", 3, "target-idx", 7);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardCloneRequest deserialized = new ShardCloneRequest(out.bytes().streamInput());

        assertEquals(original.sourceIndexUuid(), deserialized.sourceIndexUuid());
        assertEquals(original.sourceShardId(), deserialized.sourceShardId());
        assertEquals(original.targetIndexUuid(), deserialized.targetIndexUuid());
        assertEquals(original.targetShardId(), deserialized.targetShardId());
    }

    /**
     * The indexUuid becomes a blob-path segment ({@code BlobPath.cleanPath().add(indexUuid)}) that
     * the underlying store resolves without normalising, so a {@code ..} or {@code /} in it used to
     * be a path the caller chose rather than a shard the caller owns. The authoritative control is
     * the transport action's cluster-metadata resolution; this boundary check is the cheap half
     * that refuses the obviously hostile shapes outright.
     */
    public void testValidateRejectsATraversalShapedIndexUuidOnEitherSide() {
        ActionRequestValidationException source = new ShardCloneRequest("../../../etc", 0, "target-idx", 0).validate();
        assertNotNull(source);
        assertTrue(source.getMessage(), source.getMessage().contains("source indexUuid must contain only"));

        ActionRequestValidationException target = new ShardCloneRequest("source-idx", 0, "../../../etc", 0).validate();
        assertNotNull(target);
        assertTrue(target.getMessage(), target.getMessage().contains("target indexUuid must contain only"));
    }

    /** Every character a real generated uuid can carry must still be accepted. */
    public void testValidateAcceptsTheFullRealIndexUuidAlphabet() {
        assertNull(new ShardCloneRequest("Ab0-_zZ9", 0, "zZ9-_0bA", 0).validate());
    }
}
