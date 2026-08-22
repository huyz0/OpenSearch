/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ShardShrinkRequestTests extends OpenSearchTestCase {

    public void testValidateAcceptsAWellFormedRequest() {
        ShardShrinkRequest request = new ShardShrinkRequest(
            List.of(new ShardRef("source-a", 0), new ShardRef("source-b", 0)),
            "target-idx",
            0
        );
        assertNull(request.validate());
    }

    public void testValidateRejectsFewerThanTwoSources() {
        ShardShrinkRequest request = new ShardShrinkRequest(List.of(new ShardRef("source-a", 0)), "target-idx", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("sources")));
    }

    public void testValidateRejectsAnEmptySourceList() {
        ShardShrinkRequest request = new ShardShrinkRequest(List.of(), "target-idx", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("sources")));
    }

    public void testValidateRejectsAMissingTargetIndexUuid() {
        ShardShrinkRequest request = new ShardShrinkRequest(List.of(new ShardRef("source-a", 0), new ShardRef("source-b", 0)), "", 0);
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("target indexUuid")));
    }

    public void testValidateRejectsANegativeTargetShardId() {
        ShardShrinkRequest request = new ShardShrinkRequest(
            List.of(new ShardRef("source-a", 0), new ShardRef("source-b", 0)),
            "target-idx",
            -1
        );
        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertTrue(validation.validationErrors().stream().anyMatch(e -> e.contains("target shardId")));
    }

    public void testSerializationRoundTrip() throws Exception {
        ShardShrinkRequest original = new ShardShrinkRequest(
            List.of(new ShardRef("source-a", 0), new ShardRef("source-b", 1)),
            "target-idx",
            2
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardShrinkRequest deserialized = new ShardShrinkRequest(out.bytes().streamInput());

        assertEquals(original.sources(), deserialized.sources());
        assertEquals(original.targetIndexUuid(), deserialized.targetIndexUuid());
        assertEquals(original.targetShardId(), deserialized.targetShardId());
    }
}
