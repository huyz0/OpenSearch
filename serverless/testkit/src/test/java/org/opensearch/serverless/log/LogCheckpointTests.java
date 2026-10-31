/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.UUID;

public class LogCheckpointTests extends OpenSearchTestCase {

    private ShardLogId identity() {
        return new ShardLogId(UUID.randomUUID(), "index-uuid", randomIntBetween(0, 100), UUID.randomUUID());
    }

    public void testRoundTripIncludesEmptyAndMaximumCoordinates() throws Exception {
        final var id = identity();
        for (var checkpoint : new LogCheckpoint[] {
            new LogCheckpoint(id, -1, 0),
            new LogCheckpoint(id, 0, 1),
            new LogCheckpoint(id, Long.MAX_VALUE, Long.MAX_VALUE) }) {
            try (var out = new BytesStreamOutput()) {
                checkpoint.writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    assertEquals(checkpoint, LogCheckpoint.readFrom(in));
                    assertEquals(-1, in.read());
                }
            }
        }
    }

    public void testDifferentActivationsAndIncarnationsCannotShareIdentity() {
        final var id = identity();
        assertNotEquals(id, new ShardLogId(id.deploymentId(), id.indexUuid(), id.shardNumber(), UUID.randomUUID()));
        assertNotEquals(id, new ShardLogId(id.deploymentId(), "recreated-index", id.shardNumber(), id.activationId()));
        assertNotEquals(id, new ShardLogId(UUID.randomUUID(), id.indexUuid(), id.shardNumber(), id.activationId()));
        assertNotEquals(id, new ShardLogId(id.deploymentId(), id.indexUuid(), id.shardNumber() + 1, id.activationId()));
        expectThrows(IllegalArgumentException.class, () -> new LogCheckpoint(id, -2, 0));
        expectThrows(IllegalArgumentException.class, () -> new LogCheckpoint(id, -1, 1));
        expectThrows(IllegalArgumentException.class, () -> new LogCheckpoint(id, 1, -1));
        expectThrows(IllegalArgumentException.class, () -> new ShardLogId(id.deploymentId(), "é".repeat(129), 0, id.activationId()));
        expectThrows(IllegalArgumentException.class, () -> new ShardLogId(id.deploymentId(), "\uD800", 0, id.activationId()));
    }

    public void testOversizedIdentityRejectedBeforeReadingPayload() throws Exception {
        try (var out = new BytesStreamOutput()) {
            out.writeByte((byte) 1);
            out.writeLong(0);
            out.writeLong(0);
            out.writeVInt(Integer.MAX_VALUE);
            try (var in = out.bytes().streamInput()) {
                final IOException failure = expectThrows(IOException.class, () -> ShardLogId.readFrom(in));
                assertTrue(failure.getMessage(), failure.getMessage().contains("length"));
            }
        }
    }

    public void testMalformedUtf8AndFutureVersionsAreRejected() throws Exception {
        try (var out = new BytesStreamOutput()) {
            out.writeByte((byte) 1);
            out.writeLong(0);
            out.writeLong(0);
            out.writeVInt(2);
            out.writeByte((byte) 0xc0);
            out.writeByte((byte) 0xaf);
            try (var in = out.bytes().streamInput()) {
                assertTrue(expectThrows(IOException.class, () -> ShardLogId.readFrom(in)).getMessage().contains("UTF-8"));
            }
        }
        try (var out = new BytesStreamOutput()) {
            out.writeByte((byte) 2);
            try (var in = out.bytes().streamInput()) {
                expectThrows(IOException.class, () -> LogCheckpoint.readFrom(in));
            }
        }
    }
}
