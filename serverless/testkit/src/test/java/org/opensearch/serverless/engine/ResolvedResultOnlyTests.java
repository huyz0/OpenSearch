/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.get.GetResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ResolvedResultOnlyTests extends OpenSearchTestCase {
    public void testNativeNoopPreservesSourceAndIdentityWithoutAliasing() throws Exception {
        final var response = noop("id");
        final byte[] source = json("{\"count\":2,\"result\":\"updated\",\"error\":\"nested\"}");
        response.setGetResult(new GetResult("index", "id", 7, 2, 4, true, new BytesArray(source), Map.of(), Map.of()));
        final var value = ResolvedResultOnly.noOp(response, 4096);
        final byte[] expected = value.json();
        final byte[] encoded = value.encode(4096);
        final var recovered = ResolvedResultOnly.decode(encoded, 4096);
        source[0] ^= 1;
        encoded[encoded.length - 1] ^= 1;
        response.setForcedRefresh(true);
        final byte[] returned = recovered.json();
        returned[0] ^= 1;
        assertEquals(RestStatus.OK, recovered.status());
        assertArrayEquals(expected, recovered.json());
        final String text = new String(recovered.json(), StandardCharsets.UTF_8);
        assertTrue(text.contains("\"_seq_no\":7"));
        assertTrue(text.contains("\"_primary_term\":2"));
        assertTrue(text.contains("\"count\":2"));
        assertTrue(text.contains("\"result\":\"noop\""));
        assertFalse(text.contains("forced_refresh"));
    }

    public void testMissingUpsertNoopAndRealMutationRemainDifferent() throws Exception {
        final var missing = new UpdateResponse(new ShardId("index", "uuid", 0), "missing", -2, 0, -1, DocWriteResponse.Result.NOOP);
        final var value = ResolvedResultOnly.noOp(missing, 4096);
        assertFalse(new String(value.json(), StandardCharsets.UTF_8).contains("_seq_no"));
        assertArrayEquals(value.json(), ResolvedResultOnly.decode(value.encode(4096), 4096).json());
        final var updated = new UpdateResponse(new ShardId("index", "uuid", 0), "id", 8, 2, 5, DocWriteResponse.Result.UPDATED);
        expectThrows(IllegalArgumentException.class, () -> ResolvedResultOnly.noOp(updated, 4096));
        expectThrows(IllegalArgumentException.class, () -> ResolvedResultOnly.rejected(RestStatus.OK, json("{\"error\":\"bad\"}"), 4096));
    }

    public void testCompleteEncodedSizeIncludesHeaderAndJsonEscaping() throws Exception {
        final var response = noop("quote\"and\\slash".repeat(20));
        final var value = ResolvedResultOnly.noOp(response, 4096);
        final int exact = value.encode(4096).length;
        assertArrayEquals(value.encode(exact), ResolvedResultOnly.noOp(response, exact).encode(exact));
        expectThrows(IOException.class, () -> ResolvedResultOnly.noOp(response, exact - 1));
        expectThrows(IOException.class, () -> value.encode(exact - 1));
        final var large = noop("large");
        large.setGetResult(
            new GetResult(
                "index",
                "large",
                7,
                2,
                4,
                true,
                new BytesArray("{\"large\":\"" + "x".repeat(100_000) + "\"}"),
                Map.of(),
                Map.of()
            )
        );
        expectThrows(IOException.class, () -> ResolvedResultOnly.noOp(large, 4096));
        final byte[] error = json("{\"error\":\"bad\",\"status\":409}");
        final var rejected = ResolvedResultOnly.rejected(RestStatus.CONFLICT, error, 4096);
        final int rejectedSize = rejected.encode(4096).length;
        assertArrayEquals(
            rejected.encode(rejectedSize),
            ResolvedResultOnly.rejected(RestStatus.CONFLICT, error, rejectedSize).encode(rejectedSize)
        );
        expectThrows(IOException.class, () -> ResolvedResultOnly.rejected(RestStatus.CONFLICT, error, rejectedSize - 1));
        expectThrows(IllegalArgumentException.class, () -> value.encode(12));
        expectThrows(IllegalArgumentException.class, () -> value.encode(Integer.MAX_VALUE));
    }

    public void testOriginalClientErrorStatusAndBodyRemainImmutable() throws Exception {
        for (var status : List.of(
            RestStatus.BAD_REQUEST,
            RestStatus.NOT_FOUND,
            RestStatus.CONFLICT,
            RestStatus.TOO_MANY_REQUESTS,
            RestStatus.INTERNAL_SERVER_ERROR
        )) {
            final byte[] body = json("{\"error\":{\"reason\":\"snowman ☃\",\"status\":500,\"result\":\"noop\"}}");
            final byte[] expected = body.clone();
            final var value = ResolvedResultOnly.rejected(status, body, 4096);
            body[0] ^= 1;
            final var recovered = ResolvedResultOnly.decode(value.encode(4096), 4096);
            assertEquals(status, recovered.status());
            assertArrayEquals(expected, recovered.json());
        }
    }

    public void testMalformedStatusJsonAndDuplicateSemanticFieldsAreRefused() throws Exception {
        for (String invalid : List.of(
            "{}",
            "[]",
            "{\"error\":null}",
            "{\"error\":true}",
            "{\"error\":\"bad\",\"result\":\"noop\"}",
            "{\"error\":\"bad\",\"status\":500}",
            "{\"error\":\"bad\",\"status\":409.5}",
            "{\"error\":\"bad\",\"status\":\"409\"}",
            "{\"error\":\"bad\",\"status\":409,\"status\":409}",
            "{\"error\":\"one\",\"error\":\"two\"}",
            "{\"error\":\"bad\"} {}",
            "{\"error\":{",
            "{\"error\":\"bad\""
        )) {
            expectThrows(IOException.class, () -> ResolvedResultOnly.rejected(RestStatus.CONFLICT, json(invalid), 4096));
        }
        final byte[] invalidUtf8 = json("{\"error\":\"bad\"}");
        invalidUtf8[10] = (byte) 0xff;
        expectThrows(IOException.class, () -> ResolvedResultOnly.rejected(RestStatus.CONFLICT, invalidUtf8, 4096));
    }

    public void testMalformedCodecHeadersLengthsAndJsonCannotBeReplayed() throws Exception {
        final byte[] valid = ResolvedResultOnly.rejected(RestStatus.CONFLICT, json("{\"error\":\"bad\"}"), 4096).encode(4096);
        for (int at : new int[] { 0, 4, 5 }) {
            final byte[] invalid = valid.clone();
            invalid[at] = 99;
            expectThrows(IOException.class, () -> ResolvedResultOnly.decode(invalid, 4096));
        }
        for (int status : new int[] { 200, 999 }) {
            final byte[] invalid = valid.clone();
            ByteBuffer.wrap(invalid).putShort(6, (short) status);
            expectThrows(IOException.class, () -> ResolvedResultOnly.decode(invalid, 4096));
        }
        for (int length : new int[] { -1, 0, Integer.MAX_VALUE }) {
            final byte[] invalid = valid.clone();
            ByteBuffer.wrap(invalid).putInt(8, length);
            expectThrows(IOException.class, () -> ResolvedResultOnly.decode(invalid, 4096));
        }
        for (int length : new int[] { 0, 11, valid.length - 1, valid.length + 1 }) {
            final byte[] invalid = Arrays.copyOf(valid, length);
            expectThrows(IOException.class, () -> ResolvedResultOnly.decode(invalid, 4096));
        }
        final byte[] wrongShape = valid.clone();
        wrongShape[12] = '[';
        expectThrows(IOException.class, () -> ResolvedResultOnly.decode(wrongShape, 4096));
        final byte[] falseNoop = valid.clone();
        falseNoop[5] = 1;
        ByteBuffer.wrap(falseNoop).putShort(6, (short) 200);
        expectThrows(IOException.class, () -> ResolvedResultOnly.decode(falseNoop, 4096));
        expectThrows(IOException.class, () -> ResolvedResultOnly.decode(valid, valid.length - 1));
    }

    private static UpdateResponse noop(String id) {
        return new UpdateResponse(new ShardId("index", "uuid", 0), id, 7, 2, 4, DocWriteResponse.Result.NOOP);
    }

    private static byte[] json(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
