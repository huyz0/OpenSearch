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
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.serverless.log.OperationEnvelope;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable original JSON response for a result-only logical record. Its versioned shell codec is
 * independent of native transport serialization. This value proves no ownership, commitment or
 * absence of a mutation: the host must establish a non-mutating result, reserve rendering/encoded
 * bytes and append it under the same stable request identity before acknowledging it. It does not
 * classify transient admission/routing failures as durable outcomes or implement a frontend.
 */
@InternalApi
public final class ResolvedResultOnly {
    private static final int MAGIC = 0x52534f4e;
    private static final int FORMAT = 1;
    private static final int HEADER_BYTES = 12;
    private static final int NOOP = 1;
    private static final int REJECTED = 2;
    private final int kind;
    private final RestStatus status;
    private final byte[] json;

    private ResolvedResultOnly(int kind, RestStatus status, byte[] ownedJson) {
        this.kind = kind;
        this.status = status;
        json = ownedJson;
    }

    /**
     * Renders a native no-op response into bounded owned JSON, without native transport encoding.
     * The caller owns the response and its source data exclusively throughout rendering.
     * @param response complete original native no-op response
     * @param maximumBytes admitted complete result-only body, including this codec's header
     * @return immutable original result
     * @throws IOException on oversized output or invalid native response shape
     */
    public static ResolvedResultOnly noOp(UpdateResponse response, int maximumBytes) throws IOException {
        Objects.requireNonNull(response);
        checkLimit(maximumBytes);
        if (response.getResult() != DocWriteResponse.Result.NOOP || response.status() != RestStatus.OK) {
            throw new IllegalArgumentException("result-only encoding requires a native no-op");
        }
        final var output = new BoundedOutput(maximumBytes - HEADER_BYTES);
        try (var builder = new XContentBuilder(JsonXContent.jsonXContent, output)) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        } catch (RuntimeException failed) {
            // Jackson may wrap OutputStream IO limits in an unchecked rendering exception.
            throw new IOException("native result-only JSON rendering failed", failed);
        }
        final byte[] body = output.copy();
        validateJson(NOOP, RestStatus.OK, body, 0, body.length);
        return new ResolvedResultOnly(NOOP, RestStatus.OK, body);
    }

    /**
     * Copies an already budgeted original client failure response. The host must independently prove
     * that no document mutation was assigned and that this result should be durably retained.
     * @param status original supported HTTP error status
     * @param exactJson original JSON error object, owned exclusively during this call
     * @param maximumBytes admitted complete result-only body, including this codec's header
     * @return immutable error result
     * @throws IOException on oversized, malformed or inconsistent JSON
     */
    public static ResolvedResultOnly rejected(RestStatus status, byte[] exactJson, int maximumBytes) throws IOException {
        Objects.requireNonNull(status);
        Objects.requireNonNull(exactJson);
        checkLimit(maximumBytes);
        if (status.getStatus() < 400) {
            throw new IllegalArgumentException("result-only rejection requires an error status");
        }
        if (exactJson.length > maximumBytes - HEADER_BYTES) {
            throw new IOException("result-only response exceeds admitted body limit");
        }
        final byte[] owned = exactJson.clone();
        validateJson(REJECTED, status, owned, 0, owned.length);
        return new ResolvedResultOnly(REJECTED, status, owned);
    }

    /** Returns the original supported HTTP response status. */
    public RestStatus status() {
        return status;
    }

    /** Returns an independent copy of the original JSON response body. */
    public byte[] json() {
        return json.clone();
    }

    /**
     * Encodes after checking the complete size, before output allocation.
     * @param maximumBytes admitted complete body limit
     * @return independent versioned bytes
     * @throws IOException on excess size
     */
    public byte[] encode(int maximumBytes) throws IOException {
        checkLimit(maximumBytes);
        if (json.length > maximumBytes - HEADER_BYTES) {
            throw new IOException("result-only response exceeds admitted body limit");
        }
        return ByteBuffer.allocate(HEADER_BYTES + json.length)
            .putInt(MAGIC)
            .put((byte) FORMAT)
            .put((byte) kind)
            .putShort((short) status.getStatus())
            .putInt(json.length)
            .put(json)
            .array();
    }

    /**
     * Decodes a bounded verified logical payload; outer journal integrity and semantic acceptance
     * remain required. JSON is parsed without materializing a document/source map.
     * @param encoded complete independently verified payload
     * @param maximumBytes admitted complete body limit
     * @return immutable original result
     * @throws IOException on invalid version, lengths, status or response shape
     */
    public static ResolvedResultOnly decode(byte[] encoded, int maximumBytes) throws IOException {
        Objects.requireNonNull(encoded);
        checkLimit(maximumBytes);
        if (encoded.length < HEADER_BYTES || encoded.length > maximumBytes) {
            throw new IOException("invalid result-only body length");
        }
        final var input = ByteBuffer.wrap(encoded);
        if (input.getInt() != MAGIC || Byte.toUnsignedInt(input.get()) != FORMAT) {
            throw new IOException("unsupported result-only body version");
        }
        final int kind = Byte.toUnsignedInt(input.get());
        final RestStatus status = RestStatus.fromCode(Short.toUnsignedInt(input.getShort()));
        final int length = input.getInt();
        if ((kind != NOOP && kind != REJECTED)
            || status == null
            || length < 0
            || length != input.remaining()
            || (kind == NOOP ? status != RestStatus.OK : status.getStatus() < 400)) {
            throw new IOException("invalid result-only kind, status or response length");
        }
        final byte[] owned = Arrays.copyOfRange(encoded, HEADER_BYTES, encoded.length);
        validateJson(kind, status, owned, 0, owned.length);
        return new ResolvedResultOnly(kind, status, owned);
    }

    private static void checkLimit(int maximumBytes) {
        if (maximumBytes <= HEADER_BYTES || maximumBytes > OperationEnvelope.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("invalid result-only payload admission limit");
        }
    }

    private static void validateJson(int kind, RestStatus status, byte[] bytes, int offset, int length) throws IOException {
        try (
            var parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes,
                offset,
                length
            )
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IOException("result-only JSON must be an object");
            }
            boolean resultSeen = false;
            boolean errorSeen = false;
            boolean statusSeen = false;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                    throw new IOException("incomplete result-only JSON object");
                }
                final String name = parser.currentName();
                final var token = parser.nextToken();
                if (token == null) {
                    throw new IOException("incomplete result-only JSON value");
                }
                if ("result".equals(name)) {
                    if (resultSeen || kind != NOOP || token != XContentParser.Token.VALUE_STRING || "noop".equals(parser.text()) == false) {
                        throw new IOException("result-only JSON differs from native no-op result");
                    }
                    resultSeen = true;
                } else if ("error".equals(name)) {
                    if (errorSeen
                        || kind != REJECTED
                        || (token != XContentParser.Token.START_OBJECT && token != XContentParser.Token.VALUE_STRING)) {
                        throw new IOException("result-only JSON differs from error result");
                    }
                    errorSeen = true;
                } else if ("status".equals(name)) {
                    if (statusSeen
                        || token != XContentParser.Token.VALUE_NUMBER
                        || parser.numberType() != XContentParser.NumberType.INT
                        || parser.intValue() != status.getStatus()) {
                        throw new IOException("result-only JSON status differs from original response");
                    }
                    statusSeen = true;
                }
                parser.skipChildren();
            }
            if ((kind == NOOP ? resultSeen == false : errorSeen == false) || parser.nextToken() != null) {
                throw new IOException("missing result-only response or trailing JSON");
            }
        } catch (RuntimeException malformed) {
            throw new IOException("malformed result-only JSON", malformed);
        }
    }

    /** Output growth never exceeds admission, including JSON escaping and source rendering. */
    private static final class BoundedOutput extends OutputStream {
        private final int maximum;
        private byte[] buffer;
        private int size;

        private BoundedOutput(int maximum) {
            this.maximum = maximum;
            buffer = new byte[Math.min(4096, maximum)];
        }

        @Override
        public void write(int value) throws IOException {
            reserve(1);
            buffer[size++] = (byte) value;
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            reserve(length);
            System.arraycopy(bytes, offset, buffer, size, length);
            size += length;
        }

        private void reserve(int length) throws IOException {
            if (length > maximum - size) {
                throw new IOException("result-only response exceeds admitted rendering limit");
            }
            final int needed = size + length;
            if (needed > buffer.length) {
                buffer = Arrays.copyOf(buffer, (int) Math.min(maximum, Math.max(needed, 2L * buffer.length)));
            }
        }

        private byte[] copy() {
            return Arrays.copyOf(buffer, size);
        }
    }
}
