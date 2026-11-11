/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import java.io.IOException;

/**
 * The deployment's transport secrets: the current one, the one before it, and the register
 * generation they were read at.
 *
 * <p>Two secrets rather than one is what makes rotation possible without partitioning the fleet: a
 * receiver accepts a request authenticated under either until every node has re-read the register,
 * and the generation is what a node caches against so a refusal can trigger one re-read rather than
 * a read per request. Stored as JSON; a register written before generations existed holds a bare hex
 * string, which parses as generation 1 with no previous secret, so a running deployment upgrades in
 * place.
 *
 * @param generation the register generation, which rotation moves
 * @param current the secret new requests are authenticated with
 * @param previous the secret still accepted for one rotation window, or null
 * @param rotatedAtMillis when the current secret became current, in the plane's clock
 */
public record TransportSecrets(long generation, String current, String previous, long rotatedAtMillis) {

    /**
     * Parses a register value: the JSON form, or a bare hex secret from before rotation existed.
     *
     * @param stored the register's value
     * @return the secrets
     * @throws IOException if the JSON is malformed
     */
    public static TransportSecrets parse(String stored) throws IOException {
        final String trimmed = stored.trim();
        if (trimmed.isEmpty()) {
            throw new IOException("malformed transport-secret register: blank secret");
        }
        if (trimmed.startsWith("{") == false) {
            return new TransportSecrets(1L, trimmed, null, 0L);
        }
        final java.util.Map<String, Object> body;
        try (
            var parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    trimmed
                )
        ) {
            if (parser.nextToken() != org.opensearch.core.xcontent.XContentParser.Token.START_OBJECT) {
                throw new IOException("expected an object");
            }
            body = parser.map();
            if (parser.nextToken() != null) {
                throw new IOException("trailing content");
            }
        } catch (IOException | RuntimeException malformed) {
            // Parser messages and their causes can contain the register's secret bytes.
            throw new IOException("malformed transport-secret register: invalid JSON");
        }
        final Object current = body.get("current");
        final Object previous = body.get("previous");
        if (current instanceof String == false || ((String) current).isBlank()) {
            throw new IOException("malformed transport-secret register: current must be a nonblank string");
        }
        if (previous != null && (previous instanceof String == false || ((String) previous).isBlank())) {
            throw new IOException("malformed transport-secret register: previous must be a nonblank string");
        }
        return new TransportSecrets(
            body.get("generation") instanceof Number n ? n.longValue() : 1L,
            (String) current,
            (String) previous,
            body.get("rotated_at") instanceof Number n ? n.longValue() : 0L
        );
    }

    /**
     * Renders the register value.
     *
     * @return the JSON
     * @throws IOException if it cannot be written
     */
    public String toJson() throws IOException {
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("generation", generation);
            builder.field("current", current);
            builder.field("previous", previous);
            builder.field("rotated_at", rotatedAtMillis);
            builder.endObject();
            return org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString();
        }
    }

    /**
     * Reports whether a secret is one this record accepts: the current, or the previous.
     *
     * @param secret the secret presented
     * @return true if it is either
     */
    public boolean accepts(String secret) {
        return secret != null
            && (java.security.MessageDigest.isEqual(
                secret.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                current.getBytes(java.nio.charset.StandardCharsets.UTF_8)
            )
                || (previous != null
                    && java.security.MessageDigest.isEqual(
                        secret.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        previous.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                    )));
    }
}
