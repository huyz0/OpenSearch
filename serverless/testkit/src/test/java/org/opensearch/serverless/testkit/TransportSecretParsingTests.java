/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/** Corrupt control records must not turn into predictable authentication credentials. */
public class TransportSecretParsingTests extends OpenSearchTestCase {
    public void testBlankLegacySecretsAreRejected() {
        for (String body : List.of("", " \n\t")) {
            expectThrows(IOException.class, () -> MetadataPlane.TransportSecrets.parse(body));
        }
    }

    public void testStructuredSecretsRequireNonblankStrings() {
        for (String body : List.of(
            "{\"current\":\"\"}",
            "{\"current\":\"   \"}",
            "{\"current\":{}}",
            "{\"current\":[]}",
            "{\"current\":1}",
            "{\"current\":\"valid\",\"previous\":\"\"}",
            "{\"current\":\"valid\",\"previous\":{}}"
        )) {
            expectThrows(IOException.class, () -> MetadataPlane.TransportSecrets.parse(body));
        }
    }

    public void testSecretObjectsCannotIgnoreTrailingData() {
        expectThrows(IOException.class, () -> MetadataPlane.TransportSecrets.parse("{\"current\":\"first\"} {\"current\":\"second\"}"));
    }

    public void testMalformedJsonDoesNotEchoSecretTextInErrors() {
        final var failure = expectThrows(
            IOException.class,
            () -> MetadataPlane.TransportSecrets.parse("{\"current\": accidental_secret_text}")
        );
        assertFalse(failure.toString(), failure.toString().contains("accidental_secret_text"));
        assertNull("parser causes may echo sensitive input", failure.getCause());
    }

    public void testLegacySecretsAndUnknownFieldsRemainReadable() throws Exception {
        assertEquals("legacy-key", MetadataPlane.TransportSecrets.parse(" legacy-key ").current());
        final var parsed = MetadataPlane.TransportSecrets.parse(
            "{\"current\":\"first\",\"previous\":\"before\",\"generation\":2,\"rotated_at\":1000,\"future\":{\"nested\":[1,2]}}"
        );
        assertEquals("first", parsed.current());
        assertEquals("before", parsed.previous());
        assertEquals(2L, parsed.generation());
        assertEquals(1_000L, parsed.rotatedAtMillis());
    }
}
