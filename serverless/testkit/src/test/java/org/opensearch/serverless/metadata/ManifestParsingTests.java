/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.PruningDigest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ManifestParsingTests extends OpenSearchTestCase {
    public void testMalformedManifestShapesCannotBecomeEmptyCommits() {
        for (String json : new String[] {
            "[{\"term\":1,\"files\":{}}]",
            "{\"term\":1}",
            "{\"term\":1,\"files\":[]}",
            "{\"term\":1,\"files\":false}",
            "{\"term\":1,\"files\":{\"_0.cfs\":{}}}",
            "{\"term\":1,\"files\":{},\"lengths\":[]}",
            "{\"term\":1,\"files\":{}} {}" }) {
            expectThrows(
                IOException.class,
                json,
                () -> CommitManifest.fromStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)))
            );
        }
    }

    public void testUnknownManifestStructuresPreserveKnownFiles() throws Exception {
        final String json =
            "{\"term\":7,\"future\":[{\"term\":999,\"files\":{}}],\"files\":{\"_0.cfs\":\"t=3\"},\"lengths\":{\"_0.cfs\":12}}";
        final var manifest = CommitManifest.fromStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        assertEquals(7L, manifest.term());
        assertEquals(Map.of("_0.cfs", "t=3"), manifest.files());
        assertEquals(Map.of("_0.cfs", 12L), manifest.lengths());
    }

    public void testDigestRequiresItsOpeningObjectAndRejectsIncompleteValues() throws Exception {
        for (String json : new String[] { "", "[]", "null", "1", "{", "{\"field\":{", "{\"field\":{\"type\":\"long\"" }) {
            try (
                var parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)
            ) {
                parser.nextToken();
                expectThrows(IOException.class, json, () -> PruningDigest.parse(parser));
            }
        }
    }
}
