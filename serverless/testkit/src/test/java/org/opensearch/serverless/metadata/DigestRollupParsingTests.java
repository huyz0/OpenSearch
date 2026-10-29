/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

public class DigestRollupParsingTests extends OpenSearchTestCase {
    public void testInvalidRootsAndEntriesAreRefused() {
        for (String json : new String[] {
            "",
            " ",
            "[]",
            "null",
            "1",
            "\"text\"",
            "{\"entries\":null}",
            "{\"entries\":[]}",
            "{\"entries\":{\"logs-a\":null}}",
            "{\"entries\":{\"logs-a\":[]}}" }) {
            expectThrows(IOException.class, () -> DigestRollups.parse(new BytesArray(json)));
        }
    }

    public void testUnknownNestedFieldsDoNotChangeKnownEntries() throws Exception {
        final String json = """
            {"future":{"entries":{"other":null}},"entries":{"logs-a":{
              "uuid":"uuid-a","shards":1,"future":[{"shards":999}],
              "states":[{"owner":7,"future":{"owner":999}}]
            }},"later":[{}]}
            """;
        assertEquals(
            Map.of("logs-a", new DigestRollups.Entry("uuid-a", 1, java.util.List.of(new DigestRollups.ShardState(7L, null)))),
            DigestRollups.parse(new BytesArray(json))
        );
    }
}
