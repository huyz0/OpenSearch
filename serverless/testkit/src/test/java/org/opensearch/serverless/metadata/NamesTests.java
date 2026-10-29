/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class NamesTests extends OpenSearchTestCase {
    public void testMalformedUnicodeCannotBecomeAnObjectKey() {
        for (String name : List.of("\ud800", "\udc00", "ok\ud800end", "\udc00\ud800", "\ud800\ud800\udc00", "\ud800\udc00\udc00")) {
            expectThrows(IllegalArgumentException.class, () -> Names.validateIndexOrAlias(name));
            expectThrows(IllegalArgumentException.class, () -> Names.validateId(name, "pipeline"));
            expectThrows(IllegalArgumentException.class, () -> Names.validatePrefix(name));
        }
    }

    public void testWellFormedSupplementaryNamesRemainUsable() {
        for (String name : List.of("logs-\ud83d\ude00", "\ud83d\ude00logs", "a\ud83d\ude00\ud83d\ude01")) {
            Names.validateIndexOrAlias(name);
            Names.validateId(name, "pipeline");
            Names.validatePrefix(name);
        }
    }
}
