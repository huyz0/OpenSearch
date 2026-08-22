/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class ShardRefTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        ShardRef original = new ShardRef("idx", 3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardRef deserialized = new ShardRef(out.bytes().streamInput());

        assertEquals(original, deserialized);
    }
}
