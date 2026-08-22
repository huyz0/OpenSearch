/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class NodeManifestLagRequestTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        NodeManifestLagRequest original = new NodeManifestLagRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        new NodeManifestLagRequest(out.bytes().streamInput());
    }

    public void testValidateAcceptsAnEmptyRequest() {
        assertNull(new NodeManifestLagRequest().validate());
    }
}
