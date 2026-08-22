/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class SnapshotReleaseResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws Exception {
        SnapshotReleaseResponse original = new SnapshotReleaseResponse(true);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        SnapshotReleaseResponse deserialized = new SnapshotReleaseResponse(out.bytes().streamInput());

        assertTrue(deserialized.acknowledged());
    }
}
