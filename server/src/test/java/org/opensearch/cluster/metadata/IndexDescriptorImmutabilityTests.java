/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class IndexDescriptorImmutabilityTests extends OpenSearchTestCase {

    public void testCompactStreamSerializationRoundTrip() throws Exception {
        IndexDescriptor descriptor = new IndexDescriptor(
            "test-index",
            "uuid-1234",
            5,
            1,
            true,
            IndexDescriptor.State.OPEN,
            List.of("alias1"),
            100,
            false,
            false,
            false,
            false,
            2L,
            123456789L,
            5,
            1,
            0L,
            null
        );

        BytesStreamOutput out = new BytesStreamOutput();
        descriptor.writeCompact(out);

        StreamInput in = out.bytes().streamInput();
        IndexDescriptor read = IndexDescriptor.readCompact(in);

        assertEquals(descriptor.name(), read.name());
        assertEquals(descriptor.uuid(), read.uuid());
        assertEquals(descriptor.shardCount(), read.shardCount());
        assertEquals(descriptor.searchOnlyReplicaCount(), read.searchOnlyReplicaCount());
        assertEquals(descriptor.serverless(), read.serverless());
        assertEquals(descriptor.state(), read.state());
        assertEquals(descriptor.createdVersion(), read.createdVersion());
        assertEquals(descriptor.mappingGeneration(), read.mappingGeneration());
    }
}
