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

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IndexDescriptorImmutabilityTests extends OpenSearchTestCase {

    private static IndexDescriptor descriptor() {
        return new IndexDescriptor(
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
            Map.of("field1", Map.of("type", "keyword"))
        );
    }

    /**
     * The one serialisation this type has, round-tripped whole.
     *
     * <p>This used to exercise {@code writeCompact}/{@code readCompact}, a second format nothing wrote a
     * blob with and whose reader rebuilt the descriptor with no aliases and no mapping. The test passed
     * because it asserted neither. The pair is gone; what the object store actually stores is
     * {@link IndexDescriptor#writeTo} behind {@code BlobDescriptorBackend}'s format magic, so that is what
     * is asserted, including the two fields the deleted reader dropped.
     */
    public void testStreamSerializationRoundTrip() throws Exception {
        IndexDescriptor descriptor = descriptor();

        BytesStreamOutput out = new BytesStreamOutput();
        descriptor.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        IndexDescriptor read = new IndexDescriptor(in);

        assertEquals(descriptor.name(), read.name());
        assertEquals(descriptor.uuid(), read.uuid());
        assertEquals(descriptor.shardCount(), read.shardCount());
        assertEquals(descriptor.searchOnlyReplicaCount(), read.searchOnlyReplicaCount());
        // The claimed boolean sits between these fields in the stream, so the state and createdVersion
        // asserts below would misalign and fail if it were dropped from the wire format.
        assertEquals(descriptor.state(), read.state());
        assertEquals(descriptor.createdVersion(), read.createdVersion());
        assertEquals(descriptor.mappingGeneration(), read.mappingGeneration());
        assertEquals("aliases are part of the record, and the deleted compact reader dropped them", List.of("alias1"), read.aliases());
        assertEquals("as is the declared mapping", descriptor.initialMapping(), read.initialMapping());
        assertEquals(descriptor, read);
    }

    /**
     * A corrupt state ordinal is a read failure rather than an unchecked array index.
     *
     * <p>These bytes come back from an object-store blob, so truncation and corruption decode through this
     * constructor. {@code State.values()[readVInt()]} turned that into an
     * {@code ArrayIndexOutOfBoundsException} thrown from a constructor, which nothing on the descriptor read
     * path catches -- and that path's entire contract is to tell "absent" from "could not be read".
     */
    public void testACorruptStateOrdinalIsReportedAsAReadFailure() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeString("test-index");
        out.writeString("uuid-1234");
        out.writeVInt(1);
        out.writeVInt(0);
        out.writeBoolean(true);
        // One past the last State, which is what a corrupt or half-written register decodes as.
        out.writeVInt(IndexDescriptor.State.values().length);

        StreamInput in = out.bytes().streamInput();
        IOException e = expectThrows(IOException.class, () -> new IndexDescriptor(in));
        assertTrue(e.getMessage(), e.getMessage().contains("ordinal"));
    }
}
