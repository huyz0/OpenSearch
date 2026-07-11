/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class WalPositionTests extends OpenSearchTestCase {

    public void testSerializationRoundTripWithAPositiveOffset() throws Exception {
        WalPosition original = new WalPosition("epoch-1", 42L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        WalPosition deserialized = new WalPosition(StreamInput.wrap(out.bytes().toBytesRef().bytes));

        assertEquals(original, deserialized);
    }

    public void testSerializationRoundTripWithANegativeOffset() throws Exception {
        // -1 is a real, reachable sentinel (WalMirroringTranslog#lastFlushedWalChunkSequence's own
        // "nothing flushed yet" value) -- not a defensive/hypothetical case. A prior version of this
        // class used writeVLong, which throws IllegalStateException on any negative value; caught by
        // ServerlessStorageWriterFailoverIT, not by a unit test, since no existing unit test ever
        // constructed a WalPosition with a negative offset.
        WalPosition original = new WalPosition("epoch-1", -1L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        WalPosition deserialized = new WalPosition(StreamInput.wrap(out.bytes().toBytesRef().bytes));

        assertEquals(original, deserialized);
        assertEquals(-1L, deserialized.offset());
    }
}
