/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.transport;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexingPressure;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ForwardedWritePressureTests extends OpenSearchTestCase {
    public void testUnicodeSourcesAreChargedInBytes() throws Exception {
        final String source = "{\"message\":\"" + "漢".repeat(600) + "\"}";
        final var request = new ForwardedBulkRequest("index", 0, List.of(new WalRecord("id", source)), false);
        assertEquals(2L + source.getBytes(StandardCharsets.UTF_8).length, request.bytes());
        assertRejectedBeforeAndAfterWire(request);
    }

    public void testDeleteOnlyBatchesAreChargedForTheirIds() throws Exception {
        final List<WalRecord> deletes = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            deletes.add(WalRecord.deletion(i + "x".repeat(40)));
        }
        assertRejectedBeforeAndAfterWire(new ForwardedBulkRequest("index", 0, deletes, false));
    }

    public void testSupplementaryCharactersAndSingleWriteSizing() {
        final String id = "文😀";
        final String source = "{\"message\":\"😀漢\"}";
        assertEquals(
            (long) id.getBytes(StandardCharsets.UTF_8).length + source.getBytes(StandardCharsets.UTF_8).length,
            WalRecord.contentBytes(id, source)
        );
    }

    private void assertRejectedBeforeAndAfterWire(ForwardedBulkRequest request) throws Exception {
        final var pressure = new IndexingPressure(Settings.builder().put("indexing_pressure.memory.limit", "1kb").build());
        expectThrows(OpenSearchRejectedExecutionException.class, () -> pressure.markPrimaryOperationStarted(request.bytes(), false));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                final var copy = new ForwardedBulkRequest(in);
                assertEquals(request.bytes(), copy.bytes());
                expectThrows(OpenSearchRejectedExecutionException.class, () -> pressure.markPrimaryOperationStarted(copy.bytes(), false));
            }
        }
    }
}
