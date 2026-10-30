/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.store.WalStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class WalOrdinalOverflowTests extends OpenSearchTestCase {
    public void testExhaustedTermCannotFenceAtANegativeOrdinal() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var shard = BlobPath.cleanPath().add("shard");
            final var term = store.blobContainer(shard.add("wal").add("t=1"));
            final String last = String.format(Locale.ROOT, "%020d", Long.MAX_VALUE);
            term.writeBlob(last, new ByteArrayInputStream(new byte[0]), 0, true);
            store.reset();
            expectThrows(IOException.class, () -> new WalStore(store, shard).establish(1L, null));
            // ExtrasFS may inject unrelated files. The contract is no attempted write at all, not an
            // otherwise empty directory listing.
            assertEquals(0L, store.blobWrites());
            assertTrue(term.blobExists(last));
        }
    }

    public void testExhaustedWriterCannotAcknowledgeAnInvisibleAppend() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var shard = BlobPath.cleanPath().add("shard");
            final var term = store.blobContainer(shard.add("wal").add("t=1"));
            term.writeBlob(String.format(Locale.ROOT, "%020d", Long.MAX_VALUE - 1L), new ByteArrayInputStream(new byte[0]), 0, true);
            final var writer = new WalStore(store, shard);
            writer.establish(1L, null);
            final var records = List.of(new WalRecord("id", "{}", 0L, 1L, 1L));
            store.reset();
            expectThrows(IOException.class, () -> writer.append(1L, records));
            expectThrows(IOException.class, () -> writer.append(1L, records));
            assertEquals(0L, store.blobWrites());
            assertTrue(term.listBlobs().keySet().stream().noneMatch(name -> name.startsWith("-")));
            assertTrue(new WalStore(store, shard).replayable().isEmpty());
        }
    }
}
