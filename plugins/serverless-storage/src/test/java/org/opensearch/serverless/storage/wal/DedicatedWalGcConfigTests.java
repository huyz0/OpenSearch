/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

public class DedicatedWalGcConfigTests extends OpenSearchTestCase {

    private BlobContainer blobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testRejectsANonPositiveInterval() throws Exception {
        BlobContainer container = blobContainer();
        expectThrows(IllegalArgumentException.class, () -> new DedicatedWalGcConfig(container, container, TimeValue.ZERO));
        expectThrows(IllegalArgumentException.class, () -> new DedicatedWalGcConfig(container, container, TimeValue.MINUS_ONE));
    }

    public void testAcceptsAPositiveInterval() throws Exception {
        BlobContainer container = blobContainer();
        DedicatedWalGcConfig config = new DedicatedWalGcConfig(container, container, TimeValue.timeValueMinutes(1));
        assertSame(container, config.walContainer());
        assertSame(container, config.shardBlobContainer());
        assertEquals(TimeValue.timeValueMinutes(1), config.gcInterval());
    }
}
