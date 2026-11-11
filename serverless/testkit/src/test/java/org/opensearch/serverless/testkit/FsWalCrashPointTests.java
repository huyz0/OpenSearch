/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;

/** The write-ahead log crash points on the filesystem store. */
public class FsWalCrashPointTests extends WalCrashPointTestCase {
    @Override
    protected BlobStore newStore() throws Exception {
        return new FsBlobStore(1024, createTempDir(), false);
    }
}
