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

/** The log-fencing history on a filesystem store. */
public class FsLogFencingTests extends LogFencingTestCase {

    @Override
    protected BlobStore newStore() throws Exception {
        return new FsBlobStore(1024, createTempDir(), false);
    }
}
