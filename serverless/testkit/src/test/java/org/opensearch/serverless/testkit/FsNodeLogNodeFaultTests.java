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

import java.io.IOException;

/** The node-level write-log fault tests on a filesystem store. */
public class FsNodeLogNodeFaultTests extends NodeLogNodeFaultTestCase {

    @Override
    protected BlobStore newStore() throws IOException {
        return new FsBlobStore(8192, createTempDir(), false);
    }
}
