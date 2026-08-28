/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;

/**
 * Runs the conformance suite against a local filesystem.
 *
 * <p><b>Passing here is not evidence about S3.</b> {@code FsBlobContainer} implements
 * compare-and-swap with real filesystem atomicity and ranged reads with a file channel, so it satisfies
 * these properties almost by construction. What this run establishes is that the suite itself works and
 * that the shell's own assumptions are stated correctly — which is what makes pointing it at a real
 * endpoint a single command rather than a project.
 *
 * <p>See {@code r11-conformance.md} for how to run it elsewhere.
 */
public class FsBlobContainerConformanceTests extends BlobContainerConformanceTestCase {

    @Override
    protected BlobContainer newContainer() throws Exception {
        final java.nio.file.Path dir = createTempDir();
        return new FsBlobStore(1024, dir, false).blobContainer(BlobPath.cleanPath());
    }
}
