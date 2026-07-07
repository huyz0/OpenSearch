/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.io.IOException;

/**
 * The narrow read surface {@link org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer}
 * actually needs from a bundle store: fetch one file's checksum-verified bytes out of a named
 * bundle. Extracted as an interface (rather than depending on {@link BlobContainerBundleStore}
 * directly) so a caching layer -- {@link LocalDiskCachingBundleStore} -- can sit in front of the
 * real object-store-backed implementation without the materializer needing to know the
 * difference (rfc-serverless-opensearch.md &sect;9's "directory tier").
 */
public interface BundleFileReader {

    byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException;
}
