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
 * Adapts a single, node-shared {@link InMemoryPlaintextBundleCache} into a plain {@link
 * BundleFileReader} for one shard's miss path: this is the object a shard's engine factory
 * actually wires up as its read path, so the shared cache's node-wide byte budget stays
 * decoupled from how many shards happen to be on the node -- see {@link
 * InMemoryPlaintextBundleCache}'s javadoc for why it can't just hold one fixed delegate itself.
 */
public final class CachingBundleFileReader implements BundleFileReader {

    private final InMemoryPlaintextBundleCache sharedCache;
    private final BundleFileReader missDelegate;

    public CachingBundleFileReader(InMemoryPlaintextBundleCache sharedCache, BundleFileReader missDelegate) {
        this.sharedCache = sharedCache;
        this.missDelegate = missDelegate;
    }

    @Override
    public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
        return sharedCache.readFile(bundleName, entry, missDelegate);
    }
}
