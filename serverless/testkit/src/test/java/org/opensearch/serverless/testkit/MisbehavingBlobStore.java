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
import org.opensearch.common.blobstore.BlobStore;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * A store that is wrong only where it is pointed.
 *
 * <p>The deviations in {@link MisbehavingBlobContainer} matter in different places for different reasons —
 * a store that admits two winners on the <em>shard-head</em> register puts two nodes on one shard, while
 * the same fault on a <em>manifest</em> register does something else entirely. Breaking everything at once
 * would produce a heap of failures and no finding, so this breaks one container and leaves the rest honest.
 *
 * <p>The container it returns for a matching path is remembered, because a caller that configures a
 * deviation and then asks for the container again must get the one it configured.
 */
public final class MisbehavingBlobStore implements BlobStore {

    private final BlobStore delegate;
    private final Predicate<BlobPath> breakThis;
    private final Map<String, MisbehavingBlobContainer> broken = new ConcurrentHashMap<>();

    /**
     * Wraps a store.
     *
     * @param delegate the honest store
     * @param breakThis which container paths misbehave
     */
    public MisbehavingBlobStore(BlobStore delegate, Predicate<BlobPath> breakThis) {
        this.delegate = delegate;
        this.breakThis = breakThis;
    }

    /**
     * Returns the misbehaving container for a path, so a test can configure its deviation.
     *
     * @param path the container path, which must be one this store breaks
     * @return the container
     */
    public MisbehavingBlobContainer broken(BlobPath path) {
        final BlobContainer container = blobContainer(path);
        if (container instanceof MisbehavingBlobContainer misbehaving) {
            return misbehaving;
        }
        throw new IllegalArgumentException(path + " is not a path this store breaks");
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        if (breakThis.test(path) == false) {
            return delegate.blobContainer(path);
        }
        return broken.computeIfAbsent(path.buildAsString(), ignored -> new MisbehavingBlobContainer(delegate.blobContainer(path)));
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
