/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import java.util.concurrent.atomic.AtomicLong;

/**
 * One node-wide tally of real object-store requests, broken down by request shape -- &sect;18 risk
 * #1's own mitigation, "publish request-count metrics from day one, and treat them as SLOs," which
 * until now had no runtime signal at all (only a build-time regression-test guard, {@code
 * CostAccountingRegressionTests}). {@link RequestCountingBlobContainer} is the wrapper that
 * actually drives these counters from every real {@link org.opensearch.common.blobstore.BlobContainer}
 * call this plugin's production code makes.
 *
 * <p>Deliberately request-shape granularity (get/put/delete/list), not per-index or per-shard: the
 * &sect;6.4 cost-sanity argument this exists to support ("WAL request cost stays two orders of
 * magnitude below the compute cost of the node producing it") is a node-level economics question,
 * the same altitude {@link org.opensearch.serverless.storage.writerengine.ObjectStoreWriterEngine#writesPerMinute()}
 * already operates at for its own signal.
 */
public final class ObjectStoreRequestCounter {

    private final AtomicLong getCount = new AtomicLong();
    private final AtomicLong putCount = new AtomicLong();
    private final AtomicLong deleteCount = new AtomicLong();
    private final AtomicLong listCount = new AtomicLong();

    /** Creates a counter starting at zero for every request shape. */
    public ObjectStoreRequestCounter() {}

    /** Records one GET-shaped request ({@code readBlob}/{@code readRegister}). */
    public void recordGet() {
        getCount.incrementAndGet();
    }

    /** Records one PUT-shaped request ({@code writeBlob}/{@code writeBlobAtomic}/{@code compareAndSwapRegister}). */
    public void recordPut() {
        putCount.incrementAndGet();
    }

    /** Records one DELETE-shaped request ({@code delete}/{@code deleteBlobsIgnoringIfNotExists}). */
    public void recordDelete() {
        deleteCount.incrementAndGet();
    }

    /** Records one LIST-shaped request ({@code listBlobs}/{@code listBlobsByPrefix}/{@code children}). */
    public void recordList() {
        listCount.incrementAndGet();
    }

    /** Total GET-shaped requests recorded so far on this node. */
    public long getCount() {
        return getCount.get();
    }

    /** Total PUT-shaped requests recorded so far on this node. */
    public long putCount() {
        return putCount.get();
    }

    /** Total DELETE-shaped requests recorded so far on this node. */
    public long deleteCount() {
        return deleteCount.get();
    }

    /** Total LIST-shaped requests recorded so far on this node. */
    public long listCount() {
        return listCount.get();
    }
}
