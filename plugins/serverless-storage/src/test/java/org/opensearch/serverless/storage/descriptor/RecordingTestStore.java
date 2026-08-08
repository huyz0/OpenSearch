/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.MappingGenerationStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * T56 and T53. Consolidated test decorator for {@link MappingGenerationStore.Store} that records invocations
 * alongside calling threads and target index UUIDs.
 */
public class RecordingTestStore implements MappingGenerationStore.Store {

    public record Invocation(String method, String indexUuid, String threadName) {}

    private final MappingGenerationStore.Store delegate;
    private final List<Invocation> invocations = new ArrayList<>();
    private final AtomicInteger readCount = new AtomicInteger();
    private final AtomicInteger casCount = new AtomicInteger();
    private final AtomicInteger deleteCount = new AtomicInteger();

    public RecordingTestStore(MappingGenerationStore.Store delegate) {
        this.delegate = delegate;
    }

    @Override
    public MappingGenerationStore.MappingGeneration read(String indexUuid) {
        readCount.incrementAndGet();
        synchronized (invocations) {
            invocations.add(new Invocation("read", indexUuid, Thread.currentThread().getName()));
        }
        return delegate == null ? null : delegate.read(indexUuid);
    }

    @Override
    public boolean compareAndSwap(
        String indexUuid,
        long expectedGeneration,
        MappingGenerationStore.MappingGeneration updated
    ) {
        casCount.incrementAndGet();
        synchronized (invocations) {
            invocations.add(new Invocation("compareAndSwap", indexUuid, Thread.currentThread().getName()));
        }
        return delegate != null && delegate.compareAndSwap(indexUuid, expectedGeneration, updated);
    }

    @Override
    public void delete(String indexUuid) {
        deleteCount.incrementAndGet();
        synchronized (invocations) {
            invocations.add(new Invocation("delete", indexUuid, Thread.currentThread().getName()));
        }
        if (delegate != null) {
            delegate.delete(indexUuid);
        }
    }

    public int readCount() {
        return readCount.get();
    }

    public int casCount() {
        return casCount.get();
    }

    public int deleteCount() {
        return deleteCount.get();
    }

    public List<Invocation> invocations() {
        synchronized (invocations) {
            return new ArrayList<>(invocations);
        }
    }
}
