/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A {@link ClaimedIndexLifecycle} a test assembles one operation at a time.
 *
 * <p>The three write paths this covers used to be three independent static registrations, and a test armed
 * whichever of them it cared about by registering a lambda into each. They are one object now, which is the
 * point of the change but would otherwise force every such test to write out an anonymous class implementing
 * operations it has no opinion about. This keeps the call sites the shape they were: install one of these,
 * hand it the lambdas the test actually needs.
 *
 * <p><b>The lambdas still take {@link IndexDescriptor}, not {@link IndexMetadata}.</b> The SPI takes
 * metadata, because deriving the record from it is what stops a record and a cluster state entry disagreeing
 * about the same index -- but that derivation is {@link IndexDescriptor#from}, one line, and doing it here
 * keeps the existing tests asserting on the thing they were written to assert on.
 *
 * <p>Anything not armed keeps the interface default, which is what a node with no plugin answers.
 */
public final class TestClaimedIndexLifecycle implements ClaimedIndexLifecycle {

    private volatile Consumer<IndexDescriptor> recorder;
    private volatile Function<IndexDescriptor, CompletionStage<Boolean>> creator;
    private volatile BiFunction<String, UnaryOperator<IndexDescriptor>, CompletionStage<Boolean>> updater;

    /**
     * The installed one, installing a fresh one first if there is not already one there.
     *
     * <p>Idempotent on purpose. A test that arms two of the three operations arms them in two statements,
     * because that is how it read when they were two independent registrations; returning a new object from
     * the second call would silently discard the first.
     */
    public static TestClaimedIndexLifecycle install() {
        if (ClaimedIndexLifecycleRegistry.get() instanceof TestClaimedIndexLifecycle existing) {
            return existing;
        }
        TestClaimedIndexLifecycle lifecycle = new TestClaimedIndexLifecycle();
        ClaimedIndexLifecycleRegistry.register(lifecycle);
        return lifecycle;
    }

    /** Clears whatever is installed, which is how a test restores a stock node. */
    public static void uninstall() {
        ClaimedIndexLifecycleRegistry.register(null);
    }

    public TestClaimedIndexLifecycle recording(Consumer<IndexDescriptor> recorder) {
        this.recorder = recorder;
        return this;
    }

    public TestClaimedIndexLifecycle creating(Function<IndexDescriptor, CompletionStage<Boolean>> creator) {
        this.creator = creator;
        return this;
    }

    /** Arms creation with the answer a store gives when nothing contests the name. */
    public TestClaimedIndexLifecycle creatingSuccessfully() {
        return creating(descriptor -> CompletableFuture.completedFuture(Boolean.TRUE));
    }

    public TestClaimedIndexLifecycle updating(BiFunction<String, UnaryOperator<IndexDescriptor>, CompletionStage<Boolean>> updater) {
        this.updater = updater;
        return this;
    }

    @Override
    public void recordChange(IndexMetadata indexMetadata) {
        Consumer<IndexDescriptor> armed = recorder;
        if (armed == null) {
            return;
        }
        armed.accept(IndexDescriptor.from(indexMetadata));
    }

    @Override
    public CompletionStage<Boolean> createIndex(IndexMetadata indexMetadata) {
        Function<IndexDescriptor, CompletionStage<Boolean>> armed = creator;
        if (armed == null) {
            return ClaimedIndexLifecycle.super.createIndex(indexMetadata);
        }
        return armed.apply(IndexDescriptor.from(indexMetadata));
    }

    @Override
    public CompletionStage<Boolean> updateIndex(String indexName, UnaryOperator<IndexDescriptor> mutation) {
        BiFunction<String, UnaryOperator<IndexDescriptor>, CompletionStage<Boolean>> armed = updater;
        if (armed == null) {
            return ClaimedIndexLifecycle.super.updateIndex(indexName, mutation);
        }
        return armed.apply(indexName, mutation);
    }
}
