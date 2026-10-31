/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.serverless.store.WalRecord;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Persistence boundary for a shard's resolved operations. A successful append means the complete
 * batch is durable under the implementation's documented failure model. A failure may be ambiguous;
 * it does not prove that no record persisted. Ownership and incarnation validation belong to the caller.
 */
@InternalApi
@FunctionalInterface
public interface OperationAppender {

    /**
     * Appends an ordered batch, returning only after persistence. The caller retains ownership of the
     * list and must not mutate it until the call finishes. Implementations must not acknowledge a prefix
     * of the batch as though the entire batch succeeded. Empty batches do not create a durable record.
     *
     * @param primaryTerm operation primary term
     * @param records immutable resolved operations in order
     * @throws IOException when durability could not be established
     */
    void append(long primaryTerm, List<WalRecord> records) throws IOException;

    /**
     * Copies the batch and dispatches append to the supplied executor. Rejection completes the stage
     * exceptionally without appending. Cancelling a future derived from the returned stage does not
     * cancel accepted persistence work. An interrupted/timed-out caller must treat the outcome as
     * ambiguous. Supply an executor with the desired admission bounds and security-context propagation.
     *
     * @param primaryTerm operation primary term
     * @param records operations to copy before dispatch
     * @param executor persistence executor
     * @return a read-only completion stage for the whole batch
     */
    default CompletionStage<Void> appendAsync(long primaryTerm, List<WalRecord> records, Executor executor) {
        Objects.requireNonNull(executor);
        final List<WalRecord> batch = List.copyOf(records);
        final CompletableFuture<Void> completion = new CompletableFuture<>();
        try {
            executor.execute(() -> {
                try {
                    append(primaryTerm, batch);
                    completion.complete(null);
                } catch (IOException | RuntimeException e) {
                    completion.completeExceptionally(e);
                } catch (Error e) {
                    completion.completeExceptionally(e);
                    throw e;
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            completion.completeExceptionally(e);
        }
        return completion.minimalCompletionStage();
    }
}
