/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.ops;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Bounds how long a search waits for this node to open a reader it does not hold.
 *
 * <p>A cold open reads a shard's segment files from the object store one request after another, so on a shard with
 * many segments it takes minutes. A search that was placed on this node used to wait the whole open before it
 * asked anyone else, though the shard's owner or another reader could answer at once. Found by the mixed-fleet
 * soak: after a node was killed, a Java node's {@code _search} sat in its own open for longer than the client
 * waited, never trying the Rust owner that was serving the shard.
 *
 * <p>The open is not cancelled when the wait ends. It keeps running on the executor and a later search joins it,
 * so the wait spent here is not repeated.
 */
public final class ColdOpenWait {

    private ColdOpenWait() {}

    /** The open did not finish within the wait; it is still running. */
    public static final class StillOpening extends RuntimeException {
        StillOpening(long waitMillis) {
            super("the local reader is still opening after " + waitMillis + " ms");
        }
    }

    /**
     * Runs an open and waits for it.
     *
     * @param executor where the open runs when it is bounded
     * @param waitMillis how long to wait; zero or less runs the open on the calling thread and waits for it
     * @param open the open
     * @param <T> what it returns
     * @return what the open returned
     * @throws StillOpening if the wait ended first
     * @throws Exception whatever the open threw
     */
    public static <T> T await(Executor executor, long waitMillis, Callable<T> open) throws Exception {
        if (waitMillis <= 0) {
            return open.call();
        }
        final CompletableFuture<T> done = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                done.complete(open.call());
            } catch (Throwable t) {
                done.completeExceptionally(t);
            }
        });
        try {
            return done.get(waitMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new StillOpening(waitMillis);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }
}
