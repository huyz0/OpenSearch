/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * Adapts a single, node-shared {@link InMemoryPlaintextBundleCache} into a plain {@link
 * BundleFileReader} for one shard's miss path: this is the object a shard's engine factory
 * actually wires up as its read path, so the shared cache's node-wide byte budget stays
 * decoupled from how many shards happen to be on the node -- see {@link
 * InMemoryPlaintextBundleCache}'s javadoc for why it can't just hold one fixed delegate itself.
 *
 * <p><b>Single-flight per key.</b> {@link InMemoryPlaintextBundleCache#readFile} has no
 * in-flight-request tracking of its own: N concurrent misses for the same key each call the miss
 * delegate. Today that is masked, because the delegate this plugin actually wires up is {@link
 * LocalDiskCachingBundleStore}, which dedupes via its own per-path lock -- but this class's
 * constructor accepts <em>any</em> {@link BundleFileReader}, and its own javadoc explicitly
 * permits wiring it straight over a {@link BlobContainerBundleStore}. In that configuration a cold
 * burst on one file turned into N object-store GETs. This class now holds a per-key in-flight
 * future so concurrent misses share exactly one fetch, which is the property callers already
 * assume of a cache and which no longer depends on which delegate happens to be underneath.
 */
public final class CachingBundleFileReader implements BundleFileReader {

    private final InMemoryPlaintextBundleCache sharedCache;
    private final BundleFileReader missDelegate;

    /**
     * In-flight fetches, keyed the same way the shared cache keys its entries. An entry lives only
     * for the duration of one fetch and is removed in a {@code whenComplete}, so this map is
     * bounded by concurrent misses, never by cache size.
     */
    private final ConcurrentMap<String, CompletableFuture<byte[]>> inFlight = new ConcurrentHashMap<>();

    /**
     * Wires a shard's miss path behind a node-shared cache.
     *
     * @param sharedCache the node-shared cache to consult first.
     * @param missDelegate the underlying reader to fall back to on a cache miss.
     */
    public CachingBundleFileReader(InMemoryPlaintextBundleCache sharedCache, BundleFileReader missDelegate) {
        this.sharedCache = sharedCache;
        this.missDelegate = missDelegate;
    }

    @Override
    public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
        String key = InMemoryPlaintextBundleCache.cacheKey(bundleName, entry);
        // The common case, and the reason this is not simply "always go through the future": a hit
        // must not pay a map write, a future allocation, and a map removal. Only a miss does.
        CompletableFuture<byte[]> mine = null;
        CompletableFuture<byte[]> existing = inFlight.get(key);
        if (existing == null) {
            mine = new CompletableFuture<>();
            existing = inFlight.putIfAbsent(key, mine);
            if (existing == null) {
                try {
                    byte[] fetched = sharedCache.readFile(bundleName, entry, missDelegate);
                    mine.complete(fetched);
                    return fetched;
                } catch (Throwable fetchFailure) {
                    // Every follower waiting on this key sees the same failure rather than hanging
                    // forever; each of them will then retry on its own next call, exactly as it
                    // would have if it had done its own fetch and failed.
                    mine.completeExceptionally(fetchFailure);
                    throw fetchFailure;
                } finally {
                    inFlight.remove(key, mine);
                }
            }
        }
        // A concurrent caller is already fetching this exact key -- wait for its result instead of
        // issuing a second, identical object-store read.
        try {
            byte[] shared = existing.get();
            // The leader handed its own array to the shared cache, which stores a copy; every
            // follower gets its own copy too, so no two callers ever share a mutable array.
            return shared.clone();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for a concurrent fetch of " + key, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IOException("concurrent fetch of " + key + " failed", cause);
        }
    }

    /**
     * Streams straight past the in-memory tier to the miss delegate. Deliberately never cached:
     * this method exists for files too large to hold in a {@code byte[]} at all (see {@link
     * BundleFileReader#openFile}), which is precisely the set of files this bounded, heap-resident
     * cache must never try to hold.
     *
     * @param bundleName the name of the bundle blob containing the file.
     * @param entry the file's location and length within that bundle.
     * @return a stream over exactly this file's bytes; the caller closes it.
     */
    @Override
    public InputStream openFile(String bundleName, BundleFileEntry entry) throws IOException {
        return missDelegate.openFile(bundleName, entry);
    }
}
