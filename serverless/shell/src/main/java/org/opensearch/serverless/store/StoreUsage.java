/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.blobstore.BlobPath;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Which index each object-store request on this node was made for, kept per index so a tenant's share of the bill can
 * be told apart from its neighbours'.
 *
 * <p><b>Who a request is charged to</b>, most specific first:
 * <ol>
 *   <li>The index a container was handed out for: a shard's read of a file that lives in a node pack, a blob outside
 *       the shard's own path ({@link MeteredBlobStore#chargedTo}).
 *   <li>The index the key names: anything under a shard's data path, an index descriptor, a shard head, an
 *       assignment. Keys are derived from index names, so most requests need nothing more.
 *   <li>The indices the calling thread is working for ({@link #split}): an upload that carries many indices at once --
 *       a node-log blob, a node pack, a bucket manifest -- is shared between them by weight, usually their bytes in it.
 *   <li>Otherwise the node itself ({@link #NODE}): leases, membership, cluster settings, the janitor's listings.
 * </ol>
 *
 * <p>Every request lands on exactly one of these, so the per-index counters plus the node's add up to what the store
 * counted. That is checked by a test, which is how a path that forgets to say who it works for shows up.
 *
 * <p><b>Bounded.</b> A node that touches more than {@code maxIndices} indices -- a janitor pass over every shard of the
 * cluster can -- charges the rest to {@link #OTHER} rather than growing without limit.
 */
public final class StoreUsage {

    /** The line for requests no index made. */
    public static final String NODE = "_node";

    /** The line for indices past the limit. */
    public static final String OTHER = "_other";

    /** Indices tracked by default before the rest go to {@link #OTHER}: about 20 MB of counters. */
    public static final int DEFAULT_MAX_INDICES = 100_000;

    // The key layout, as metadata.RegisterMap writes it. Spelled out here because the store sits below metadata; a test
    // resolves RegisterMap's own paths through these, so a layout change that forgets them fails there.
    private static final String SEGMENTS = "segments";
    private static final String INDICES = "indices";
    private static final String SHARDS = "shards";
    private static final String ASSIGNMENTS_DIR = "assignments";
    private static final char SHARD_SEPARATOR = '#';

    private static final ThreadLocal<Scope> SCOPE = new ThreadLocal<>();

    private final ConcurrentHashMap<String, IndexUsage> byIndex = new ConcurrentHashMap<>();
    private final IndexUsage node = new IndexUsage();
    private final IndexUsage other = new IndexUsage();
    private final int maxIndices;

    /**
     * Creates empty usage.
     *
     * @param maxIndices how many indices to track before the rest are counted together
     */
    public StoreUsage(int maxIndices) {
        if (maxIndices < 0) {
            throw new IllegalArgumentException("maxIndices must not be negative: " + maxIndices);
        }
        this.maxIndices = maxIndices;
    }

    /**
     * Returns an index's counters, creating them if there is room.
     *
     * @param index the index name, {@link #NODE}, {@link #OTHER}, or null for the node
     * @return its counters
     */
    public IndexUsage of(String index) {
        if (index == null || NODE.equals(index)) {
            return node;
        }
        if (OTHER.equals(index)) {
            return other;
        }
        final IndexUsage existing = byIndex.get(index);
        if (existing != null) {
            return existing;
        }
        if (byIndex.size() >= maxIndices) {
            return other;
        }
        return byIndex.computeIfAbsent(index, k -> new IndexUsage());
    }

    /**
     * Returns every line: each tracked index, then {@link #NODE} and {@link #OTHER}.
     *
     * @return the lines, by name; a copy of the map, sharing the live counters
     */
    public Map<String, IndexUsage> lines() {
        final Map<String, IndexUsage> lines = new TreeMap<>(byIndex);
        lines.put(NODE, node);
        lines.put(OTHER, other);
        return Collections.unmodifiableMap(lines);
    }

    /**
     * Returns the sum of one counter over every line.
     *
     * @param kind the counter
     * @return the sum
     */
    public long total(IndexUsage.Kind kind) {
        long sum = node.get(kind) + other.get(kind);
        for (IndexUsage usage : byIndex.values()) {
            sum += usage.get(kind);
        }
        return sum;
    }

    /**
     * Charges an amount to whoever should pay for it, by the rules in the class comment.
     *
     * @param owner the index the container or key names, or null if it names none
     * @param kind what is charged
     * @param amount thousandths of a request, or bytes
     */
    void charge(String owner, IndexUsage.Kind kind, long amount) {
        if (owner != null) {
            of(owner).add(kind, amount);
            return;
        }
        final Scope scope = SCOPE.get();
        if (scope == null) {
            node.add(kind, amount);
        } else {
            scope.charge(this, kind, amount);
        }
    }

    /**
     * Returns who a request with this owner would be charged to, fixed now, for work that charges later on another
     * thread: bytes pulled through a stream after the call that opened it returned.
     *
     * @param owner the index the container or key names, or null
     * @return the payer
     */
    Scope payer(String owner) {
        if (owner != null) {
            return Scope.of(owner);
        }
        final Scope scope = SCOPE.get();
        return scope != null ? scope : Scope.of(NODE);
    }

    /**
     * Runs work whose requests are shared between indices by weight.
     *
     * @param weights index name to its weight, usually its bytes in the upload; indices of weight zero or less, and null
     *                names, are left out; empty charges the node
     * @param work the work
     * @param <T> what it returns
     * @return what it returned
     * @throws IOException whatever it threw
     */
    public static <T> T split(Map<String, Long> weights, CheckedSupplier<T, IOException> work) throws IOException {
        return within(Scope.of(weights), work);
    }

    /**
     * Returns who requests on this thread are being charged to, so work handed to another thread can carry it.
     *
     * @return the scope, or null if none
     */
    public static Scope currentScope() {
        return SCOPE.get();
    }

    /**
     * Runs work under a scope taken from {@link #currentScope} on another thread.
     *
     * @param scope the scope, or null for none
     * @param work the work
     * @param <T> what it returns
     * @return what it returned
     * @throws IOException whatever it threw
     */
    public static <T> T within(Scope scope, CheckedSupplier<T, IOException> work) throws IOException {
        final Scope previous = SCOPE.get();
        if (scope == null) {
            SCOPE.remove();
        } else {
            SCOPE.set(scope);
        }
        try {
            return work.get();
        } finally {
            if (previous == null) {
                SCOPE.remove();
            } else {
                SCOPE.set(previous);
            }
        }
    }

    /**
     * Returns the index a container's path belongs to: the shard named after {@code segments/}.
     *
     * @param path the container's path
     * @return the index name, or null if the path is not under a shard
     */
    public static String indexOfContainer(BlobPath path) {
        if (path == null) {
            return null;
        }
        final String[] parts = path.toArray();
        for (int i = 0; i + 1 < parts.length; i++) {
            if (SEGMENTS.equals(parts[i])) {
                final String shard = parts[i + 1];
                final int separator = shard.indexOf(SHARD_SEPARATOR);
                if (separator > 0 && shard.indexOf(SHARD_SEPARATOR, separator + 1) > separator) {
                    return shard.substring(0, separator);
                }
            }
        }
        return null;
    }

    /** How a container's blobs are named, when each blob is one index's: see {@link #indexOfBlob}. */
    enum BlobNaming {
        /** Blobs belong to no index, or the container itself does. */
        NONE,
        /** Index descriptors: the blob is the index name. */
        DESCRIPTORS,
        /** Shard heads: two hex digits, a dash, then {@code index#shard}. */
        HEADS,
        /** A node's assignments: {@code index#shard}. */
        ASSIGNMENTS;

        static BlobNaming of(BlobPath path) {
            if (path == null) {
                return NONE;
            }
            final String[] parts = path.toArray();
            final int n = parts.length;
            if (n == 0) {
                return NONE;
            }
            final String last = parts[n - 1];
            if (INDICES.equals(last)) {
                return DESCRIPTORS;
            }
            if (SHARDS.equals(last)) {
                return HEADS;
            }
            if (n >= 2 && ASSIGNMENTS_DIR.equals(parts[n - 2])) {
                return ASSIGNMENTS;
            }
            return NONE;
        }
    }

    /**
     * Returns the index a blob belongs to, from its name, in a container whose blobs are each one index's.
     *
     * @param naming how the container names its blobs
     * @param blobName the blob
     * @return the index name, or null
     */
    static String indexOfBlob(BlobNaming naming, String blobName) {
        if (blobName == null) {
            return null;
        }
        switch (naming) {
            case DESCRIPTORS:
                return blobName.isEmpty() ? null : blobName;
            case HEADS: {
                final int separator = blobName.lastIndexOf(SHARD_SEPARATOR);
                return blobName.length() > 3 && blobName.charAt(2) == '-' && separator > 3 ? blobName.substring(3, separator) : null;
            }
            case ASSIGNMENTS: {
                final int separator = blobName.lastIndexOf(SHARD_SEPARATOR);
                return separator > 0 ? blobName.substring(0, separator) : null;
            }
            default:
                return null;
        }
    }

    /** Who a thread's requests are charged to: one index, or several by weight. */
    public static final class Scope {

        private final String[] indices;
        private final long[] weights;
        private final long totalWeight;
        private final int heaviest;

        private Scope(String[] indices, long[] weights) {
            this.indices = indices;
            this.weights = weights;
            long total = 0L;
            int heaviest = 0;
            for (int i = 0; i < weights.length; i++) {
                total += weights[i];
                if (weights[i] > weights[heaviest]) {
                    heaviest = i;
                }
            }
            this.totalWeight = total;
            this.heaviest = heaviest;
        }

        static Scope of(String index) {
            return new Scope(new String[] { index }, new long[] { 1L });
        }

        static Scope of(Map<String, Long> weightByIndex) {
            int n = 0;
            for (Map.Entry<String, Long> entry : weightByIndex.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null && entry.getValue() > 0L) {
                    n++;
                }
            }
            if (n == 0) {
                return of(NODE);
            }
            final String[] indices = new String[n];
            final long[] weights = new long[n];
            int i = 0;
            for (Map.Entry<String, Long> entry : weightByIndex.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null && entry.getValue() > 0L) {
                    indices[i] = entry.getKey();
                    weights[i] = entry.getValue();
                    i++;
                }
            }
            return new Scope(indices, weights);
        }

        /**
         * Charges an amount, split by weight. Each index gets its share rounded down and the heaviest the remainder, so
         * the shares add up to the amount exactly.
         */
        void charge(StoreUsage usage, IndexUsage.Kind kind, long amount) {
            if (indices.length == 1) {
                usage.of(indices[0]).add(kind, amount);
                return;
            }
            long given = 0L;
            for (int i = 0; i < indices.length; i++) {
                if (i == heaviest) {
                    continue;
                }
                final long share = amount <= Long.MAX_VALUE / weights[i]
                    ? amount * weights[i] / totalWeight
                    : (long) ((double) amount * weights[i] / totalWeight);
                usage.of(indices[i]).add(kind, share);
                given += share;
            }
            usage.of(indices[heaviest]).add(kind, amount - given);
        }
    }
}
