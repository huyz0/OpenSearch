/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.snapshot;

import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.repository.SnapshotAliases;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

/**
 * Reads the aliases that name the indices a take captured.
 *
 * <p>An index's descriptor carries a hint of the aliases that name it, which is not the truth: an alias is recorded for
 * an index only when its own register still lists the index. Each distinct alias is read once, however many indices it
 * names, with at most {@link #FAN_OUT} reads in flight, so a take of {@code d} distinct aliases costs {@code d} reads and
 * one extra round. An index in more than {@link SnapshotAliases#MAX_ALIASES} aliases is recorded with the first of them
 * by name and flagged as cut.
 */
final class SnapshotAliasCapture {

    /** The most alias registers read at once. */
    static final int FAN_OUT = 16;

    /** What one index's record entry carries: the {@code aliases} member, and whether the list was cut. */
    record Captured(Map<String, Object> aliases, boolean truncated) {
    }

    private SnapshotAliasCapture() {}

    /**
     * Captures the aliases of each index.
     *
     * @param metadata the metadata plane
     * @param executor where the reads run, or null to read one after another
     * @param indices each captured index's name and the descriptor the take read for it
     * @return each index's entry, only for an index that has something to record
     * @throws IOException if a register cannot be read
     */
    static Map<String, Captured> capture(MetadataPlane metadata, Executor executor, Map<String, IndexDescriptor> indices)
        throws IOException {
        final Map<String, List<String>> hinted = new LinkedHashMap<>();
        final Set<String> truncated = new LinkedHashSet<>();
        final Set<String> distinct = new LinkedHashSet<>();
        for (Map.Entry<String, IndexDescriptor> index : indices.entrySet()) {
            final List<String> names = new ArrayList<>(new TreeSet<>(index.getValue().aliasedBy()));
            if (names.size() > SnapshotAliases.MAX_ALIASES) {
                truncated.add(index.getKey());
                names.subList(SnapshotAliases.MAX_ALIASES, names.size()).clear();
            }
            hinted.put(index.getKey(), names);
            distinct.addAll(names);
        }
        final Map<String, AliasRecord> read = read(metadata, executor, distinct);
        final Map<String, Captured> captured = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> index : hinted.entrySet()) {
            final Map<String, Boolean> flags = new LinkedHashMap<>();
            for (String name : index.getValue()) {
                final AliasRecord alias = read.get(name);
                if (alias != null && alias.dataStream() == false && alias.indices().contains(index.getKey())) {
                    flags.put(name, index.getKey().equals(alias.writeMember()));
                }
            }
            final boolean cut = truncated.contains(index.getKey());
            if (flags.isEmpty() == false || cut) {
                captured.put(index.getKey(), new Captured(flags.isEmpty() ? null : SnapshotAliases.encode(flags), cut));
            }
        }
        return captured;
    }

    private static Map<String, AliasRecord> read(MetadataPlane metadata, Executor executor, Set<String> names) throws IOException {
        final Map<String, AliasRecord> read = new LinkedHashMap<>();
        if (executor == null) {
            for (String name : names) {
                read.put(name, metadata.resolve(name).alias());
            }
            return read;
        }
        final Semaphore inFlight = new Semaphore(FAN_OUT);
        final Map<String, CompletableFuture<AliasRecord>> pending = new LinkedHashMap<>();
        try {
            for (String name : names) {
                inFlight.acquire();
                pending.put(name, CompletableFuture.supplyAsync(() -> {
                    try {
                        return metadata.resolve(name).alias();
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    } finally {
                        inFlight.release();
                    }
                }, executor));
            }
            for (Map.Entry<String, CompletableFuture<AliasRecord>> each : pending.entrySet()) {
                read.put(each.getKey(), each.getValue().join());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while reading aliases", e);
        } catch (CompletionException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
        return read;
    }
}
