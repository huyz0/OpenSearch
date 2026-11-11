/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.serverless.cluster.Zones;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Bytes this node exchanged with its peers, split by whether they stayed in its zone.
 *
 * <p>Traffic between zones is what the clouds bill, so it is what every zone preference in the shell is
 * judged by, and it is counted where the shell talks to peers: every forwarded request this node sends and
 * every answer it reads back. Each exchange is counted once, by the node that started it, so a fleet's total
 * is the sum of its nodes' counts.
 *
 * <p>Payload bytes, not wire bytes: the transport's own framing and headers are left out, and so is
 * compression, if any. That keeps the count comparable across settings and is within a few percent of the bill.
 *
 * <p>"Unknown" is an exchange where either side declares no zone. It is reported apart rather than guessed,
 * because a fleet that never configured zones should read as unmeasured, not as one zone.
 */
public final class ZoneTraffic {

    /** Whether the meter counts, {@code true} by default; the count costs less than the request's MAC already does. */
    public static final String ENABLED = "serverless.network.zone_meter.enabled";

    /** Where an exchange went, relative to this node's zone. */
    public enum Path {
        /** Both sides in the same known zone. */
        SAME_ZONE,
        /** Both zones known and different: the billed case. */
        CROSS_ZONE,
        /** Either side declares no zone. */
        UNKNOWN
    }

    private final Map<Path, Map<String, LongAdder>> byAction = new ConcurrentHashMap<>();

    /** Creates an empty meter. */
    public ZoneTraffic() {
        for (Path path : Path.values()) {
            byAction.put(path, new ConcurrentHashMap<>());
        }
    }

    /**
     * Classifies an exchange between two zones.
     *
     * @param local this node's zone, or null
     * @param peer the peer's zone, or null
     * @return where it went
     */
    public static Path pathBetween(String local, String peer) {
        if (local == null || peer == null) {
            return Path.UNKNOWN;
        }
        return Zones.same(local, peer) ? Path.SAME_ZONE : Path.CROSS_ZONE;
    }

    /**
     * Counts bytes sent or read for one action.
     *
     * @param path where they went
     * @param action the transport action
     * @param bytes how many
     */
    public void record(Path path, String action, long bytes) {
        if (bytes > 0) {
            byAction.get(path).computeIfAbsent(action, a -> new LongAdder()).add(bytes);
        }
    }

    /**
     * Returns the bytes counted on one path, all actions together.
     *
     * @param path the path
     * @return the total
     */
    public long total(Path path) {
        long sum = 0;
        for (LongAdder adder : byAction.get(path).values()) {
            sum += adder.sum();
        }
        return sum;
    }

    /**
     * Returns the bytes counted on one path, per action, sorted by action name.
     *
     * @param path the path
     * @return action to bytes
     */
    public Map<String, Long> byAction(Path path) {
        final Map<String, Long> out = new TreeMap<>();
        byAction.get(path).forEach((action, adder) -> out.put(action, adder.sum()));
        return out;
    }

    /** Forgets every count, for a meter that measures one stretch of work. */
    public void reset() {
        for (Map<String, LongAdder> counts : byAction.values()) {
            counts.clear();
        }
    }

    /**
     * Returns how many bytes a value serialises to, without keeping them.
     *
     * @param value the value
     * @return its serialised size
     * @throws IOException if it cannot be serialised
     */
    public static long sizeOf(Writeable value) throws IOException {
        try (Counting out = new Counting()) {
            value.writeTo(out);
            return out.count;
        }
    }

    private static final class Counting extends StreamOutput {
        long count;

        @Override
        public void writeByte(byte b) {
            count++;
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) {
            count += length;
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}

        @Override
        public void reset() {
            count = 0;
        }
    }
}
