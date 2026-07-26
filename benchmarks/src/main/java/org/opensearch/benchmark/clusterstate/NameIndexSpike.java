/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The last thing in the partitioned design that resists partitioning.
 *
 * <p>Everything else scales by hashing on the index: descriptors are fetched on demand and cached by
 * whoever the hash points at, and placement is computed rather than stored (S12). Wildcard and alias
 * resolution cannot work that way. Answering {@code logs-*} requires knowing every index name, which is
 * global by construction, so either one tier holds all 100M names or every wildcard scatters to every
 * partition.
 *
 * <p>This measures whether holding them is affordable. Two representations of the same 100M-name
 * problem, at a sample size that extrapolates cleanly:
 *
 * <ul>
 *   <li>{@code map} -- the shape core uses today, a {@link HashMap} keyed by name. Every entry is a
 *       {@link String} object with its own header, hash field and backing byte array, plus a map node.
 *   <li>{@code compact} -- names concatenated into one sorted {@code byte[]} with an offset array, UUIDs
 *       as raw 16-byte values rather than 36-char strings, state as a single byte. Wildcards become a
 *       binary search plus a forward scan, which is what sorting buys.
 * </ul>
 *
 * <p>The comparison matters because the per-entry object overhead, not the data, is what decides
 * whether this fits. A name is about 40 bytes of actual text; the question is what Java charges to hold
 * 100M of them addressably.
 *
 * <pre>{@code
 * java -da -dsa -Xms8g -Xmx8g -cp <runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.NameIndexSpike
 * }</pre>
 */
public final class NameIndexSpike {

    private static final int SAMPLE = 2_000_000;
    private static final long TARGET = 100_000_000L;

    private NameIndexSpike() {}

    public static void main(String[] args) throws Exception {
        System.out.println("Global name index, " + SAMPLE + " names sampled, extrapolated to " + TARGET + "\n");

        measure("map", () -> buildMap(SAMPLE));
        measure("compact", () -> buildCompact(SAMPLE));

        // Correctness and speed of the operation the compact form exists to serve. A prefix wildcard is
        // a binary search to the first match then a forward scan, so its cost is proportional to the
        // number of matches rather than to the size of the index -- which is the property that makes a
        // single global tier viable at all.
        CompactNameIndex index = buildCompact(SAMPLE);
        String prefix = "tenant-0000";
        long start = System.nanoTime();
        int matches = index.countPrefix(prefix);
        long elapsed = System.nanoTime() - start;
        System.out.printf("%nprefix scan \"%s*\": %d matches in %.1f us%n", prefix, matches, elapsed / 1000.0);

        String missing = "zzzz-no-such-prefix";
        start = System.nanoTime();
        int none = index.countPrefix(missing);
        elapsed = System.nanoTime() - start;
        System.out.printf("prefix scan \"%s*\": %d matches in %.1f us%n", missing, none, elapsed / 1000.0);
    }

    private static void measure(String label, ThrowingSupplier supplier) throws Exception {
        Object warm = supplier.get();
        if (warm.hashCode() == Integer.MIN_VALUE) {
            throw new IllegalStateException("unreachable, keeps warm live");
        }
        warm = null;
        forceGc();

        long before = usedHeapBytes();
        Object held = supplier.get();
        forceGc();
        long after = usedHeapBytes();

        double perName = (double) (after - before) / SAMPLE;
        double totalGib = perName * TARGET / (1024.0 * 1024 * 1024);
        System.out.printf("%-9s %8.1f B/name   %8.1f GiB at %dM%n", label, perName, totalGib, TARGET / 1_000_000);

        if (held.hashCode() == Integer.MIN_VALUE) {
            throw new IllegalStateException("unreachable, keeps held live");
        }
    }

    private static Map<String, byte[]> buildMap(int count) {
        Map<String, byte[]> map = new HashMap<>(count * 2);
        Random random = new Random(42);
        for (int i = 0; i < count; i++) {
            // uuid + state, the minimum a wildcard resolver needs beyond the name itself
            byte[] value = new byte[17];
            random.nextBytes(value);
            map.put(name(i), value);
        }
        return map;
    }

    private static CompactNameIndex buildCompact(int count) {
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add(name(i));
        }
        // Sorted so a prefix query is a binary search plus a scan. Sorting once at build time is what
        // removes the need for a per-entry index structure.
        names.sort(null);

        int totalBytes = 0;
        for (String n : names) {
            totalBytes += n.length();
        }

        byte[] blob = new byte[totalBytes];
        int[] offsets = new int[count + 1];
        byte[] uuids = new byte[count * 16];
        byte[] states = new byte[count];

        Random random = new Random(42);
        int pos = 0;
        for (int i = 0; i < count; i++) {
            byte[] encoded = names.get(i).getBytes(StandardCharsets.UTF_8);
            System.arraycopy(encoded, 0, blob, pos, encoded.length);
            offsets[i] = pos;
            pos += encoded.length;
            random.nextBytes(new byte[0]);
            states[i] = (byte) (i & 1);
        }
        offsets[count] = pos;
        random.nextBytes(uuids);

        return new CompactNameIndex(blob, offsets, uuids, states);
    }

    /**
     * Names in one blob, addressed by an offset array, sorted. UUIDs raw rather than as 36-character
     * strings. State as a byte. No per-entry object at all, which is the entire point.
     */
    static final class CompactNameIndex {
        private final byte[] blob;
        private final int[] offsets;
        private final byte[] uuids;
        private final byte[] states;

        CompactNameIndex(byte[] blob, int[] offsets, byte[] uuids, byte[] states) {
            this.blob = blob;
            this.offsets = offsets;
            this.uuids = uuids;
            this.states = states;
        }

        int size() {
            return states.length;
        }

        /** Binary search to the first name at or after the prefix, then scan forward while it matches. */
        int countPrefix(String prefix) {
            byte[] target = prefix.getBytes(StandardCharsets.UTF_8);
            int lo = 0;
            int hi = size();
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (compareAt(mid, target) < 0) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            int count = 0;
            for (int i = lo; i < size() && startsWith(i, target); i++) {
                count++;
            }
            return count;
        }

        private int compareAt(int i, byte[] target) {
            int start = offsets[i];
            int len = offsets[i + 1] - start;
            int n = Math.min(len, target.length);
            for (int k = 0; k < n; k++) {
                int diff = (blob[start + k] & 0xFF) - (target[k] & 0xFF);
                if (diff != 0) {
                    return diff;
                }
            }
            return len - target.length;
        }

        private boolean startsWith(int i, byte[] target) {
            int start = offsets[i];
            int len = offsets[i + 1] - start;
            if (len < target.length) {
                return false;
            }
            for (int k = 0; k < target.length; k++) {
                if (blob[start + k] != target[k]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return blob.length + offsets.length + uuids.length + states.length;
        }
    }

    /** Representative tenant index name: a stable prefix plus a hex discriminator, about 40 bytes. */
    private static String name(int i) {
        return "tenant-" + String.format("%08x", i) + "-index";
    }

    private interface ThrowingSupplier {
        Object get() throws Exception;
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void forceGc() throws InterruptedException {
        for (int i = 0; i < 4; i++) {
            System.gc();
            Thread.sleep(120);
        }
    }
}
