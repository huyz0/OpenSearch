/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.bucket.BucketManifest;
import org.opensearch.serverless.bucket.ManifestCodec;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * What a bucket manifest costs to hold and to read, by encoding: its size, the time to write it, to read all of it as the
 * owner's checkpoint does, and to read one member as a reader opening that member does.
 *
 * <p>Members are shaped as the small-index meter's are: eight unmerged segments of three files and a commit point,
 * every file in a node pack. Off unless {@code -Dtests.meter.bucket_codec=true}.
 */
public class BucketManifestCodecMeterTests extends OpenSearchTestCase {

    private static final String NODES = "ABCDEFGHIJKLMNOPQRSTUV";

    private static String nodeId(Random random) {
        final StringBuilder id = new StringBuilder();
        for (int i = 0; i < 22; i++) {
            id.append(NODES.charAt(random.nextInt(NODES.length())));
        }
        return id.toString();
    }

    /** A bucket of members written by a few nodes, each member's files in a handful of that node's packs. */
    static BucketManifest bucket(int members, int segments, long seed) {
        final Random random = new Random(seed);
        final List<String> writers = List.of(nodeId(random), nodeId(random), nodeId(random));
        BucketManifest manifest = BucketManifest.empty("s05-1");
        final Map<String, BucketManifest.Member> entries = new LinkedHashMap<>();
        long ordinal = 1000;
        for (int m = 0; m < members; m++) {
            final String writer = writers.get(random.nextInt(writers.size()));
            final String epoch = String.format(Locale.ROOT, "e=%020d", 1794472211738L + random.nextInt(3));
            final Map<String, String> files = new LinkedHashMap<>();
            final Map<String, Long> lengths = new LinkedHashMap<>();
            for (int s = 0; s < segments; s++) {
                final String pack = String.format(Locale.ROOT, "@%s/%s/%020d", writer, epoch, ordinal + random.nextInt(400));
                long offset = random.nextInt(60_000);
                for (String ext : new String[] { ".cfs", ".cfe", ".si" }) {
                    final long length = ext.equals(".cfs") ? 2000 + random.nextInt(4000) : ext.equals(".cfe") ? 549 : 333;
                    files.put("_" + Integer.toString(s, 36) + ext, pack + "#" + offset + ":" + length);
                    lengths.put("_" + Integer.toString(s, 36) + ext, length);
                    offset += length;
                }
            }
            final String commitPack = String.format(Locale.ROOT, "@%s/%s/%020d", writer, epoch, ordinal + 400);
            files.put("segments_" + Integer.toString(segments + 2, 36), commitPack + "#11135:895");
            lengths.put("segments_" + Integer.toString(segments + 2, 36), 895L);
            final CommitManifest commit = new CommitManifest(1, files, writer, lengths, null, 40 + m).withStageSequence(7);
            entries.put(uuid(random), new BucketManifest.Member("tenant-" + random.nextInt(1_000_000) + "-logs", commit, 3));
            ordinal += random.nextInt(3);
        }
        for (Map.Entry<String, BucketManifest.Member> e : entries.entrySet()) {
            manifest = manifest.withMember(e.getKey(), e.getValue());
        }
        return manifest;
    }

    private static String uuid(Random random) {
        final byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    @FunctionalInterface
    private interface Op {
        void run() throws Exception;
    }

    /** Microseconds per run, the median of several timed batches after a warm-up. */
    private static double micros(Op op) throws Exception {
        // Warm up for about a second, then time batches of about a tenth of a second each.
        final long warmUntil = System.nanoTime() + 1_000_000_000L;
        int perBatch = 0;
        while (System.nanoTime() < warmUntil) {
            op.run();
            perBatch++;
        }
        final int runs = Math.max(1, perBatch / 10);
        final List<Double> batches = new ArrayList<>();
        for (int b = 0; b < 7; b++) {
            final long start = System.nanoTime();
            for (int i = 0; i < runs; i++) {
                op.run();
            }
            batches.add((System.nanoTime() - start) / 1000.0 / runs);
        }
        batches.sort(Double::compare);
        return batches.get(batches.size() / 2);
    }

    public void testManifestEncodings() throws Exception {
        assumeTrue(
            "a meter runs only when -Dtests.meter.bucket_codec=true",
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.meter.bucket_codec", "false"))
        );
        for (int members : new int[] { 256, 1024, 4096 }) {
            final BucketManifest manifest = bucket(members, 8, members);
            final String someMember = new ArrayList<>(manifest.members().keySet()).get(members / 2);
            final StringBuilder out = new StringBuilder();
            final BytesReference json = manifest.toBytes();
            assertEquals(manifest, BucketManifest.parse(json));
            out.append(
                String.format(
                    Locale.ROOT,
                    "|json %d KB (%d B/member) write %.0f us, read all %.0f us, read one %.0f us",
                    json.length() / 1024,
                    json.length() / members,
                    micros(manifest::toBytes),
                    micros(() -> BucketManifest.parse(json)),
                    micros(() -> BucketManifest.parseMember(json, someMember))
                )
            );
            for (ManifestCodec codec : ManifestCodec.values()) {
                final BytesReference bytes = manifest.toBytes(codec);
                assertEquals(manifest, BucketManifest.parse(bytes));
                final BucketManifest.Member one = BucketManifest.parseMember(bytes, someMember).orElseThrow();
                assertEquals(manifest.member(someMember).orElseThrow().commit().files(), one.commit().files());
                assertEquals(manifest.member(someMember).orElseThrow().commit().lengths(), one.commit().lengths());
                out.append(
                    String.format(
                        Locale.ROOT,
                        "|%s %d KB (%d B/member) write %.0f us, read all %.0f us, read one %.0f us",
                        codec.name().toLowerCase(Locale.ROOT),
                        bytes.length() / 1024,
                        bytes.length() / members,
                        micros(() -> manifest.toBytes(codec)),
                        micros(() -> BucketManifest.parse(bytes)),
                        micros(() -> BucketManifest.parseMember(bytes, someMember))
                    )
                );
            }
            final BytesReference auto = manifest.toBytesCompressingIfWorthIt(ManifestCodec.ZSTD);
            out.append(
                String.format(
                    Locale.ROOT,
                    "|auto (%s) %d KB read one %.0f us",
                    ManifestCodec.values()[auto.get(5)].name().toLowerCase(Locale.ROOT),
                    auto.length() / 1024,
                    micros(() -> BucketManifest.parseMember(auto, someMember))
                )
            );
            logger.info("MANIFEST|{} members{}", members, out);
        }
    }
}
