/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.IndexDescriptor;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.zip.DeflaterOutputStream;

/**
 * C3a. What manifest sharding actually saves, measured before any of it is written.
 *
 * <p>C2 attached a stop condition to the go decision: the saving is bounded by how many indices change
 * per cluster state version, not by index count, so a version that touches every index touches every
 * shard and saves nothing. This is that measurement, and it is deliberately the last cheap place to
 * stop -- C4, C5 and C6 all assume the answer.
 *
 * <p>Nothing here needs the sharding implementation. The bytes written for a version are fully
 * determined by the design: the top-level manifest listing S shard references, plus the full contents
 * of every shard containing at least one changed index. Unchanged shards are carried forward by
 * reference and cost nothing. So the question is arithmetic over a hash partition, and simulating it
 * is both exact and free.
 *
 * <p>Compression matters and is applied per blob rather than to the whole manifest, which is part of
 * what is being measured: N/S entries compress slightly worse per entry than N do, so the sharded
 * total is a little larger than the unsharded one when everything changes. That penalty is real and
 * reported rather than hidden.
 *
 * <p>Run with assertions disabled, like every other measurement here.
 */
public class ManifestShardingWriteAmplificationEstimate {

    private static int SHARD_COUNT = 64;

    public static void main(String[] args) throws Exception {
        System.out.println("C3a: manifest bytes written per cluster state version, sharded vs not");
        System.out.println("Entries carry the C6 descriptor. DEFLATE per blob.\n");

        int indices = 100_000;
        int wholeManifest;
        SHARD_COUNT = 1;
        wholeManifest = compressedSize(allEntries(indices));
        System.out.printf(Locale.ROOT, "%,d indices, unsharded, any version: %,d bytes%n%n", indices, wholeManifest);

        for (int shards : new int[] { 64, 256, 1024, 4096 }) {
            SHARD_COUNT = shards;
            System.out.printf(Locale.ROOT, "--- %,d shards (%,d entries each) ---%n", shards, indices / shards);
            System.out.println("changedIndices | shardsTouched | shardedBytes | vs unsharded");
            for (int changed : new int[] { 1, 10, 100, 1_000, 10_000 }) {
                report(indices, changed, wholeManifest);
            }
            System.out.println();
        }
    }

    private static void report(int indices, int changed, int wholeManifest) throws Exception {
        // Which shards a set of changed indices touches. Modelling the changed set as the first K
        // indices is equivalent to any other choice under a uniform hash, and avoids a random seed.
        boolean[] touched = new boolean[SHARD_COUNT];
        for (int i = 0; i < changed; i++) {
            touched[shardOf(i)] = true;
        }
        int shardsTouched = 0;
        for (boolean t : touched) {
            if (t) {
                shardsTouched++;
            }
        }

        // Every touched shard is rewritten in full, because a shard blob is immutable and holds every
        // index that hashes to it, changed or not.
        int bytes = 0;
        for (int shard = 0; shard < SHARD_COUNT; shard++) {
            if (touched[shard]) {
                bytes += compressedSize(entriesInShard(indices, shard));
            }
        }
        bytes += topLevelManifestSize();

        System.out.printf(
            Locale.ROOT,
            "%,14d | %13d | %,12d | %s%n",
            changed,
            shardsTouched,
            bytes,
            bytes < wholeManifest
                ? String.format(Locale.ROOT, "%.1fx less", (double) wholeManifest / bytes)
                : String.format(Locale.ROOT, "%.1f%% MORE", ((double) bytes / wholeManifest - 1) * 100)
        );
    }

    private static int shardOf(int index) {
        // Stands in for murmurhash3(uuid) mod shardCount. Any uniform function gives the same answer
        // for this measurement; what matters is that a changed index lands in one shard.
        return Math.floorMod(Integer.hashCode(index) * 0x9E3779B1, SHARD_COUNT);
    }

    private static List<Integer> allEntries(int indices) {
        List<Integer> all = new ArrayList<>(indices);
        for (int i = 0; i < indices; i++) {
            all.add(i);
        }
        return all;
    }

    private static List<Integer> entriesInShard(int indices, int shard) {
        List<Integer> in = new ArrayList<>(indices / SHARD_COUNT + 1);
        for (int i = 0; i < indices; i++) {
            if (shardOf(i) == shard) {
                in.add(i);
            }
        }
        return in;
    }

    /**
     * The top-level manifest still exists and is still rewritten every version. It carries S shard
     * references instead of N index entries, so it is small and constant, but it is not free and a
     * measurement that omitted it would overstate the saving at low changed-index counts.
     */
    private static int topLevelManifestSize() throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startArray();
        for (int shard = 0; shard < SHARD_COUNT; shard++) {
            builder.startObject();
            builder.field("shard_id", shard);
            builder.field("blob_name", String.format(Locale.ROOT, "1__42__%d__%016x", shard, shard * 0x9E3779B1L));
            builder.field("entry_count", 1500);
            builder.endObject();
        }
        builder.endArray();
        return deflate(BytesReference.toBytes(BytesReference.bytes(builder)));
    }

    private static int compressedSize(List<Integer> entries) throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startArray();
        for (int i : entries) {
            builder.startObject();
            entry(i).toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
        builder.endArray();
        return deflate(BytesReference.toBytes(BytesReference.bytes(builder)));
    }

    private static int deflate(byte[] raw) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DeflaterOutputStream deflater = new DeflaterOutputStream(out)) {
            deflater.write(raw);
        }
        return out.size();
    }

    /** Distinct names and a distinct alias per entry, so compression is not flattered. */
    private static UploadedIndexMetadata entry(int i) {
        String name = String.format(Locale.ROOT, "tenant-%016x", i);
        return new UploadedIndexMetadata(
            name,
            name + "-uuid",
            "index-file__2",
            UploadedIndexMetadata.COMPONENT_PREFIX,
            ClusterMetadataManifest.CODEC_V5,
            IndexDescriptor.of(ManifestDescriptorSizeEstimate.index(i, 1))
        );
    }
}
