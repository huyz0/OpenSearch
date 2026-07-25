/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.clusterstate;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.IndexDescriptor;

import java.util.Locale;

/**
 * What carrying the index descriptor costs the cluster metadata manifest.
 *
 * <p>C6 put the descriptor in the manifest so a node reading full cluster state does not have to fetch
 * every index blob. That trade is only worth making if the manifest stays affordable, and the manifest
 * is the one file rewritten in full on <em>every</em> cluster state version -- so its size is a
 * per-version write cost, not a one-off. At the index counts this work targets, a per-entry increase
 * that looks small can dominate.
 *
 * <p>Measures the serialized entry with and without the descriptor, at 0, 1 and 3 aliases, and
 * extrapolates to the manifest sizes that matter.
 *
 * <pre>{@code
 * java -da -dsa -cp <benchmarks runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.ManifestDescriptorSizeEstimate
 * }</pre>
 */
public final class ManifestDescriptorSizeEstimate {

    private ManifestDescriptorSizeEstimate() {}

    private static final int[] INDEX_COUNTS = { 10_000, 100_000, 1_000_000 };

    public static void main(String[] args) throws Exception {
        System.out.println("Serialized size of one manifest index entry\n");
        System.out.printf(Locale.ROOT, "%-16s %10s %12s %10s%n", "aliases", "without", "with", "increase");

        long baseline = 0;
        long withOneAlias = 0;
        for (int aliases : new int[] { 0, 1, 3 }) {
            IndexMetadata index = index(aliases);
            int without = size(entry(null));
            int with = size(entry(IndexDescriptor.of(index)));
            System.out.printf(
                Locale.ROOT,
                "%-16d %8d B %10d B %+9.0f%%%n",
                aliases,
                without,
                with,
                ((double) with / without - 1) * 100
            );
            if (aliases == 0) {
                baseline = with - without;
            }
            if (aliases == 1) {
                withOneAlias = with - without;
            }
        }

        System.out.println("\nManifest growth, rewritten once per cluster state version");
        System.out.printf(Locale.ROOT, "%-14s %18s %18s%n", "indices", "no aliases", "one alias each");
        for (int count : INDEX_COUNTS) {
            System.out.printf(
                Locale.ROOT,
                "%-14s %15.1f MB %15.1f MB%n",
                String.format(Locale.ROOT, "%,d", count),
                baseline * (double) count / 1024 / 1024,
                withOneAlias * (double) count / 1024 / 1024
            );
        }
        System.out.println("\nAdded bytes per entry: " + baseline + " B with no aliases, " + withOneAlias + " B with one.");

        // The manifest blob is compressed on the way to the repository, and these entries are about as
        // compressible as data gets: a shared name prefix, the same field names repeated, and mostly the
        // same boolean values. The uncompressed figures above are the wrong ones to plan capacity from.
        System.out.println("\nCompressed, 100k entries (DEFLATE, as the blob store applies)");
        System.out.printf(Locale.ROOT, "%-16s %14s %14s %10s%n", "", "without", "with", "increase");
        int without = compressedSize(100_000, false);
        int with = compressedSize(100_000, true);
        System.out.printf(
            Locale.ROOT,
            "%-16s %11.1f MB %11.1f MB %+9.0f%%%n",
            "one alias each",
            without / 1024.0 / 1024,
            with / 1024.0 / 1024,
            ((double) with / without - 1) * 100
        );
    }

    /**
     * Serializes {@code count} entries and DEFLATEs the result, as the manifest blob is written.
     *
     * <p>Each entry gets its own descriptor with its own alias name. Reusing one descriptor across all
     * of them would compress far better than anything real, and the whole point of measuring compressed
     * is to avoid quoting a number that flatters the change.
     */
    private static int compressedSize(int count, boolean withDescriptor) throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startArray();
        for (int i = 0; i < count; i++) {
            IndexDescriptor descriptor = withDescriptor ? IndexDescriptor.of(index(i, 1)) : null;
            builder.startObject();
            entry(i, descriptor).toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
        builder.endArray();
        byte[] raw = BytesReference.toBytes(BytesReference.bytes(builder));

        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        try (java.util.zip.DeflaterOutputStream deflater = new java.util.zip.DeflaterOutputStream(out)) {
            deflater.write(raw);
        }
        return out.size();
    }

    /** Distinct index names, so compression is not flattered by identical entries. */
    private static UploadedIndexMetadata entry(int i, IndexDescriptor descriptor) {
        String name = String.format(Locale.ROOT, "tenant-%016x", i);
        if (descriptor == null) {
            return new UploadedIndexMetadata(name, name + "-uuid", "index-file__2");
        }
        return new UploadedIndexMetadata(
            name,
            name + "-uuid",
            "index-file__2",
            UploadedIndexMetadata.COMPONENT_PREFIX,
            ClusterMetadataManifest.CODEC_V5,
            descriptor
        );
    }

    private static UploadedIndexMetadata entry(IndexDescriptor descriptor) {
        if (descriptor == null) {
            return new UploadedIndexMetadata("tenant-0000000000000000", "tenant-0000000000000000-uuid", "index-file__2");
        }
        return new UploadedIndexMetadata(
            "tenant-0000000000000000",
            "tenant-0000000000000000-uuid",
            "index-file__2",
            UploadedIndexMetadata.COMPONENT_PREFIX,
            ClusterMetadataManifest.CODEC_V5,
            descriptor
        );
    }

    private static int size(UploadedIndexMetadata entry) throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        entry.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return BytesReference.bytes(builder).length();
    }

    private static IndexMetadata index(int aliases) {
        return index(0, aliases);
    }

    private static IndexMetadata index(int ordinal, int aliases) {
        String name = String.format(Locale.ROOT, "tenant-%016x", ordinal);
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
            )
            .numberOfShards(3)
            .numberOfReplicas(1);
        for (int i = 0; i < aliases; i++) {
            builder.putAlias(AliasMetadata.builder(name + "-alias-" + i).build());
        }
        return builder.build();
    }
}
