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
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Random;
import java.util.zip.Deflater;

/**
 * Spike S8: how big is a routing descriptor <em>on the wire</em>?
 *
 * <p>Spike S2 measured retained Java heap (289 B with no alias, 577 B with one), which governs how
 * many descriptors a node's page cache can hold. It does not govern object-store page size or GET
 * cost -- those depend on the serialized form, which the S5 paging simulation assumed was ~120 B
 * without measuring it. Since the whole read-cost model in S5 is denominated in page bytes, that
 * assumption is worth checking rather than carrying forward.
 *
 * <p>Also measures the same descriptors packed into a page and DEFLATE-compressed, because pages of
 * near-identical tenant descriptors should compress extremely well -- highly repetitive field
 * layouts, shared prefixes in names, mostly-identical routing math. If so, the effective per-index
 * wire cost is far below the uncompressed figure, and page sizing should be based on the compressed
 * number.
 *
 * <p>Compared against a full {@link IndexMetadata} serialized the same way, to give the wire-side
 * equivalent of S2's heap-side comparison.
 *
 * <pre>{@code
 * java -cp <benchmarks runtime classpath> \
 *   org.opensearch.benchmark.clusterstate.DescriptorWireSizeEstimate
 * }</pre>
 */
public final class DescriptorWireSizeEstimate {

    private DescriptorWireSizeEstimate() {}

    private static final int PAGE_DESCRIPTORS = 1000;

    public static void main(String[] args) throws Exception {
        System.out.println("=== S8: descriptor wire size ===");
        System.out.println();

        long descriptorBytes = 0;
        long descriptorNoAliasBytes = 0;
        long fullMetadataBytes = 0;

        BytesStreamOutput pageOut = new BytesStreamOutput();
        BytesStreamOutput pageNoAliasOut = new BytesStreamOutput();

        Random random = new Random(42);
        for (int i = 0; i < PAGE_DESCRIPTORS; i++) {
            String uuid = Long.toHexString(random.nextLong()) + Long.toHexString(random.nextLong());
            String name = "tenant-" + uuid;
            String aliasName = "tenant-alias-" + uuid;

            BytesStreamOutput one = new BytesStreamOutput();
            writeDescriptor(one, name, uuid, aliasName);
            descriptorBytes += one.size();
            writeDescriptor(pageOut, name, uuid, aliasName);

            BytesStreamOutput oneNoAlias = new BytesStreamOutput();
            writeDescriptor(oneNoAlias, name, uuid, null);
            descriptorNoAliasBytes += oneNoAlias.size();
            writeDescriptor(pageNoAliasOut, name, uuid, null);

            BytesStreamOutput full = new BytesStreamOutput();
            fullIndexMetadata(name, uuid, aliasName).writeTo(full);
            fullMetadataBytes += full.size();
        }

        byte[] page = org.opensearch.core.common.bytes.BytesReference.toBytes(pageOut.bytes());
        byte[] pageNoAlias = org.opensearch.core.common.bytes.BytesReference.toBytes(pageNoAliasOut.bytes());
        int pageCompressed = deflate(page);
        int pageNoAliasCompressed = deflate(pageNoAlias);

        System.out.printf(java.util.Locale.ROOT, "descriptors per page: %d%n%n", PAGE_DESCRIPTORS);
        System.out.println("uncompressed wire bytes per index:");
        System.out.printf(java.util.Locale.ROOT, "  descriptor, 1 alias  : %.1f B%n", (double) descriptorBytes / PAGE_DESCRIPTORS);
        System.out.printf(java.util.Locale.ROOT, "  descriptor, no alias : %.1f B%n", (double) descriptorNoAliasBytes / PAGE_DESCRIPTORS);
        System.out.printf(java.util.Locale.ROOT, "  full IndexMetadata   : %.1f B%n", (double) fullMetadataBytes / PAGE_DESCRIPTORS);
        System.out.println();
        System.out.println("page totals:");
        System.out.printf(
            java.util.Locale.ROOT,
            "  1 alias  : %d B raw -> %d B deflated (%.1f B/index effective, %.1fx)%n",
            page.length,
            pageCompressed,
            (double) pageCompressed / PAGE_DESCRIPTORS,
            (double) page.length / pageCompressed
        );
        System.out.printf(
            java.util.Locale.ROOT,
            "  no alias : %d B raw -> %d B deflated (%.1f B/index effective, %.1fx)%n",
            pageNoAlias.length,
            pageNoAliasCompressed,
            (double) pageNoAliasCompressed / PAGE_DESCRIPTORS,
            (double) pageNoAlias.length / pageNoAliasCompressed
        );
    }

    /**
     * Serializes the S1-verified descriptor field set. Written by hand rather than via a Writeable
     * implementation because the descriptor is a measurement subject, not a proposed core type --
     * the point is the byte count of this field set, not a wire contract.
     */
    private static void writeDescriptor(StreamOutput out, String name, String uuid, String aliasName) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
        // routing math
        out.writeVInt(1);   // routingNumShards
        out.writeVInt(1);   // routingFactor
        out.writeVInt(1);   // routingPartitionSize
        out.writeVInt(0);   // numberOfVirtualShards (+1 encoded)
        out.writeVInt(1);   // numberOfShards
        out.writeVInt(0);   // numberOfReplicas
        out.writeVInt(0);   // numberOfSearchOnlyReplicas
        // small typed fields
        out.writeByte((byte) 0);                 // state OPEN
        out.writeVInt(Version.CURRENT.id);       // creationVersion
        out.writeVInt(1);                        // waitForActiveShards
        out.writeOptionalString(null);           // defaultSearchPipelineId
        // eight flags packed into one byte
        out.writeByte((byte) 0);
        // four version longs
        out.writeVLong(1);
        out.writeVLong(1);
        out.writeVLong(1);
        out.writeVLong(1);
        // aliases
        if (aliasName == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(1);
            AliasMetadata.builder(aliasName).build().writeTo(out);
        }
        // one shard placement
        out.writeVInt(1);
        out.writeVInt(0);                        // shardId
        out.writeOptionalString("node-0");       // currentNodeId
        out.writeOptionalString(null);           // relocatingNodeId
        out.writeOptionalString("alloc-" + uuid);// allocationId
        out.writeByte((byte) 2);                 // state STARTED
        out.writeBoolean(true);                  // primary
        out.writeBoolean(false);                 // searchOnly
    }

    private static IndexMetadata fullIndexMetadata(String name, String uuid, String aliasName) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(aliasName).build())
            .build();
    }

    private static int deflate(byte[] input) {
        Deflater deflater = new Deflater(Deflater.BEST_SPEED);
        try {
            deflater.setInput(input);
            deflater.finish();
            byte[] buffer = new byte[input.length + 1024];
            int total = 0;
            while (deflater.finished() == false) {
                int n = deflater.deflate(buffer, total, buffer.length - total);
                if (n == 0) {
                    break;
                }
                total += n;
            }
            return total;
        } finally {
            deflater.end();
        }
    }
}
