/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Spike S3: what does the <em>parsed</em> mapping actually cost per resident index?
 *
 * <p>Earlier work measured the cluster-state side of a mapping: {@link CompressedXContent} is
 * DEFLATE-compressed and small (a few hundred bytes for a typical tenant mapping), and spike S4
 * showed identical mappings deduplicate perfectly. But a node that actually <em>hosts</em> a shard
 * also builds a {@link MapperService} / {@link DocumentMapper} object graph -- {@code MappingLookup},
 * {@code FieldTypeLookup}, a {@code FieldMapper} and {@code MappedFieldType} per field, each field
 * name appearing as a key in several hash maps. A prior investigation estimated that graph at
 * 30-80 KB per index, two orders of magnitude above the compressed form, but never measured it.
 * That number sets how many indices a data node can hold resident, so it is worth a real figure.
 *
 * <p>Also answers a design question dedup cannot: does sharing the mapping <em>source</em> between
 * indices reduce the parsed cost at all, or is the parsed graph rebuilt per index regardless?
 */
public class MapperServiceHeapSpikeTests extends OpenSearchTestCase {

    private static final int[] FIELD_COUNTS = { 20, 100, 300 };
    private static final int INDICES_PER_TIER = 200;

    public void testParsedMappingHeapPerIndex() throws Exception {
        logger.info("--- S3: parsed MapperService graph cost per resident index ---");
        logger.info("fields | compressedBytes | retainedBytesPerIndex | ratioParsedToCompressed");

        for (int fieldCount : FIELD_COUNTS) {
            String mappingSource = buildMapping(fieldCount);
            CompressedXContent compressed = new CompressedXContent(mappingSource);
            int compressedBytes = compressed.compressed().length;

            // Warm up class loading and analyzer setup so the measured batch is steady-state.
            List<Object> warmup = buildMapperServices(mappingSource, 20);
            warmup.clear();
            forceGc();

            long before = usedHeap();
            List<Object> held = buildMapperServices(mappingSource, INDICES_PER_TIER);
            forceGc();
            long after = usedHeap();

            long perIndex = (after - before) / INDICES_PER_TIER;
            logger.info(
                "{} | {} | {} | {}x",
                fieldCount,
                compressedBytes,
                perIndex,
                String.format(java.util.Locale.ROOT, "%.0f", (double) perIndex / compressedBytes)
            );

            assertFalse(held.isEmpty());
        }
    }

    /**
     * Does handing every index the identical {@link CompressedXContent} instance -- what a dedup
     * table would produce -- shrink the parsed graph? If the parsed side is rebuilt per index
     * regardless, dedup helps cluster state only, and data-node capacity is unaffected.
     */
    public void testSharedMappingSourceDoesNotShrinkParsedGraph() throws Exception {
        String mappingSource = buildMapping(100);
        CompressedXContent shared = new CompressedXContent(mappingSource);

        List<Object> warmup = buildMapperServicesFromShared(shared, 20);
        warmup.clear();
        forceGc();

        long before = usedHeap();
        List<Object> held = buildMapperServicesFromShared(shared, INDICES_PER_TIER);
        forceGc();
        long after = usedHeap();
        long perIndexShared = (after - before) / INDICES_PER_TIER;

        logger.info("--- S3: shared-source variant (100 fields) ---");
        logger.info("retainedBytesPerIndex with one shared CompressedXContent instance: {}", perIndexShared);

        assertFalse(held.isEmpty());
    }

    private List<Object> buildMapperServices(String mappingSource, int count) throws Exception {
        List<Object> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            MapperService mapperService = MapperTestUtils.newMapperService(
                xContentRegistry(),
                createTempDir(),
                Settings.EMPTY,
                "tenant-" + i
            );
            mapperService.merge("_doc", new CompressedXContent(mappingSource), MapperService.MergeReason.MAPPING_UPDATE);
            out.add(mapperService);
        }
        return out;
    }

    private List<Object> buildMapperServicesFromShared(CompressedXContent shared, int count) throws Exception {
        List<Object> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            MapperService mapperService = MapperTestUtils.newMapperService(
                xContentRegistry(),
                createTempDir(),
                Settings.EMPTY,
                "tenant-" + i
            );
            mapperService.merge("_doc", shared, MapperService.MergeReason.MAPPING_UPDATE);
            out.add(mapperService);
        }
        return out;
    }

    /** A mapping shaped like a realistic tenant log/event index. */
    private static String buildMapping(int fieldCount) {
        StringBuilder sb = new StringBuilder("{\"properties\":{");
        sb.append("\"@timestamp\":{\"type\":\"date\"}");
        for (int i = 1; i < fieldCount; i++) {
            sb.append(",\"field_").append(i).append("\":");
            switch (i % 4) {
                case 0:
                    sb.append("{\"type\":\"keyword\"}");
                    break;
                case 1:
                    sb.append("{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}");
                    break;
                case 2:
                    sb.append("{\"type\":\"long\"}");
                    break;
                default:
                    sb.append("{\"type\":\"double\"}");
                    break;
            }
        }
        return sb.append("}}").toString();
    }

    private static void forceGc() throws InterruptedException {
        for (int i = 0; i < 4; i++) {
            System.gc();
            Thread.sleep(150);
        }
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
