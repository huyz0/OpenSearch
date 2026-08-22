/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Spike S4: does content-addressed mapping deduplication actually pay off for a multi-tenant fleet?
 *
 * <p>An earlier investigation established that OpenSearch has no cross-index mapping dedup at all
 * (Elasticsearch 8.x added {@code mappingsByHash}; OpenSearch never ported it), and that adding it
 * would collapse the cluster-state mapping cost from N copies to one shared instance plus a
 * reference each. That arithmetic assumes tenants provisioned from a common template actually
 * produce <em>byte-identical</em> {@link CompressedXContent}. If anything index-specific leaks into
 * the stored mapping -- an index name in {@code _meta}, non-deterministic field ordering, a
 * per-index dynamic field -- the identity check never fires and the whole saving evaporates.
 *
 * <p>This measures that assumption rather than trusting it, and also quantifies how quickly the
 * saving degrades once tenants diverge, since real fleets are rarely perfectly homogeneous.
 */
public class MappingDedupRealismSpikeTests extends OpenSearchTestCase {

    private static final String TENANT_MAPPING = "{\"properties\":{"
        + "\"@timestamp\":{\"type\":\"date\"},"
        + "\"tenant_id\":{\"type\":\"keyword\"},"
        + "\"level\":{\"type\":\"keyword\"},"
        + "\"message\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},"
        + "\"status_code\":{\"type\":\"long\"},"
        + "\"latency_ms\":{\"type\":\"double\"},"
        + "\"service\":{\"type\":\"keyword\"},"
        + "\"host\":{\"type\":\"keyword\"}"
        + "}}";

    /**
     * The core question: N indices built from one identical mapping source. Do their stored
     * {@link MappingMetadata} instances hash and compare equal, so a canonicalization table would
     * collapse them to one?
     */
    public void testIdenticalMappingSourceCollapsesToOneInstance() throws Exception {
        int indexCount = 1000;
        Map<CompressedXContent, Integer> distinctByContent = new HashMap<>();
        Map<Integer, Integer> distinctByHash = new HashMap<>();

        for (int i = 0; i < indexCount; i++) {
            IndexMetadata indexMetadata = tenantIndex("tenant-" + i, TENANT_MAPPING);
            CompressedXContent source = indexMetadata.mapping().source();
            distinctByContent.merge(source, 1, Integer::sum);
            distinctByHash.merge(source.hashCode(), 1, Integer::sum);
        }

        logger.info("--- S4: identical mapping source across {} tenant indices ---", indexCount);
        logger.info("distinct by equals(): {}", distinctByContent.size());
        logger.info("distinct by hashCode() (the CRC32 a dedup table would key on): {}", distinctByHash.size());

        assertEquals("byte-identical mappings must collapse to one entry for dedup to pay off", 1, distinctByContent.size());
        assertEquals("CRC32 hash must also collapse, since that is the proposed table key", 1, distinctByHash.size());

        // Confirm they are genuinely separate instances today -- i.e. there is a real saving to
        // capture, not something the JVM or the builder already shares.
        IndexMetadata a = tenantIndex("tenant-a", TENANT_MAPPING);
        IndexMetadata b = tenantIndex("tenant-b", TENANT_MAPPING);
        assertEquals(a.mapping().source(), b.mapping().source());
        assertNotSame(
            "if these were already the same instance there would be nothing for dedup to save",
            a.mapping().source(),
            b.mapping().source()
        );
    }

    /**
     * Degradation curve: how much of the saving survives when a fraction of tenants carry a
     * per-tenant field? A fleet where every tenant has even one unique field gets nothing from
     * dedup, so the shape of this curve decides whether the optimization is worth shipping.
     */
    public void testSavingDegradesAsTenantsDiverge() throws Exception {
        int indexCount = 1000;
        logger.info("--- S4: dedup effectiveness vs tenant divergence ({} indices) ---", indexCount);
        logger.info("divergentPct | distinctMappings | dedupRatio");

        for (int divergentPct : new int[] { 0, 1, 10, 50, 100 }) {
            Map<CompressedXContent, Integer> distinct = new HashMap<>();
            for (int i = 0; i < indexCount; i++) {
                boolean divergent = (i % 100) < divergentPct;
                String mapping = divergent
                    // A per-tenant field is the realistic way a fleet diverges: same template,
                    // plus something dynamic or tenant-specific.
                    ? TENANT_MAPPING.replace("\"properties\":{", "\"properties\":{\"tenant_field_" + i + "\":{\"type\":\"keyword\"},")
                    : TENANT_MAPPING;
                distinct.merge(tenantIndex("tenant-" + i, mapping).mapping().source(), 1, Integer::sum);
            }
            double ratio = (double) indexCount / distinct.size();
            logger.info("{} | {} | {}x", divergentPct, distinct.size(), String.format(java.util.Locale.ROOT, "%.1f", ratio));
        }
    }

    /** Does the stored mapping stay byte-identical regardless of index name/UUID/creation date? */
    public void testStoredMappingCarriesNoIndexIdentity() throws Exception {
        CompressedXContent first = tenantIndex("aaa", TENANT_MAPPING).mapping().source();
        CompressedXContent second = tenantIndex("zzz-with-a-much-longer-name", TENANT_MAPPING).mapping().source();
        assertEquals("index identity must not leak into the stored mapping, or dedup can never fire", first, second);
    }

    /**
     * Builds an index the way a tenant would be provisioned: same mapping source, but index name,
     * UUID and creation date all differ. Those three are exactly what would break dedup if they
     * leaked into the stored mapping.
     */
    private static IndexMetadata tenantIndex(String name, String mappingSource) throws Exception {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, randomAlphaOfLength(22))
                    .put(IndexMetadata.SETTING_CREATION_DATE, randomNonNegativeLong())
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putMapping(mappingSource)
            .build();
    }
}
