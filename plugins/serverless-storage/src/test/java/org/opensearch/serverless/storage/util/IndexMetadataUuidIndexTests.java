/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.util;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class IndexMetadataUuidIndexTests extends OpenSearchTestCase {

    private static IndexMetadata indexWithUuid(String name, String uuid) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            .build();
        return IndexMetadata.builder(name).settings(settings).build();
    }

    public void testFindsAnIndexByItsUuid() {
        Metadata metadata = Metadata.builder()
            .put(indexWithUuid("alpha", "alpha-uuid"), true)
            .put(indexWithUuid("beta", "beta-uuid"), true)
            .build();

        IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();

        assertEquals("alpha", uuidIndex.findByUuid(metadata, "alpha-uuid").getIndex().getName());
        assertEquals("beta", uuidIndex.findByUuid(metadata, "beta-uuid").getIndex().getName());
    }

    public void testAnUnknownUuidResolvesToNull() {
        Metadata metadata = Metadata.builder().put(indexWithUuid("alpha", "alpha-uuid"), true).build();
        IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();

        assertNull(uuidIndex.findByUuid(metadata, "never-existed-uuid"));
    }

    public void testAnEmptyMetadataResolvesEveryUuidToNull() {
        IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();
        assertNull(uuidIndex.findByUuid(Metadata.EMPTY_METADATA, "anything"));
    }

    public void testRepeatedLookupsAgainstTheSameInstanceStayCorrect() {
        // Not a timing assertion (see T40's own rule against those) -- this proves the cache path taken
        // on the second and third call, not just the rebuild path the first call takes, still returns
        // the right answer.
        Metadata metadata = Metadata.builder()
            .put(indexWithUuid("alpha", "alpha-uuid"), true)
            .put(indexWithUuid("beta", "beta-uuid"), true)
            .put(indexWithUuid("gamma", "gamma-uuid"), true)
            .build();
        IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();

        assertEquals("alpha", uuidIndex.findByUuid(metadata, "alpha-uuid").getIndex().getName());
        assertEquals("beta", uuidIndex.findByUuid(metadata, "beta-uuid").getIndex().getName());
        assertEquals("gamma", uuidIndex.findByUuid(metadata, "gamma-uuid").getIndex().getName());
        assertNull(uuidIndex.findByUuid(metadata, "delta-uuid"));
    }

    public void testANewMetadataInstanceInvalidatesTheCache() {
        Metadata first = Metadata.builder().put(indexWithUuid("alpha", "alpha-uuid"), true).build();
        IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();
        assertEquals("alpha", uuidIndex.findByUuid(first, "alpha-uuid").getIndex().getName());

        // A second, distinct Metadata instance -- built fresh rather than derived from the first, so
        // this is a genuine identity change, not the same instance reused.
        Metadata second = Metadata.builder()
            .put(indexWithUuid("alpha", "alpha-uuid"), true)
            .put(indexWithUuid("beta", "beta-uuid"), true)
            .build();

        assertEquals(
            "an index added in a newer Metadata instance must be found -- a stale cache would still "
                + "answer only from the first instance's contents",
            "beta",
            uuidIndex.findByUuid(second, "beta-uuid").getIndex().getName()
        );
    }
}
