/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexMetadataHolder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link IndexDescriptor} is what lets a node build {@code Metadata} from the manifest alone. It is
 * only useful if it survives the round trip through the manifest exactly, and if the holder it
 * produces answers from the descriptor rather than reaching for the index.
 */
public class IndexDescriptorTests extends OpenSearchTestCase {

    public void testXContentRoundTrip() throws IOException {
        IndexDescriptor original = IndexDescriptor.of(index());

        XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(builder);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytes)) {
            assertEquals(original, IndexDescriptor.fromXContent(parser));
        }
    }

    public void testStreamRoundTrip() throws IOException {
        IndexDescriptor original = IndexDescriptor.of(index());
        assertEquals(original, copyWriteable(original, writableRegistry(), IndexDescriptor::new));
    }

    /** A closed, hidden, system index must round trip as such, not fall back to defaults. */
    public void testNonDefaultFlagsSurviveTheRoundTrip() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder(index())
            .state(IndexMetadata.State.CLOSE)
            .settings(Settings.builder().put(index().getSettings()).put(IndexMetadata.SETTING_INDEX_HIDDEN, true))
            .system(true)
            .build();
        IndexDescriptor original = IndexDescriptor.of(metadata);

        assertEquals(IndexMetadata.State.CLOSE, original.getState());
        assertTrue(original.isHidden());
        assertTrue(original.isSystem());

        XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            IndexDescriptor parsed = IndexDescriptor.fromXContent(parser);
            assertEquals(original, parsed);
            assertEquals(IndexMetadata.State.CLOSE, parsed.getState());
            assertTrue(parsed.isHidden());
            assertTrue(parsed.isSystem());
        }
        assertEquals(original, copyWriteable(original, writableRegistry(), IndexDescriptor::new));
    }

    /** The descriptor has to agree with the index it describes, or the lookup it builds is wrong. */
    public void testDescriptorAgreesWithTheIndexItDescribes() {
        IndexMetadata metadata = index();
        IndexDescriptor descriptor = IndexDescriptor.of(metadata);

        assertEquals(metadata.getState(), descriptor.getState());
        assertEquals(metadata.getAliases(), descriptor.getAliases());
        assertEquals(metadata.isSystem(), descriptor.isSystem());
        assertEquals(metadata.isHidden(), descriptor.isHidden());
        assertEquals(metadata.isRemoteSnapshot(), descriptor.isRemoteSnapshot());
        assertEquals(metadata.isWarmIndex(), descriptor.isWarmIndex());
        assertEquals(metadata.getTotalNumberOfShards(), descriptor.getTotalNumberOfShards());
    }

    /** The holder must answer everything descriptor-level without calling the loader. */
    public void testHolderAnswersFromTheDescriptorWithoutLoading() {
        IndexMetadata metadata = index();
        AtomicInteger loads = new AtomicInteger();
        IndexMetadataHolder holder = IndexDescriptor.of(metadata).toHolder(metadata.getIndex(), () -> {
            loads.incrementAndGet();
            return metadata;
        });

        assertFalse(holder.isResolved());
        assertEquals(metadata.getIndex(), holder.getIndex());
        assertEquals(metadata.getState(), holder.getState());
        assertEquals(metadata.getAliases(), holder.getAliases());
        assertEquals(metadata.isSystem(), holder.isSystem());
        assertEquals(metadata.isHidden(), holder.isHidden());
        assertEquals(metadata.isRemoteSnapshot(), holder.isRemoteSnapshot());
        assertEquals(metadata.isWarmIndex(), holder.isWarmIndex());
        assertEquals(metadata.getTotalNumberOfShards(), holder.getTotalNumberOfShards());
        assertEquals("nothing above should need the index itself", 0, loads.get());

        assertSame(metadata, holder.get());
        assertEquals(1, loads.get());
        assertTrue(holder.isResolved());
    }

    /** An unknown field is a newer writer's, and must be skipped rather than fail the read. */
    public void testUnknownFieldIsSkipped() throws IOException {
        String json = "{\"state\":0,\"system\":false,\"hidden\":false,\"remote_snapshot\":false,\"warm\":false,"
            + "\"total_shards\":6,\"something_from_the_future\":\"x\",\"aliases\":{}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(json))) {
            IndexDescriptor parsed = IndexDescriptor.fromXContent(parser);
            assertEquals(6, parsed.getTotalNumberOfShards());
            assertEquals(IndexMetadata.State.OPEN, parsed.getState());
        }
    }

    private static IndexMetadata index() {
        return IndexMetadata.builder("descriptor-test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "descriptor-test-uuid-0000")
            )
            .numberOfShards(3)
            .numberOfReplicas(1)
            .putAlias(AliasMetadata.builder("descriptor-alias").searchRouting("r").build())
            .build();
    }
}
