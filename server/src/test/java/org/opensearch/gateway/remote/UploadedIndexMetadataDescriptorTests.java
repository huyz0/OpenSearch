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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * The manifest is a persisted format, so the descriptor added at
 * {@link ClusterMetadataManifest#CODEC_V5} has to be additive in both directions: a V5 manifest must
 * round trip with the descriptor intact, and a manifest written before V5 -- or by a V5 writer that
 * was not configured to include it -- must read exactly as it did before.

 * <p>The codec itself goes to V5 for everyone, as every prior bump did. Making it depend on whether any
 * entry carries a descriptor was tried and reverted: it breaks the invariant that the writer always
 * writes {@code MANIFEST_CURRENT_CODEC_VERSION}, and it lets a cluster flap between V4 and V5 as
 * descriptor-carrying indices come and go.
 */
public class UploadedIndexMetadataDescriptorTests extends OpenSearchTestCase {

    public void testV5EntryRoundTripsWithItsDescriptor() throws IOException {
        IndexDescriptor descriptor = IndexDescriptor.of(index());
        UploadedIndexMetadata original = v5Entry(descriptor);

        UploadedIndexMetadata parsed = parse(original, ClusterMetadataManifest.CODEC_V5);

        assertEquals(original, parsed);
        assertNotNull(parsed.getDescriptor());
        assertEquals(descriptor, parsed.getDescriptor());
    }

    /** A V5 writer with the descriptor turned off writes an entry that reads back without one. */
    public void testV5EntryWithoutADescriptorIsFine() throws IOException {
        UploadedIndexMetadata original = v5Entry(null);

        UploadedIndexMetadata parsed = parse(original, ClusterMetadataManifest.CODEC_V5);

        assertEquals(original, parsed);
        assertNull(parsed.getDescriptor());
    }

    /**
     * The load-bearing back-compat case. An entry from a pre-V5 manifest carries no descriptor field at
     * all, and parsing it at its own codec must produce exactly what it did before, not an entry
     * stamped V5.
     */
    public void testPreV5EntryStillParsesAtItsOwnCodec() throws IOException {
        UploadedIndexMetadata original = new UploadedIndexMetadata("idx", "idx-uuid", "file-name");

        UploadedIndexMetadata parsed = parse(original, ClusterMetadataManifest.CODEC_V2);

        assertEquals(original, parsed);
        assertNull("a V2 entry has no descriptor", parsed.getDescriptor());
    }

    /** The descriptor must survive the transport stream form too, not just the blob form. */
    public void testStreamRoundTripCarriesTheDescriptor() throws IOException {
        UploadedIndexMetadata original = v5Entry(IndexDescriptor.of(index()));

        UploadedIndexMetadata parsed = copyWriteable(original, writableRegistry(), UploadedIndexMetadata::new);

        assertEquals(original, parsed);
        assertEquals(original.getDescriptor(), parsed.getDescriptor());
    }

    public void testStreamRoundTripWithoutADescriptor() throws IOException {
        UploadedIndexMetadata original = new UploadedIndexMetadata("idx", "idx-uuid", "file-name");

        UploadedIndexMetadata parsed = copyWriteable(original, writableRegistry(), UploadedIndexMetadata::new);

        assertEquals(original, parsed);
        assertNull(parsed.getDescriptor());
    }

    /** Two entries differing only in their descriptor are not the same entry. */
    public void testDescriptorParticipatesInEquality() {
        UploadedIndexMetadata withDescriptor = v5Entry(IndexDescriptor.of(index()));
        UploadedIndexMetadata without = v5Entry(null);

        assertNotEquals(withDescriptor, without);
        assertNotEquals(withDescriptor.hashCode(), without.hashCode());
    }

    /**
     * A V5 parser can read a pre-V5 entry, because the descriptor is optional. That is exactly why the
     * dispatch has to route by the manifest's codec instead of always using the newest parser: parsing a
     * V2 entry with the V5 parser succeeds but stamps it V5, and an entry's codec is what decides which
     * fields it writes back out.
     */
    public void testAV2EntryParsedAtV5WouldBeStampedV5() throws IOException {
        UploadedIndexMetadata original = new UploadedIndexMetadata("idx", "idx-uuid", "file-name");

        UploadedIndexMetadata atOwnCodec = parse(original, ClusterMetadataManifest.CODEC_V2);
        UploadedIndexMetadata atV5 = parse(original, ClusterMetadataManifest.CODEC_V5);

        // Both round trip the data; the difference is the codec they carry forward, which is why the
        // dispatch matters.
        assertEquals(original, atOwnCodec);
        assertEquals(original, atV5);
        assertNull(atOwnCodec.getDescriptor());
        assertNull(atV5.getDescriptor());
    }

    private static UploadedIndexMetadata v5Entry(IndexDescriptor descriptor) {
        return new UploadedIndexMetadata(
            "idx",
            "idx-uuid",
            "file-name",
            UploadedIndexMetadata.COMPONENT_PREFIX,
            ClusterMetadataManifest.CODEC_V5,
            descriptor
        );
    }

    private UploadedIndexMetadata parse(UploadedIndexMetadata entry, long codecVersion) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        entry.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            return UploadedIndexMetadata.fromXContent(parser, codecVersion);
        }
    }

    private static IndexMetadata index() {
        return IndexMetadata.builder("idx")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "idx-uuid")
            )
            .numberOfShards(2)
            .numberOfReplicas(1)
            .putAlias(AliasMetadata.builder("idx-alias").build())
            .build();
    }
}
