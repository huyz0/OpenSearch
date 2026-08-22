/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * The stored descriptor format says what it is, and refuses what it cannot read.
 *
 * <h2>What this is defending against</h2>
 *
 * {@link IndexDescriptor} appends fields to its wire form without gating them on a version, and these bytes
 * never travel over a transport connection, so there is no version on them to gate against. A reader always
 * believes the newest fields are present. That means every field ever added to that class silently
 * invalidated everything already in the store, and two had been added before this existed.
 *
 * <p>Silently is the operative word. Without a marker, a blob written before a field was added is not
 * detectably different from a current one: the parser reads the name, then reads whichever later fields it
 * believes in, and returns a descriptor assembled from misaligned bytes. A shard count read out of the
 * middle of a uuid is worse than an exception, because it is an answer, and it is an answer that routes.
 *
 * <h2>Why a tombstone makes this matter more than it looks</h2>
 *
 * A live descriptor is rewritten whenever its index changes, so an unreadable one heals on the next write.
 * A tombstone is written once and then has to survive alone for a retention window, which is precisely long
 * enough for someone to add a field. The object that most needs a stable format is the one with no writer
 * left to fix it.
 */
public class StoredDescriptorFormatTests extends OpenSearchTestCase {

    private static IndexDescriptor descriptor() {
        return IndexDescriptor.from(
            IndexMetadata.builder("serverless_tenant-a")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "serverless_tenant-a-uuid")
                        .build()
                )
                .numberOfShards(3)
                .numberOfReplicas(1)
                .build()
        );
    }

    public void testADescriptorSurvivesTheRoundTrip() throws IOException {
        IndexDescriptor original = descriptor();

        IndexDescriptor restored = BlobDescriptorBackend.decode(BlobDescriptorBackend.encode(original));

        assertEquals(original.name(), restored.name());
        assertEquals(original.uuid(), restored.uuid());
        assertEquals(original.shardCount(), restored.shardCount());
        assertEquals(original.routingNumShards(), restored.routingNumShards());
    }

    /** A tombstone's age has to survive storage, since that is the whole reason it is recorded. */
    public void testATombstonesAgeSurvivesTheRoundTrip() throws IOException {
        IndexDescriptor tombstone = descriptor().tombstoned(1_700_000_000_000L);

        IndexDescriptor restored = BlobDescriptorBackend.decode(BlobDescriptorBackend.encode(tombstone));

        assertFalse(restored.exists());
        assertEquals(1_700_000_000_000L, restored.deletedAtMillis());
    }

    /**
     * A blob written before the format was versioned is refused, not misparsed.
     *
     * <p>This is the case the marker exists for. The bytes below are exactly what the old encoder produced,
     * a bare {@code writeTo}, and they are still a perfectly well-formed descriptor of the previous layout.
     * Nothing in them says so.
     */
    public void testAnUnversionedBlobIsRefusedRatherThanMisread() throws IOException {
        BytesReference legacy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            descriptor().writeTo(out);
            legacy = out.bytes();
        }

        IOException thrown = expectThrows(IOException.class, () -> BlobDescriptorBackend.decode(legacy));
        assertTrue(
            "the message has to name the version boundary, or an operator sees an EOF from inside a parser: " + thrown.getMessage(),
            thrown.getMessage().contains("no format marker")
        );
    }

    /** A blob from a newer node is refused with its version named, rather than read as though it were this one. */
    public void testAFutureFormatIsRefusedWithItsVersionNamed() throws IOException {
        BytesReference future;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeInt(0xD3C21B1F);
            out.writeVInt(BlobDescriptorBackend.FORMAT_VERSION + 1);
            descriptor().writeTo(out);
            future = out.bytes();
        }

        IOException thrown = expectThrows(IOException.class, () -> BlobDescriptorBackend.decode(future));
        assertTrue(thrown.getMessage(), thrown.getMessage().contains(String.valueOf(BlobDescriptorBackend.FORMAT_VERSION + 1)));
    }

    /**
     * The longest legal index name still cannot produce bytes that look like the marker.
     *
     * <p>This is the claim the whole scheme rests on, so it is exercised at the boundary rather than argued
     * from. The marker's first three bytes all set the vInt continuation bit, which would describe a name of
     * roughly 450,000 characters, and OpenSearch caps an index name at 255 bytes. A 255-byte name is the
     * closest anything legal can get: its length vInt is two bytes, not three, so the third byte is already
     * name data and the prefixes diverge.
     *
     * <p>Asserted by refusal rather than by comparing bytes, because refusal is the behaviour that matters:
     * a collision would show up as a legacy blob being accepted and misread.
     */
    public void testTheLongestLegalNameCannotLookLikeTheMarker() throws IOException {
        String longestName = "n".repeat(255);
        IndexDescriptor named = IndexDescriptor.from(
            IndexMetadata.builder(longestName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "long-uuid")
                        .build()
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build()
        );

        BytesReference legacy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            named.writeTo(out);
            legacy = out.bytes();
        }

        IOException thrown = expectThrows(IOException.class, () -> BlobDescriptorBackend.decode(legacy));
        assertTrue(thrown.getMessage(), thrown.getMessage().contains("no format marker"));
    }

    /** Truncated or foreign bytes are refused too, rather than read as a descriptor of some shape. */
    public void testTooShortToHoldAMarkerIsRefused() {
        expectThrows(
            IOException.class,
            () -> BlobDescriptorBackend.decode(new org.opensearch.core.common.bytes.BytesArray(new byte[] { 1, 2 }))
        );
    }
}
