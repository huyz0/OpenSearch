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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Whether the tombstone keyspace can be enumerated at all, which decides whether it can be reclaimed.
 *
 * <h2>Why this is a test and not an assumption</h2>
 *
 * Any scheme for reclaiming tombstones has to be able to find them. The obvious way is a prefix listing over
 * {@code tombstones/}, and on S3 that works, because keys there are flat strings and the slash is just a
 * character. On a filesystem repository it is a directory separator, and {@code FsBlobContainer} lists with
 * {@code Files.newDirectoryStream(path, prefix + "*")}, which iterates one directory level only.
 *
 * <p>So the same call enumerates every tombstone on one store and returns nothing on the other, without
 * failing on either. A reclaimer built on it would look correct, log nothing, and free nothing. This branch's
 * own code warns that this divergence "has already bitten this branch three times", which is why it is
 * asserted here rather than reasoned about.
 */
public class TombstoneListabilityTests extends OpenSearchTestCase {

    private static IndexDescriptor descriptor(String name) {
        return IndexDescriptor.from(
            IndexMetadata.builder(name)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                        .build()
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build()
        );
    }

    /**
     * A tombstone exists and is readable by name, and a prefix listing over the tombstone space does not see
     * it.
     *
     * <p>Both halves matter. The first says the tombstone is really there, so the second is a listing
     * limitation rather than a missing write. Together they say a prefix-listing reclaimer is silently a
     * no-op on this store.
     */
    public void testATombstoneIsReadableByNameButInvisibleToAPrefixListing() throws Exception {
        BlobContainer container = new FsBlobStore(1024, createTempDir(), false).blobContainer(BlobPath.cleanPath());
        BlobDescriptorBackend backend = new BlobDescriptorBackend(container);

        backend.create(descriptor("tenant-doomed"));
        backend.putTombstoneAsync(descriptor("tenant-doomed").tombstoned(1_000L));

        IndexDescriptor byName = backend.get("tenant-doomed");
        assertNotNull("the tombstone has to exist for this test to be about listing", byName);
        assertFalse(byName.exists());

        assertTrue(
            "a prefix listing over the tombstone space sees nothing on a filesystem store, because the "
                + "separator makes a directory rather than part of a key. On S3 the same call would return "
                + "every tombstone, so a reclaimer built on it works in production and silently frees "
                + "nothing in a test or on a filesystem repository",
            container.listBlobsByPrefix(BlobDescriptorBackend.TOMBSTONE_PREFIX).isEmpty()
        );
    }

    /**
     * The change log, by contrast, is enumerable on both, because it nests through {@link BlobPath} rather
     * than putting a separator in a blob name.
     *
     * <p>Included so the fix is named by the test rather than left to be rediscovered: the tombstone space
     * needs the same treatment, a child container per slice, not a prefixed key.
     */
    public void testTheChangeLogIsEnumerableBecauseItNestsProperly() throws Exception {
        FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        BlobDescriptorChangeLog log = new BlobDescriptorChangeLog(store::blobContainer, BlobPath.cleanPath());

        log.append(new DescriptorChange("tenant-a", "tenant-a-uuid", DescriptorChange.Kind.UPDATED, 0L));

        assertEquals("nesting through BlobPath is what makes a keyspace walkable on every store", 1, log.since(null).size());
    }
}
