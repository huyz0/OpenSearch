/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Persists and retrieves {@link CommitManifest}s against a real {@link BlobContainer}, using each
 * manifest's canonical {@link CommitManifest#manifestName()} as its blob name
 * (rfc-serverless-opensearch.md &sect;6.1/&sect;6.3).
 */
public final class BlobContainerManifestStore {

    private final BlobContainer blobContainer;

    /**
     * Creates a manifest store backed by the given blob container.
     *
     * @param blobContainer the blob container to persist manifests in
     */
    public BlobContainerManifestStore(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /**
     * Writes {@code manifest} under its canonical name. Manifests are immutable, so this always
     * writes a brand new blob. Uses {@link BlobContainer#writeBlobAtomic} rather than plain
     * {@code writeBlob}: a reader must never observe a partially-written manifest.
     *
     * @param manifest the manifest to write
     */
    public void writeManifest(CommitManifest manifest) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        manifest.writeTo(out);
        byte[] bytes = BytesReference.toBytes(out.bytes());
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            blobContainer.writeBlobAtomic(manifest.manifestName(), in, bytes.length, true);
        }
    }

    /**
     * Reads and deserializes the manifest identified by {@code (primaryTerm, generation)}.
     *
     * @param primaryTerm the primary term of the manifest to read
     * @param generation the generation of the manifest to read
     * @return the deserialized manifest
     */
    public CommitManifest readManifest(long primaryTerm, long generation) throws IOException {
        String name = CommitManifest.manifestName(primaryTerm, generation);
        try (InputStream in = blobContainer.readBlob(name)) {
            return new CommitManifest(StreamInput.wrap(in.readAllBytes()));
        }
    }

    /**
     * Whether a manifest for this (primaryTerm, generation) has already been written.
     *
     * @param primaryTerm the primary term to check
     * @param generation the generation to check
     * @return {@code true} if a manifest already exists for this (primaryTerm, generation)
     */
    public boolean manifestExists(long primaryTerm, long generation) throws IOException {
        return blobContainer.blobExists(CommitManifest.manifestName(primaryTerm, generation));
    }

    /**
     * Every manifest ever written to this shard's container, read and deserialized in full. This
     * is the one operation in this class that legitimately enumerates storage wholesale -- every
     * other method here is a targeted read/write by exact name -- so it is expected to be slow and
     * called rarely (retention-policy evaluation, not any request path), the same tradeoff {@link
     * org.opensearch.serverless.storage.directory.DirectoryRebuildService} already makes for the
     * same reason.
     *
     * <p>A best-effort snapshot, not a transaction: a manifest present in the initial listing can
     * legitimately vanish before its own read completes if {@code GcSchedulerTask}'s deletion sweep
     * races this call (found by a real concurrent-sweep test throwing {@code NoSuchFileException}
     * here, not assumed) -- silently skipped rather than failing the whole listing, since a manifest
     * that GC has already deleted was, by definition, already both superseded and safe to omit from
     * any caller's view. {@code NoSuchFileException} specifically because {@code FsBlobContainer} is
     * this plugin's only backend today (see {@code ServerlessStoragePlugin}'s own class javadoc); a
     * future non-FS backend would need its own equivalent "blob vanished mid-list" exception handled
     * here too.
     */
    public List<CommitManifest> listManifests() throws IOException {
        List<CommitManifest> manifests = new ArrayList<>();
        for (String blobName : blobContainer.listBlobsByPrefix(CommitManifest.NAME_PREFIX).keySet()) {
            try (InputStream in = blobContainer.readBlob(blobName)) {
                manifests.add(new CommitManifest(StreamInput.wrap(in.readAllBytes())));
            } catch (NoSuchFileException e) {
                // Concurrently deleted by a GC sweep between the listing above and this read --
                // see this method's own javadoc.
            }
        }
        return manifests;
    }

    /**
     * Deletes exactly the given manifests, ignoring any already absent. Callers must have already
     * proven each one is safe to delete (rfc-serverless-opensearch.md &sect;6.5:
     * {@link org.opensearch.serverless.storage.gc.ManifestRetentionPolicy}) -- this class has no
     * opinion on that decision. Deleting a manifest does not touch the bundles it referenced; a
     * caller sweeping both must delete bundles first (see {@code
     * BlobContainerBundleStore#deleteBundles}'s own ordering note), so that a crash between the two
     * steps leaves, at worst, a still-listed manifest pointing at already-gone bundles -- itself
     * still correctly classified deletable and retried by the very next sweep -- rather than an
     * orphaned bundle no future sweep would ever revisit.
     *
     * @param manifests the manifests to delete
     */
    public void deleteManifests(Collection<CommitManifest> manifests) throws IOException {
        if (manifests.isEmpty()) {
            return;
        }
        List<String> names = new ArrayList<>(manifests.size());
        for (CommitManifest manifest : manifests) {
            names.add(manifest.manifestName());
        }
        blobContainer.deleteBlobsIgnoringIfNotExists(names);
    }
}
