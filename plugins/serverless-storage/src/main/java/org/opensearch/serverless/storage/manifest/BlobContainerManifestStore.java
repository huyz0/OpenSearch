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
import java.util.Map;

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
        return listManifests(null);
    }

    /**
     * Same as {@link #listManifests()}, except a manifest whose blob name is already a key in
     * {@code readCache} is served from there instead of a fresh {@code readBlob} call, and any
     * manifest newly read here is added to it. {@code null} disables caching entirely -- exactly
     * {@link #listManifests()}'s own behavior.
     *
     * <p>Safe because manifests are immutable and write-once (see {@link #writeManifest}'s own
     * javadoc): once a given blob name has been read successfully, its content can never change
     * later, only the blob's continued existence can (a GC sweep deleting it). This method still
     * lists the container fresh every call -- so a manifest's existence is always current, and a
     * newly-written manifest never seen before is always read fresh -- only the body of an
     * already-seen, still-existing manifest is served from {@code readCache} rather than re-fetched.
     * A caller that owns and reuses the same {@code readCache} instance across many calls (e.g. a
     * recurring per-shard scheduler task, one cache per task instance) turns what would otherwise be
     * a full re-read of every retained manifest on every call into a read of only the manifests
     * newly written since the last call -- the same size as the container's actual per-tick growth,
     * not its whole retained history.
     *
     * @param readCache a mutable map this call may read from and add to, or {@code null} to disable
     *                  caching; the caller owns its lifetime (typically one instance per recurring
     *                  task, node-local and reset on restart). Entries for a blob name no longer
     *                  present in this call's own fresh listing are evicted before returning, so the
     *                  cache stays bounded to the container's currently retained manifests -- never
     *                  every manifest the shard has ever written -- rather than growing unboundedly
     *                  as GC deletes old ones out from under it.
     */
    public List<CommitManifest> listManifests(Map<String, CommitManifest> readCache) throws IOException {
        List<CommitManifest> manifests = new ArrayList<>();
        java.util.Set<String> currentBlobNames = blobContainer.listBlobsByPrefix(CommitManifest.NAME_PREFIX).keySet();
        for (String blobName : currentBlobNames) {
            CommitManifest cached = readCache == null ? null : readCache.get(blobName);
            if (cached != null) {
                manifests.add(cached);
                continue;
            }
            try (InputStream in = blobContainer.readBlob(blobName)) {
                CommitManifest manifest = new CommitManifest(StreamInput.wrap(in.readAllBytes()));
                manifests.add(manifest);
                if (readCache != null) {
                    readCache.put(blobName, manifest);
                }
            } catch (NoSuchFileException e) {
                // Concurrently deleted by a GC sweep between the listing above and this read --
                // see this method's own javadoc.
            }
        }
        if (readCache != null) {
            readCache.keySet().retainAll(currentBlobNames);
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
