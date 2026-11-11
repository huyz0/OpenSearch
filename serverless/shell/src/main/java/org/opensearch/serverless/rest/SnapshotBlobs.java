/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.SnapshotRecord;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Copies a shard's published files between the live index and a snapshot, and carries the index settings a
 * snapshot keeps. Split out of {@link SnapshotHandler}.
 */
final class SnapshotBlobs {

    private SnapshotBlobs() {}

    /**
     * An index's settings as a snapshot records them: the structured form the descriptor itself writes,
     * so a list-valued setting (an analyzer's filters) comes back as a list rather than as the bracketed
     * string {@code Settings#get} would render it.
     */
    static Map<String, Object> settingsOf(IndexDescriptor descriptor) throws IOException {
        try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.startObject();
            descriptor.extraSettings().toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return org.opensearch.common.xcontent.XContentHelper.convertToMap(
                BytesReference.bytes(builder),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
        }
    }

    /**
     * The captured settings a restored index is created with: everything core's restore carries over,
     * and none of what it does not. The uuid, creation date, version and provided name are the new
     * index's own; the shard count is structural and travels as {@code numberOfShards}; replicas are
     * refused here. A descriptor never stores these in its extra settings, but a snapshot record is data
     * this handler did not necessarily write, and a restored descriptor carrying another index's uuid is
     * a shard that opens under the wrong storage path.
     */
    static org.opensearch.common.settings.Settings restorableSettings(Map<String, Object> captured) {
        if (captured.isEmpty()) {
            return org.opensearch.common.settings.Settings.EMPTY;
        }
        final org.opensearch.common.settings.Settings.Builder settings = org.opensearch.common.settings.Settings.builder()
            .loadFromMap(captured)
            .normalizePrefix(org.opensearch.cluster.metadata.IndexMetadata.INDEX_SETTING_PREFIX);
        for (String key : new String[] {
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_HISTORY_UUID,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_UPGRADED,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_PROVIDED_NAME,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS }) {
            settings.remove(key);
        }
        return settings.build();
    }

    /**
     * Copies every blob a captured shard's manifest names into a new location, under one fresh term
     * container. Used both by a standard snapshot's own capture (source: the live index; destination: the
     * repository's own storage) and by every restore (source: either of those, by
     * {@link SnapshotRecord#shallow()}; destination: the new index being created) — one copy routine, not
     * two, because both are the same operation with different endpoints.
     *
     * @param blobStore the backing store
     * @param sourceBase where the manifest's blob paths are relative to
     * @param destBase where to copy them to
     * @param source the manifest to copy
     * @return the manifest to record or publish at the destination
     * @throws IOException if a referenced blob is missing, or copying fails
     */
    static CommitManifest copyShardBlobs(BlobStore blobStore, BlobPath sourceBase, BlobPath destBase, CommitManifest source)
        throws IOException {
        final String destTermDir = SegmentPublisher.termSegment(1L);
        final BlobContainer destContainer = blobStore.blobContainer(destBase.add(destTermDir));
        final Map<String, String> destFiles = new LinkedHashMap<>();
        final Map<String, Long> lengths = new LinkedHashMap<>();
        final Map<String, java.util.Set<String>> missingByTerm = new LinkedHashMap<>();
        for (var file : source.files().entrySet()) {
            final Long known = source.lengthOf(file.getKey());
            if (known != null) {
                if (known < 0L) {
                    throw new IOException("snapshot contains a negative file length");
                }
                lengths.put(file.getKey(), known);
            } else {
                missingByTerm.computeIfAbsent(file.getValue(), ignored -> new java.util.HashSet<>()).add(file.getKey());
            }
        }
        // Older manifests have no lengths. Page their term inventory rather than retaining every
        // historical file in that term, and stop as soon as the captured files have been found.
        for (var term : missingByTerm.entrySet()) {
            final BlobContainer container = blobStore.blobContainer(sourceBase.add(term.getKey()));
            String cursor = null;
            while (term.getValue().isEmpty() == false) {
                final var page = container.listBlobsByPrefix("", cursor, 1000);
                for (BlobMetadata metadata : page) {
                    if (term.getValue().remove(metadata.name())) {
                        lengths.put(metadata.name(), metadata.length());
                    }
                }
                if (page.size() < 1000) {
                    break;
                }
                final String next = page.get(page.size() - 1).name();
                if (next.equals(cursor)) {
                    throw new IOException("snapshot source listing did not advance");
                }
                cursor = next;
            }
            if (term.getValue().isEmpty() == false) {
                throw new IOException("snapshot references blobs that no longer exist in " + term.getKey());
            }
        }
        for (var file : source.files().entrySet()) {
            final String name = file.getKey();
            final org.opensearch.serverless.store.FileLocation location = source.location(name);
            final BlobContainer sourceContainer = blobStore.blobContainer(location.containerPath(sourceBase));
            // S3 requires mark/reset for upload retries. Reopen a ranged read on reset rather
            // than buffering a potentially multi-gigabyte segment in memory. A packed file is copied out of its pack
            // to a blob of its own.
            try (
                var input = new org.opensearch.serverless.store.RepeatableBlobInputStream(
                    sourceContainer,
                    location.blob(),
                    location.isPacked() ? location.offset() : -1L,
                    lengths.get(name)
                )
            ) {
                destContainer.writeBlob(name, input, lengths.get(name), false);
            }
            destFiles.put(name, destTermDir);
        }
        return new CommitManifest(1L, destFiles, null, lengths);
    }
}
