/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.ManifestShardContent;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST;

/**
 * Wrapper class for uploading/downloading
 * one {@link ManifestShardContent} blob to/from remote blob store, the same role {@link
 * RemoteClusterMetadataManifest} plays for the top-level manifest and {@code RemoteIndexMetadata} plays
 * for one index's metadata.
 *
 * <p>Shard blobs live under {@code manifest/shards/} rather than directly under {@code manifest/} --
 * see {@link #getBlobPathParameters()} -- so a listing of the manifest directory for the latest manifest
 * file (see {@code RemoteManifestManager#getLatestManifestFileName}) never has to filter shard blobs out.
 *
 * <p>Unlike the top-level manifest, a shard blob is never listed to find "the latest" one -- it is
 * always addressed by the exact blob name a {@link org.opensearch.gateway.remote.UploadedManifestShard}
 * reference already carries. Its file name still encodes shard id, term, and version for operability
 * (so a blob can be traced back to what wrote it by inspection), but nothing parses those back out of
 * the name the way the manifest's own codec suffix is parsed -- see {@code getManifestShardId()}, kept
 * only for tests and diagnostics.
 */
public class RemoteManifestShard extends AbstractClusterMetadataWriteableBlobEntity<ManifestShardContent> {

    public static final String MANIFEST_SHARDS = "shards";
    public static final String MANIFEST_SHARD_PREFIX = "manifest-shard";
    public static final String MANIFEST_SHARD_NAME_FORMAT = "%s";

    public static final ChecksumBlobStoreFormat<ManifestShardContent> MANIFEST_SHARD_FORMAT = new ChecksumBlobStoreFormat<>(
        "manifest-shard",
        MANIFEST_SHARD_NAME_FORMAT,
        ManifestShardContent::fromXContent
    );

    private ManifestShardContent content;
    private final int shardId;
    private final long clusterTerm;
    private final long stateVersion;

    /** For writing a fresh shard blob. */
    public RemoteManifestShard(
        final ManifestShardContent content,
        final int shardId,
        final long clusterTerm,
        final long stateVersion,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.content = content;
        this.shardId = shardId;
        this.clusterTerm = clusterTerm;
        this.stateVersion = stateVersion;
    }

    /** For reading back a shard blob by its already-known full blob name (from an {@code UploadedManifestShard} reference). */
    public RemoteManifestShard(
        final String blobName,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
        this.shardId = -1;
        this.clusterTerm = -1;
        this.stateVersion = -1;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(MANIFEST, MANIFEST_SHARDS), MANIFEST_SHARD_PREFIX);
    }

    @Override
    public String getType() {
        return MANIFEST_SHARD_PREFIX;
    }

    @Override
    public String generateBlobFileName() {
        // manifest/shards/manifest-shard__<shardId>__<inverted_term>__<inverted_version>__<inverted_timestamp>
        // Immutable once written (see UploadedManifestShard's own javadoc on why): the timestamp
        // component alone guarantees a fresh name every time this is called, which is all "immutable"
        // requires here -- nothing ever overwrites an existing shard blob in place.
        String blobFileName = String.join(
            DELIMITER,
            MANIFEST_SHARD_PREFIX,
            String.valueOf(shardId),
            RemoteStoreUtils.invertLong(clusterTerm),
            RemoteStoreUtils.invertLong(stateVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis())
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(MANIFEST_SHARD_PREFIX, blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        return MANIFEST_SHARD_FORMAT.serialize(content, generateBlobFileName(), getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS)
            .streamInput();
    }

    @Override
    public ManifestShardContent deserialize(final InputStream inputStream) throws IOException {
        return MANIFEST_SHARD_FORMAT.deserialize(blobName, getNamedXContentRegistry(), Streams.readFully(inputStream));
    }
}
