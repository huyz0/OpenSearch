/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A reference from the top-level cluster metadata manifest to one shard of its index list.
 *
 * <p>The manifest is rewritten in full on every cluster state version and lists every index in the
 * cluster, so it is about 0.8 MB compressed at 100k indices and that cost is paid per version even
 * when nothing changed. Sharding the index list moves those entries into separate immutable blobs, so
 * a version rewrites only the shards holding a changed index and carries the rest forward by
 * reference. `ManifestShardingWriteAmplificationEstimate` measured that at 125x less written for a
 * single changed index at 256 shards. See `rfc-manifest-sharding-design.md`.
 *
 * <p>This type is the reference itself, and it is deliberately the whole of the first slice: nothing
 * writes or reads a shard blob yet. The same order worked for the index descriptor -- serialize first,
 * populate second -- because it keeps the wire format reviewable on its own.
 *
 * <h2>Why the entry count is here</h2>
 *
 * Reading a shard does not need it. It exists so that a reader, and in particular the cleanup sweep,
 * can tell a shard it read completely from one it read partially. That distinction matters more here
 * than elsewhere: {@code RemoteClusterStateCleanupManager} decides what to delete by subtraction, so a
 * shard that silently appears to reference nothing turns into a delete of everything it actually
 * named. That is the same failure class the codec guard in that class already covers, one level down.
 *
 * @opensearch.internal
 */
public class UploadedManifestShard implements Writeable, ToXContentObject {

    static final String SHARD_ID_FIELD = "shard_id";
    static final String BLOB_NAME_FIELD = "blob_name";
    static final String ENTRY_COUNT_FIELD = "entry_count";

    private final int shardId;
    private final String blobName;
    private final int entryCount;

    public UploadedManifestShard(int shardId, String blobName, int entryCount) {
        if (shardId < 0) {
            throw new IllegalArgumentException("manifest shard id must not be negative, got [" + shardId + "]");
        }
        if (entryCount < 0) {
            throw new IllegalArgumentException("manifest shard entry count must not be negative, got [" + entryCount + "]");
        }
        this.shardId = shardId;
        this.blobName = Objects.requireNonNull(blobName, "manifest shard blob name must not be null");
        this.entryCount = entryCount;
    }

    public UploadedManifestShard(StreamInput in) throws IOException {
        this.shardId = in.readVInt();
        this.blobName = in.readString();
        this.entryCount = in.readVInt();
    }

    /** Which partition of the index list this shard holds; the position in the top-level list is not it. */
    public int getShardId() {
        return shardId;
    }

    /**
     * The blob this shard's entries live in. Immutable: a shard that did not change in this version
     * carries the previous version's name forward verbatim, which is where the whole saving comes
     * from.
     */
    public String getBlobName() {
        return blobName;
    }

    /** How many index entries the blob holds, so a truncated read is detectable rather than silent. */
    public int getEntryCount() {
        return entryCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeString(blobName);
        out.writeVInt(entryCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field(SHARD_ID_FIELD, shardId)
            .field(BLOB_NAME_FIELD, blobName)
            .field(ENTRY_COUNT_FIELD, entryCount)
            .endObject();
    }

    /**
     * Hand-written rather than a {@code ConstructingObjectParser}, matching {@link IndexDescriptor}:
     * unknown fields are skipped rather than rejected, so a shard reference written by a newer node
     * with extra fields still parses here.
     */
    public static UploadedManifestShard fromXContent(XContentParser parser) throws IOException {
        Integer shardId = null;
        String blobName = null;
        Integer entryCount = null;

        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        while (token != XContentParser.Token.END_OBJECT && token != null) {
            if (token != XContentParser.Token.FIELD_NAME) {
                token = parser.nextToken();
                continue;
            }
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case SHARD_ID_FIELD:
                    shardId = parser.intValue();
                    break;
                case BLOB_NAME_FIELD:
                    blobName = parser.text();
                    break;
                case ENTRY_COUNT_FIELD:
                    entryCount = parser.intValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
            token = parser.nextToken();
        }

        if (shardId == null || blobName == null || entryCount == null) {
            throw new IOException(
                "manifest shard reference is missing a required field: shard_id="
                    + shardId
                    + " blob_name="
                    + blobName
                    + " entry_count="
                    + entryCount
            );
        }
        return new UploadedManifestShard(shardId, blobName, entryCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UploadedManifestShard that = (UploadedManifestShard) o;
        return shardId == that.shardId && entryCount == that.entryCount && blobName.equals(that.blobName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, blobName, entryCount);
    }

    @Override
    public String toString() {
        return "UploadedManifestShard{shardId=" + shardId + ", blobName='" + blobName + "', entryCount=" + entryCount + '}';
    }
}
