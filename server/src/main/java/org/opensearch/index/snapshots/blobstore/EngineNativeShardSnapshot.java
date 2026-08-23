/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.snapshots.blobstore;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;

import java.io.IOException;
import java.util.Objects;

/**
 * Shard snapshot metadata for an engine-native snapshot -- one written by {@code
 * BlobStoreRepository#snapshotEngineNative} when a shard's {@link
 * org.opensearch.index.engine.Engine#attemptEngineNativeSnapshot} returned a non-empty pointer
 * instead of a copyable local Lucene commit. Unlike {@link BlobStoreIndexShardSnapshot} (classic,
 * one entry per uploaded file) or {@link RemoteStoreShardShallowCopySnapshot} (remote-store
 * shallow copy, one entry per remote-store file name), this type carries no file-level detail at
 * all -- just an opaque byte payload the producing engine alone can interpret, round-tripped
 * verbatim by core.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class EngineNativeShardSnapshot implements ToXContentFragment, IndexShardSnapshot {

    private final String snapshot;
    private final String engineId;
    private final long startTime;
    private final long time;
    private final byte[] payload;

    static final String NAME = "name";
    static final String ENGINE_ID = "engine_id";
    static final String START_TIME = "start_time";
    static final String TIME = "time";
    static final String PAYLOAD = "payload";

    private static final ParseField PARSE_NAME = new ParseField(NAME);
    private static final ParseField PARSE_ENGINE_ID = new ParseField(ENGINE_ID);
    private static final ParseField PARSE_START_TIME = new ParseField(START_TIME);
    private static final ParseField PARSE_TIME = new ParseField(TIME);
    private static final ParseField PARSE_PAYLOAD = new ParseField(PAYLOAD);

    public EngineNativeShardSnapshot(String snapshot, String engineId, long startTime, long time, byte[] payload) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.engineId = Objects.requireNonNull(engineId, "engineId");
        this.startTime = startTime;
        this.time = time;
        this.payload = Objects.requireNonNull(payload, "payload");
    }

    /** The snapshot name this shard-level record belongs to. */
    public String snapshot() {
        return snapshot;
    }

    /**
     * Opaque tag identifying which engine produced
     * (and, on delete, must release) this snapshot -- core only ever compares it for equality
     * against a registered releaser's own tag, never interprets it.
     */
    public String engineId() {
        return engineId;
    }

    public long startTime() {
        return startTime;
    }

    public long time() {
        return time;
    }

    /**
     * The exact bytes {@link org.opensearch.index.engine.Engine#attemptEngineNativeSnapshot}
     * returned at snapshot-creation time -- opaque to core, handed back verbatim to {@link
     * org.opensearch.index.shard.ShardRecoveryStrategy.EngineNativeSnapshots#restore} on restore and
     * to {@link org.opensearch.index.shard.ShardRecoveryStrategy.EngineNativeSnapshots#release} on delete.
     */
    public byte[] payload() {
        return payload;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, snapshot);
        builder.field(ENGINE_ID, engineId);
        builder.field(START_TIME, startTime);
        builder.field(TIME, time);
        builder.field(PAYLOAD, payload);
        return builder;
    }

    public static EngineNativeShardSnapshot fromXContent(XContentParser parser) throws IOException {
        String snapshot = null;
        String engineId = null;
        long startTime = 0;
        long time = 0;
        byte[] payload = null;

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        XContentParser.Token token;
        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (PARSE_NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                    snapshot = parser.text();
                } else if (PARSE_ENGINE_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    engineId = parser.text();
                } else if (PARSE_START_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    startTime = parser.longValue();
                } else if (PARSE_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    time = parser.longValue();
                } else if (PARSE_PAYLOAD.match(currentFieldName, parser.getDeprecationHandler())) {
                    payload = parser.binaryValue();
                } else {
                    throw new OpenSearchParseException("unknown parameter [{}]", currentFieldName);
                }
            } else {
                parser.skipChildren();
            }
        }

        if (snapshot == null || engineId == null || payload == null) {
            throw new OpenSearchParseException("missing required field(s) in engine-native shard snapshot");
        }
        return new EngineNativeShardSnapshot(snapshot, engineId, startTime, time, payload);
    }

    @Override
    public IndexShardSnapshotStatus getIndexShardSnapshotStatus() {
        // No file-level detail exists for an opaque payload -- file/byte counts are meaningless
        // here, same accepted limitation RemoteStoreShardShallowCopySnapshot already has for the
        // shallow-copy path. payload.length is reported as totalSize purely as a rough diagnostic,
        // not a real byte-accounting figure.
        return IndexShardSnapshotStatus.newDone(startTime, time, 0, 0, 0, payload.length, null);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EngineNativeShardSnapshot that = (EngineNativeShardSnapshot) obj;
        return startTime == that.startTime
            && time == that.time
            && Objects.equals(snapshot, that.snapshot)
            && Objects.equals(engineId, that.engineId)
            && java.util.Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(snapshot, engineId, startTime, time);
        result = 31 * result + java.util.Arrays.hashCode(payload);
        return result;
    }
}
