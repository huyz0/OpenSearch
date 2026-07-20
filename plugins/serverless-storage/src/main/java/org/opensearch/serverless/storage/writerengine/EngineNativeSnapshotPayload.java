/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.serverless.storage.manifest.CommitManifest;

import java.io.IOException;

/**
 * The wire format for this plugin's {@code Engine#attemptEngineNativeSnapshot} pointer bytes --
 * entirely opaque to core, read and written only by this plugin's own {@link
 * ObjectStoreWriterEngine#attemptEngineNativeSnapshot} and {@link EngineNativeSnapshotSupport}.
 *
 * <p>Carries the pinned {@link CommitManifest} (which already carries its own {@code indexUuid}/
 * {@code shardId}, enough to resolve the original shard's container at restore/release time) plus
 * a separate {@code pinId} -- deliberately <b>not</b> derived from the manifest's own {@code
 * (primaryTerm, generation)} identity. Two independent snapshots can reference the same generation
 * (e.g. a second snapshot taken with no writes in between); pinning by generation alone would let
 * releasing one such snapshot un-pin a generation the other still needs. {@code pinId} is set to
 * the originating {@code SnapshotId}'s UUID at creation time, giving each snapshot's pin its own
 * independent lifetime.
 */
final class EngineNativeSnapshotPayload {

    private final String pinId;
    private final CommitManifest manifest;

    EngineNativeSnapshotPayload(String pinId, CommitManifest manifest) {
        this.pinId = pinId;
        this.manifest = manifest;
    }

    String pinId() {
        return pinId;
    }

    CommitManifest manifest() {
        return manifest;
    }

    byte[] toBytes() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeString(pinId);
            manifest.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    static EngineNativeSnapshotPayload fromBytes(byte[] bytes) throws IOException {
        try (StreamInput in = StreamInput.wrap(bytes)) {
            String pinId = in.readString();
            CommitManifest manifest = new CommitManifest(in);
            return new EngineNativeSnapshotPayload(pinId, manifest);
        }
    }
}
