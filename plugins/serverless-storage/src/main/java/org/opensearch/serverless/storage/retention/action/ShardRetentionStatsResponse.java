/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * The result of a {@link ShardRetentionStatsAction} request: one shard's manifest/bundle/pin
 * counts, computed the same way {@code GcSchedulerTask}'s real sweep would, plus this node's
 * currently configured retention windows.
 */
public class ShardRetentionStatsResponse extends ActionResponse implements ToXContentObject {

    private final int manifestCount;
    private final int deletableManifestCount;
    private final int bundleCount;
    private final int deletableBundleCount;
    private final int durablePinCount;
    private final int pitrPinCount;
    private final long gcRetentionWindowMillis;
    private final long pitrWindowMillis;

    /**
     * Creates a response.
     *
     * @param manifestCount every manifest this shard has ever published, still known to the manifest store.
     * @param deletableManifestCount of those, how many {@code ManifestRetentionPolicy} currently considers safe to delete.
     * @param bundleCount every bundle known to exist in this shard's own bundle store.
     * @param deletableBundleCount of those, how many are referenced by no manifest that would remain after a sweep.
     * @param durablePinCount every durable pin currently held on this shard, from every pinning reason.
     * @param pitrPinCount of those, how many are specifically PITR-reason pins.
     * @param gcRetentionWindowMillis this node's currently configured GC retention window (millis).
     * @param pitrWindowMillis this node's currently configured PITR window (millis), or a
     *                         non-positive value if PITR retention is disabled on this node.
     */
    public ShardRetentionStatsResponse(
        int manifestCount,
        int deletableManifestCount,
        int bundleCount,
        int deletableBundleCount,
        int durablePinCount,
        int pitrPinCount,
        long gcRetentionWindowMillis,
        long pitrWindowMillis
    ) {
        this.manifestCount = manifestCount;
        this.deletableManifestCount = deletableManifestCount;
        this.bundleCount = bundleCount;
        this.deletableBundleCount = deletableBundleCount;
        this.durablePinCount = durablePinCount;
        this.pitrPinCount = pitrPinCount;
        this.gcRetentionWindowMillis = gcRetentionWindowMillis;
        this.pitrWindowMillis = pitrWindowMillis;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardRetentionStatsResponse}.
     */
    public ShardRetentionStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.manifestCount = in.readVInt();
        this.deletableManifestCount = in.readVInt();
        this.bundleCount = in.readVInt();
        this.deletableBundleCount = in.readVInt();
        this.durablePinCount = in.readVInt();
        this.pitrPinCount = in.readVInt();
        this.gcRetentionWindowMillis = in.readZLong();
        this.pitrWindowMillis = in.readZLong();
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(manifestCount);
        out.writeVInt(deletableManifestCount);
        out.writeVInt(bundleCount);
        out.writeVInt(deletableBundleCount);
        out.writeVInt(durablePinCount);
        out.writeVInt(pitrPinCount);
        out.writeZLong(gcRetentionWindowMillis);
        out.writeZLong(pitrWindowMillis);
    }

    /** Every manifest this shard has ever published, still known to the manifest store. */
    public int manifestCount() {
        return manifestCount;
    }

    /** Of {@link #manifestCount()}, how many {@code ManifestRetentionPolicy} currently considers safe to delete. */
    public int deletableManifestCount() {
        return deletableManifestCount;
    }

    /** Every bundle known to exist in this shard's own bundle store. */
    public int bundleCount() {
        return bundleCount;
    }

    /** Of {@link #bundleCount()}, how many are referenced by no manifest that would remain after a sweep. */
    public int deletableBundleCount() {
        return deletableBundleCount;
    }

    /** Every durable pin currently held on this shard, from every pinning reason. */
    public int durablePinCount() {
        return durablePinCount;
    }

    /** Of {@link #durablePinCount()}, how many are specifically PITR-reason pins. */
    public int pitrPinCount() {
        return pitrPinCount;
    }

    /** This node's currently configured GC retention window (millis). */
    public long gcRetentionWindowMillis() {
        return gcRetentionWindowMillis;
    }

    /** This node's currently configured PITR window (millis), or a non-positive value if PITR retention is disabled. */
    public long pitrWindowMillis() {
        return pitrWindowMillis;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("manifest_count", manifestCount)
            .field("deletable_manifest_count", deletableManifestCount)
            .field("bundle_count", bundleCount)
            .field("deletable_bundle_count", deletableBundleCount)
            .field("durable_pin_count", durablePinCount)
            .field("pitr_pin_count", pitrPinCount)
            .field("gc_retention_window_millis", gcRetentionWindowMillis)
            .field("pitr_window_millis", pitrWindowMillis)
            .endObject();
    }
}
