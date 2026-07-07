/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.shardstate;

import java.util.Objects;

/**
 * A {@link ShardHead} together with the opaque version token needed to CAS it. The version is
 * store-defined (e.g. an S3 ETag, a GCS generation number, or &mdash; for the reference FS
 * implementation &mdash; a monotonic counter) and must be passed back unchanged to
 * {@link ShardStateStore#compareAndSet} to detect whether the head changed since it was read.
 */
public final class VersionedShardHead {

    private final ShardHead head;
    private final long version;

    public VersionedShardHead(ShardHead head, long version) {
        this.head = Objects.requireNonNull(head, "head");
        this.version = version;
    }

    public ShardHead head() {
        return head;
    }

    public long version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VersionedShardHead)) return false;
        VersionedShardHead that = (VersionedShardHead) o;
        return version == that.version && head.equals(that.head);
    }

    @Override
    public int hashCode() {
        return Objects.hash(head, version);
    }

    @Override
    public String toString() {
        return "VersionedShardHead{head=" + head + ", version=" + version + '}';
    }
}
