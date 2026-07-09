/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

/** Which role a shard is currently open under on the node a {@link ShardDirectoryEntry} points to. */
public enum ShardRole {
    /** The shard is open for indexing/compaction, holding the writer/compactor lease. */
    WRITER,
    /** The shard is open read-only, serving search traffic from a manifest snapshot. */
    READER
}
