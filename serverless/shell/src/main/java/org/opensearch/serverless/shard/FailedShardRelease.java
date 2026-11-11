/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

/** The role of the failed engine removed, or whether a replacement already owns its slot. */
public enum FailedShardRelease {
    NOT_CURRENT,
    READER,
    WRITER
}
