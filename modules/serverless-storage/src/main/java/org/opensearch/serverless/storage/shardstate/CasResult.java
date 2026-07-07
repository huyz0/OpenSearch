/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.shardstate;

/** Outcome of a {@link ShardStateStore#compareAndSet} attempt. */
public enum CasResult {
    /** The write succeeded; the caller now owns the state it wrote. */
    SUCCESS,
    /** The expected version did not match the current stored version (or absence); the caller lost the race and must re-read and retry. */
    VERSION_CONFLICT
}
