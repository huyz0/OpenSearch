/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * The shell's shard lifecycle.
 *
 * <p>This package replaces {@code IndicesClusterStateService}, which is the only class in the reused
 * tree that assumes an elected cluster-manager and the only place a projected view could be misread as
 * an instruction to close shards. See {@code rfc-serverless-shell.md} sections 5.2 and 10.5.
 */
package org.opensearch.serverless.shard;
