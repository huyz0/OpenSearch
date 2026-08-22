/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * REST and transport action exposing a cluster-wide, read-only scale-to-zero candidate policy
 * evaluation -- the "policy" half of rfc-serverless-opensearch.md &sect;7.3/&sect;10's
 * still-open scale-to-zero story, joining the writer-idle and reader-freshness signals
 * {@code org.opensearch.serverless.storage.writerengine.action}/{@code
 * org.opensearch.serverless.storage.readerengine.action} already expose per-node. Deliberately
 * does not implement the "mechanism" half (actually suspending or reactivating a shard) -- that
 * intersects {@code IndexShard}/allocation lifecycle in core and is out of scope for a single
 * node-local plugin action.
 */
package org.opensearch.serverless.storage.scaletozero.action;
