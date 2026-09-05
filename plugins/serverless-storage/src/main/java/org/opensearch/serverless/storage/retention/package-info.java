/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Durable retention pins (snapshots, PITR) that keep specific manifest generations alive regardless of
 * normal GC.
 *
 * <h2>Two &sect;14 promises that are not built</h2>
 *
 * Named here rather than left to be discovered at recovery time (rfc-serverless-opensearch.md &sect;14).
 *
 * <ul>
 * <li><b>A PIT or scroll context is not a pinned manifest.</b> &sect;6.5 requires a lease tier carrying
 *     "manifest pins (open searchers, PIT/scroll contexts)" and &sect;14 says another reader can resume the
 *     same pinned generation. Nothing anywhere pins on searcher acquisition, and {@code GcSchedulerTask}
 *     passes a permanently empty lease-pin set (deliberately -- see its own javadoc for why the node-local
 *     directory tier would be worse than no check at all). A lagging reader is protected by the retention
 *     window; a scroll or PIT held open longer than that window is not. The registry already supports the
 *     right shape -- an expiring {@link org.opensearch.serverless.storage.retention.PinRecord} whose pin id
 *     is {@code pit:} followed by the context id, renewed with the context's keep-alive and released on
 *     close -- so what is missing is the call site, not the mechanism.</li>
 * <li><b>Cross-cluster and cross-account restore do not exist.</b> There is no import-manifests path; the
 *     deep-snapshot action is the interchange export half only.</li>
 * </ul>
 *
 * <p>See {@link org.opensearch.serverless.storage.retention.PitrRetentionConfig} for the third gap -- what
 * the PITR window does and does not guarantee without WAL retention.
 */
package org.opensearch.serverless.storage.retention;
