/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Resharding (rfc-serverless-opensearch.md &sect;16 Phase 5): split a shard into {@code
 * numPartitions} zero-copy target shards without rewriting any bundle bytes, the reader-side
 * doc-routing partition filter ({@link org.opensearch.serverless.storage.resharding.PartitionFilteringDirectoryReader})
 * that makes each target correctly serve only its own slice of the pre-split document space, the
 * background {@link org.opensearch.serverless.storage.resharding.PartitionRewriteSchedulerTask}
 * that later physically rewrites a target down to just its own partition (dropping the doc-routing
 * filter's job once done), and {@link org.opensearch.serverless.storage.resharding.ShardShrinker},
 * shrink's inverse operation (merging several shards' document spaces back into one via a real
 * Lucene merge).
 *
 * <p>Explicitly out of scope: real client-facing routing cutover after a split (directing writes to
 * the right target partition instead of the source) -- a wholly separate design problem this
 * package's own split/rewrite primitives have nothing to build it on top of yet; and deleting a
 * shrink's now-superseded source shards afterward, a deliberately-not-automatic operator decision.
 */
package org.opensearch.serverless.storage.resharding;
