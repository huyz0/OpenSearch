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
 * <p>Client-facing routing cutover after a split is implemented too, via
 * {@link org.opensearch.serverless.storage.resharding.action.CutoverSplitRoutingAction} (points an
 * alias at the split targets) and, optionally,
 * {@link org.opensearch.serverless.storage.resharding.action.EnableWritePartitionRoutingAction}
 * (assigns each target a write partition under that alias), chained together with provisioning and
 * per-partition split by {@link org.opensearch.serverless.storage.resharding.action.OrchestrateShardSplitAction}
 * into one resumable, operator-triggered sequence. This is additive-alias routing, not source
 * fencing -- it never renames, blocks, or otherwise touches the source index itself, so the source
 * must not be receiving direct writes when this is triggered; see
 * {@code TransportOrchestrateShardSplitAction}'s own javadoc for the full precondition and why
 * closing that gap (real source write-fencing, or a dual-write bridge) remains separately out of
 * scope.
 *
 * <p>Explicitly out of scope: auto-triggering a split off a write-rate/size threshold alone --
 * {@code ShardSplitCandidatesAction} surfaces the signal for an operator (or a future controller) to
 * act on manually via the routing-cutover sequence above, since nothing today automatically decides
 * when a candidate is safe to actually split unattended; and deleting a shrink's now-superseded
 * source shards afterward, a deliberately-not-automatic operator decision (see {@link
 * org.opensearch.serverless.storage.resharding.action.RetireShrinkSourceAction}'s own "verify then
 * delete" pattern).
 */
package org.opensearch.serverless.storage.resharding;
