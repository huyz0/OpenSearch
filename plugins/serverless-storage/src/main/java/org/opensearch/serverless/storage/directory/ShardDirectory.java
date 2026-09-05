/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

import java.util.Optional;

/**
 * The directory tier (rfc-serverless-metadata-plane.md &sect;8/&sect;9/&sect;11): a soft,
 * best-effort cache of "which node currently has this shard open," so a coordinator can route a
 * request directly instead of paying a shard-head object-store read on every request. This is the
 * component that makes 100M-shard scale reachable without a master-held routing table: unlike
 * {@code RoutingTable}, a directory is never enumerated or published wholesale -- it is looked up
 * and reported to one shard at a time, and at target scale (100M shards, ~2% active) only holds
 * entries for the active set, not every shard that has ever existed.
 *
 * <p><b>Status, stated up front because the rest of this javadoc describes a design rather than a
 * deployment.</b> One implementation is constructed in production ({@link InMemoryShardDirectory}, one
 * per node, in process) and it is written to but never read from -- see {@link #lookup}. The multi-node
 * fan-out this interface was shaped for is not built: a partitioned implementation and its consistent-hash
 * ring existed with no caller at all and were deleted on 2026-09-05 rather than left looking wired, and
 * {@link DirectoryRebuildService} remains, with its own javadoc saying the same thing about itself. What
 * routes a request today is {@code ComputedRoutingTable}.
 *
 * <p>Never a source of truth: {@link org.opensearch.serverless.storage.shardstate.ShardHead}
 * (the CAS-arbitrated object-store record) is truth; a directory entry is only ever a hint a
 * caller should be prepared to have be wrong. Implementations are free to lose entries, serve
 * stale ones past their nominal freshness, or disagree with each other -- correctness of the
 * system never depends on this interface's answers being right, only on {@code ShardHead}'s CAS
 * guard being correctly enforced downstream regardless of how a request got routed.
 */
public interface ShardDirectory {

    /**
     * The current best-known hint for where shard {@code (indexUuid, shardId)} is open, if any.
     *
     * <p><b>Nothing in production calls this, and the tier is therefore write-only today.</b> The engines
     * report and refresh entries ({@code ObjectStoreWriterEngine}, {@code ObjectStoreReaderEngine}) into
     * the one production instance, which is a per-node, in-process, unreplicated {@link
     * InMemoryShardDirectory}; no coordinator, allocator or routing path reads them back. Routing for
     * gated indices is instead <em>computed</em> -- rendezvous hashing over a membership set published
     * through the cluster manager, see {@code ComputedRoutingTable} -- which is a different design from the
     * hint-and-verify one this interface describes, and the one that is actually in force.
     *
     * <p>That means the refresh schedules the engines run against this tier's TTL cost a map write and buy
     * nothing: the "constant-load caching" property they are documented as providing cannot exist without a
     * remote tier to cache against. Wiring {@code lookup} into the coordinator ahead of the computed answer
     * is a real option, and it is deliberately <em>not</em> the first one: a hint is only safe when the
     * shard head's compare-and-swap fences a writer that acts on a stale one, and that fencing has only
     * just been given a token to work with (see {@code ShardHead#withTakenOverLease}). Routing on hints
     * before both halves of that are enforced end to end converts a latency question into a correctness
     * one.
     *
     * @param indexUuid the index the shard belongs to
     * @param shardId the shard id within the index
     * @return the best-known hint, or empty if none is held (or it has expired)
     */
    Optional<ShardDirectoryEntry> lookup(String indexUuid, int shardId);

    /**
     * Record (or refresh) a hint: this shard is open in {@code entry.role()} on {@code entry.nodeId()}.
     *
     * @param indexUuid the index the shard belongs to
     * @param shardId the shard id within the index
     * @param entry the hint to record
     */
    void report(String indexUuid, int shardId, ShardDirectoryEntry entry);

    /**
     * Remove any hint for this shard, e.g. because it was just closed/idled-out on the reporting node.
     *
     * @param indexUuid the index the shard belongs to
     * @param shardId the shard id within the index
     */
    void drop(String indexUuid, int shardId);

    /**
     * Removes the hint for this shard only if it is still exactly {@code expectedEntry} -- a no-op
     * if the currently-held entry is anything else (including absent). For a caller that itself
     * reported {@code expectedEntry} and is now closing/idling out, this is the safe way to clean
     * up after itself: an unconditional {@link #drop} would also discard a <em>different</em>,
     * newer entry some other caller reported in the meantime (e.g. this same shard already
     * relocated and reopened on another node), silently discarding a hint that was still correct.
     *
     * @param indexUuid the index the shard belongs to
     * @param shardId the shard id within the index
     * @param expectedEntry the entry to remove, if it is still the one on record
     */
    void dropIfMatches(String indexUuid, int shardId, ShardDirectoryEntry expectedEntry);
}
