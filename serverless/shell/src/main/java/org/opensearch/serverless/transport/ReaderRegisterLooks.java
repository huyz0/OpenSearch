/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.HotShardSpread;
import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Lets a search node skip the periodic register look for a reader whose live writer pushes it every commit.
 *
 * <p><b>Why.</b> A search node reads the register of every reader it holds every other lease renewal and on every
 * backstop pass, about five reads a minute per reader, to learn of a newer commit. With {@link CommitPush} on, the
 * writer already sends each commit it publishes to the search nodes placement names for the shard, and the reader moves
 * onto it at once. For those readers the look finds nothing new almost every time: on a lightly used index it was the
 * largest steady background read a search node made, about 290 reads per reader-hour.
 *
 * <p><b>When the look is skipped.</b> Only while all of these hold, each answered from memory: this node has received a
 * push from the writer that published the reader's commit (so that writer pushes); that writer's lease is live in the
 * membership snapshot; placement, as this node sees the membership, names this node among the shard's push targets; and
 * the last look was less than {@link #BACKSTOP_MILLIS} ago. A
 * writer that dies, hands the shard to another node or stops pushing, or a membership change that moves the shard's
 * targets, brings the look back at the next pass. What the backstop bounds is a push lost on the wire for the last
 * publish before a shard goes quiet: that reader moves at the next look, at most the backstop later, instead of up to
 * twenty seconds later.
 *
 * <p><b>What it does not change.</b> The check a search makes before a reader answers ({@code ReaderValidation}) is
 * untouched: a reader whose writer died is still refused until a successor has replayed and published. A pushed commit
 * still moves a reader at once, and a write answered as refreshed is still made visible through the tail overlay. On
 * unless {@value #ENABLED} is set to false.
 */
public final class ReaderRegisterLooks {

    /** Whether a search node skips the periodic register look for readers its live writer pushes. */
    public static final String ENABLED = "serverless.search.commit_push.trust";

    /** The longest a reader goes without a register look while it is skipped, in milliseconds. */
    public static final String BACKSTOP = "serverless.search.commit_push.trust_backstop_millis";

    /** The default of {@value #BACKSTOP}: two minutes, against twenty seconds between looks without pushes. */
    public static final long BACKSTOP_MILLIS = 120_000L;

    /** How many shards' last looks are kept before those past the backstop are dropped. */
    static final int MAX_TRACKED = 65_536;

    private final ServerlessNode node;
    private final Supplier<MetadataPlane> plane;
    private final boolean enabled;
    private final long backstopNanos;
    /** When a push was last received from each writer, by node id. */
    private final Map<String, Long> pushedFromAtNanos = new ConcurrentHashMap<>();
    /** When each shard's register was last looked at, by index uuid and shard. */
    private final Map<String, Long> lastLooks = new ConcurrentHashMap<>();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicLong looked = new AtomicLong();

    ReaderRegisterLooks(ServerlessNode node, Supplier<MetadataPlane> plane, Settings settings) {
        this.node = node;
        this.plane = plane;
        this.enabled = settings.getAsBoolean(ENABLED, true);
        this.backstopNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, settings.getAsLong(BACKSTOP, BACKSTOP_MILLIS)));
    }

    /** @return whether looks are skipped at all on this node */
    public boolean enabled() {
        return enabled;
    }

    /** @return register looks skipped, and register looks made */
    public long[] counts() {
        return new long[] { skipped.get(), looked.get() };
    }

    /**
     * Records a push received from a writer.
     *
     * @param writer the node id of the writer that published the pushed commit, or null if the commit does not say
     */
    void pushedFrom(String writer) {
        if (writer != null) {
            pushedFromAtNanos.put(writer, System.nanoTime());
        }
    }

    /**
     * Records that a reader's register was just read, which starts its backstop window.
     *
     * @param shardId the reader's shard
     */
    public void looked(ShardId shardId) {
        if (enabled == false) {
            return;
        }
        looked.incrementAndGet();
        final long now = System.nanoTime();
        if (lastLooks.size() > MAX_TRACKED) {
            lastLooks.values().removeIf(at -> now - at > backstopNanos);
        }
        lastLooks.put(key(shardId), now);
    }

    /**
     * Whether a reader's periodic register look can be skipped because its writer pushes it every commit; see the class.
     *
     * @param shardId the reader's shard
     * @param open the commit the reader is serving
     * @return true to skip this look
     */
    public boolean skippable(ShardId shardId, CommitManifest open) {
        if (enabled == false || open == null || open.writer() == null) {
            return false;
        }
        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return false;
        }
        final Long last = lastLooks.get(key(shardId));
        if (last == null || System.nanoTime() - last >= backstopNanos) {
            return false;
        }
        final String writer = open.writer();
        if (pushedFromAtNanos.containsKey(writer) == false) {
            return false;
        }
        final java.util.Set<NodeLease> members = metadata.membership().current();
        final long clock = metadata.clock().getAsLong();
        boolean writerLive = false;
        for (NodeLease lease : members) {
            if (writer.equals(lease.nodeId()) && lease.isExpiredAt(clock) == false) {
                writerLive = true;
                break;
            }
        }
        if (writerLive == false) {
            return false;
        }
        // The targets the writer pushes to, as this node sees the membership: CommitPush#readerTargets ranks the same way.
        final HotShardSpread spread = HotShardSpread.of(node, node.settings());
        final List<String> targets = ReaderPlacement.candidatesFor(
            shardId.getIndexName(),
            shardId.id(),
            members,
            ServerlessNode.ROLE_SEARCH,
            Math.max(2, spread.maxReaders())
        );
        if (targets.contains(node.localNode().getId()) == false) {
            return false;
        }
        skipped.incrementAndGet();
        return true;
    }

    private static String key(ShardId shardId) {
        return shardId.getIndex().getUUID() + "#" + shardId.id();
    }
}
