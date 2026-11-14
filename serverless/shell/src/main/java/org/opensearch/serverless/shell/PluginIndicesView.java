/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.Rendezvous;
import org.opensearch.serverless.metadata.AliasAtGeneration;
import org.opensearch.serverless.metadata.ChangeStripes;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The plugins' own indices and their aliases, kept in every node's local view.
 *
 * <p><b>Why.</b> A node's view holds the indices it serves or writes. A plugin reads its own indices from the cluster state
 * as it does on a classic node: the alerting plugin finds the index behind its history alias in
 * {@code clusterService.state().metadata()} before it writes a monitor's results, index management and anomaly detection
 * check that their configuration index exists the same way. With only the served indices in the view, those reads found
 * nothing on any node that did not happen to hold the index. The indices a plugin declares through
 * {@code SystemIndexPlugin#getSystemIndexDescriptors} are few -- dozens across every stock plugin, never the tenants'
 * indices -- so every node keeps them all.
 *
 * <p><b>What it costs.</b> One listing per declared name prefix at start (prefixes covered by a shorter one are listed once).
 * After that nothing is listed: a change this node's plugins make through their client is applied at once, and a change
 * made elsewhere is learned from the change stripes every descriptor change already bumps, read every
 * {@value #REFRESH_INTERVAL} (sixteen small reads, shared with the reads writes make of them). A stripe that dropped entries
 * since the last read, or moved to a new epoch, sends the node back to listing.
 *
 * <p><b>What it does not follow.</b> An alias removed from a plugin index on another node, with no other change to that
 * index, is seen only at the next change to any plugin index or at restart: an alias names its indices in its own register,
 * and only additions leave a mark on the index's descriptor. A rollover, the usual way an alias moves, creates an index and
 * is followed.
 *
 * <p><b>Each shard has an owner in the view.</b> A plugin that schedules work from its own index sweeps the shards the cluster
 * state puts on its node -- the alerting plugin and Job Scheduler both do -- so a shard unassigned everywhere was swept by
 * nobody, and the alerting plugin descheduled every monitor once the writer of its configuration index went idle. A shard no
 * node holds is shown started on the live member that rendezvous hashing of its name and number picks, which every node
 * computes the same from the members it already reads; a node writing the shard shows it on itself. The routing table here
 * only describes: the node opens shards through its reconciler, never from the view, so naming an owner opens nothing.
 */
final class PluginIndicesView {

    private static final Logger logger = LogManager.getLogger(PluginIndicesView.class);

    /** Whether the view holds the plugins' indices: on by default, and only on a node whose plugins declare any. */
    static final String ENABLED = "serverless.plugin_indices_view.enabled";

    /** How often changes made by other nodes are looked for; 0 to look only at start. Default 30 seconds. */
    static final String REFRESH_INTERVAL = "serverless.plugin_indices_view.refresh_interval";

    private static final int LIST_CAP = 10_000;

    private record Held(IndexDescriptor descriptor, List<AliasMetadata> aliases) {
    }

    private final ServerlessNode node;
    private final Map<String, Held> held = new ConcurrentHashMap<>();
    private final long[] epochs = new long[ChangeStripes.STRIPES];
    private final long[] generations = new long[ChangeStripes.STRIPES];
    private volatile boolean loaded;
    private volatile Scheduler.Cancellable refresher;

    PluginIndicesView(ServerlessNode node) {
        this.node = node;
    }

    private boolean active() {
        return node.settings.getAsBoolean(ENABLED, true) && node.systemIndices != null && node.systemIndices.isEmpty() == false;
    }

    /**
     * Loads the plugins' indices, applies the view, and starts following other nodes' changes. Called once the node has its
     * metadata plane and before the plugins are told it started, so their first read finds their indices.
     */
    void start(MetadataPlane plane) {
        if (active() == false) {
            return;
        }
        try {
            synchronized (this) {
                markStripes(plane);
                load(plane);
            }
            apply();
        } catch (Exception e) {
            logger.warn("could not load the plugins' indices into the local view; plugins will not find them there", e);
        }
        final TimeValue every = node.settings.getAsTime(REFRESH_INTERVAL, TimeValue.timeValueSeconds(30));
        if (every.millis() > 0) {
            try {
                refresher = node.threadPool().scheduleWithFixedDelay(() -> refresh(plane, every), every, ThreadPool.Names.GENERIC);
            } catch (RuntimeException e) {
                // Shutting down.
            }
        }
    }

    /** Stops following other nodes' changes. */
    void stop() {
        final Scheduler.Cancellable running = refresher;
        if (running != null) {
            running.cancel();
        }
    }

    /**
     * The plugin indices to project.
     *
     * @return their descriptors
     */
    Collection<IndexDescriptor> descriptors() {
        final List<IndexDescriptor> descriptors = new ArrayList<>(held.size());
        for (Held each : held.values()) {
            descriptors.add(each.descriptor());
        }
        return descriptors;
    }

    /**
     * A projected view with the plugin indices' aliases added: the projection renders descriptors, and an alias is not part of
     * one.
     *
     * @param state the projected view
     * @return the view with the aliases
     */
    ClusterState withAliases(ClusterState state) {
        Metadata.Builder metadata = null;
        for (Map.Entry<String, Held> entry : held.entrySet()) {
            final IndexMetadata index = state.metadata().index(entry.getKey());
            if (index == null || entry.getValue().aliases().isEmpty()) {
                continue;
            }
            final IndexMetadata.Builder withAliases = IndexMetadata.builder(index);
            for (AliasMetadata alias : entry.getValue().aliases()) {
                withAliases.putAlias(alias);
            }
            if (metadata == null) {
                metadata = Metadata.builder(state.metadata());
            }
            metadata.put(withAliases.build(), false);
        }
        if (metadata == null) {
            return state;
        }
        try {
            return ClusterState.builder(state).metadata(metadata).build();
        } catch (RuntimeException e) {
            // An alias that clashes with an index name in the view: the indices without their aliases rather than no view.
            logger.debug("not adding the plugin indices' aliases to the local view", e);
            return state;
        }
    }

    private volatile Set<String> ownersFrom = Set.of();

    /**
     * A view with every unassigned shard of a plugin index shown started on its owner among the view's nodes.
     *
     * @param state the view, with its peers
     * @return the view with owners
     */
    ClusterState withOwners(ClusterState state) {
        if (held.isEmpty()) {
            return state;
        }
        final List<String> nodeIds = new ArrayList<>();
        for (DiscoveryNode member : state.nodes()) {
            nodeIds.add(member.getId());
        }
        ownersFrom = Set.copyOf(nodeIds);
        RoutingTable.Builder routing = null;
        for (String name : held.keySet()) {
            final IndexRoutingTable table = state.routingTable().index(name);
            if (table == null) {
                continue;
            }
            final IndexRoutingTable.Builder owned = IndexRoutingTable.builder(table.getIndex());
            boolean changed = false;
            for (IndexShardRoutingTable shard : table) {
                if (shard.primaryShard() == null || shard.primaryShard().unassigned() == false) {
                    owned.addIndexShard(shard);
                    continue;
                }
                final String owner = Rendezvous.owner(name + "/" + shard.shardId().id(), nodeIds);
                owned.addIndexShard(
                    new IndexShardRoutingTable.Builder(shard.shardId()).addShard(
                        ShardRouting.newUnassigned(
                            shard.shardId(),
                            true,
                            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "plugin index owner")
                        ).initialize(owner, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted()
                    ).build()
                );
                changed = true;
            }
            if (changed) {
                if (routing == null) {
                    routing = RoutingTable.builder(state.routingTable());
                }
                routing.add(owned.build());
            }
        }
        return routing == null ? state : ClusterState.builder(state).routingTable(routing.build()).build();
    }

    /** Whether the members differ from the ones the applied owners were chosen among: the view's nodes come from the same leases. */
    private boolean membersChanged(MetadataPlane plane) {
        final Set<String> members = new TreeSet<>();
        members.add(node.localNode.getId());
        for (org.opensearch.serverless.membership.NodeLease lease : plane.membership().current()) {
            members.add(lease.nodeId());
        }
        return held.isEmpty() == false && members.equals(new TreeSet<>(ownersFrom)) == false;
    }

    /**
     * Brings the named indices up to date after this node changed them, and applies the view before returning, so a plugin
     * that created an index and reads the cluster state next finds it. Names that are not plugin indices are ignored.
     *
     * @param names the indices changed
     */
    void changed(Collection<String> names) {
        final MetadataPlane plane = node.metadataPlane;
        if (loaded == false || plane == null || names.isEmpty()) {
            return;
        }
        try {
            final boolean any;
            synchronized (this) {
                any = update(plane, names);
            }
            if (any) {
                apply();
            }
        } catch (Exception e) {
            logger.debug("could not update the plugin indices " + names + " in the local view", e);
        }
    }

    /** Reads the change stripes and updates the indices they name, or lists again when they cannot say. */
    private void refresh(MetadataPlane plane, TimeValue every) {
        try {
            final boolean any;
            synchronized (this) {
                final Set<String> named = new TreeSet<>();
                boolean relist = false;
                for (int stripe = 0; stripe < ChangeStripes.STRIPES; stripe++) {
                    final ChangeStripes.Snapshot read = node.fences.stripeNoOlderThan(plane, stripe, every.nanos() / 2);
                    if (read.epoch() != epochs[stripe] || read.oldestKept() > generations[stripe] + 1) {
                        relist = true;
                    } else {
                        for (Map.Entry<String, Long> change : read.changed().entrySet()) {
                            if (change.getValue() > generations[stripe]) {
                                named.add(change.getKey());
                            }
                        }
                    }
                    epochs[stripe] = read.epoch();
                    generations[stripe] = read.generation();
                }
                if (relist) {
                    load(plane);
                    any = true;
                } else {
                    any = update(plane, named) || membersChanged(plane);
                }
            }
            if (any) {
                apply();
            }
        } catch (Exception e) {
            logger.debug("could not follow changes to the plugin indices", e);
        }
    }

    private void markStripes(MetadataPlane plane) throws IOException {
        for (int stripe = 0; stripe < ChangeStripes.STRIPES; stripe++) {
            final ChangeStripes.Snapshot read = plane.changeStripes().read(stripe);
            epochs[stripe] = read.epoch();
            generations[stripe] = read.generation();
        }
    }

    /** Lists every declared prefix and describes what matches, replacing what was held. */
    private void load(MetadataPlane plane) throws IOException {
        final Set<String> found = new TreeSet<>();
        for (String prefix : prefixes(node.systemIndices.patterns())) {
            for (String name : plane.namesWithPrefix(prefix, LIST_CAP)) {
                if (node.systemIndices.contains(name)) {
                    found.add(name);
                }
            }
        }
        held.keySet().retainAll(found);
        for (String name : found) {
            plane.describe(name).ifPresent(descriptor -> held.put(name, new Held(descriptor, List.of())));
        }
        readAliases(plane);
        loaded = true;
    }

    /** Re-describes the named plugin indices; true if any was one. */
    private boolean update(MetadataPlane plane, Collection<String> names) throws IOException {
        boolean any = false;
        for (String name : names) {
            if (node.systemIndices.contains(name) == false) {
                continue;
            }
            any = true;
            final Optional<IndexDescriptor> descriptor = plane.describe(name);
            if (descriptor.isPresent()) {
                held.put(name, new Held(descriptor.get(), List.of()));
            } else {
                held.remove(name);
            }
        }
        if (any) {
            // An alias moves between indices, so a change to one index can change another's aliases.
            readAliases(plane);
        }
        return any;
    }

    private void readAliases(MetadataPlane plane) throws IOException {
        for (Map.Entry<String, Held> entry : held.entrySet()) {
            final List<AliasMetadata> aliases = new ArrayList<>();
            for (String alias : plane.aliasesOf(entry.getValue().descriptor())) {
                final Optional<AliasAtGeneration> record = plane.aliasWithGeneration(alias);
                aliases.add(
                    record.isPresent()
                        ? PluginIndexAdmin.aliasMetadata(record.get().alias(), entry.getKey())
                        : AliasMetadata.builder(alias).build()
                );
            }
            entry.setValue(new Held(entry.getValue().descriptor(), List.copyOf(aliases)));
        }
    }

    private void apply() throws Exception {
        if (node.projector != null) {
            node.projectAndApply(List.of(), List.of());
        }
    }

    /**
     * The name prefixes to list for a set of patterns: each pattern up to its first wildcard, without the ones a shorter prefix
     * already covers. A pattern that starts with a wildcard is never listed, because its prefix is every index.
     *
     * @param patterns the declared patterns
     * @return the prefixes, shortest first among those sharing a start
     */
    static List<String> prefixes(Collection<String> patterns) {
        final TreeSet<String> sorted = new TreeSet<>();
        for (String pattern : patterns) {
            final int wildcard = pattern.indexOf('*');
            final String prefix = wildcard < 0 ? pattern : pattern.substring(0, wildcard);
            if (prefix.isEmpty()) {
                logger.warn("not keeping indices matching [{}] in the local view: it would list every index", pattern);
                continue;
            }
            sorted.add(prefix);
        }
        final List<String> kept = new ArrayList<>();
        for (String prefix : sorted) {
            if (kept.isEmpty() || prefix.startsWith(kept.get(kept.size() - 1)) == false) {
                kept.add(prefix);
            }
        }
        return kept;
    }

    /**
     * The indices an index administration request from a plugin changed, for {@link #changed}.
     *
     * @param request the request
     * @param response its answer
     * @return the index names, empty for a read
     */
    static List<String> touchedBy(ActionRequest request, ActionResponse response) {
        final List<String> names = new ArrayList<>();
        if (request instanceof PutMappingRequest put) {
            if (put.getConcreteIndex() != null) {
                names.add(put.getConcreteIndex().getName());
            }
            addAll(names, put.indices());
        } else if (request instanceof UpdateSettingsRequest update) {
            addAll(names, update.indices());
        } else if (request instanceof DeleteIndexRequest delete) {
            addAll(names, delete.indices());
        } else if (request instanceof IndicesAliasesRequest aliases) {
            for (IndicesAliasesRequest.AliasActions action : aliases.getAliasActions()) {
                addAll(names, action.indices());
            }
        }
        if (response instanceof RolloverResponse rolled) {
            names.add(rolled.getOldIndex());
            names.add(rolled.getNewIndex());
        }
        return names;
    }

    private static void addAll(List<String> into, String[] names) {
        if (names != null) {
            for (String name : names) {
                into.add(PluginIndexAdmin.dateMath(name));
            }
        }
    }
}
