/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Builds the parts of a {@link ServerlessNode} that are made from its settings alone: the block cache, the write path's
 * limiter and group committer, the REST client, and the discovery node it describes itself with.
 */
public final class NodeComponents {

    private NodeComponents() {}

    /**
     * Builds the block cache from the node's settings.
     *
     * @param settings the node settings
     * @return the block cache
     */
    static org.opensearch.serverless.store.BlockCache blockCache(Settings settings) {
        // Block size is a real tuning knob, not a constant: it trades request count against wasted
        // bytes, and the right value depends on the object store's per-request cost and the index's
        // access pattern. Exposed so that can be measured rather than guessed.
        return new org.opensearch.serverless.store.BlockCache(
            settings.getAsInt("serverless.block_cache.block_size", org.opensearch.serverless.store.BlockCache.DEFAULT_BLOCK_SIZE),
            // Sized from the heap when not configured: the cache lives outside the breakers (accounting it
            // there would trip every request check by its own size), so its bound has to come from what
            // the process actually has rather than from a constant that was right on one machine.
            settings.getAsInt(
                "serverless.block_cache.max_blocks",
                (int) Math.max(
                    256L,
                    Runtime.getRuntime().maxMemory() / 10 / settings.getAsInt(
                        "serverless.block_cache.block_size",
                        org.opensearch.serverless.store.BlockCache.DEFAULT_BLOCK_SIZE
                    )
                )
            )
        );
    }

    /**
     * Builds the write path's limiter on how many writes may wait on the object store at once.
     *
     * @param settings the node settings
     * @param maxInFlight the most writes it may ever let wait
     * @return the limiter
     */
    static org.opensearch.serverless.store.WriteBackpressure writeBackpressure(Settings settings, int maxInFlight) {
        return new org.opensearch.serverless.store.WriteBackpressure(
            settings.getAsLong("serverless.write.backpressure.target_millis", 1_000L),
            settings.getAsLong("serverless.write.backpressure.budget_millis", 5_000L),
            settings.getAsInt("serverless.write.backpressure.min_in_flight", 4),
            maxInFlight
        );
    }

    /**
     * Builds the committer that shares one object-store PUT between the concurrent writes to a shard.
     *
     * @param settings the node settings
     * @return the group committer
     */
    static org.opensearch.serverless.store.WalGroupCommitter walCommitter(Settings settings) {
        // Group commit for the log. Batching only ever happens while a PUT for the same shard is already
        // in flight, so these bound the size of a group rather than schedule one -- there is no window to
        // configure. Setting max_records to 1 restores one PUT per write, which is what this replaced.
        return new org.opensearch.serverless.store.WalGroupCommitter(
            settings.getAsInt(
                "serverless.wal.group_commit.max_records",
                org.opensearch.serverless.store.WalGroupCommitter.DEFAULT_MAX_RECORDS
            ),
            settings.getAsLong("serverless.wal.group_commit.max_bytes", org.opensearch.serverless.store.WalGroupCommitter.DEFAULT_MAX_BYTES)
        );
    }

    /**
     * Builds the client every REST handler is handed, wired to the shell's own operations.
     *
     * @param settings the node settings
     * @param threadPool the node's thread pool
     * @param services the node's services, whose client enforces the allowlist
     * @return the client
     */
    static NodeClient nodeClient(Settings settings, ThreadPool threadPool, NodeServices services) {
        // It used to be a bare NodeClient with no action registry, which the shell's own handlers
        // could ignore because they reach the node directly. A plugin's handler cannot: getRestHandlers
        // gives it this and nothing else, and every REST handler in the OpenSearch ecosystem is written
        // against it. Left unwired it would have thrown on the first call a real plugin made -- found
        // by writing the shell's authentication as a plugin, whose user-management endpoint needs to
        // write to an index from inside a handler.
        //
        // Overriding doExecute rather than calling initialize(...) is the point: initialize wants a map
        // of ActionType to TransportAction, which is the action layer this shell does not build. This
        // delegates to the same allowlist ServerlessClient enforces, so a plugin's handler and a
        // plugin's component get identical answers, including identical refusals.
        return new NodeClient(settings, threadPool) {
            @Override
            public <
                Request extends org.opensearch.action.ActionRequest,
                Response extends org.opensearch.core.action.ActionResponse> void doExecute(
                    org.opensearch.action.ActionType<Response> action,
                    Request request,
                    org.opensearch.core.action.ActionListener<Response> listener
                ) {
                services.client().execute(action, request, listener);
            }
        };
    }

    /**
     * Describes a node advertising the given lease roles.
     *
     * @param nodeName the node name
     * @param nodeId the node id
     * @param address the node's transport address
     * @param leaseRoles the roles from the lease or the {@code serverless.roles} setting
     * @return the discovery node
     */
    static DiscoveryNode discoveryNode(String nodeName, String nodeId, TransportAddress address, Set<String> leaseRoles) {
        // Roles from the lease's roles, never BUILT_IN_ROLES. The role is a lease attribute,
        // and every DiscoveryNode this shell built used to say cluster_manager+data+ingest+warm
        // regardless -- a cluster-manager-role node that merely was not elected, which is exactly the
        // latent arming of cluster-manager-only paths to avoid. Data always: this node opens
        // IndexShards whatever it serves. Ingest only when it accepts writer activation. Never
        // cluster_manager, never core's search role -- that one means "hosts search replicas" and may
        // not be combined with data, which is not what a serverless search node is.
        return new DiscoveryNode(
            nodeName,
            nodeId,
            address,
            discoveryAttributesFor(leaseRoles),
            discoveryRolesFor(leaseRoles),
            Version.CURRENT
        );
    }

    /**
     * The data plane's roles for a node advertising the given lease roles: data always, ingest when it
     * accepts writer activation, never cluster_manager. See {@link #discoveryNode} for why.
     *
     * @param leaseRoles the roles from the lease or the {@code serverless.roles} setting
     * @return the discovery roles
     */
    public static Set<DiscoveryNodeRole> discoveryRolesFor(Set<String> leaseRoles) {
        final java.util.SortedSet<DiscoveryNodeRole> out = new java.util.TreeSet<>();
        out.add(DiscoveryNodeRole.DATA_ROLE);
        if (leaseRoles.contains(ServerlessNode.ROLE_INGEST)) {
            out.add(DiscoveryNodeRole.INGEST_ROLE);
        }
        return Collections.unmodifiableSortedSet(out);
    }

    /**
     * The lease roles as a node attribute, so the serverless role is readable off a {@code DiscoveryNode} too.
     *
     * @param leaseRoles the roles from the lease or the {@code serverless.roles} setting
     * @return the attributes to describe a node with
     */
    public static Map<String, String> discoveryAttributesFor(Set<String> leaseRoles) {
        return Map.of("serverless.roles", String.join(",", new java.util.TreeSet<>(leaseRoles)));
    }
}
