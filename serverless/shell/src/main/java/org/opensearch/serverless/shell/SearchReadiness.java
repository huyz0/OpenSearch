/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Whether a search node is ready to be sent searches: the one flag that keeps a node out of search traffic from both
 * sides until its cache is warm, and takes it out again when it drains.
 *
 * <p>A search reaches a reader two ways. A client's request comes through the ingress to whichever node coordinates
 * it; a coordinator then picks a reader for each shard by ranking the nodes whose lease advertises the search role.
 * Kubernetes decides the first by the readiness probe; only the lease decides the second. So one flag drives three
 * things:
 *
 * <ul>
 *   <li>the search role in this node's lease, which coordinators rank readers by;</li>
 *   <li>{@code /_serverless/ready}, which a readiness probe reads, and so the ingress;</li>
 *   <li>whether a search this node coordinates may answer from a reader open here, which while warming would be a
 *       reader whose cache is still filling.</li>
 * </ul>
 *
 * <p>With {@code serverless.search.warm_on_join} off, the default, a search node is ready from the start, as before.
 * With it on, the node joins with its lease but without the search role, and turns ready once warmed. A node that
 * does not serve searches is always ready: the flag says nothing about it.
 */
public final class SearchReadiness {

    /** Whether a search node joins unready and turns ready only once it has warmed. */
    public static final String WARM_ON_JOIN = "serverless.search.warm_on_join";

    /** Where the node is in its life as a reader. */
    public enum State {
        /** Joined, not yet sent searches: its cache is warming. */
        WARMING,
        /** Sent searches. */
        READY,
        /** Handing its shards on before it stops: not sent new searches. */
        DRAINING
    }

    private static final Map<ServerlessNode, SearchReadiness> BY_NODE = new WeakHashMap<>();

    private final boolean servesSearches;
    private volatile State state;

    SearchReadiness(boolean servesSearches, boolean warmOnJoin) {
        this.servesSearches = servesSearches;
        this.state = servesSearches && warmOnJoin ? State.WARMING : State.READY;
    }

    /**
     * Returns a node's readiness, made from its settings on first use.
     *
     * @param node the node
     * @return its readiness
     */
    public static SearchReadiness of(ServerlessNode node) {
        synchronized (BY_NODE) {
            return BY_NODE.computeIfAbsent(
                node,
                n -> new SearchReadiness(n.roles().contains(ServerlessNode.ROLE_SEARCH), n.settings().getAsBoolean(WARM_ON_JOIN, false))
            );
        }
    }

    /**
     * Returns where the node is.
     *
     * @return the state
     */
    public State state() {
        return state;
    }

    /**
     * Whether the node is sent searches, and may answer one it coordinates from a reader open here.
     *
     * @return true once ready and until it drains
     */
    public boolean ready() {
        return state == State.READY;
    }

    /**
     * Whether this node serves searches at all, whatever its state.
     *
     * @return true if it has the search role
     */
    public boolean servesSearches() {
        return servesSearches;
    }

    /** Marks a warming node ready; its next lease renewal advertises the search role. A draining node stays draining. */
    public void markReady() {
        synchronized (this) {
            if (state == State.WARMING) {
                state = State.READY;
            }
        }
    }

    /**
     * Starts draining: the next lease renewal drops the search role, and the readiness endpoint answers not ready.
     *
     * @return true if this call started it, false if the node was already draining
     */
    public boolean startDraining() {
        synchronized (this) {
            if (state == State.DRAINING) {
                return false;
            }
            state = State.DRAINING;
            return true;
        }
    }

    /**
     * The roles the node's lease advertises: its configured roles, less the search role while it is not ready.
     *
     * @param roles the configured roles
     * @return the roles to advertise
     */
    public Set<String> advertised(Set<String> roles) {
        if (ready() || roles.contains(ServerlessNode.ROLE_SEARCH) == false) {
            return roles;
        }
        final Set<String> without = new LinkedHashSet<>(roles);
        without.remove(ServerlessNode.ROLE_SEARCH);
        return Set.copyOf(without);
    }
}
