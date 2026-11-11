/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.membership.NodeLease;

import java.util.Collection;
import java.util.List;

/**
 * Which zone a node runs in, and whether two nodes share one.
 *
 * <p><b>The zone is stock OpenSearch's awareness attribute.</b> A node declares it the way any OpenSearch
 * node does, as {@code node.attr.zone}, and a deployment that names its attribute differently says so in
 * {@code cluster.routing.allocation.awareness.attributes}, whose first entry is then read. On Kubernetes
 * the value is the node's {@code topology.kubernetes.io/zone} label, handed to the setting through the
 * usual environment substitution ({@code node.attr.zone: ${NODE_ZONE}}); nothing here calls a cloud API,
 * so the same configuration runs on EKS, GKE or anywhere else.
 *
 * <p><b>Why zones matter here at all.</b> The clouds this runs on bill traffic between zones by the byte and
 * do not bill traffic inside one, and the object store is free from every zone in a region. So every peer
 * choice that has a same-zone option should take it. It is a preference and never a rule: a peer in
 * another zone, or one whose zone is unknown, is still a peer, because availability across zones is the
 * reason to run in several.
 *
 * <p><b>Unknown is not a zone.</b> A node with no attribute set is in no zone, and two such nodes are not
 * in the same one: otherwise a fleet that never configured zones would look like one zone and stop
 * spreading, and a half-configured fleet would bunch every unconfigured node together.
 */
public final class Zones {

    /** The stock awareness setting; its first attribute names the zone, and {@value #DEFAULT_ATTRIBUTE} when unset. */
    public static final String AWARENESS_ATTRIBUTES = "cluster.routing.allocation.awareness.attributes";

    /** The attribute stock OpenSearch deployments use for availability zones. */
    public static final String DEFAULT_ATTRIBUTE = "zone";

    private Zones() {}

    /**
     * Returns the name of the node attribute that holds the zone.
     *
     * @param settings the node's settings
     * @return the first awareness attribute, or {@value #DEFAULT_ATTRIBUTE}
     */
    public static String attribute(Settings settings) {
        final List<String> named = settings.getAsList(AWARENESS_ATTRIBUTES, List.of());
        for (String name : named) {
            if (name != null && name.isBlank() == false) {
                return name.trim();
            }
        }
        return DEFAULT_ATTRIBUTE;
    }

    /**
     * Returns this node's zone, from {@code node.attr.<attribute>}.
     *
     * @param settings the node's settings
     * @return the zone, or null when the node does not declare one
     */
    public static String localZone(Settings settings) {
        return normalise(settings.get("node.attr." + attribute(settings)));
    }

    /**
     * Returns a member's zone as its lease advertises it.
     *
     * @param members the live leases
     * @param nodeId the member
     * @return the zone, or null when the member is unknown or advertises none
     */
    public static String zoneOf(Collection<NodeLease> members, String nodeId) {
        for (NodeLease lease : members) {
            if (lease.nodeId().equals(nodeId)) {
                return lease.zone();
            }
        }
        return null;
    }

    /**
     * Whether two zones are known and the same.
     *
     * @param a one zone, or null
     * @param b the other, or null
     * @return true only when both are known and equal
     */
    public static boolean same(String a, String b) {
        return a != null && a.equals(b);
    }

    /**
     * Whether two zones are known and different: the case the clouds bill.
     *
     * @param a one zone, or null
     * @param b the other, or null
     * @return true only when both are known and differ
     */
    public static boolean crosses(String a, String b) {
        return a != null && b != null && a.equals(b) == false;
    }

    static String normalise(String zone) {
        if (zone == null || zone.isBlank()) {
            return null;
        }
        return zone.trim();
    }
}
