/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.membership;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.serverless.store.NodeLogEpoch;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * One node's lease: the single object that says a node is alive and what it is willing to do.
 *
 * <p>Each node has its own register, written only by that node, so it has zero contention by
 * construction. Roles live here rather than in cluster topology, which
 * is what lets one binary serve as ingest or search without a deployment-level distinction.
 *
 * <p>Liveness is expressed as a writer-declared expiry. That trusts the writer's clock — see
 * {@code BlobLeaseMembership} for what that assumes and does not assume.
 */
public final class NodeLease {

    private final String nodeId;
    private final String ephemeralId;
    private final String address;
    private final Set<String> roles;
    private final long expiresAtMillis;
    private final String name;
    private final String version;
    private final boolean revoked;
    private final int held;
    private final int cap;
    private final int inUse;
    private final int refused;
    private final List<NodeLogEpoch> nodeLog;
    private final String zone;

    /**
     * Creates a lease.
     *
     * @param nodeId stable node identity
     * @param ephemeralId changes on every restart, so a restarted node is distinguishable
     * @param address transport address, as a hint for peers
     * @param roles what this node currently accepts
     * @param expiresAtMillis wall-clock expiry declared by the writer
     */
    public NodeLease(String nodeId, String ephemeralId, String address, Set<String> roles, long expiresAtMillis) {
        this(nodeId, ephemeralId, address, roles, expiresAtMillis, null, null);
    }

    /**
     * Creates a lease that also describes the node.
     *
     * <p><b>Why a lease carries a name and a version at all.</b> This register is the only place one node
     * learns about another — it is already the address book write forwarding routes through. Answering
     * {@code GET /_nodes} means reporting a peer's name and version, and the alternatives to putting them
     * here are worse: asking each peer over the transport turns a listing into a fan-out, and reporting the
     * local node's version for a peer would be a guess that is wrong during exactly the upgrade it matters
     * in. A node knows its own name and version, so it publishes them.
     *
     * <p>Both are optional, and a lease written before they existed parses with them absent — the name
     * falls back to the node id, and the version is reported as unknown rather than invented.
     *
     * @param nodeId stable node identity
     * @param ephemeralId changes on every restart, so a restarted node is distinguishable
     * @param address transport address, as a hint for peers
     * @param roles what this node currently accepts
     * @param expiresAtMillis wall-clock expiry declared by the writer
     * @param name the node's configured name, or null
     * @param version the node's build version, or null
     */
    public NodeLease(
        String nodeId,
        String ephemeralId,
        String address,
        Set<String> roles,
        long expiresAtMillis,
        String name,
        String version
    ) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.ephemeralId = Objects.requireNonNull(ephemeralId);
        this.address = Objects.requireNonNull(address);
        this.roles = Set.copyOf(roles);
        this.expiresAtMillis = expiresAtMillis;
        this.name = name;
        this.version = version;
        this.revoked = false;
        this.held = -1;
        this.cap = -1;
        this.inUse = -1;
        this.refused = -1;
        this.nodeLog = List.of();
        this.zone = null;
    }

    private NodeLease(NodeLease lease, boolean revoked, long expiresAtMillis, int held, int cap) {
        this(lease, revoked, expiresAtMillis, held, cap, lease.inUse, lease.refused);
    }

    private NodeLease(NodeLease lease, boolean revoked, long expiresAtMillis, int held, int cap, int inUse, int refused) {
        this(lease, revoked, expiresAtMillis, held, cap, inUse, refused, lease.nodeLog);
    }

    private NodeLease(
        NodeLease lease,
        boolean revoked,
        long expiresAtMillis,
        int held,
        int cap,
        int inUse,
        int refused,
        List<NodeLogEpoch> nodeLog
    ) {
        this.nodeId = lease.nodeId;
        this.ephemeralId = lease.ephemeralId;
        this.address = lease.address;
        this.roles = lease.roles;
        this.expiresAtMillis = expiresAtMillis;
        this.name = lease.name;
        this.version = lease.version;
        this.revoked = revoked;
        this.held = held;
        this.cap = cap;
        this.inUse = inUse;
        this.refused = refused;
        this.nodeLog = List.copyOf(nodeLog);
        this.zone = lease.zone;
    }

    private NodeLease(NodeLease lease, String zone) {
        this.nodeId = lease.nodeId;
        this.ephemeralId = lease.ephemeralId;
        this.address = lease.address;
        this.roles = lease.roles;
        this.expiresAtMillis = lease.expiresAtMillis;
        this.name = lease.name;
        this.version = lease.version;
        this.revoked = lease.revoked;
        this.held = lease.held;
        this.cap = lease.cap;
        this.inUse = lease.inUse;
        this.refused = lease.refused;
        this.nodeLog = lease.nodeLog;
        this.zone = zone == null || zone.isBlank() ? null : zone;
    }

    /**
     * Returns this lease carrying the failure domain its node runs in: on Kubernetes, the value of the
     * node's {@code topology.kubernetes.io/zone} label.
     *
     * <p>Peers read it to keep traffic inside a zone where they can, because traffic between zones is
     * billed by the byte on the clouds this runs on and traffic inside one is not. It is a preference
     * only: nothing may refuse a peer for being in another zone, and a lease without one is a peer
     * whose zone is unknown, never one that is excluded.
     *
     * @param zone the zone, or null or blank when unknown
     * @return the copy
     */
    public NodeLease withZone(String zone) {
        return new NodeLease(this, zone);
    }

    /**
     * Returns the zone the node runs in.
     *
     * @return the zone, or null when the lease does not say
     */
    public String zone() {
        return zone;
    }

    /**
     * Returns this lease carrying how full its node is and how much of that is demand: shards used in the last minute, and
     * shards it turned away at its cap in the last minute. Held counts a cache that fills whatever the load; these two are
     * what an autoscaler sizes the fleet on.
     *
     * @param held shards this node holds
     * @param cap the most it will hold, or -1 when unbounded
     * @param inUse shards used in the last minute
     * @param refused shards turned away at the cap in the last minute
     * @return the copy
     */
    public NodeLease withLoad(int held, int cap, int inUse, int refused) {
        return new NodeLease(this, revoked, expiresAtMillis, held, cap, inUse, refused);
    }

    /**
     * Returns how many shards the node used in the last minute.
     *
     * @return the count, or -1 when the lease does not say
     */
    public int inUse() {
        return inUse;
    }

    /**
     * Returns how many shards the node turned away at its cap in the last minute.
     *
     * @return the count, or -1 when the lease does not say
     */
    public int refused() {
        return refused;
    }

    /**
     * Returns this lease carrying how full its node is.
     *
     * <p>The lease is renewed every few seconds and read by every peer already, so it is where a node can say how
     * many shards it holds against its cap at no extra request: a full node uses it to find a member with room to hand
     * work to, and an operator or an autoscaler reads it fleet-wide from one listing.
     *
     * @param held shards this node holds
     * @param cap the most it will hold, or -1 when unbounded
     * @return the copy
     */
    public NodeLease withLoad(int held, int cap) {
        return new NodeLease(this, revoked, expiresAtMillis, held, cap, inUse, refused);
    }

    /**
     * Returns how many shards the node held when it last renewed.
     *
     * @return the count, or -1 when the lease does not say
     */
    public int held() {
        return held;
    }

    /**
     * Returns the most shards the node will hold.
     *
     * @return the cap, or -1 when the lease does not say or there is none
     */
    public int cap() {
        return cap;
    }

    /**
     * Returns this lease revoked: a node that took one of its holder's shards has marked it so, and the
     * holder's next renewal, which expects the generation it last wrote, fails on the change.
     *
     * <p>Still a whole lease rather than a bare marker, because every reader of this register parses it
     * and treats an unreadable one as alive -- a marker that did not parse would block every takeover.
     *
     * @return the revoked copy
     */
    public NodeLease revokedCopy() {
        return new NodeLease(this, true, expiresAtMillis, held, cap);
    }

    /**
     * Reports whether a successor has revoked this lease.
     *
     * @return true if revoked
     */
    public boolean revoked() {
        return revoked;
    }

    /**
     * Returns this lease carrying the epochs of its node's write log that a successor would have to fence and replay.
     *
     * <p>The lease is where a successor learns them, so an epoch is recorded here before its first blob is written, and
     * the trim point and landed hint of each are refreshed on every renewal. A successor reads no listing to find them.
     *
     * @param epochs the open epochs, oldest first
     * @return the copy
     */
    public NodeLease withNodeLog(List<NodeLogEpoch> epochs) {
        return new NodeLease(this, revoked, expiresAtMillis, held, cap, inUse, refused, epochs);
    }

    /**
     * Returns the epochs of this node's write log that still hold records not yet published; empty for a node that
     * writes no such log, or a lease written before the field.
     *
     * @return the epochs, oldest first
     */
    public List<NodeLogEpoch> nodeLog() {
        return nodeLog;
    }

    /**
     * Returns the node's name, falling back to its id when the lease predates the field.
     *
     * @return a name that is always usable for display
     */
    public String name() {
        return name == null || name.isBlank() ? nodeId : name;
    }

    /**
     * Returns the node's build version, if it published one.
     *
     * @return the version, or null when unknown
     */
    public String version() {
        return version;
    }

    /**
     * Returns the stable node identity.
     *
     * @return the node id
     */
    public String nodeId() {
        return nodeId;
    }

    /**
     * Returns the identity that changes on every restart.
     *
     * @return the ephemeral id
     */
    public String ephemeralId() {
        return ephemeralId;
    }

    /**
     * Returns the transport address, which is a hint for peers.
     *
     * @return the address
     */
    public String address() {
        return address;
    }

    /**
     * Returns the roles this node currently accepts.
     *
     * @return the roles
     */
    public Set<String> roles() {
        return roles;
    }

    /**
     * Returns the expiry stamped by the writer.
     *
     * @return expiry in wall-clock millis
     */
    public long expiresAtMillis() {
        return expiresAtMillis;
    }

    /**
     * Reports whether this lease has expired.
     *
     * @param nowMillis the observer's clock
     * @return whether this lease has expired from the observer's point of view
     */
    public boolean isExpiredAt(long nowMillis) {
        // A revoked lease is dead whatever it says about its expiry.
        return revoked || nowMillis >= expiresAtMillis;
    }

    /**
     * Copies this lease with a new expiry, for heartbeat renewal.
     *
     * @param newExpiryMillis the new expiry
     * @return the renewed lease
     */
    public NodeLease renewedUntil(long newExpiryMillis) {
        return new NodeLease(this, false, newExpiryMillis, held, cap);
    }

    /**
     * Serializes this lease.
     *
     * @return this lease as JSON bytes
     * @throws IOException if serialization fails
     */
    public BytesReference toBytes() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("node_id", nodeId);
            builder.field("ephemeral_id", ephemeralId);
            builder.field("address", address);
            builder.field("roles", roles.stream().sorted().toArray(String[]::new));
            builder.field("expires_at_millis", expiresAtMillis);
            // Written only when known, so a lease from a node that does not publish them is byte-identical
            // to what this class wrote before they existed.
            if (name != null) {
                builder.field("name", name);
            }
            if (version != null) {
                builder.field("version", version);
            }
            if (revoked) {
                builder.field("revoked", true);
            }
            if (held >= 0) {
                builder.field("held", held);
            }
            if (cap >= 0) {
                builder.field("cap", cap);
            }
            if (inUse >= 0) {
                builder.field("in_use", inUse);
            }
            if (refused >= 0) {
                builder.field("refused", refused);
            }
            if (zone != null) {
                builder.field("zone", zone);
            }
            if (nodeLog.isEmpty() == false) {
                builder.startArray("node_log");
                for (NodeLogEpoch epoch : nodeLog) {
                    builder.startObject();
                    builder.field("epoch", epoch.epoch());
                    builder.field("trim", epoch.trimPoint());
                    builder.field("landed", epoch.landed());
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Parses a lease from its JSON form.
     *
     * @param input the serialized lease
     * @return the parsed lease
     * @throws IOException if the bytes are not a well-formed lease
     */
    public static NodeLease fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IOException("malformed node lease: expected an object");
            }
            String nodeId = null;
            String ephemeralId = null;
            String address = null;
            String name = null;
            String version = null;
            boolean revoked = false;
            long expiry = 0L;
            int held = -1;
            int cap = -1;
            int inUse = -1;
            int refused = -1;
            String zone = null;
            final Set<String> roles = new LinkedHashSet<>();
            final List<NodeLogEpoch> nodeLog = new ArrayList<>();
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == null) {
                    throw new IOException("malformed node lease: missing end of object");
                }
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY && "roles".equals(field)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        roles.add(parser.text());
                    }
                } else if (token == XContentParser.Token.START_ARRAY && "node_log".equals(field)) {
                    while (parser.nextToken() == XContentParser.Token.START_OBJECT) {
                        nodeLog.add(parseNodeLogEpoch(parser));
                    }
                } else if (token.isValue()) {
                    switch (field == null ? "" : field) {
                        case "node_id" -> nodeId = parser.text();
                        case "ephemeral_id" -> ephemeralId = parser.text();
                        case "address" -> address = parser.text();
                        case "expires_at_millis" -> expiry = parser.longValue();
                        case "name" -> name = parser.text();
                        case "version" -> version = parser.text();
                        case "revoked" -> revoked = parser.booleanValue();
                        case "held" -> held = parser.intValue();
                        case "cap" -> cap = parser.intValue();
                        case "in_use" -> inUse = parser.intValue();
                        case "refused" -> refused = parser.intValue();
                        case "zone" -> zone = parser.text();
                        default -> {
                            // forward compatibility: ignore fields written by a newer node
                        }
                    }
                } else {
                    parser.skipChildren();
                }
            }
            if (nodeId == null || ephemeralId == null || address == null) {
                throw new IOException("malformed node lease: missing a required field");
            }
            final NodeLease lease = new NodeLease(nodeId, ephemeralId, address, roles, expiry, name, version).withLoad(
                held,
                cap,
                inUse,
                refused
            ).withNodeLog(nodeLog).withZone(zone);
            return revoked ? lease.revokedCopy() : lease;
        }
    }

    private static NodeLogEpoch parseNodeLogEpoch(XContentParser parser) throws IOException {
        long epoch = -1L;
        long trim = 0L;
        long landed = 0L;
        String field = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == null) {
                throw new IOException("malformed node lease: unterminated node_log entry");
            }
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
            } else if (token.isValue()) {
                switch (field == null ? "" : field) {
                    case "epoch" -> epoch = parser.longValue();
                    case "trim" -> trim = parser.longValue();
                    case "landed" -> landed = parser.longValue();
                    default -> {
                        // forward compatibility: ignore fields written by a newer node
                    }
                }
            } else {
                parser.skipChildren();
            }
        }
        if (epoch < 0) {
            throw new IOException("malformed node lease: a node_log entry has no epoch");
        }
        return new NodeLogEpoch(epoch, trim, landed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof NodeLease other) {
            return expiresAtMillis == other.expiresAtMillis
                && revoked == other.revoked
                && nodeId.equals(other.nodeId)
                && ephemeralId.equals(other.ephemeralId)
                && address.equals(other.address)
                && roles.equals(other.roles);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, ephemeralId, address, roles, expiresAtMillis);
    }

    @Override
    public String toString() {
        return "NodeLease[" + nodeId + "/" + ephemeralId + " roles=" + roles + " expires=" + expiresAtMillis + "]";
    }
}
