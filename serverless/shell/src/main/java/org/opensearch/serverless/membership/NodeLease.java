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

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * One node's lease: the single object that says a node is alive and what it is willing to do.
 *
 * <p>{@code rfc-serverless-shell.md} §9.3 gives this a register per node, written only by that node, so
 * it has zero contention by construction. §10.4 puts roles here rather than in cluster topology, which
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

    /**
     * Creates a lease.
     *
     * @param nodeId stable node identity
     * @param ephemeralId changes on every restart, so a restarted node is distinguishable
     * @param address transport address, as a hint for peers
     * @param roles what this node currently accepts, per §10.4
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
     * @param roles what this node currently accepts, per §10.4
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
        return nowMillis >= expiresAtMillis;
    }

    /**
     * Copies this lease with a new expiry, for heartbeat renewal.
     *
     * @param newExpiryMillis the new expiry
     * @return the renewed lease
     */
    public NodeLease renewedUntil(long newExpiryMillis) {
        return new NodeLease(nodeId, ephemeralId, address, roles, newExpiryMillis, name, version);
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
            String nodeId = null;
            String ephemeralId = null;
            String address = null;
            String name = null;
            String version = null;
            long expiry = 0L;
            final Set<String> roles = new LinkedHashSet<>();
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY && "roles".equals(field)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        roles.add(parser.text());
                    }
                } else if (token.isValue()) {
                    switch (field == null ? "" : field) {
                        case "node_id" -> nodeId = parser.text();
                        case "ephemeral_id" -> ephemeralId = parser.text();
                        case "address" -> address = parser.text();
                        case "expires_at_millis" -> expiry = parser.longValue();
                        case "name" -> name = parser.text();
                        case "version" -> version = parser.text();
                        default -> {
                            // forward compatibility: ignore fields written by a newer node
                        }
                    }
                }
            }
            if (nodeId == null || ephemeralId == null || address == null) {
                throw new IOException("malformed node lease: missing a required field");
            }
            return new NodeLease(nodeId, ephemeralId, address, roles, expiry, name, version);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof NodeLease other) {
            return expiresAtMillis == other.expiresAtMillis
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
