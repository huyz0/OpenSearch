/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import java.util.function.Supplier;

/**
 * The transport secrets a node's peers present to one another, as last read from the metadata plane, re-read at
 * most once per lease TTL and on demand.
 */
public final class TransportSecretCache {

    private final Supplier<org.opensearch.serverless.metadata.MetadataPlane> metadataPlane;

    /**
     * Creates an empty cache.
     *
     * @param metadataPlane the node's metadata plane, null before one is adopted
     */
    TransportSecretCache(Supplier<org.opensearch.serverless.metadata.MetadataPlane> metadataPlane) {
        this.metadataPlane = metadataPlane;
    }

    private record CachedTransportSecrets(org.opensearch.serverless.metadata.MetadataPlane plane,
        org.opensearch.serverless.metadata.TransportSecrets secrets, long readAt) {
    }

    private volatile CachedTransportSecrets transportSecrets;
    private final Object transportSecretsLock = new Object();

    /**
     * Returns the transport secrets as last read, re-reading the register at most once per lease TTL and
     * on demand.
     *
     * <p>The bearer used to be read once and kept for the life of the process, so rotating it meant
     * restarting every node. The MAC design ({@code TransportAuthenticator}) verifies under the current
     * generation and, for one window, the previous one, and re-reads when it sees a generation it does
     * not know -- that is the {@code refresh} argument. The TTL bound on the cache is what makes a
     * rotation reach every node inside one lease even when nobody forwards it anything under the new
     * generation.
     *
     * @param forceReread true to re-read now -- a verifier that met a generation newer than its copy
     * @return the secrets, or null before a metadata plane is adopted
     * @throws java.io.UncheckedIOException if the register cannot be read
     */
    public org.opensearch.serverless.metadata.TransportSecrets transportSecrets(boolean forceReread) {
        final var plane = metadataPlane.get();
        if (plane == null) {
            return null;
        }
        final CachedTransportSecrets observed = transportSecrets;
        final long now = plane.clock().getAsLong();
        if (forceReread || expiredSecrets(observed, plane, now)) {
            synchronized (transportSecretsLock) {
                CachedTransportSecrets cached = transportSecrets;
                // Another reader may have refreshed while this one waited. Publish the value and its
                // timestamp together, and coalesce concurrent explicit refreshes of the same snapshot.
                if ((forceReread && cached == observed) || expiredSecrets(cached, plane, plane.clock().getAsLong())) {
                    try {
                        final var fresh = plane.transportSecrets();
                        cached = new CachedTransportSecrets(plane, fresh, plane.clock().getAsLong());
                        transportSecrets = cached;
                    } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
                return cached.secrets();
            }
        }
        return observed.secrets();
    }

    private static boolean expiredSecrets(CachedTransportSecrets cached, org.opensearch.serverless.metadata.MetadataPlane plane, long now) {
        return cached == null || cached.plane() != plane || now < cached.readAt() || now - cached.readAt() > plane.leaseTtlMillis();
    }

    /**
     * Returns the secret this deployment's nodes present to one another on forwarded requests: the
     * current generation of {@link #transportSecrets(boolean)}.
     *
     * @return the secret, or null before a metadata plane is adopted
     */
    public String transportToken() {
        final var secrets = transportSecrets(false);
        return secrets == null ? null : secrets.current();
    }
}
