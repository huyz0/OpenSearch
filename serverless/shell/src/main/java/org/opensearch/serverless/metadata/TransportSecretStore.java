/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.ConditionalBlobContainer;

import java.io.IOException;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Reads, mints and rotates the deployment's transport secret, which lives in one register beside the cluster
 * config.
 */
final class TransportSecretStore {

    private final ConditionalBlobContainer container;
    private final LongSupplier clock;

    /**
     * Creates the store over the cluster-config container.
     *
     * @param container the container the secret's register lives in
     * @param clock source of wall-clock millis, stamped on a minted or rotated secret
     */
    TransportSecretStore(BlobContainer container, LongSupplier clock) {
        this.container = ConditionalBlobContainer.of(container);
        this.clock = clock;
    }

    /**
     * Reads the transport secrets, minting the first under put-if-absent when there are none.
     *
     * @return the secrets and the register generation they sit at
     * @throws IOException if the register cannot be read or created
     */
    MetadataPlane.TransportSecrets transportSecrets() throws IOException {
        final Optional<org.opensearch.common.blobstore.BlobRegister> existing = container.readRegister(TRANSPORT_SECRET_BLOB);
        if (existing.isPresent()) {
            return atGeneration(MetadataPlane.TransportSecrets.parse(existing.get().value().utf8ToString()), existing.get().generation());
        }
        final MetadataPlane.TransportSecrets minted = new MetadataPlane.TransportSecrets(1L, freshHex(), null, clock.getAsLong());
        final var created = container.createRegisterIfAbsent(
            TRANSPORT_SECRET_BLOB,
            new org.opensearch.core.common.bytes.BytesArray(minted.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );
        if (created.applied()) {
            return atGeneration(minted, created.currentGeneration());
        }
        final Optional<org.opensearch.common.blobstore.BlobRegister> raced = container.readRegister(TRANSPORT_SECRET_BLOB);
        if (raced.isEmpty()) {
            throw new IOException("transport secret vanished");
        }
        return atGeneration(MetadataPlane.TransportSecrets.parse(raced.get().value().utf8ToString()), raced.get().generation());
    }

    /**
     * Rotates the transport secret: the current one becomes the previous, a fresh one becomes current.
     *
     * <p>A compare-and-swap on the register, so two operators rotating at once produce one new
     * generation rather than two secrets neither knows about. The previous secret stays accepted until
     * the next rotation, which is the window every node has to re-read the register.
     *
     * @return the secrets as rotated
     * @throws IOException if the register cannot be read or the swap keeps losing
     */
    MetadataPlane.TransportSecrets rotateTransportSecret() throws IOException {
        for (int attempt = 0; attempt < 8; attempt++) {
            final MetadataPlane.TransportSecrets old = transportSecrets();
            final MetadataPlane.TransportSecrets next = new MetadataPlane.TransportSecrets(
                old.generation() + 1,
                freshHex(),
                old.current(),
                clock.getAsLong()
            );
            final var swapped = container.compareAndSwapRegister(
                TRANSPORT_SECRET_BLOB,
                old.generation(),
                new org.opensearch.core.common.bytes.BytesArray(next.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8))
            );
            if (swapped.applied()) {
                return atGeneration(next, swapped.currentGeneration());
            }
        }
        throw new IOException("transport secret rotation lost the compare-and-swap eight times; something else is rotating it");
    }

    /**
     * The register's own generation is the one a node caches against, so the record carries it rather
     * than the counter it was written with -- the two agree except for a register written before
     * generations existed, whose stored counter is a placeholder.
     */
    private static MetadataPlane.TransportSecrets atGeneration(MetadataPlane.TransportSecrets secrets, long registerGeneration) {
        return new MetadataPlane.TransportSecrets(registerGeneration, secrets.current(), secrets.previous(), secrets.rotatedAtMillis());
    }

    private static String freshHex() {
        final byte[] fresh = new byte[32];
        new java.security.SecureRandom().nextBytes(fresh);
        return org.opensearch.common.hash.MessageDigests.toHexString(fresh);
    }

    private static final String TRANSPORT_SECRET_BLOB = "transport-secret";
}
