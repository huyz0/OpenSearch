/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.common.bytes.BytesArray;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Establishes, against the store actually in use, whether a conditional delete is honoured.
 *
 * <p><b>Why a probe rather than trust.</b> Deleting a register outright, instead of leaving a tombstone,
 * is safe only if a delete conditioned on a generation the register no longer holds is refused. An
 * S3-compatible store can accept {@code If-Match} on {@code DeleteObject} and ignore it, and then a delete
 * racing a write removes the write. Nothing in the API says which kind of store it is, so this asks it: a
 * register is written, moved on through a second container, and then deleted through the first at the
 * generation it no longer holds. A store that deletes it anyway fails the probe, and the caller keeps
 * tombstones.
 *
 * <p>Two containers, because a container may remember the version it last saw and send its token without
 * reading first; a stale token is exactly what the store must be seen to refuse.
 */
public final class ConditionalDeleteProbe {

    private ConditionalDeleteProbe() {}

    /**
     * Runs the probe.
     *
     * @param blobStore the store
     * @param path where the probe may write, and removes from again
     * @return true only if a stale conditional delete was refused and a current one applied
     */
    public static boolean honoured(BlobStore blobStore, BlobPath path) {
        final String name = "conditional-delete-" + UUIDs.randomBase64UUID();
        final BlobContainer first = blobStore.blobContainer(path);
        final BlobContainer second = blobStore.blobContainer(path);
        try {
            final BlobRegisterCasResult created = first.createRegisterIfAbsent(name, value("created"));
            if (created.applied() == false) {
                return false;
            }
            // Read back through the first container, so one that caches versions knows this one's token.
            final Optional<BlobRegister> seen = first.readRegister(name);
            if (seen.isEmpty()) {
                return false;
            }
            final BlobRegisterCasResult moved = second.compareAndSwapRegister(name, seen.get().generation(), value("moved"));
            if (moved.applied() == false) {
                return false;
            }
            if (first.deleteRegisterIfUnchanged(name, seen.get().generation())) {
                // The store deleted a register at a generation it no longer held.
                return false;
            }
            final Optional<BlobRegister> survived = second.readRegister(name);
            if (survived.isEmpty() || survived.get().generation() != moved.currentGeneration()) {
                return false;
            }
            return second.deleteRegisterIfUnchanged(name, moved.currentGeneration()) && second.readRegister(name).isEmpty();
        } catch (UnsupportedOperationException | IOException e) {
            return false;
        } finally {
            try {
                second.deleteBlobsIgnoringIfNotExists(List.of(name));
            } catch (IOException | UnsupportedOperationException e) {
                // The probe's own blob; left behind it harms nothing.
            }
        }
    }

    private static BytesArray value(String text) {
        return new BytesArray(text.getBytes(StandardCharsets.UTF_8));
    }
}
