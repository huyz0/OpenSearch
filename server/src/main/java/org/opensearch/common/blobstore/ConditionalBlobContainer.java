/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Optional;

/**
 * A blob container that can arbitrate writes to "register" blobs: small blobs read with a generation and replaced
 * only if that generation is still current. It gives callers a compare-and-swap primitive over whatever atomicity the
 * store provides natively, such as S3 {@code If-Match} / {@code If-None-Match}, GCS generation preconditions, Azure
 * ETags, or file locks on a filesystem.
 *
 * <p>Opt-in. A {@link BlobContainer} that cannot offer these guarantees simply does not implement this interface, and
 * callers that need it obtain it with {@link #of(BlobContainer)}. A container that wraps another one implements this
 * interface by delegating through {@link #of(BlobContainer)}, so a wrapped store keeps its conditional writes and an
 * unsupported one still fails clearly.
 *
 * @opensearch.experimental
 */
public interface ConditionalBlobContainer extends BlobContainer {

    /**
     * Returns {@code container} as a conditional container.
     *
     * @throws UnsupportedOperationException if the container does not implement conditional register writes
     */
    static ConditionalBlobContainer of(BlobContainer container) {
        if (container instanceof ConditionalBlobContainer conditional) {
            return conditional;
        }
        throw new UnsupportedOperationException(container.getClass() + " does not support conditional register writes");
    }

    /**
     * Reads the current value of a register blob and the generation it was read at.
     *
     * @return empty if the register has never been written (generation {@link BlobRegister#ABSENT_GENERATION})
     */
    Optional<BlobRegister> readRegister(String blobName) throws IOException;

    /**
     * Atomically replaces a register's value, succeeding only if its current generation equals
     * {@code expectedGeneration}. Pass {@link BlobRegister#ABSENT_GENERATION} to require that the register does not
     * exist yet.
     *
     * <p>The store's own conditional write is the guard: a concurrent writer racing between a read and this call is
     * caught by the store, not missed.
     *
     * @return whether the write applied, and its new generation on success; on refusal an observed conflicting
     *     generation, or {@link BlobRegisterCasResult#UNKNOWN_GENERATION} if the store cannot report it atomically
     */
    BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue) throws IOException;

    /**
     * Writes a register only if it does not exist yet, in as few round trips as the store allows.
     *
     * <p>Equivalent to {@code compareAndSwapRegister(blobName, ABSENT_GENERATION, value)}, which is the default. It
     * exists because the general form has to learn the current generation, so an object-store implementation reads
     * before it writes. Creation does not need to: "must not exist" is a precondition on the write itself, so a store
     * can create in one request.
     *
     * <p>Implementations may report {@link BlobRegisterCasResult#UNKNOWN_GENERATION} on conflict rather than pay for a
     * read that could describe a later state. For compatibility, a refused create may also report
     * {@link BlobRegister#ABSENT_GENERATION}. Callers that need the current value must read it. A successful create
     * always reports the newly stored generation.
     *
     * @return whether this caller created the register
     */
    default BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
        return compareAndSwapRegister(blobName, BlobRegister.ABSENT_GENERATION, value);
    }

    /**
     * Deletes a register only if it still holds {@code expectedGeneration}, with the store's own conditional delete
     * (S3 and Azure {@code If-Match} on the ETag, a GCS generation precondition, a lock on a filesystem).
     *
     * <p>This lets a caller delete a register outright instead of leaving a tombstone. An unconditional delete races a
     * concurrent write and can remove a value written just after the caller read it. Together with
     * {@link BlobRegister#initialGeneration()}, which starts a re-created register at a generation its predecessor
     * almost surely never had, a compare-and-swap carrying a generation from before the delete cannot land on what is
     * created after it.
     *
     * <p><b>Not every store that accepts the call honours the condition.</b> An S3-compatible store may accept
     * {@code If-Match} on a delete and ignore it. A caller relying on this for safety should first check that a delete
     * against a wrong generation is refused.
     *
     * @param blobName the register
     * @param expectedGeneration the generation the caller read
     * @return true if this call deleted it; false if it held another generation or did not exist
     * @throws UnsupportedOperationException if the store has no conditional delete
     */
    default boolean deleteRegisterIfUnchanged(String blobName, long expectedGeneration) throws IOException {
        throw new UnsupportedOperationException(getClass() + " does not support deleteRegisterIfUnchanged");
    }
}
