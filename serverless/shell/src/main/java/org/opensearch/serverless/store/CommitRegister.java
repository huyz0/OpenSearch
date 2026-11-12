/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Optional;

/**
 * Where a shard's published commit is recorded: its own manifest register, or its entry in a bucket's manifest.
 *
 * <p>A publisher reads and swaps its commit only through this, so a bucket member and a shard of its own publish,
 * stage, check point and adopt alike. The generation is whatever tells this shard's commit apart from the one before:
 * the register's generation for a shard of its own, the member entry's version for a bucket member, so a sibling's
 * swap never looks like this shard's commit moving.
 */
public interface CommitRegister {

    /**
     * Reads the commit.
     *
     * @return the commit's bytes and generation, or empty if none is published
     * @throws IOException if it cannot be read
     */
    Optional<BlobRegister> read() throws IOException;

    /**
     * Replaces the commit, only if it is still at the generation given.
     *
     * @param expectedGeneration the generation read, or {@link BlobRegister#ABSENT_GENERATION} for none
     * @param value the new commit's bytes
     * @return whether it applied, and the generation now
     * @throws IOException if it cannot be written
     */
    BlobRegisterCasResult compareAndSwap(long expectedGeneration, BytesReference value) throws IOException;

    /**
     * The register a shard of its own keeps under its data path.
     *
     * @param container the shard's container
     * @param name the register's name
     * @return the register
     */
    static CommitRegister of(ConditionalBlobContainer container, String name) {
        return new CommitRegister() {
            @Override
            public Optional<BlobRegister> read() throws IOException {
                return container.readRegister(name);
            }

            @Override
            public BlobRegisterCasResult compareAndSwap(long expectedGeneration, BytesReference value) throws IOException {
                return container.compareAndSwapRegister(name, expectedGeneration, value);
            }
        };
    }
}
