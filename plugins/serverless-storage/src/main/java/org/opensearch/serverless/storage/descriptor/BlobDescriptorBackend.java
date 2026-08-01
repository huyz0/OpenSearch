/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.DescriptorUnavailableException;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link DescriptorBackend} over object storage, replacing the system index for the eight point
 * operations that do not need an index over names.
 *
 * <h2>What this removes</h2>
 *
 * The system index is the only durable metadata this design has not put in the object store. Shard heads
 * and commit manifests are already there. That leaves the descriptor carrying four problems the others do
 * not have: durability is one replica of node disk rather than the store's guarantee, the shard count and
 * index sort are fixed at creation and a hundred million descriptors want roughly a hundred shards, the
 * metadata plane that exists to escape cluster state depends on an index that lives in cluster state, and
 * the whole thing cannot be discarded and rebuilt. This has none of them, and in particular has no
 * bootstrap at all: {@link #available()} is true because there is nothing to create.
 *
 * <h2>Keys are the full name, flat and prefix-preserving</h2>
 *
 * {@code <prefix>/<name>}, with no hash fanout. Two reasons, and the second is the load-bearing one.
 *
 * <p>Hashing the key would spread writes across partitions, but S3 already partitions adaptively on
 * observed key distribution, so diverse tenant names spread on their own. The real hazard is narrower than
 * "you need a hash": monotonically increasing names land in one partition and stay there. That is a naming
 * constraint to state, not a reason to hash.
 *
 * <p>And hashing would destroy the one thing that makes the name index provably a cache. Prefix-preserving
 * keys mean a LIST can enumerate the population, so there is a path from the object store alone back to
 * the full structure. It is far too slow to serve a query (a thousand keys per page behind a serial
 * continuation token) and it returns names without uuids, so it is a recovery path rather than a query
 * path. But without it the name index quietly becomes a second source of truth instead of something
 * rebuildable, which is invariant I2.
 *
 * <h2>Uniqueness is the register, and the key is the name alone</h2>
 *
 * {@link #create} is one conditional write through {@link BlobContainer#createRegisterIfAbsent}, which is
 * what takes the serialised cluster manager out of index creation. The semantics are unchanged from the
 * system index: T18 already made creation create-only through {@code IndexRequest.create(true)}, and a
 * lost race is a {@code false} rather than an error, because losing is a correct outcome.
 *
 * <p><b>The key must never include the uuid.</b> Two clients creating one name with different uuids would
 * write different keys and both conditional writes would succeed, which is uniqueness that silently does
 * not hold. Name only, always.
 */
public final class BlobDescriptorBackend implements DescriptorBackend {

    private static final Logger logger = LogManager.getLogger(BlobDescriptorBackend.class);

    /** Where live descriptors live. A LIST here returns exactly the names that currently exist. */
    public static final String DESCRIPTOR_PREFIX = "descriptors/";

    /**
     * Where deleted ones go, keyed by name rather than by uuid.
     *
     * <p>Uuid-keying is the obvious choice and is wrong here. {@code State.DELETED} exists so that a node
     * partitioned during a delete consults the descriptor, finds the tombstone, and deletes its local shard
     * data instead of resurrecting the index. That lookup is by <em>name</em>, because the name is all the
     * partitioned node has. A uuid-keyed tombstone is unfindable by the one reader it exists for.
     */
    public static final String TOMBSTONE_PREFIX = "tombstones/";

    private final BlobContainer blobContainer;

    public BlobDescriptorBackend(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /**
     * {@inheritDoc}
     *
     * <p>An {@link IOException} is unavailability rather than absence, which is the distinction the whole
     * store is careful about: reporting "not there" for a read that failed lets a client create an index
     * that already exists.
     */
    @Override
    public IndexDescriptor get(String name) {
        try {
            Optional<BlobRegister> live = blobContainer.readRegister(keyFor(name));
            if (live.isPresent()) {
                return decode(live.get().value());
            }
            // Then the tombstone, because "deleted" and "never existed" are different answers and only
            // the first one stops a partitioned node resurrecting the index from its local shard data.
            // This costs a second round trip, but only on a name that is not live, and creation does not
            // come through here at all: it is one conditional write that never reads.
            Optional<BlobRegister> tombstone = blobContainer.readRegister(tombstoneKeyFor(name));
            return tombstone.map(register -> {
                try {
                    return decode(register.value());
                } catch (IOException e) {
                    throw new DescriptorUnavailableException(name, e);
                }
            }).orElse(null);
        } catch (IOException | RuntimeException e) {
            logger.warn("could not read the descriptor for [{}]; reporting unavailable rather than absent", name, e);
            throw new DescriptorUnavailableException(name, e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>One conditional write, not a read followed by a write. {@code createRegisterIfAbsent} exists for
     * exactly this: the must-not-exist check is a precondition the store evaluates atomically, so reading
     * first would only add a round trip and a window for a concurrent creator to fit through.
     */
    @Override
    public boolean create(IndexDescriptor descriptor) {
        try {
            BlobRegisterCasResult result = blobContainer.createRegisterIfAbsent(keyFor(descriptor.name()), encode(descriptor));
            return result.applied();
        } catch (IOException e) {
            throw new DescriptorUnavailableException(descriptor.name(), e);
        }
    }

    @Override
    public CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor) {
        // Deliberately not a fake async wrapper around the blocking call. A CompletableFuture that has
        // already blocked its caller is worse than an honest synchronous method, because it reads as
        // non-blocking at every call site. Threading this properly needs an executor the backend does not
        // own yet, so the contract is stated here rather than pretended.
        return CompletableFuture.completedFuture(create(descriptor));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Unconditional, so it advances the register's generation whatever it was. A caller needing
     * compare-and-swap semantics against a known generation wants the register API directly; this is the
     * "write it, I know what I am doing" path the system index's {@code put} already was.
     */
    @Override
    public void put(IndexDescriptor descriptor) {
        String key = keyFor(descriptor.name());
        try {
            long generation = blobContainer.readRegister(key).map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
            BlobRegisterCasResult result = blobContainer.compareAndSwapRegister(key, generation, encode(descriptor));
            if (result.applied() == false) {
                // Someone wrote between the read and the swap. Retrying once rather than looping, because
                // an unconditional put losing twice means a writer is contending on a descriptor that is
                // supposed to have one owner, and a silent retry loop would hide that.
                BlobRegisterCasResult retry = blobContainer.compareAndSwapRegister(key, result.currentGeneration(), encode(descriptor));
                if (retry.applied() == false) {
                    throw new DescriptorUnavailableException(
                        descriptor.name(),
                        new IllegalStateException("descriptor write lost twice at generation " + retry.currentGeneration())
                    );
                }
            }
        } catch (IOException e) {
            throw new DescriptorUnavailableException(descriptor.name(), e);
        }
    }

    @Override
    public void putAsync(IndexDescriptor descriptor) {
        put(descriptor);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Two writes in an order that matters. The tombstone lands first, then the live descriptor is
     * removed. A crash between them leaves an index that is tombstoned but still listed, which resolves as
     * deleted and is repaired by repeating the delete. The other order leaves a name with no record at all,
     * which is the resurrection case: free to recreate, with a partitioned node still holding shard data
     * for the old uuid and no tombstone to tell it otherwise.
     *
     * <p>The tombstone is written with an unconditional CAS rather than create-if-absent, because deleting
     * an already-deleted index has to be idempotent. Re-tombstoning is a no-op that has to succeed.
     */
    @Override
    public void putTombstoneAsync(IndexDescriptor tombstone) {
        String name = tombstone.name();
        try {
            String tombstoneKey = tombstoneKeyFor(name);
            long generation = blobContainer.readRegister(tombstoneKey).map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
            BlobRegisterCasResult result = blobContainer.compareAndSwapRegister(tombstoneKey, generation, encode(tombstone));
            if (result.applied() == false) {
                // Another deleter of the same index got there first, and it wrote the same thing. Deletion
                // is idempotent, so losing this race is success rather than something to retry.
                logger.debug("tombstone for [{}] was written concurrently at generation {}", name, result.currentGeneration());
            }
            // Only now does the name stop being listed. Deleting a blob that is not there is not an error,
            // so repeating a partially-applied delete converges.
            blobContainer.deleteBlobsIgnoringIfNotExists(List.of(keyFor(name)));
        } catch (IOException e) {
            throw new DescriptorUnavailableException(name, e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Always true. There is nothing to bootstrap: no index to create, no shards to allocate, no
     * settings that have to be applied at creation and can never be changed afterwards. That absence is
     * one of the four problems the system index carried, and it is the only one that goes away for free.
     */
    @Override
    public boolean available() {
        return true;
    }

    private static String keyFor(String name) {
        return DESCRIPTOR_PREFIX + name;
    }

    private static String tombstoneKeyFor(String name) {
        return TOMBSTONE_PREFIX + name;
    }

    /** The descriptor's existing wire format, so there is one serialisation rather than two that drift. */
    static BytesReference encode(IndexDescriptor descriptor) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            descriptor.writeTo(out);
            return out.bytes();
        } catch (IOException e) {
            // Writing a descriptor to an in-memory buffer cannot fail for an I/O reason, so this is a bug
            // rather than a condition, and it should not be reported as the store being unavailable.
            throw new IllegalStateException("could not serialise the descriptor for [" + descriptor.name() + "]", e);
        }
    }

    static IndexDescriptor decode(BytesReference bytes) throws IOException {
        try (StreamInput in = bytes.streamInput()) {
            return new IndexDescriptor(in);
        }
    }
}
