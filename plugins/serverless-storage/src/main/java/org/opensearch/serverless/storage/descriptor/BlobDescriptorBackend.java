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
import java.util.concurrent.Executor;

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
    private final Executor executor;

    /**
     * The same read path {@code DescriptorStore} uses, which is the reuse T7 extracted it for.
     *
     * <p>T7 moved the freshness window, eviction and in-flight collapsing out of the store so "the blob
     * backend reuses it rather than growing a second copy", and then the blob backend never took it. Every
     * point read went to the object store, so switching the backend, which is the whole point of the design,
     * turned each of the eleven synchronous resolution sites into a network round trip. On a filesystem that
     * is invisible. At the 20 to 40 ms a real GET costs it is the difference between the design working and
     * not, and a miss costs two round trips because absence is confirmed against the tombstone prefix.
     *
     * <p>Nothing about the cache is blob-specific, which is why it needed no changes: it caches hits, never
     * misses, and bounds itself by bytes rather than entries.
     *
     * <p><b>The window is the cost lever, and it was left at the local-index default.</b>
     * {@link DescriptorCache}'s own javadoc says a one second window is "right for a local index, wrong by
     * two orders of magnitude for an object store", and made the window a constructor parameter for that
     * reason. This class then called the no-argument constructor and took the one second anyway, so a node
     * serving T active tenants re-read all T of them every second whatever the request rate above it.
     *
     * <p>What makes a long window safe is that freshness does not depend on it. A descriptor changed on
     * another node is invalidated here by {@code DescriptorChangeTailer} within its poll interval, so the
     * window is a backstop for a change log entry that was lost, not the mechanism by which deletes become
     * visible. A minute bounds that backstop while cutting steady-state reads by sixty.
     */
    private final DescriptorCache descriptorCache;

    /**
     * Same-thread form, for a caller that is already somewhere blocking is allowed.
     *
     * <p>Named for what it does rather than offered as a default, because the whole point of the other
     * constructor is that the difference matters. Tests use this; the plugin does not.
     */
    public BlobDescriptorBackend(BlobContainer blobContainer) {
        this(blobContainer, Runnable::run);
    }

    /**
     * The default freshness window, a minute rather than the cache's own one second.
     *
     * <p>One second was chosen against a sub-millisecond local read. Against a 20 to 40 ms GET it means a
     * node re-reads every active tenant once a second forever, which is a floor on request rate that no
     * amount of caching above it can lower. Sixty seconds cuts that by sixty and remains a backstop rather
     * than the freshness mechanism, which is the change log tailer.
     */
    public static final long DEFAULT_CACHE_TTL_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(60);

    /**
     * The form with somewhere to run, which the {@code *Async} methods below actually need.
     *
     * <p>They used to complete synchronously inside an already-completed future, and this class said so in
     * its own javadoc: an honest synchronous method beats a fake asynchronous one. That was fine while
     * nothing called them. The first real caller is {@code DescriptorGate}, which registers them as hooks
     * that run <b>on the cluster state thread</b>, and that thread must not block on I/O. The gate's own
     * comment records what happens otherwise: "registering the blocking put hung the node instead of
     * failing, which is how the constraint was found."
     *
     * <p>So the executor is a constructor argument rather than something the backend reaches for, because
     * which pool this runs on is the caller's decision and getting it wrong hangs a node rather than
     * slowing one down.
     */
    public BlobDescriptorBackend(BlobContainer blobContainer, Executor executor) {
        this(blobContainer, executor, DEFAULT_CACHE_TTL_NANOS);
    }

    /** The form that names its own freshness window, which the plugin uses to make it a setting. */
    public BlobDescriptorBackend(BlobContainer blobContainer, Executor executor, long cacheTtlNanos) {
        this.blobContainer = blobContainer;
        this.executor = executor;
        this.descriptorCache = new DescriptorCache(
            System::nanoTime,
            cacheTtlNanos,
            DescriptorCache.DEFAULT_COLLAPSE_WAIT_MILLIS,
            DescriptorCache.DEFAULT_CAPACITY,
            DescriptorCache.DEFAULT_BYTES
        );
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
        return descriptorCache.get(name, this::readFromStore);
    }

    /** One read of one descriptor from the object store, with no caching of its own. */
    private IndexDescriptor readFromStore(String name) {
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

    /**
     * {@inheritDoc}
     *
     * <p>Genuinely off the calling thread, which the cluster state thread requires. The future carries the
     * outcome so a caller that wants to know whether it took the name still can, and a caller that does
     * not simply ignores it.
     */
    @Override
    public CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor) {
        return CompletableFuture.supplyAsync(() -> create(descriptor), executor);
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
            // Invalidated after the write, never before. Dropping the entry first would leave a window
            // where a concurrent read repopulates the cache from the old value and then outlives the write.
            descriptorCache.invalidate(descriptor.name());
        } catch (IOException e) {
            throw new DescriptorUnavailableException(descriptor.name(), e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Off-thread for the same reason as {@link #createAsync}, and additionally because {@link #put}
     * reads before it writes: two round trips on the cluster state thread rather than one.
     *
     * <p>A failure is logged rather than propagated. There is nobody to propagate it to, since the caller
     * is a cluster state hook that has already returned, and the descriptor write being lost is what the
     * publisher's own retry contract covers.
     */
    @Override
    public void putAsync(IndexDescriptor descriptor) {
        executor.execute(() -> {
            try {
                put(descriptor);
            } catch (RuntimeException e) {
                logger.warn("could not write the descriptor for [{}]", descriptor.name(), e);
            }
        });
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
        putTombstoneAsync(tombstone, org.opensearch.core.action.ActionListener.wrap(() -> {}));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Still on the executor, because a blob write blocks and the caller is a cluster state thread. What
     * the listener adds is an answer: the write either landed or it did not, and a deletion waiting on
     * durability is entitled to know which.
     */
    @Override
    public void putTombstoneAsync(IndexDescriptor tombstone, org.opensearch.core.action.ActionListener<Void> whenDurable) {
        try {
            executor.execute(() -> {
                try {
                    writeTombstone(tombstone);
                    whenDurable.onResponse(null);
                } catch (RuntimeException e) {
                    // Louder than the others. A lost tombstone is the one descriptor write that cannot be
                    // reconstructed: for a gated index there is no cluster state entry and no graveyard entry
                    // behind it, so the name silently stays live.
                    logger.error("could not write the tombstone for [{}]; the index may resurrect", tombstone.name(), e);
                    whenDurable.onFailure(e);
                }
            });
        } catch (Exception e) {
            // A rejected execution has to fail the listener too, or the deletion waits forever on a write
            // that was never going to run.
            logger.error("could not submit the tombstone write for [{}]; the index may resurrect", tombstone.name(), e);
            whenDurable.onFailure(e);
        }
    }

    private void writeTombstone(IndexDescriptor tombstone) {
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
            // A cached live descriptor would otherwise outlive the delete for a whole freshness window, and
            // a deleted index that still resolves is worse than a slow one: a write routed to it would be
            // accepted against a shard the cluster no longer believes in.
            descriptorCache.invalidate(name);
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

    /**
     * {@inheritDoc}
     *
     * <p>A blob read per name, moved off the calling thread rather than made concurrent. An object store
     * has no multi-get, so the round trips this saves are the caller's, not the store's: the request thread
     * returns immediately instead of waiting out M sequential reads inside the document loop.
     *
     * <p>Sequential on the executor deliberately. Fanning M names across the pool would let one bulk
     * request over many tenants occupy the whole of GENERIC, and prefetching is the lowest-value work on
     * the node -- everything it warms would otherwise be read inline anyway.
     */
    @Override
    public void warmAsync(java.util.Collection<String> names, org.opensearch.core.action.ActionListener<Void> listener) {
        if (names.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        try {
            executor.execute(() -> {
                for (String name : names) {
                    try {
                        get(name);
                    } catch (RuntimeException e) {
                        // One name failing must not abandon the rest, and must not fail the request.
                        logger.debug("could not prefetch the descriptor for [{}]", name, e);
                    }
                }
                listener.onResponse(null);
            });
        } catch (Exception e) {
            // A rejected execution is a busy node, which is exactly when prefetching should be skipped.
            logger.debug("could not submit the descriptor prefetch", e);
            listener.onResponse(null);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Safe for a name never cached, which is the common case for a tailer: it sees every change in the
     * cluster and most concern names this node has never read.
     */
    @Override
    public void invalidate(String name) {
        descriptorCache.invalidate(name);
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
