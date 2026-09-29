/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Index descriptors as object-store registers.
 *
 * <p>Creation is a single put-if-absent, which is what makes name uniqueness free: there is no global
 * lock, no elected serializer and no sweep over the existing population. That last point is the whole
 * reason this design exists — {@code plan-area-h-metadata-off-cluster-state.md} measured index creation
 * at 7.4 ms with 200 indices present and 98.8 ms at 6,000, because
 * {@code Metadata.Builder.build()} rebuilds name lookups across every index on every create. A CAS on
 * one blob costs the same at zero indices and at a hundred million.
 *
 * <p>{@code createRegisterIfAbsent} exists as a distinct call rather than a CAS against
 * {@code ABSENT_GENERATION} because the general form has to learn the current generation first, so it
 * reads before it writes. Creation does not need to, and a factor of two matters on the one operation
 * this design expects to run at 10^8.
 *
 * <p>Per D5, exercised against {@code FsBlobContainer} only. See this package's documentation.
 */
public final class DescriptorStore {

    /**
     * How a tombstone begins. Every tombstone this store ever wrote starts this way, with or without the
     * {@code deleted_at} that later ones carry, and no descriptor or alias record does.
     */
    private static final String TOMBSTONE_PREFIX = "{\"tombstone\":true";

    /**
     * How long a tombstone is kept before {@link #sweepTombstones} may remove it.
     *
     * <p>The tombstone exists so a compare-and-swap carrying a generation read before the delete cannot
     * land on the recreated name. Such a swap is an operation in flight — a mapping update, a settings
     * change — and none of them is in flight for hours. Kept longer than that, tombstones only cost:
     * every listing reads them, and every prefix pattern counts them.
     */
    public static final long DEFAULT_TOMBSTONE_QUARANTINE_MILLIS = 6L * 60L * 60L * 1000L;

    /** The width of one tombstone-marker bucket: a sweep lists a bucket only once its oldest name could have expired. */
    public static final long TOMBSTONE_BUCKET_MILLIS = 60L * 60L * 1000L;

    /**
     * How long a sweep may take, by its own clock, between claiming a tombstone and deleting it.
     *
     * <p>Past this the sweep leaves the tombstone for a later pass rather than delete it. The delete is
     * unconditional -- the object store has none other -- so it is only safe while no create can have
     * swapped over the claim, and creates are held off for {@link #TOMBSTONE_REAP_HANDOFF_MILLIS}.
     */
    public static final long TOMBSTONE_REAP_DEADLINE_MILLIS = 10_000L;

    /**
     * How long a create refuses a claimed tombstone before treating the claim as abandoned.
     *
     * <p>Six times the deadline: the gap is what absorbs clock skew between the sweeping node and the
     * creating one, the same trade the node lease makes with its own margin.
     */
    public static final long TOMBSTONE_REAP_HANDOFF_MILLIS = 60_000L;

    /** Buckets are named for the UTC hour they start at, so an operator reading a bucket can tell when. */
    private static final DateTimeFormatter BUCKET_FORMAT = DateTimeFormatter.ofPattern("uuuuMMddHH", java.util.Locale.ROOT)
        .withZone(ZoneOffset.UTC);

    private final BlobContainer container;
    private final BlobStore blobStore;
    private final BlobPath tombstonesPath;
    private final LongSupplier clock;

    /**
     * Told the name of every record this store changes, so a cache over it cannot go stale through a
     * mutation nobody remembered to announce.
     *
     * <p>A callback rather than a call at each site in {@code MetadataPlane}: the sites are eight and
     * growing, and one of them forgetting is a routing cache serving a descriptor this very node has just
     * replaced. Announcing from the one class that does the writing makes that impossible to get wrong.
     */
    private volatile java.util.function.Consumer<String> onChanged = name -> {};

    private static boolean isTombstone(org.opensearch.core.common.bytes.BytesReference value) {
        if (value.length() < TOMBSTONE_PREFIX.length()) {
            return false;
        }
        final byte[] head = org.opensearch.core.common.bytes.BytesReference.toBytes(value.slice(0, TOMBSTONE_PREFIX.length()));
        return new String(head, java.nio.charset.StandardCharsets.UTF_8).equals(TOMBSTONE_PREFIX);
    }

    /** Renders a tombstone recording when the delete happened. */
    private static org.opensearch.core.common.bytes.BytesArray tombstone(long deletedAtMillis) {
        return tombstone(deletedAtMillis, 0L);
    }

    /**
     * Renders a tombstone recording when the delete happened and, if non-zero, when a sweep claimed it for
     * removal. Still begins with {@link #TOMBSTONE_PREFIX}, so every reader that treats a tombstone as
     * absent treats a claimed one the same way.
     */
    private static org.opensearch.core.common.bytes.BytesArray tombstone(long deletedAtMillis, long reapingAtMillis) {
        final String claim = reapingAtMillis == 0L ? "" : ",\"reaping_at\":" + reapingAtMillis;
        return new org.opensearch.core.common.bytes.BytesArray(
            (TOMBSTONE_PREFIX + ",\"deleted_at\":" + deletedAtMillis + claim + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
    }

    /** When a tombstone's delete happened, or 0 for one written before that was recorded. */
    private static long tombstoneDeletedAt(org.opensearch.core.common.bytes.BytesReference value) throws IOException {
        return tombstoneField(value, "deleted_at");
    }

    /** When a sweep claimed a tombstone for removal, or 0 if none has. */
    private static long tombstoneReapingAt(org.opensearch.core.common.bytes.BytesReference value) throws IOException {
        return tombstoneField(value, "reaping_at");
    }

    private static long tombstoneField(org.opensearch.core.common.bytes.BytesReference value, String field) throws IOException {
        try (
            org.opensearch.core.xcontent.XContentParser parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    value.streamInput()
                )
        ) {
            final Object stamp = parser.map().get(field);
            return stamp instanceof Number number ? number.longValue() : 0L;
        }
    }

    /** The bucket a delete at this moment drops its marker into. */
    static String tombstoneBucket(long millis) {
        return BUCKET_FORMAT.format(Instant.ofEpochMilli(millis));
    }

    /** When a bucket starts, or -1 for a name that is not a bucket this store wrote. */
    static long tombstoneBucketStart(String bucket) {
        if (bucket.length() != 10 || bucket.chars().allMatch(Character::isDigit) == false) {
            return -1L;
        }
        try {
            return LocalDateTime.of(
                Integer.parseInt(bucket.substring(0, 4)),
                Integer.parseInt(bucket.substring(4, 6)),
                Integer.parseInt(bucket.substring(6, 8)),
                Integer.parseInt(bucket.substring(8, 10)),
                0
            ).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (java.time.DateTimeException e) {
            return -1L;
        }
    }

    /**
     * Creates a store.
     *
     * @param container the container holding the {@code indices/} prefix
     * @param blobStore the store, for the per-hour tombstone-marker buckets
     * @param tombstonesPath where those buckets live; see {@link RegisterMap#tombstones}
     * @param clock the clock tombstones are stamped with and a sweep's claim is timed by
     */
    public DescriptorStore(BlobContainer container, BlobStore blobStore, BlobPath tombstonesPath, LongSupplier clock) {
        this.container = container;
        this.blobStore = blobStore;
        this.tombstonesPath = tombstonesPath;
        this.clock = clock;
    }

    /**
     * Registers what to tell when a record changes.
     *
     * @param listener told the name of each created, updated or deleted record
     * @return this, for chaining
     */
    public DescriptorStore onChanged(java.util.function.Consumer<String> listener) {
        this.onChanged = listener == null ? name -> {} : listener;
        return this;
    }

    /**
     * Creates an index descriptor, failing if the name is taken.
     *
     * @param descriptor the descriptor to write
     * @return the generation the register now holds
     * @throws IndexAlreadyExistsException if a descriptor for that name already exists
     * @throws IOException if the write fails
     */
    public long create(IndexDescriptor descriptor) throws IOException {
        Names.validateIndexOrAlias(descriptor.name());
        return createRegister(descriptor.name(), descriptor.toBytes());
    }

    /**
     * Creates a register under a name that is either absent or a tombstone.
     *
     * <p><b>A tombstone is kept, not removed, and creation swaps over it.</b> A register's generation is
     * minted by the store and restarts at 1 when a blob is deleted and re-created, so a delete followed
     * by a create handed a recreated name the same generation numbers its predecessor had -- and every
     * compare-and-swap on this surface expects a generation: a mapping update that read the old index's
     * descriptor at generation 1 then wrote it over the new index's descriptor at generation 1, uuid and
     * all, and the collector deleted the new index's shards as orphans. Swapping over the tombstone keeps
     * the generation moving, so a number read before a delete never matches a number after it.
     *
     * @param name the register name
     * @param value what to store
     * @return the generation as stored
     * <p><b>Except a tombstone a sweep has claimed.</b> The sweep's delete cannot be made conditional, so a
     * create that swapped in between the sweep's claim and its delete would be deleted with the tombstone
     * -- an acknowledged index gone. A claim is refused for {@link #TOMBSTONE_REAP_HANDOFF_MILLIS} and the
     * sweep deletes within {@link #TOMBSTONE_REAP_DEADLINE_MILLIS} of making it or not at all; past the
     * handoff the claim was abandoned and the tombstone is taken as usual.
     *
     * @throws IOException if the store cannot be read or written
     * @throws IndexAlreadyExistsException if the name is taken by a live record
     * @throws NameBeingReclaimedException if a sweep is removing the name's tombstone right now
     */
    private long createRegister(String name, org.opensearch.core.common.bytes.BytesReference value) throws IOException {
        final String blobName = RegisterMap.descriptorBlob(name);
        // Put-if-absent first, so the common case -- a name never used -- stays one request. Only a name
        // that is taken is read, to tell a tombstone from a live record.
        final BlobRegisterCasResult created = container.createRegisterIfAbsent(blobName, value);
        if (created.applied()) {
            onChanged.accept(name);
            return created.currentGeneration();
        }
        final Optional<BlobRegister> current = container.readRegister(blobName);
        if (current.isPresent() && isTombstone(current.get().value())) {
            final long reapingAt = tombstoneReapingAt(current.get().value());
            if (reapingAt != 0L && clock.getAsLong() < reapingAt + TOMBSTONE_REAP_HANDOFF_MILLIS) {
                throw new NameBeingReclaimedException(name);
            }
            final BlobRegisterCasResult swapped = container.compareAndSwapRegister(blobName, current.get().generation(), value);
            if (swapped.applied()) {
                onChanged.accept(name);
                return swapped.currentGeneration();
            }
        }
        // Name uniqueness, arbitrated by the object store rather than by a cluster-manager.
        throw new IndexAlreadyExistsException(name);
    }

    /**
     * Reads a descriptor.
     *
     * @param indexName the index
     * @return the descriptor, or empty if no such index exists
     * @throws IOException if the register exists but cannot be read or parsed
     */
    public Optional<IndexDescriptor> get(String indexName) throws IOException {
        Names.validateIndexOrAlias(indexName);
        final Optional<BlobRegister> register = container.readRegister(RegisterMap.descriptorBlob(indexName));
        if (register.isEmpty()) {
            return Optional.empty();
        }
        if (isTombstone(register.get().value())) {
            // Deleted, and the blob has not been removed yet. Absent is the truthful answer.
            return Optional.empty();
        }
        if (isAlias(register.get().value())) {
            // The name is taken by an alias, so there is no index by that name. Empty rather than an
            // error: a caller asking whether an index exists has its answer, and one that wants to know
            // what the name does resolve to asks resolve().
            return Optional.empty();
        }
        // Absent and unreadable are different answers, and conflating them is how a deleted index and a
        // corrupt one become indistinguishable. A parse failure propagates.
        try (InputStream in = register.get().value().streamInput()) {
            return Optional.of(IndexDescriptor.fromStream(in));
        }
    }

    /**
     * Creates an alias, failing if the name is taken by anything.
     *
     * <p>The same put-if-absent an index uses, against the same key, so an alias and an index cannot share
     * a name — not because anything checks, but because the second one to arrive is refused by the object
     * store.
     *
     * @param alias the alias to write
     * @return the generation the register now holds
     * @throws IndexAlreadyExistsException if the name is already an index or an alias
     * @throws IOException if the write fails
     */
    /**
     * Replaces an alias, only if it has not changed underneath the caller.
     *
     * <p>The compare-and-swap is what makes {@code POST /_aliases} atomic for the case that matters — moving
     * one alias from one index to another. Both actions touch a single register, so the whole move is one
     * swap, and a caller searching the alias sees either the old index or the new one and never neither.
     *
     * @param alias the alias as it should now be
     * @param expectedGeneration the generation the caller read
     * @return the new generation, or empty if another writer got there first
     * @throws IOException if the swap fails
     */
    public Optional<Long> updateAlias(org.opensearch.serverless.cluster.AliasRecord alias, long expectedGeneration) throws IOException {
        final BlobRegisterCasResult result = container.compareAndSwapRegister(
            RegisterMap.descriptorBlob(alias.name()),
            expectedGeneration,
            alias.toBytes()
        );
        if (result.applied()) {
            onChanged.accept(alias.name());
        }
        return result.applied() ? Optional.of(result.currentGeneration()) : Optional.empty();
    }

    /**
     * Creates an alias record, refusing if one of that name already exists.
     *
     * @param alias the record to create
     * @return the generation it was created at
     * @throws IOException if the name is not a legal one, or the register cannot be written
     */
    public long createAlias(org.opensearch.serverless.cluster.AliasRecord alias) throws IOException {
        Names.validateIndexOrAlias(alias.name());
        return createRegister(alias.name(), alias.toBytes());
    }

    /**
     * Reads what a name stands for: an index, an alias, or nothing.
     *
     * <p>One register read for both, which is the point of them sharing a namespace.
     *
     * @param name the name to resolve
     * @return what it names
     * @throws IOException if the register cannot be read or parsed
     */
    public Resolution resolve(String name) throws IOException {
        Names.validateIndexOrAlias(name);
        final Optional<BlobRegister> register = container.readRegister(RegisterMap.descriptorBlob(name));
        final long generation = register.map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
        if (register.isEmpty() || isTombstone(register.get().value())) {
            return new Resolution(null, null, generation);
        }
        if (isAlias(register.get().value())) {
            try (InputStream in = register.get().value().streamInput()) {
                return new Resolution(null, org.opensearch.serverless.cluster.AliasRecord.fromStream(in), generation);
            }
        }
        try (InputStream in = register.get().value().streamInput()) {
            return new Resolution(IndexDescriptor.fromStream(in), null, generation);
        }
    }

    /**
     * Whether a stored value is an alias record rather than an index descriptor.
     *
     * <p>By parsing the object and looking for the field, not by looking at where it happens to appear in
     * the bytes: a mapping can contain the word "alias" anywhere, and a discriminator that a document's
     * own contents can forge is not a discriminator. The record is small and this is one read's worth of
     * parsing on a path already doing a round trip to an object store.
     */
    private static boolean isAlias(org.opensearch.core.common.bytes.BytesReference value) throws IOException {
        try (
            org.opensearch.core.xcontent.XContentParser parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    value.streamInput()
                )
        ) {
            return parser.map().containsKey(org.opensearch.serverless.cluster.AliasRecord.DISCRIMINATOR);
        }
    }

    /** What a name turned out to be, and the register generation it was read at -- one read for both. */
    public static final class Resolution {

        private final IndexDescriptor index;
        private final org.opensearch.serverless.cluster.AliasRecord alias;
        private final long generation;

        Resolution(IndexDescriptor index, org.opensearch.serverless.cluster.AliasRecord alias) {
            this(index, alias, BlobRegister.ABSENT_GENERATION);
        }

        Resolution(IndexDescriptor index, org.opensearch.serverless.cluster.AliasRecord alias, long generation) {
            this.index = index;
            this.alias = alias;
            this.generation = generation;
        }

        /**
         * Returns the generation the register held when this was read, for a compare-and-swap on what
         * was read -- never a generation read separately, which guards nothing (see
         * {@code MetadataPlane.AliasAtGeneration}).
         *
         * @return the generation, or {@link BlobRegister#ABSENT_GENERATION} when the name is absent
         */
        public long generation() {
            return generation;
        }

        /**
         * Returns the index, if the name is one.
         *
         * @return the descriptor, or null
         */
        public IndexDescriptor index() {
            return index;
        }

        /**
         * Returns the alias, if the name is one.
         *
         * @return the alias, or null
         */
        public org.opensearch.serverless.cluster.AliasRecord alias() {
            return alias;
        }

        /**
         * Reports whether the name stands for nothing at all.
         *
         * @return true if absent
         */
        public boolean absent() {
            return index == null && alias == null;
        }
    }

    /**
     * Returns the generation a descriptor register currently holds, for a compare-and-swap.
     *
     * @param indexName the index
     * @return the generation, or {@link BlobRegister#ABSENT_GENERATION} if absent
     * @throws IOException if the register cannot be read
     */
    public long generationOf(String indexName) throws IOException {
        Names.validateIndexOrAlias(indexName);
        return container.readRegister(RegisterMap.descriptorBlob(indexName))
            .map(BlobRegister::generation)
            .orElse(BlobRegister.ABSENT_GENERATION);
    }

    /**
     * Replaces a descriptor, succeeding only if the register still holds the expected generation.
     *
     * @param descriptor the new descriptor
     * @param expectedGeneration the generation the caller read
     * @return the new generation, or empty if another writer got there first
     * @throws IOException if the write fails
     */
    public Optional<Long> update(IndexDescriptor descriptor, long expectedGeneration) throws IOException {
        final BlobRegisterCasResult result = container.compareAndSwapRegister(
            RegisterMap.descriptorBlob(descriptor.name()),
            expectedGeneration,
            descriptor.toBytes()
        );
        if (result.applied()) {
            onChanged.accept(descriptor.name());
        }
        return result.applied() ? Optional.of(result.currentGeneration()) : Optional.empty();
    }

    /**
     * Deletes a descriptor. Idempotent.
     *
     * @param indexName the index to delete
     * @throws IOException if the delete fails
     */
    public void delete(String indexName) throws IOException {
        container.deleteBlobsIgnoringIfNotExists(List.of(RegisterMap.descriptorBlob(indexName)));
        onChanged.accept(indexName);
    }

    /**
     * Deletes an index only if its descriptor still holds the generation the caller read.
     *
     * <p>Every other index-lifecycle operation is a compare-and-swap — creation is put-if-absent, a
     * mapping change is {@link #update} — and delete was the one that was not. An unconditional delete
     * races: a concurrent mapping update and a delete could both report success, with the update's
     * write landing on an object the delete then removed, or the delete removing a descriptor the
     * caller had never seen. Lifecycle is the one place in this design where operations on a single
     * index are ordered, and it is only ordered if <em>all</em> of them go through the register.
     *
     * <p>Implemented as a swap to a tombstone followed by removal, because the object store has no
     * conditional delete. The swap is the linearization point: after it, {@link #get} reports the index
     * as absent whether or not the blob has actually gone yet.
     *
     * <p>The tombstone records when the delete happened, in the caller's clock, so
     * {@link #sweepTombstones} can tell one that has outlived every operation it could have been guarding
     * from one written a moment ago.
     *
     * <p><b>The marker goes first.</b> The sweep finds tombstones only through the marker a delete drops
     * into its hour's bucket, so a tombstone written without one would never be collected. Written before
     * the swap, a crash between the two leaves a marker for a delete that never happened, which the sweep
     * reads, finds a live record or nothing behind, and drops. Written after, the same crash would leave a
     * tombstone nothing will ever find.
     *
     * @param indexName the index to delete
     * @param expectedGeneration the generation the caller read
     * @param nowMillis when, in the clock the sweep will be run with
     * @return true if this caller deleted it; false if the descriptor changed first
     * @throws IOException if the write fails
     */
    public boolean deleteIfUnchanged(String indexName, long expectedGeneration, long nowMillis) throws IOException {
        final String blobName = RegisterMap.descriptorBlob(indexName);
        blobStore.blobContainer(tombstonesPath.add(tombstoneBucket(nowMillis)))
            .writeBlob(blobName, new ByteArrayInputStream(new byte[0]), 0, false);
        final BlobRegisterCasResult swapped = container.compareAndSwapRegister(blobName, expectedGeneration, tombstone(nowMillis));
        if (swapped.applied() == false) {
            return false;
        }
        onChanged.accept(indexName);
        // The tombstone stays, for the quarantine: see createRegister for why a deleted name must keep
        // its generation, and sweepTombstones for why not forever.
        return true;
    }

    /**
     * Deletes an index only if its descriptor still holds the generation the caller read, stamping the
     * tombstone with this store's clock.
     *
     * @param indexName the index to delete
     * @param expectedGeneration the generation the caller read
     * @return true if this caller deleted it; false if the descriptor changed first
     * @throws IOException if the write fails
     */
    public boolean deleteIfUnchanged(String indexName, long expectedGeneration) throws IOException {
        return deleteIfUnchanged(indexName, expectedGeneration, clock.getAsLong());
    }

    /**
     * Removes tombstones that have outlived their quarantine.
     *
     * <p><b>Why they cannot stay forever.</b> A tombstone guards against one thing: a compare-and-swap
     * carrying a generation read before the delete landing on the recreated name, whose generations would
     * otherwise restart at the numbers the old one had. That swap is an operation in flight, and no
     * operation is in flight for hours. Past that, a tombstone is a register every listing reads and every
     * prefix pattern counts — a tenant creating and deleting a daily index poisoned {@code logs-*} after
     * five hundred days with one live index under it.
     *
     * <p><b>Found through markers, not by walking the population.</b> Every delete drops a marker into its
     * hour's bucket ({@link RegisterMap#tombstones}), so this lists the buckets, skips any too young to
     * hold an expired name, and reads only the names in the rest. It used to list every descriptor and
     * read each one, on every node, hourly: at a million indices that was a million reads per node per
     * hour to find the few hundred names deleted that hour. The cost is now one listing of the buckets,
     * plus a read and a claim per name deleted in the buckets this node owns.
     *
     * <p><b>Claimed, then deleted, never deleted on the strength of a read.</b> The object store has no
     * conditional delete, and the walk this replaced read a tombstone, moved on, and deleted it at the end:
     * a name recreated in between -- hours, at scale -- lost its new descriptor, and the collector then
     * deleted the new index's shards as orphans. Now a tombstone is first swapped to a claimed one, which
     * {@code createRegister} refuses to swap over, and deleted only if the claim is still younger than
     * {@link #TOMBSTONE_REAP_DEADLINE_MILLIS} by this node's clock. What is left is the pause between
     * that check and the delete landing, the same residual every lease-timed write here carries.
     *
     * <p><b>Safe to run on more than one node.</b> Two sweeps of the same bucket contend on the claim and
     * one of them loses it; which is what lets {@code ownsBucket} be a hint drawn from membership rather
     * than an election.
     *
     * <p>A tombstone written before the delete time was recorded reads as infinitely old. One written
     * before markers existed has none and is never found; those are left where they are.
     *
     * @param nowMillis the current time, in the clock the tombstones were stamped with
     * @param quarantineMillis how long a tombstone must have stood
     * @return the names whose tombstones were removed
     * @throws IOException if listing, reading or deleting fails
     */
    public List<String> sweepTombstones(long nowMillis, long quarantineMillis) throws IOException {
        return sweepTombstones(nowMillis, quarantineMillis, bucket -> true);
    }

    /**
     * Removes tombstones that have outlived their quarantine, from the buckets this caller owns.
     *
     * @param nowMillis the current time, in the clock the tombstones were stamped with
     * @param quarantineMillis how long a tombstone must have stood
     * @param ownsBucket whether this caller should sweep a bucket; see {@link #sweepTombstones(long, long)}
     * @return the names whose tombstones were removed
     * @throws IOException if listing, reading or deleting fails
     */
    public List<String> sweepTombstones(long nowMillis, long quarantineMillis, Predicate<String> ownsBucket) throws IOException {
        final List<String> removed = new ArrayList<>();
        for (String bucket : new ArrayList<>(blobStore.blobContainer(tombstonesPath).children().keySet())) {
            final long start = tombstoneBucketStart(bucket);
            // Only a bucket whose oldest possible name has passed the quarantine; younger ones are not
            // listed at all. Not waiting for the whole bucket keeps the quarantine exact rather than up to
            // a bucket late, and costs at most one more listing of it, since each name is still checked.
            if (start < 0L || start + quarantineMillis > nowMillis || ownsBucket.test(bucket) == false) {
                continue;
            }
            sweepBucket(bucket, start, nowMillis, quarantineMillis, removed);
        }
        return removed;
    }

    /** What a sweep did with one marker. */
    private enum Reaped {
        /** The tombstone is claimed and waiting on the batched delete. */
        CLAIMED,
        /** Nothing for this marker to do: the name is live, gone, or deleted again later. */
        STALE,
        /** Not now: inside its quarantine, or another sweep's claim. */
        KEEP
    }

    /** The default number of markers a sweep lists and holds at once. */
    public static final int DEFAULT_MARKER_PAGE = 1000;

    private volatile int markerPage = DEFAULT_MARKER_PAGE;

    /**
     * Sets how many markers a sweep lists and holds at once; for tests that drain a bucket in several pages.
     *
     * @param markers the page size
     * @return this, for chaining
     */
    public DescriptorStore setMarkerPage(int markers) {
        this.markerPage = Math.max(1, markers);
        return this;
    }

    private void sweepBucket(String bucket, long bucketStart, long nowMillis, long quarantineMillis, List<String> removed)
        throws IOException {
        final BlobContainer markers = blobStore.blobContainer(tombstonesPath.add(bucket));
        final int page = markerPage;
        boolean kept = false;
        // In pages rather than one listing held whole: an hour's bucket at a thousand deletes a second is
        // millions of names. There is no resuming a listing through BlobContainer, but there need not be --
        // every page deletes the markers it finished, so listing the first page again lists the next ones.
        while (true) {
            final List<org.opensearch.common.blobstore.BlobMetadata> listed = markers.listBlobsByPrefixInSortedOrder(
                "",
                page,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
            final List<String> finished = new ArrayList<>();
            final List<String> claimed = new ArrayList<>();
            long oldestClaim = 0L;
            for (org.opensearch.common.blobstore.BlobMetadata marker : listed) {
                final String blobName = marker.name();
                // Deletes are batched, but a claim only licenses a delete for the deadline, so the batch is
                // flushed well inside it rather than whenever it happens to fill.
                if (claimed.isEmpty() == false
                    && (claimed.size() >= 1000 || clock.getAsLong() >= oldestClaim + TOMBSTONE_REAP_DEADLINE_MILLIS / 2)) {
                    kept |= flushClaims(claimed, oldestClaim, finished, removed) == false;
                }
                final long claimAt = clock.getAsLong();
                switch (reap(blobName, bucketStart, nowMillis, quarantineMillis, claimAt)) {
                    case CLAIMED -> {
                        if (claimed.isEmpty()) {
                            oldestClaim = claimAt;
                        }
                        claimed.add(blobName);
                    }
                    case STALE -> finished.add(blobName);
                    case KEEP -> kept = true;
                }
            }
            kept |= flushClaims(claimed, oldestClaim, finished, removed) == false;
            if (finished.isEmpty() == false) {
                markers.deleteBlobsIgnoringIfNotExists(finished);
            }
            // A short page was the last. A page that finished nothing holds only markers kept for later,
            // and listing again would return that same page.
            if (listed.size() < page || finished.isEmpty()) {
                break;
            }
        }
        // An emptied bucket goes too, or a filesystem store would keep a directory an hour forever. Listed
        // again first, so a marker a badly skewed clock dropped in meanwhile is not taken with it.
        if (kept == false && markers.listBlobs().isEmpty()) {
            markers.delete();
        }
    }

    /**
     * Deletes the claimed tombstones if the claims are still inside the deadline.
     *
     * @return false if they were not, and the markers are kept for a later sweep
     */
    private boolean flushClaims(List<String> claimed, long oldestClaim, List<String> finished, List<String> removed) throws IOException {
        if (claimed.isEmpty()) {
            return true;
        }
        try {
            if (clock.getAsLong() >= oldestClaim + TOMBSTONE_REAP_DEADLINE_MILLIS) {
                // Too late to be sure no create has taken a claim over. Left claimed; a later sweep finds the
                // marker, sees the claim has aged past the handoff, and claims it afresh.
                return false;
            }
            container.deleteBlobsIgnoringIfNotExists(claimed);
            removed.addAll(claimed);
            finished.addAll(claimed);
            return true;
        } finally {
            claimed.clear();
        }
    }

    private Reaped reap(String blobName, long bucketStart, long nowMillis, long quarantineMillis, long claimAt) throws IOException {
        final Optional<BlobRegister> register = container.readRegister(blobName);
        if (register.isEmpty() || isTombstone(register.get().value()) == false) {
            // Already removed, recreated since, or a delete whose swap lost after its marker was written.
            return Reaped.STALE;
        }
        final org.opensearch.core.common.bytes.BytesReference value = register.get().value();
        final long deletedAt = tombstoneDeletedAt(value);
        if (deletedAt >= bucketStart + TOMBSTONE_BUCKET_MILLIS) {
            // Recreated and deleted again later. That delete dropped its own marker in its own bucket.
            return Reaped.STALE;
        }
        if (nowMillis - deletedAt < quarantineMillis) {
            // Stamped by a clock running ahead of this one. Its quarantine is not this sweep's to shorten.
            return Reaped.KEEP;
        }
        final long reapingAt = tombstoneReapingAt(value);
        if (reapingAt != 0L && claimAt < reapingAt + TOMBSTONE_REAP_HANDOFF_MILLIS) {
            // Another sweep's claim, not yet abandoned.
            return Reaped.KEEP;
        }
        final BlobRegisterCasResult swapped = container.compareAndSwapRegister(
            blobName,
            register.get().generation(),
            tombstone(deletedAt, claimAt)
        );
        return swapped.applied() ? Reaped.CLAIMED : Reaped.KEEP;
    }

    /**
     * One bounded page of descriptors, in name order.
     *
     * <p><b>This is the only enumeration a deployment at target scale may use.</b> "List every index" is
     * not a slow operation at 100 million indices; it is not an operation at all — the answer does not
     * fit in a response, cannot be consumed by a caller, and is stale before it finishes. So the API is
     * a cursor walk, and callers that want everything pay for everything, one page at a time, visibly.
     *
     * <p><b>Two costs, and only one of them is bounded.</b> The descriptor <b>reads</b> are bounded here
     * by {@code limit}, on any backend. The <b>listing</b> is not bounded on any backend, including S3:
     * this calls {@code listBlobs()}, which paginates the whole container, and there is no way to do
     * better through {@code BlobContainer} because its listing API carries a prefix and a limit but no
     * {@code start-after}, which is what resuming a cursor needs.
     *
     * <p>An earlier version of this comment said the listing was bounded natively by S3's
     * {@code ListObjectsV2} {@code start-after} plus {@code max-keys}. It never was — the code has always
     * called the unbounded listing, and the claim described what the API would need rather than what it
     * does. See {@link #namesWithPrefix}, which <em>is</em> bounded, for the shape that works: a prefix
     * and a maximum, with no resumption to carry.
     *
     * @param after return names strictly greater than this, or null to start at the beginning
     * @param limit maximum descriptors to return
     * @return the page
     * @throws IOException if listing or reading fails
     */
    public Page listPage(String after, int limit) throws IOException {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
        final List<String> names = new ArrayList<>(container.listBlobs().keySet());
        names.sort(String::compareTo);

        final Map<String, IndexDescriptor> page = new LinkedHashMap<>();
        String last = null;
        for (String blobName : names) {
            if (after != null && blobName.compareTo(after) <= 0) {
                continue;
            }
            if (page.size() >= limit) {
                // There is more. Report a cursor rather than a total: knowing how many there are in
                // total is itself a full scan, and is the question this API refuses to answer.
                return new Page(page, last);
            }
            final Optional<BlobRegister> register = container.readRegister(blobName);
            if (register.isEmpty()) {
                continue;
            }
            if (isTombstone(register.get().value())) {
                last = blobName;
                continue;
            }
            try (InputStream in = register.get().value().streamInput()) {
                final IndexDescriptor descriptor = IndexDescriptor.fromStream(in);
                page.put(descriptor.name(), descriptor);
                last = blobName;
            }
        }
        return new Page(page, null);
    }

    /** A bounded page of descriptors, and where to resume. */
    public static final class Page {

        private final Map<String, IndexDescriptor> descriptors;
        private final String nextAfter;

        Page(Map<String, IndexDescriptor> descriptors, String nextAfter) {
            this.descriptors = Map.copyOf(descriptors);
            this.nextAfter = nextAfter;
        }

        /**
         * Returns the descriptors in this page.
         *
         * @return descriptors by index name
         */
        public Map<String, IndexDescriptor> descriptors() {
            return descriptors;
        }

        /**
         * Returns the cursor to resume from, or null when the walk is complete.
         *
         * @return the next cursor, or null
         */
        public String nextAfter() {
            return nextAfter;
        }

        /**
         * Reports whether another page follows.
         *
         * @return true when there is more
         */
        public boolean hasMore() {
            return nextAfter != null;
        }
    }

    /**
     * More names share a prefix than a caller is willing to be handed.
     *
     * <p>A refusal rather than a truncation, and that is the whole point of the class existing: an answer
     * cut off at a limit looks exactly like a complete one, and a search that quietly covered the first
     * five hundred indices of a thousand is the confident partial answer this design refuses everywhere
     * else.
     */
    public static final class TooManyMatchesException extends IOException {

        private final String prefix;
        private final int cap;

        TooManyMatchesException(String prefix, int cap) {
            super(
                "more than "
                    + cap
                    + " indices begin with ["
                    + prefix
                    + "]. Narrowing the pattern or naming the indices is the answer; returning the first "
                    + cap
                    + " would be a partial result that looks complete."
            );
            this.prefix = prefix;
            this.cap = cap;
        }

        /**
         * Returns the prefix that matched too much.
         *
         * @return the prefix
         */
        public String prefix() {
            return prefix;
        }

        /**
         * Returns the cap that was exceeded.
         *
         * @return the cap
         */
        public int cap() {
            return cap;
        }
    }

    /**
     * Returns the names beginning with a prefix, up to a cap.
     *
     * <p><b>This is what makes an index pattern answerable without enumerating the deployment.</b> §6.3
     * refuses enumeration on a request path, and that refusal was the reason {@code logs-*} was refused
     * too. It does not have to be: a prefix listing with a maximum key count is one request whose cost is
     * set by the cap rather than by the population, so a deployment with a hundred million indices pays
     * exactly what one with ten pays.
     *
     * <p><b>The cap is asked for plus one, deliberately.</b> A listing that returns exactly the cap is
     * indistinguishable from one that was cut off there. Asking for one more is what makes "there are more
     * than this" a fact rather than a guess, and it costs one key.
     *
     * <p><b>Names, not descriptors.</b> Reading each one would turn a bounded listing back into work
     * proportional to what matched, and the caller reads them anyway to find out what it got — an index,
     * an alias, or a tombstone left by a deletion. Which of those it is belongs to the caller, not here.
     *
     * <p><b>Only live names count against the cap.</b> A deleted name leaves a tombstone for its
     * quarantine, and a tenant that creates and deletes a name a day accumulates hundreds under one
     * prefix; a cap that counted them refused {@code logs-*} with one live index beneath it. So when the
     * bounded listing overflows, the names sharing the prefix are read to tell live from deleted, and the
     * cap is applied to the live ones. That read is proportional to the tombstones under the prefix — the
     * cost the tombstone sweep exists to bound — and happens only when the bounded listing alone could not
     * answer. In that case the tombstones are also filtered out of the answer, since they were read anyway.
     *
     * @param prefix the prefix, which may be empty to mean every name
     * @param cap the most names to return
     * @return the matching names, in lexicographic order
     * @throws TooManyMatchesException if more than {@code cap} live names match
     * @throws IOException if the listing fails
     */
    public List<String> namesWithPrefix(String prefix, int cap) throws IOException {
        Names.validatePrefix(prefix);
        if (cap < 1) {
            throw new IllegalArgumentException("cap must be positive, got " + cap);
        }
        final List<org.opensearch.common.blobstore.BlobMetadata> found = container.listBlobsByPrefixInSortedOrder(
            prefix,
            cap + 1,
            BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
        );
        if (found.size() <= cap) {
            final List<String> names = new ArrayList<>(found.size());
            for (org.opensearch.common.blobstore.BlobMetadata blob : found) {
                names.add(blob.name());
            }
            return names;
        }
        // More names than the cap, deleted ones included. The bounded listing cannot be resumed past its
        // cap, so this is the one place a prefix is listed whole: the population under it is bounded by
        // the live indices plus the tombstones still in quarantine, never by the deployment.
        final List<String> under = new ArrayList<>(container.listBlobsByPrefix(prefix).keySet());
        under.sort(String::compareTo);
        final List<String> live = new ArrayList<>();
        for (String name : under) {
            final Optional<BlobRegister> register = container.readRegister(name);
            if (register.isEmpty() || isTombstone(register.get().value())) {
                continue;
            }
            live.add(name);
            if (live.size() > cap) {
                throw new TooManyMatchesException(prefix, cap);
            }
        }
        return live;
    }

    /**
     * Lists every descriptor.
     *
     * <p><b>Do not put this on a request path.</b> It is O(population) by construction and phase 9
     * measured what that costs: 2N+1 operations when it backed {@code truthFor}. It survives for offline
     * work — a sweep that already intends to visit everything — and {@link #listPage} is what an API
     * uses. A caller that reaches for this to answer a user is answering the wrong question.
     *
     * @return descriptors by index name
     * @throws IOException if listing or reading fails
     */
    public Map<String, IndexDescriptor> listAll() throws IOException {
        final Map<String, IndexDescriptor> descriptors = new LinkedHashMap<>();
        final List<String> names = new ArrayList<>(container.listBlobs().keySet());
        for (String blobName : names) {
            final Optional<BlobRegister> register = container.readRegister(blobName);
            if (register.isEmpty()) {
                continue;
            }
            if (isTombstone(register.get().value())) {
                continue;
            }
            try (InputStream in = register.get().value().streamInput()) {
                final IndexDescriptor descriptor = IndexDescriptor.fromStream(in);
                descriptors.put(descriptor.name(), descriptor);
            }
        }
        return descriptors;
    }
}
