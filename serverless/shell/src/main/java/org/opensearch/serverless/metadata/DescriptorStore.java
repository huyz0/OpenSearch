/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    private final BlobContainer container;

    private static boolean isTombstone(org.opensearch.core.common.bytes.BytesReference value) {
        if (value.length() < TOMBSTONE_PREFIX.length()) {
            return false;
        }
        final byte[] head = org.opensearch.core.common.bytes.BytesReference.toBytes(value.slice(0, TOMBSTONE_PREFIX.length()));
        return new String(head, java.nio.charset.StandardCharsets.UTF_8).equals(TOMBSTONE_PREFIX);
    }

    /** Renders a tombstone recording when the delete happened. */
    private static org.opensearch.core.common.bytes.BytesArray tombstone(long deletedAtMillis) {
        return new org.opensearch.core.common.bytes.BytesArray(
            (TOMBSTONE_PREFIX + ",\"deleted_at\":" + deletedAtMillis + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
    }

    /** When a tombstone's delete happened, or 0 for one written before that was recorded. */
    private static long tombstoneDeletedAt(org.opensearch.core.common.bytes.BytesReference value) throws IOException {
        try (
            org.opensearch.core.xcontent.XContentParser parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                .createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    value.streamInput()
                )
        ) {
            final Object deletedAt = parser.map().get("deleted_at");
            return deletedAt instanceof Number number ? number.longValue() : 0L;
        }
    }

    /**
     * Creates a store over a container.
     *
     * @param container the container holding the {@code indices/} prefix
     */
    public DescriptorStore(BlobContainer container) {
        this.container = container;
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
     * @throws IOException if the store cannot be read or written
     * @throws IndexAlreadyExistsException if the name is taken by a live record
     */
    private long createRegister(String name, org.opensearch.core.common.bytes.BytesReference value) throws IOException {
        final String blobName = RegisterMap.descriptorBlob(name);
        // Put-if-absent first, so the common case -- a name never used -- stays one request. Only a name
        // that is taken is read, to tell a tombstone from a live record.
        final BlobRegisterCasResult created = container.createRegisterIfAbsent(blobName, value);
        if (created.applied()) {
            return created.currentGeneration();
        }
        final Optional<BlobRegister> current = container.readRegister(blobName);
        if (current.isPresent() && isTombstone(current.get().value())) {
            final BlobRegisterCasResult swapped = container.compareAndSwapRegister(blobName, current.get().generation(), value);
            if (swapped.applied()) {
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
     * @param indexName the index to delete
     * @param expectedGeneration the generation the caller read
     * @param nowMillis when, in the clock the sweep will be run with
     * @return true if this caller deleted it; false if the descriptor changed first
     * @throws IOException if the write fails
     */
    public boolean deleteIfUnchanged(String indexName, long expectedGeneration, long nowMillis) throws IOException {
        final String blobName = RegisterMap.descriptorBlob(indexName);
        final BlobRegisterCasResult swapped = container.compareAndSwapRegister(blobName, expectedGeneration, tombstone(nowMillis));
        if (swapped.applied() == false) {
            return false;
        }
        // The tombstone stays, for the quarantine: see createRegister for why a deleted name must keep
        // its generation, and sweepTombstones for why not forever.
        return true;
    }

    /**
     * Deletes an index only if its descriptor still holds the generation the caller read, stamping the
     * tombstone with the wall clock.
     *
     * <p>Prefer the form that takes the plane's clock, so the stamp and the sweep agree on what "now"
     * means; this one exists for a caller that has no clock to hand.
     *
     * @param indexName the index to delete
     * @param expectedGeneration the generation the caller read
     * @return true if this caller deleted it; false if the descriptor changed first
     * @throws IOException if the write fails
     */
    public boolean deleteIfUnchanged(String indexName, long expectedGeneration) throws IOException {
        return deleteIfUnchanged(indexName, expectedGeneration, System.currentTimeMillis());
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
     * <p>A tombstone written before the delete time was recorded reads as infinitely old and goes on the
     * first sweep; it predates every operation that could still be in flight.
     *
     * <p>O(population) reads, like every whole-deployment walk here: for a sweep, not a request path.
     *
     * @param nowMillis the current time, in the clock the tombstones were stamped with
     * @param quarantineMillis how long a tombstone must have stood
     * @return the names whose tombstones were removed
     * @throws IOException if listing, reading or deleting fails
     */
    public List<String> sweepTombstones(long nowMillis, long quarantineMillis) throws IOException {
        final List<String> removed = new ArrayList<>();
        for (String blobName : new ArrayList<>(container.listBlobs().keySet())) {
            final Optional<BlobRegister> register = container.readRegister(blobName);
            if (register.isEmpty() || isTombstone(register.get().value()) == false) {
                continue;
            }
            if (nowMillis - tombstoneDeletedAt(register.get().value()) < quarantineMillis) {
                continue;
            }
            removed.add(blobName);
        }
        // One call rather than one per tombstone: the container batches, and a deployment that has been
        // creating and deleting a daily index for a year sweeps a year's worth in a single request.
        //
        // The reads above are the real cost of this method and batching does not touch them -- it is
        // O(population) register reads by construction, which is why it runs on a slow cadence and not on
        // any request path. Making the deletes free does not make this cheap; it makes it one request
        // instead of hundreds on top of a walk that was always going to be proportional to the population.
        if (removed.isEmpty() == false) {
            container.deleteBlobsIgnoringIfNotExists(removed);
        }
        return removed;
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
