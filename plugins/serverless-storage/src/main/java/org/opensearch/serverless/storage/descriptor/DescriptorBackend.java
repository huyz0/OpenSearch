/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexDescriptor;

import java.util.concurrent.CompletableFuture;

/**
 * Where descriptors are actually stored, separated from caching so the two can vary independently.
 *
 * <h2>Why this is only point operations</h2>
 *
 * T3 audited the eleven public operations of the index-backed store this replaced against what an object
 * store can serve. Eight are point operations on a single named descriptor and map onto blob primitives
 * directly. Three are prefix searches, and no object store answers those: a bucket has no inverted index,
 * and a LIST over a hundred million keys is a rebuild path rather than a query path.
 *
 * <p>So they are a separate interface, {@link DescriptorPrefixBackend}, rather than methods here that one
 * implementation throws from. The split is the point: it makes "this backend cannot resolve
 * {@code logs-*} the same way it resolves a name" a fact the compiler knows, instead of a runtime surprise
 * on the code path that is hardest to test.
 *
 * <p>The ordering constraint that split was originally stating is spent. It read that a name index tier had
 * to serve prefix resolution before the descriptor system index could be removed; in the end the tier was
 * deleted and the index removed anyway, because {@code DescriptorEnumerator} answers a wildcard from one
 * bounded listing with a cap, which is a query path after all. The separation is kept for the first reason,
 * which was always the better one.
 *
 * <h2>What stays above this</h2>
 *
 * Caching, TTL, eviction and in-flight request collapsing live in {@link DescriptorCache}. A backend does
 * I/O and nothing else. That matters more under an object store than it did under a system index:
 * collapsing turns M concurrent readers of one cold descriptor into a single round trip, and duplicating
 * that per backend would mean two chances to get it subtly different.
 *
 * <h2>Absence and unavailability are different answers</h2>
 *
 * Every read here must distinguish "there is no such descriptor" from "I could not find out". Returning
 * null for the second reports every gated index in the cluster as non-existent, and a client acting on
 * that could create an index that already exists. Implementations throw
 * {@link org.opensearch.cluster.metadata.DescriptorUnavailableException} for the second case, which is the
 * contract {@code readFromIndex} already established.
 */
public interface DescriptorBackend {

    /**
     * The descriptor for an exact name if present in memory cache, or null if cold or absent. Non-blocking.
     */
    default IndexDescriptor getIfFresh(String name) {
        return null;
    }

    /**
     * The descriptor for an exact name, or null if there is none.
     *
     * <p>Must be read-your-writes against this backend: an index created a moment ago has to be nameable
     * immediately, which is why a miss is never cached above this and why an implementation must not serve
     * this from anything eventually consistent.
     *
     * @throws org.opensearch.cluster.metadata.DescriptorUnavailableException if the store could not be read
     */
    IndexDescriptor get(String name);

    /**
     * Records a descriptor for a name that must not already be taken, returning whether this call took it.
     *
     * <p>This is the uniqueness mechanism, and the reason it can be one store operation is the whole of
     * H3: the store says a name is taken, not the cluster manager. A lost race is a {@code false} rather
     * than an exception, because losing is a correct outcome rather than a failure.
     */
    boolean create(IndexDescriptor descriptor);

    /** {@link #create} without blocking the caller. */
    CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor);

    /**
     * {@link #create}, but able to tell "someone else took this name" from "my own earlier attempt took
     * it and I never heard back".
     *
     * <h4>Why this is needed at all</h4>
     *
     * Under a cluster state update an ambiguous timeout was resolvable: the caller read the next published
     * state and saw whether its index was there. Creation through a conditional write has no such vantage
     * point. The first attempt may have reached the store and had its acknowledgement lost, so the retry
     * gets a conflict, and a conflict is indistinguishable from another client having won the name.
     *
     * <p>Reporting that as a collision fails a creation that actually succeeded. Reporting it as success
     * without checking would hand the name to a client that never got it. So the retry has to be able to
     * recognise its own work, which means carrying something stable across attempts.
     *
     * <p>The descriptor's uuid already is that thing, provided the caller reuses it on retry rather than
     * minting a new one. That is the contract: <b>a retry must resend the same descriptor</b>. A caller
     * that generates a fresh uuid each attempt is asking a question this cannot answer, and will be told
     * the name is taken.
     *
     * @return {@link CreateOutcome#CREATED} if this call took the name, {@link CreateOutcome#ALREADY_MINE}
     *         if it was already held by a descriptor with this uuid, {@link CreateOutcome#TAKEN} if
     *         somebody else holds it.
     */
    default CreateOutcome createIdempotently(IndexDescriptor descriptor) {
        if (create(descriptor)) {
            return CreateOutcome.CREATED;
        }
        IndexDescriptor existing = get(descriptor.name());
        if (existing == null) {
            // Created and then deleted between the two calls, or a store that lost the write. Either way
            // this caller does not hold the name and saying so is the only honest answer.
            return CreateOutcome.TAKEN;
        }
        return descriptor.uuid().equals(existing.uuid()) ? CreateOutcome.ALREADY_MINE : CreateOutcome.TAKEN;
    }

    /** What {@link #createIdempotently} found. */
    enum CreateOutcome {
        /** This call took the name. */
        CREATED,
        /** The name was already held by this same descriptor, so an earlier attempt by this caller won. */
        ALREADY_MINE,
        /** Somebody else holds the name. */
        TAKEN;

        /** Whether the caller ends up owning the name, which is what most callers actually want to know. */
        public boolean owned() {
            return this != TAKEN;
        }
    }

    /**
     * A descriptor as the store currently holds it, together with the version that read observed.
     *
     * <p>The version is opaque and belongs to the backend: for the blob backend it is the register's
     * generation. It exists so a caller that intends to change the descriptor can hand back what it based
     * that change on, which is the difference between a compare-and-swap and a hope.
     *
     * @param descriptor      what the store holds, or null when the name has no record at all
     * @param storeVersion    what to pass to {@link #compareAndSwap} to mean "only if nothing has changed
     *                        since", or {@link #UNVERSIONED} from a backend that cannot answer conditionally
     */
    record VersionedDescriptor(IndexDescriptor descriptor, long storeVersion) {
    }

    /** What a backend with no conditional write reports, and what {@link #compareAndSwap} then cannot check. */
    long UNVERSIONED = -1L;

    /**
     * The descriptor for a name, read past any cache, with the version to write it back under.
     *
     * <p><b>Not the same call as {@link #get} and not interchangeable with it.</b> {@code get} is the
     * request path and is allowed to answer from a cache whose freshness window is a minute. That is safe
     * for resolving a name and unsafe for deciding what to write next: a read-modify-write based on a cached
     * descriptor reverts everything that changed within the window, and does so silently. Every caller that
     * intends to write must come through here.
     *
     * @throws org.opensearch.cluster.metadata.DescriptorUnavailableException if the store could not be read
     */
    default VersionedDescriptor getForUpdate(String name) {
        return new VersionedDescriptor(get(name), UNVERSIONED);
    }

    /**
     * Writes a descriptor only if the store is still at {@code expectedStoreVersion}.
     *
     * <p>This is the write half of {@link #getForUpdate}, and the pair is what makes a read-modify-write
     * over a descriptor safe. Losing is a {@code false} rather than an exception, because losing is a
     * correct outcome: the caller re-reads, re-applies its change to what it now finds, and tries again.
     * That is the same contract {@code MappingGenerationStore.Store#compareAndSwap} states one level up, and
     * it must never be satisfied by writing anyway.
     *
     * @return true when the write applied, false when something else wrote first
     * @throws org.opensearch.cluster.metadata.DescriptorUnavailableException if the store could not be written
     */
    default boolean compareAndSwap(IndexDescriptor descriptor, long expectedStoreVersion) {
        // A backend with no conditional write can only do the unconditional one, and saying so here keeps
        // the degradation in one place rather than letting each caller invent its own.
        put(descriptor);
        return true;
    }

    /**
     * Where this backend's own writes run, for a caller composing several of them off the calling thread.
     *
     * <p>Same-thread by default, which is right for an in-memory backend and for a test. A backend whose
     * writes are network I/O overrides it, because the callers here are cluster state hooks: the gate's own
     * comment records what happens otherwise -- "registering the blocking put hung the node instead of
     * failing, which is how the constraint was found."
     */
    default java.util.concurrent.Executor writeExecutor() {
        return Runnable::run;
    }

    /**
     * Writes a descriptor whether or not the name is taken, and whatever it currently says.
     *
     * <p><b>Not for a read-modify-write.</b> This overwrites, so a caller that read the descriptor, changed
     * part of it and put it back reverts every other change made in between -- and the read it based that on
     * is usually a cached one. {@link #getForUpdate} with {@link #compareAndSwap} is that caller's pair. This
     * is for a writer that already holds the whole truth about the index, which in practice means the
     * publisher writing a descriptor derived from the cluster state entry that <em>is</em> the truth.
     */
    void put(IndexDescriptor descriptor);

    /** {@link #put} without blocking the caller. */
    void putAsync(IndexDescriptor descriptor);

    /** {@link #putAsync} notifying listener when durable. */
    default void putAsync(IndexDescriptor descriptor, org.opensearch.core.action.ActionListener<Void> listener) {
        putAsync(descriptor);
        listener.onResponse(null);
    }

    /**
     * Marks a name deleted durably.
     *
     * <p>Kept distinct from {@link #putAsync} of a tombstone-shaped descriptor because the two backends
     * put it in different places: the system index writes it as another document, and a blob backend has
     * to move it out from under the descriptor prefix entirely, or every prefix listing has to read each
     * key to find out whether it is alive.
     */
    void putTombstoneAsync(IndexDescriptor tombstone);

    /**
     * {@link #putTombstoneAsync(IndexDescriptor)}, reporting when the write is actually durable.
     *
     * <p>Needed because a tombstone is the one descriptor write that is not safe to lose. For a gated index
     * there is no cluster state entry and no graveyard entry standing behind it, so if the tombstone is lost
     * a node holding that shard's data can adopt it again on rejoin. {@code DescriptorBackedIndexLifecycle}
     * defers the deletion's acknowledgement until this completes, which is the only window where the write
     * can be both
     * off the cluster state thread and ahead of the client being told the delete succeeded.
     *
     * <p>Must fail the listener rather than complete it when the write cannot be made. A delete that could
     * not record its tombstone has not achieved what a delete promises.
     */
    void putTombstoneAsync(IndexDescriptor tombstone, org.opensearch.core.action.ActionListener<Void> whenDurable);

    /**
     * Whether the backing store is ready to be read and written.
     *
     * <p>Named for the question rather than for the mechanism, because the mechanisms do not resemble each
     * other. The system index has to exist, be allocated and carry the settings W8 established. A blob
     * backend has none of that: there is nothing to bootstrap, which is one of the four problems the
     * system index charged against it that simply does not arise.
     */
    boolean available();

    /**
     * Reads these names into whatever cache serves {@link #get}, without blocking the caller.
     *
     * <p>This is C1's prefetch, and "without blocking" is the requirement rather than a nicety. The caller
     * is {@code TransportBulkAction.doExecute}, which runs on a transport worker and, through an
     * acknowledgement continuation, sometimes on the cluster applier thread. Both forbid blocking, and the
     * first implementation looped over the blocking {@link #get}.
     *
     * <p>Best effort: a backend that cannot warm must still complete the listener successfully, because a
     * request must not fail for want of a warm cache. Whatever does not warm is resolved inline later.
     *
     * <p>Deliberately not a defaulted no-op. A default would let a backend opt out of the one mechanism C1
     * exists for without anyone noticing, which is the "correct and unreachable" failure this area keeps
     * producing -- and a prefetch that does nothing is indistinguishable from a fast one.
     */
    void warmAsync(java.util.Collection<String> names, org.opensearch.core.action.ActionListener<Void> listener);

    /**
     * Drops any cached answer for this name, so the next read goes to the store.
     *
     * <p>The write paths invalidate their own name already. This is for a change made <em>elsewhere</em>:
     * a descriptor written on another node is invisible here until the freshness window expires, so a node
     * tailing the change log needs a way to say "whatever you think you know about this name, forget it".
     * Without it the only bound on staleness is the cache TTL, which is a second of a deleted index still
     * resolving as live.
     *
     * <p>Must be safe for a name that was never cached, since a tailer sees every change in the cluster and
     * most of them concern names this node has never read.
     */
    void invalidate(String name);
}
