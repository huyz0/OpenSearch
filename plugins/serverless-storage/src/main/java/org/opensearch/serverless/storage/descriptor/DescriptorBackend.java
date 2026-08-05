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

    /** Writes a descriptor whether or not the name is taken. */
    void put(IndexDescriptor descriptor);

    /** {@link #put} without blocking the caller. */
    void putAsync(IndexDescriptor descriptor);

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
     * a node holding that shard's data can adopt it again on rejoin. {@code DurableTombstones} defers the
     * deletion's acknowledgement until this completes, which is the only window where the write can be both
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
