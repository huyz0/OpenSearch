/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * What a plugin does to the records it holds for indices that live outside cluster state -- as whole
 * operations core asks for, rather than a set of hooks core calls at points of its own choosing.
 *
 * <p>Two so far, and they are the two things core cannot do for such an index because the entry it would
 * work from is not there: {@link #removeIndices} when the index is deleted, and {@link #putMapping} when its
 * mapping changes. Both were previously a protocol core ran itself against static seams -- five of them
 * between the two -- and in both cases core's remaining share is the single decision that is genuinely its
 * own: which indices cluster state cannot serve.
 *
 * <p><b>What this replaces, and why the shape changed.</b> Deletion used to reach two separate seams from
 * three separate places. {@code DurableTombstones.whenDurable} was consulted twice ({@code
 * MetadataDeleteIndexService}'s ordinary path and its all-gated path) to defer the acknowledgement on a
 * tombstone write, and {@code MappingGenerationStore.deleteMapping} was called once per deleted index
 * afterwards to prune the stored mapping. Between them core carried about two hundred lines that touched no
 * cluster state at all: it partitioned the request, sequenced the two seams, owned the ordering rule
 * between them, and logged the failures of the second. That is a storage-plane deletion protocol, written
 * out in core, for indices core does not own.
 *
 * <p>Stated as one operation instead, all of that is the implementer's: core says <em>these indices are
 * gone, remove whatever you hold for them</em> and waits for the answer. Core keeps the one decision that
 * is genuinely its own -- which indices those are -- because that is {@link IndexCreationStrategy}'s and
 * {@link IndexCatalog}'s question and it stays there.
 *
 * <p><b>Every removed index is handed over, not only the claimed ones.</b> That is deliberate and it is
 * what both replaced seams already did: the tombstone writer received every index the delete removed, and
 * so did the mapping prune. An implementation that only wants the ones it claims recognises them itself --
 * it is the only thing that can, since by the time this is called the cluster state entry is already gone
 * and "was this claimed" is a question about the implementer's own records.
 *
 * <p><b>{@link IndexMetadata} rather than {@link org.opensearch.core.index.Index}.</b> A removal record is
 * derived from the metadata -- a descriptor tombstone carries the uuid, the shard count and the settings
 * that say what dangling data to reclaim, not just a name -- and by the time this runs there is nowhere
 * left to look that metadata up: the cluster state entry has been removed, and for a claimed index there
 * never was one. Core captures it while the deletion transform runs and passes it here, which is the same
 * reason {@code DurableTombstones.Writer} took metadata rather than names.
 *
 * <p><b>The acknowledgement contract, which is the load-bearing part.</b>
 *
 * <p>Core defers the client's acknowledgement on the returned stage, and fails the delete if the stage
 * fails. That deferral is why this returns a {@link CompletionStage} at all rather than being fire and
 * forget.
 *
 * <p>The removal record is what stops a resurrection: a node partitioned during a delete rejoins holding
 * shard data for an index cluster state no longer mentions, and without a durable no it imports that data
 * back. For a claimed index there is no cluster state entry and no {@link IndexGraveyard} entry behind it,
 * so that record is the only one there is. Inferring it instead was considered and rejected: {@code
 * DanglingIndicesState.isDeleted} deliberately does not treat a missing record as a deletion, because a
 * store that is unavailable and a store with no record give the same answer, and discarding live shard
 * data on that basis is worse than the failure it would fix.
 *
 * <p>So durability has to come from ordering. This is called after the cluster state is committed and
 * before the client is told the delete succeeded, which is the one window where a write can be both off the
 * cluster state thread and ahead of the acknowledgement. A failure completes the stage exceptionally rather
 * than silently succeeding -- a delete whose record could not be written has not achieved what a delete
 * promises, and saying so is the whole point of doing the write before the acknowledgement rather than
 * after it.
 *
 * <p><b>Nothing blocks.</b> The acknowledgement is deferred rather than waited on: it is sent when the
 * stage completes, from whatever thread completes it. That is what lets this be durable without
 * reintroducing the deadlock that made the descriptor publish path asynchronous in the first place.
 *
 * <p><b>Best-effort cleanup goes after the stage completes, not before it.</b>
 *
 * <p>An implementation may well have more to do than the part a delete must wait for -- pruning a stored
 * mapping is the case this seam was built around. That work belongs after the stage is completed, and an
 * implementation is required to complete the stage as its first act rather than chaining the cleanup in
 * front of it. Two reasons, both measured:
 *
 * <ul>
 *   <li><b>Latency.</b> A prune is a blocking round trip. A request naming five hundred claimed indices
 *       would otherwise make the client wait for five hundred of them -- including for every index that
 *       never declared a mapping, since removing an absent document still costs a round trip -- before
 *       hearing about a deletion that had already happened. Making the caller wait for work whose result is
 *       then discarded is the worst of both.</li>
 *   <li><b>Safety.</b> The durable record is what makes the deletion real. Pruning first would mean a
 *       failure between the two left an index that still exists and whose declared fields are gone, which
 *       is a silent loss; this way the same failure leaves a document nobody references, which is exactly
 *       the status quo before any of this existed.</li>
 * </ul>
 *
 * <p>It follows that a cleanup failure must not fail the stage. The index is gone either way, and reporting
 * the delete as failed would invite a retry of something that already happened.
 *
 * <p><b>Registration.</b>
 *
 * <p>Supplied by at most one {@link org.opensearch.plugins.ClusterPlugin#getClaimedIndexLifecycle()} per
 * node and injected into {@code MetadataDeleteIndexService} by {@code ClusterModule}. Deliberately not a
 * static registry, unlike the pair it replaces: the one core caller is an injectable singleton, so the
 * plane can simply be a constructor argument and there is no static holder to keep in step or to leak
 * between tests. A node with no such plugin gets {@link #NOOP} and deletes exactly as it always has.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ClaimedIndexLifecycle {

    /**
     * What a node with no plugin supplying one gets: nothing to remove, and an acknowledgement that is
     * never deferred. Identical to what the two seams this replaces answered with nothing registered.
     */
    ClaimedIndexLifecycle NOOP = indices -> CompletableFuture.completedFuture(null);

    /**
     * Removes whatever this plane holds for indices core has just deleted, as one operation.
     *
     * @param indices the metadata of every index the deletion removed, captured while the deletion transform
     *                ran. Never empty -- core does not call this with nothing to remove.
     * @return a stage completing once the removal is durable, or completing exceptionally if it cannot be
     *         made durable. Core defers the delete's acknowledgement on it and fails the request on a
     *         failure; see this interface's own javadoc for why that ordering is the point, and for why any
     *         further best-effort cleanup must run after this stage is completed rather than before.
     */
    CompletionStage<Void> removeIndices(Collection<IndexMetadata> indices);

    /**
     * Applies a mapping change to indices this plane holds, for a request no cluster state entry can serve.
     *
     * <p><b>What this replaces.</b> {@code MetadataMappingService} used to carry the whole of this itself:
     * it resolved each index's descriptor to refuse a write against an already-tombstoned uuid, parsed the
     * request source, reduced it to a flat field map, refused the request when that reduction would have
     * dropped something, and then drove {@code MappingGenerationStore}'s compare-and-swap loop. None of that
     * touches cluster state. It is a storage-plane merge protocol -- reading a generation, merging fields
     * onto it, writing conditionally, retrying on contention -- written out in core for indices core does
     * not own, and it was the last thing keeping a mapping store in core at all.
     *
     * <p><b>What core keeps.</b> One decision, and it is the same one deletion makes: whether the request
     * can be served from cluster state. Every index the request names is absent from it, so it cannot be,
     * and the request goes here instead. Core does not ask whether a plane is installed first -- {@link
     * #NOOP}'s answer to "apply this to indices I do not hold" is the {@link
     * org.opensearch.index.IndexNotFoundException} the ordinary path would have raised at its own metadata
     * lookup, so a node with no plugin fails such a request exactly as it always did, without submitting a
     * cluster state task that was only ever going to fail.
     *
     * <p><b>Called off the cluster manager's update thread</b>, on {@code GENERIC}, so an implementation may
     * block. That dispatch is not a convenience: this path used to run inside the put-mapping executor,
     * where a store read and a store write both stood on the single thread every cluster state change
     * queues behind, and tripped the "Expected current thread to not be the cluster-manager service thread"
     * assertion doing it.
     *
     * <p><b>A change is not applied until the stage completes.</b> Core defers the client's acknowledgement
     * on it and fails the request if it fails -- there is no cluster state entry standing behind this write
     * that would still carry the change, so acknowledging a mapping update that did not land would be the
     * silent success this area has produced repeatedly.
     *
     * @param indices       every index the request names, all of them absent from cluster state. Never
     *                      empty.
     * @param mappingSource the request's mapping source, verbatim, exactly as the ordinary path would have
     *                      merged it. Passed unparsed because what a plane can represent is the plane's
     *                      question: the one refusal core used to raise here named a limitation of a
     *                      particular store's format, which core has no business knowing.
     * @return a stage completing once the change is durable, or completing exceptionally if it cannot be
     *         applied. Never null.
     */
    default CompletionStage<Void> putMapping(Collection<Index> indices, String mappingSource) {
        return CompletableFuture.failedFuture(
            new IndexNotFoundException(
                "no plugin holds indices outside cluster state on this node, so there is nowhere for this "
                    + "mapping change to be recorded",
                indices.iterator().next().getName()
            )
        );
    }
}
