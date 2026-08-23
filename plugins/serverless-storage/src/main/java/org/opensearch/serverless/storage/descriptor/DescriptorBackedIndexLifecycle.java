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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.ClaimedIndexLifecycle;
import org.opensearch.cluster.metadata.DescriptorRepresentable;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexNotFoundException;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Everything this plugin holds for an index that has no cluster state entry, changed and removed as the
 * whole operations core asks for.
 *
 * <p><b>What this is made of.</b> Two protocols core used to run itself. {@link #removeIndices} is the
 * deletion one; {@link #putMapping} is the mapping one, which core reached through a third static seam and
 * about a hundred lines that resolved descriptors, reduced a mapping source to fields, refused what would
 * not round-trip and then drove a compare-and-swap loop -- none of it touching cluster state, all of it
 * about records core does not own. Both are moved rather than rewritten: the same writes, in the same
 * order, with the same failure policy for each half. What changed is who sequences them.
 *
 * <p><b>The deletion protocol, in detail.</b> Two things core used to run itself, in that order: the durable tombstone
 * write it deferred the acknowledgement on ({@code DurableTombstones}, a static seam this class retires),
 * and the stored-mapping prune it issued afterwards ({@code MetadataDeleteIndexService#removeStoredMappings},
 * whose body is below, javadoc and all). Both are moved rather than rewritten: the same writes in the same
 * order, with the same failure policy for each half. What changed is who sequences them. Core no longer
 * knows there are two.
 *
 * <p><b>Installed-or-not is read live, not captured.</b> The plugin returns this from {@code
 * ClusterPlugin#getClaimedIndexLifecycle()} unconditionally, exactly as it returns {@code
 * SupplierBackedIndexCatalog} -- the object exists for the node's lifetime while {@link DescriptorGate}
 * fills and empties underneath it. With the gate uninstalled there is no store to write a tombstone to and
 * no mapping store to prune from, and this answers with an already-completed stage: identical to what an
 * unregistered {@code DurableTombstones.Writer} answered, which is what makes deletion on a node without
 * the feature cost exactly what it always cost.
 *
 * @see ClaimedIndexLifecycle
 */
public final class DescriptorBackedIndexLifecycle implements ClaimedIndexLifecycle {

    private static final Logger logger = LogManager.getLogger(DescriptorBackedIndexLifecycle.class);

    /**
     * Writes a tombstone for every deleted index, completing when the last one is durable, and prunes their
     * stored mappings afterwards.
     *
     * <p>The publisher {@link DescriptorGate} registers is fire-and-forget by necessity: it runs inside
     * cluster state construction, where blocking deadlocks. That is fine for a creation, whose index is in
     * cluster state anyway, and not fine for a tombstone. For a gated index there is no cluster state entry
     * and no graveyard entry standing behind the tombstone, so losing it means a node holding that shard's
     * data can adopt it again on rejoin -- the resurrection {@code IndexGraveyard} exists to prevent,
     * reintroduced.
     *
     * <p>This runs after the state is committed and before the client is told the delete succeeded, which is
     * the one window where a write can be both off the cluster state thread and ahead of the
     * acknowledgement. Nothing blocks; the acknowledgement is deferred, not waited on.
     *
     * <p><b>The stage is completed before the prune is started, and that order is required of an
     * implementation rather than incidental to this one</b> -- see {@link ClaimedIndexLifecycle}'s own
     * javadoc for the two reasons. Completing it inline, as the first statement of the completion handler,
     * is what makes core's acknowledgement land on the same thread at the same point it always did.
     */
    @Override
    public CompletionStage<Void> removeIndices(Collection<IndexMetadata> indices) {
        final DescriptorBackend store = DescriptorGate.installedStore();
        if (store == null || indices.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        final CompletableFuture<Void> durable = new CompletableFuture<>();
        // Grouped so the acknowledgement waits for all of them and reports the first failure. A delete
        // naming several indices is not durable until the last tombstone is.
        ActionListener<Void> perTombstone = new GroupedActionListener<>(ActionListener.wrap(ignored -> {
            // Completed first, then the prune. Core's acknowledgement is a dependent of this future, so it
            // runs inside this call -- before the round trips below, exactly as it did when this ordering
            // was written out in MetadataDeleteIndexService.
            durable.complete(null);
            removeStoredMappings(indices);
        }, durable::completeExceptionally), indices.size());
        for (IndexMetadata metadata : indices) {
            IndexDescriptor tombstone = IndexDescriptor.from(metadata).tombstoned(System.currentTimeMillis());
            // The change log entry for a deletion, which had no writer at all until this was registered, and
            // its absence was the worst of the five.
            //
            // The publisher has an exists()==false branch that records a DELETED, and nothing reaches it:
            // publish() is only ever called with metadata for an index that is in cluster state, and an
            // all-gated delete does not go through a cluster state update in the first place --
            // MetadataDeleteIndexService.deleteGatedIndices records the removal and acknowledges. So no node
            // was ever told that a gated index had been deleted. Every other node went on resolving it from
            // cache as live for the whole freshness window, a minute by default, and *accepted acknowledged
            // writes against a deleted index* for that long. It also left DescriptorChangeTailer's
            // delete-driven shard release unreachable in production, so the only thing closing those shards
            // was the periodic sweep.
            //
            // Recorded after the write is durable rather than beside it, because an entry for a tombstone
            // that never landed would have other nodes drop a live index's shards.
            store.putTombstoneAsync(tombstone, ActionListener.wrap(ignored -> {
                DescriptorGate.recordWrite(tombstone);
                perTombstone.onResponse(null);
            }, perTombstone::onFailure));
        }
        return durable;
    }

    /**
     * Removes the mappings of indices that have just been deleted.
     *
     * <p>Nothing ever removed one before this: {@code MappingGenerationStore.Store} had no delete at all, so
     * {@code .opensearch-index-mappings} grew with every index that had ever existed rather than with the
     * live population. That is the residency problem this area exists to remove, reproduced one level down,
     * and churn is what makes it bite -- a tenant that creates and drops an index a day leaves a document a
     * day behind forever.
     *
     * <p><b>After the tombstone is durable, and that order is the safety argument.</b> The tombstone is what
     * makes the deletion real. Removing the mapping first would mean a failure between the two left an index
     * that still exists and whose declared fields are gone, which is a silent loss; this way the same
     * failure leaves a document nobody references, which is exactly the status quo before this existed.
     *
     * <p><b>A failure here does not fail the deletion.</b> The index is gone either way, and reporting the
     * delete as failed would invite a retry of something that already happened. A stranded document is the
     * lesser outcome, and it is logged rather than swallowed.
     *
     * <p><b>After the acknowledgement, not before it.</b> Each prune is a blocking round trip, and a request
     * naming five hundred gated indices would otherwise make the client wait for five hundred of them --
     * including for every index that never declared a mapping, since removing an absent document still costs
     * a round trip -- before hearing about a deletion that had already happened. With an unassigned primary
     * on the mapping index each one waits out the replication timeout instead. Making the caller wait for
     * work whose result is then discarded is the worst of both.
     *
     * <p>Catching {@link Throwable} rather than {@link Exception}, which is deliberate and narrow. A
     * blocking client call from the wrong thread raises an {@code AssertionError}, and an {@code Error}
     * escaping here would leave the completion chain broken on a path whose entire purpose is best-effort
     * cleanup after the client has already been answered.
     */
    private static void removeStoredMappings(Collection<IndexMetadata> deleted) {
        // Read once, here, rather than through a static entry point per index. MappingGenerationStore's own
        // deleteMapping wrapper existed only so core could reach a store it never otherwise touched on this
        // path, and it went with the protocol that called it.
        MappingGenerationStore.Store mappingStore = DescriptorGate.installedMappingStore();
        if (mappingStore == null) {
            return;
        }
        for (IndexMetadata metadata : deleted) {
            try {
                mappingStore.delete(metadata.getIndexUUID());
            } catch (Throwable e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "{} was deleted but its stored mapping could not be removed, leaving a document nobody references",
                        metadata.getIndex()
                    ),
                    e
                );
            }
        }
    }

    /**
     * Records a put-mapping's fields against each index's mapping generation.
     *
     * <p>Moved here whole from {@code MetadataMappingService#recordGatedMapping}, including the refusal
     * below and the tombstone check it opens with. Core kept the one decision that is its own -- every index
     * this request names is absent from cluster state, so cluster state cannot serve it -- and handed the
     * rest over.
     *
     * <p><b>Runs on {@code GENERIC}</b>, dispatched there by core before this is called, so the blocking
     * reads and writes below are safe. They were not always: this protocol used to run inside the
     * put-mapping cluster state executor, two blocking round trips deep, on the single thread every cluster
     * state change queues behind.
     *
     * <p><b>Extraction is {@link DescriptorRepresentable#fieldDefinitionsOrNull(Object)}</b>, the same call
     * the creation path makes. Sharing it is the point rather than a tidy-up: these were two implementations
     * documented as mirroring each other, and the divergence was not cosmetic -- a mapping the creation path
     * refused outright, this one accepted and silently reduced. The call now crosses the SPI boundary, which
     * is the reason that method is public; growing a second copy on this side would be the same divergence
     * in a place where it is harder rather than easier to notice.
     *
     * <p><b>With no mapping store installed this fails rather than succeeding quietly.</b> Nothing here holds
     * the index, so there is nowhere for the change to be recorded, and that is the {@link
     * IndexNotFoundException} the ordinary cluster state path would have raised -- the same answer {@link
     * ClaimedIndexLifecycle#putMapping}'s own default gives on a node with no plugin at all.
     */
    @Override
    public CompletionStage<Void> putMapping(Collection<Index> indices, String mappingSource) {
        // Asked of the registry updateMapping below actually consults, rather than of DescriptorGate's
        // parallel copy. The two are set and cleared together by install/uninstall, so they agree in
        // production -- but checking one and using the other is how they would stop agreeing without
        // anything noticing.
        if (MappingGenerationStore.isRegistered() == false) {
            return CompletableFuture.failedFuture(
                new IndexNotFoundException(
                    "this node holds no mapping store, so there is nowhere for the mapping change to be recorded",
                    indices.iterator().next().getName()
                )
            );
        }
        try {
            for (Index index : indices) {
                refuseIfDescriptorShowsTheIndexIsGone(index);
            }
            Map<String, Object> parsed = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), mappingSource, false);
            Map<String, Object> fields = DescriptorRepresentable.fieldDefinitionsOrNull(parsed.get("properties"));
            if (fields == null) {
                // Refused rather than partially recorded, and this is the half of the defect that was
                // worse. The extraction here used to skip any property it could not read and record the
                // rest, then report success -- so a put-mapping adding an object field to a gated index was
                // acknowledged with the field silently absent. The same field at creation was refused and
                // kept the index resident, which is exactly the "gated at creation and refused on update,
                // or the reverse" that the shared extractor's javadoc says must not exist.
                //
                // There is no fallback available: the index is not in cluster state, so this cannot be
                // applied the ordinary way. Failing is the only answer that does not lose the field.
                throw new IllegalArgumentException(
                    "index ["
                        + indices.iterator().next().getName()
                        + "] is held outside cluster state and its mapping store holds a flat map of field "
                        + "name to type, so it cannot carry an object or nested field, or a field parameter "
                        + "such as a date format or an analyzer. Applying this mapping would drop them"
                );
            }
            if (fields.isEmpty() == false) {
                for (Index index : indices) {
                    MappingGenerationStore.updateMapping(index.getUUID(), fields);
                }
            }
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Refuses a put-mapping whose index the descriptor plane no longer says is live.
     *
     * <p>Core decides "cluster state cannot serve this" purely from the index's absence there, and that is
     * equally true of a live gated index and one the deletion-time prune already removed: cluster
     * state never carried an entry for either. The uuid this request names was resolved by the
     * coordinating node from its own descriptor cache, which invalidates on a tombstone it has seen but
     * not on one it has not -- so a node whose cache has not yet caught up can still route a put-mapping
     * or a dynamic field inference at a uuid whose tombstone is already durable elsewhere. Nothing
     * previously asked the store whether that uuid was still current, so the write landed, recreated the
     * document the deletion had pruned, and nothing was ever going to remove it again.
     *
     * <p><b>The contract this closes the hole by:</b> refusing here, not letting the write land and sweeping
     * it up later. The alternative -- a second pass that prunes stray mappings after the fact -- was
     * tried first, keyed off the tombstone the deletion already wrote, and rejected as a second writer
     * racing the same records. This resolves the descriptor for the uuid fresh, on the thread doing the
     * write, which is off the cluster manager's update thread already and so may block. For a live index the
     * resolution is a cache hit against the same descriptor cache every other resolution on this path
     * already pays for, not a new cost.
     *
     * <p>Skipped when descriptor resolution is not registered at all, which is not a hole: {@link
     * DescriptorGate} registers the descriptor supplier and the mapping store together in one {@code
     * install} call, so a node that reached this method by way of the store also has descriptor resolution
     * to consult. A caller that registers only the store, as several tests below the descriptor plane do, is
     * exercising the mapping store in isolation and has no tombstone to consult in the first place.
     *
     * <p><b>A null answer is not treated as "gone".</b> {@link AbsentIndexDescriptorSuppliers#supply}
     * documents null as "no answer" and swallows any failure that is not a {@code
     * DescriptorUnavailableException} into it, precisely so a resolver bug degrades a request rather than
     * failing it -- every other caller of this seam relies on that. Refusing on null as well as on a
     * confirmed tombstone would let that same resolver bug fail a live index's put-mapping outright,
     * which is a regression this must not introduce to close a narrower one. It is also not what a
     * genuine deletion looks like here: {@code BlobDescriptorBackend#get} answers a tombstoned name with
     * the tombstone record, not null, so this only ever refuses on an answer that actually says so.
     *
     * <p>Deliberately still {@link AbsentIndexDescriptorSuppliers} directly, not the catalog-backed {@code
     * Metadata} accessors: this needs the raw {@link IndexDescriptor}'s own {@code uuid()} and the
     * three-way null/tombstoned/live distinction, which {@code IndexCatalog}'s generic, collapsed "null
     * means absent" contract deliberately does not expose.
     */
    private static void refuseIfDescriptorShowsTheIndexIsGone(Index index) {
        if (AbsentIndexDescriptorSuppliers.isRegistered() == false) {
            return;
        }
        IndexDescriptor current = AbsentIndexDescriptorSuppliers.supply(index.getName());
        if (current != null && (current.exists() == false || current.uuid().equals(index.getUUID()) == false)) {
            throw new IndexNotFoundException(
                "its tombstone is already durable; the mapping store refuses a write against a uuid the descriptor plane no "
                    + "longer resolves as live",
                index.getName()
            );
        }
    }
}
