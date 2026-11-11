/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Changes aliases safely: each mutation is prepared, published and recovered by compare-and-swap, with a hint left on
 * every index it touches so a reader can finish or abort one a crashed writer left half done. Split out of
 * {@link MetadataPlane}, which keeps the public entry points.
 */
final class AliasMutations {

    private final DescriptorStore descriptors;
    private final DigestRollups rollups;
    private final LongSupplier clock;
    private final long leaseTtlMillis;
    private final MetadataPlane plane;

    AliasMutations(DescriptorStore descriptors, DigestRollups rollups, LongSupplier clock, long leaseTtlMillis, MetadataPlane plane) {
        this.descriptors = descriptors;
        this.rollups = rollups;
        this.clock = clock;
        this.leaseTtlMillis = leaseTtlMillis;
        this.plane = plane;
    }

    /**
     * Drops a deleted index from every alias that named it, each under its own compare-and-swap.
     *
     * <p>An alias whose <em>only</em> index is deleted keeps naming it. Removing the alias would make a
     * search through it "no such index: {alias}", and the answer this surface gives is the one a named
     * index gives -- refused, naming the index that is gone -- which is what lets a caller tell a deleted
     * index from a typo in the alias. The hint-side under-approximation this method closes is the one
     * with other indices left; for the last one, the alias record is the whole truth and says so. A data
     * stream keeps its stream fields on the way through; its write index cannot reach here, because
     * {@code DELETE /{index}} refuses it.
     */
    void detachFromAliases(IndexDescriptor deleted) throws IOException {
        for (String name : deleted.aliasedBy()) {
            boolean done = false;
            for (int attempt = 0; attempt < 4 && done == false; attempt++) {
                final DescriptorResolution resolved = descriptors.resolveRaw(name);
                if (resolved.alias() != null && resolved.alias().mutationPending() && resolved.alias().protectsHint(deleted.name())) {
                    abortAlias(resolved.alias(), resolved.generation());
                    continue;
                }
                if (resolved.alias() == null || resolved.alias().indices().contains(deleted.name()) == false) {
                    break;
                }
                final java.util.List<String> now = new java.util.ArrayList<>(resolved.alias().indices());
                now.remove(deleted.name());
                if (now.isEmpty()) {
                    break; // the last index: the alias keeps naming it, and a search through it says so
                }
                done = descriptors.updateAlias(withIndices(resolved.alias(), now), resolved.generation()).isPresent();
            }
            if (done == false) {
                MetadataPlane.LOGGER.warn(
                    "alias [{}] may still name the deleted index [{}]; it is being changed concurrently",
                    name,
                    deleted.name()
                );
            }
        }
    }

    /**
     * Reads an alias and its generation together, briefly waiting for a pending mutation.
     *
     * @param name the alias
     * @return the record and its generation, or empty when the name is not an alias
     * @throws IOException if the read fails
     */
    public Optional<MetadataPlane.AliasAtGeneration> aliasWithGeneration(String name) throws IOException {
        final DescriptorResolution resolved = resolveForAliasMutation(name);
        return resolved.alias() == null
            ? Optional.empty()
            : Optional.of(new MetadataPlane.AliasAtGeneration(resolved.alias(), resolved.generation()));
    }

    /**
     * Returns the same record naming different indices, with a data stream's fields carried through.
     *
     * <p>Every alias mutation used to rebuild the record with the two-argument constructor, which made a
     * data stream a plain alias the moment anything touched it.
     *
     * @param record the record
     * @param indices the indices it should name
     * @return the record as it should now be written
     */
    public static org.opensearch.serverless.cluster.AliasRecord withIndices(
        org.opensearch.serverless.cluster.AliasRecord record,
        java.util.List<String> indices
    ) {
        return new org.opensearch.serverless.cluster.AliasRecord(
            record.name(),
            indices,
            record.dataStream(),
            record.generation(),
            record.timestampField()
        );
    }

    /**
     * Replaces an alias under a compare-and-swap, for a caller that has not read it.
     *
     * <p>Costs one read more than the three-argument {@code updateAlias}, which is the form
     * every request path uses: the read here is only for the hints, and the token still has to come from
     * the caller's own read of the record.
     *
     * @param alias the alias as it should now be
     * @param expectedGeneration the generation the caller read, with the record
     * @return the new generation, or empty if another writer got there first
     * @throws IOException if the swap fails
     */
    public Optional<Long> updateAlias(org.opensearch.serverless.cluster.AliasRecord alias, long expectedGeneration) throws IOException {
        return updateAlias(recoverAlias(alias.name()).alias(), alias, expectedGeneration);
    }

    /**
     * Replaces an alias under a compare-and-swap.
     *
     * @param before the record as the caller read it, which decides which hints change; null if none
     * @param alias the alias as it should now be
     * @param expectedGeneration the generation the caller read, with {@code before}, in one read
     * @return the new generation, or empty if another writer got there first
     * @throws IOException if the swap fails
     */
    public Optional<Long> updateAlias(
        org.opensearch.serverless.cluster.AliasRecord before,
        org.opensearch.serverless.cluster.AliasRecord alias,
        long expectedGeneration
    ) throws IOException {
        if (before == null || before.mutationPending()) {
            recoverAlias(alias.name());
            return Optional.empty();
        }
        rollups.markAlias(alias.name());
        final List<String> additions = new ArrayList<>(alias.indices());
        additions.removeAll(before.indices());
        final var pending = before.preparing(additions, mutationDeadline(), false);
        final Optional<Long> reserved = descriptors.updateAlias(pending, expectedGeneration);
        if (reserved.isEmpty()) {
            return Optional.empty();
        }
        return publishAlias(pending, alias, reserved.get());
    }

    /**
     * Creates an alias, and lists it on each index it names.
     *
     * @param alias the alias to create
     * @return the generation the alias record was created at
     * @throws IOException if a descriptor or the alias register cannot be written
     */
    public long createAlias(org.opensearch.serverless.cluster.AliasRecord alias) throws IOException {
        rollups.markAlias(alias.name());
        recoverAlias(alias.name());
        final var pending = alias.preparing(alias.indices(), mutationDeadline(), true);
        final long reserved = descriptors.createAlias(pending);
        return publishAlias(pending, alias, reserved).orElseThrow(() -> new IOException("alias creation was superseded: " + alias.name()));
    }

    long mutationDeadline() {
        final long now = Math.max(1L, clock.getAsLong());
        return now > Long.MAX_VALUE - leaseTtlMillis ? Long.MAX_VALUE : now + leaseTtlMillis;
    }

    Optional<Long> publishAlias(
        org.opensearch.serverless.cluster.AliasRecord pending,
        org.opensearch.serverless.cluster.AliasRecord target,
        long reserved
    ) throws IOException {
        final Optional<Long> applied;
        try {
            for (String index : target.indices()) {
                prepareAliasHint(index, pending, reserved);
            }
            applied = descriptors.updateAlias(target.published(), reserved);
        } catch (IOException | RuntimeException e) {
            try {
                abortAlias(pending, reserved);
            } catch (IOException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        if (applied.isEmpty()) {
            abortAlias(pending, reserved);
        } else if (pending.creationPending() == false) {
            for (String index : pending.indices()) {
                if (target.indices().contains(index) == false) {
                    removeAliasHint(index, target.name(), false);
                }
            }
        }
        return applied;
    }

    void abortAlias(org.opensearch.serverless.cluster.AliasRecord pending, long reserved) throws IOException {
        if (pending.creationPending()) {
            descriptors.deleteIfUnchanged(pending.name(), reserved, clock.getAsLong());
        } else {
            descriptors.updateAlias(pending.published(), reserved);
        }
        for (String index : pending.pendingAdditions()) {
            removeAliasHint(index, pending.name(), true);
        }
    }

    /** Expired preparations are aborted; the owner can publish only while its generation still holds. */
    DescriptorResolution recoverAlias(String name) throws IOException {
        for (int attempt = 0; attempt < 4; attempt++) {
            final var current = descriptors.resolveRaw(name);
            if (current.alias() == null
                || current.alias().mutationPending() == false
                || current.alias().mutationUntil() > clock.getAsLong()) {
                return current;
            }
            abortAlias(current.alias(), current.generation());
        }
        return descriptors.resolveRaw(name);
    }

    /**
     * Answers which aliases name an index, verified.
     *
     * <p>The descriptor's {@code aliased_by} list is a hint that may carry a name whose alias was never
     * created or has since stopped naming this index. Each name costs one register read to confirm, which
     * is bounded by how many aliases name the index rather than by how many exist -- the enumeration this
     * design refuses.
     *
     * @param descriptor the index
     * @return the aliases that currently name it, in the order they were recorded
     * @throws IOException if a register cannot be read
     */
    public java.util.List<String> aliasesOf(IndexDescriptor descriptor) throws IOException {
        final java.util.List<String> confirmed = new java.util.ArrayList<>();
        for (String name : descriptor.aliasedBy()) {
            final DescriptorResolution resolved = plane.resolve(name);
            if (resolved.alias() != null && resolved.alias().indices().contains(descriptor.name())) {
                confirmed.add(name);
            }
        }
        return confirmed;
    }

    /** Prepare hints before publication, fencing removers even when the hint is already present. */
    void prepareAliasHint(String index, org.opensearch.serverless.cluster.AliasRecord pending, long reserved) throws IOException {
        for (int attempt = 0; attempt < 4; attempt++) {
            final var current = descriptors.resolveRaw(index);
            if (current.index() == null) {
                if (pending.pendingAdditions().contains(index)) {
                    throw new IOException("alias member does not exist: " + index);
                }
                return; // An existing alias may still name a deleted index for diagnostics.
            }
            final var owner = descriptors.resolveRaw(pending.name());
            if (owner.generation() != reserved || owner.alias() == null || owner.alias().mutationPending() == false) {
                throw new IOException("alias preparation was superseded: " + pending.name());
            }
            final List<String> names = new ArrayList<>(current.index().aliasedBy());
            if (names.contains(pending.name()) == false) {
                names.add(pending.name());
            } else if (pending.pendingAdditions().contains(index) == false) {
                return;
            }
            if (descriptors.update(current.index().withAliasedBy(names), current.generation()).isPresent()) {
                return;
            }
        }
        throw new IOException("could not prepare alias hint: " + pending.name() + " on " + index);
    }

    /** Read the index before fresh alias truth so a later preparation fences this removal's CAS. */
    void removeAliasHint(String index, String alias, boolean fencePreparation) throws IOException {
        for (int attempt = 0; attempt < 4; attempt++) {
            final var current = descriptors.resolveRaw(index);
            if (current.index() == null) {
                return;
            }
            final var truth = descriptors.resolveRaw(alias);
            if (truth.alias() != null && truth.alias().protectsHint(index)) {
                return;
            }
            final List<String> names = new ArrayList<>(current.index().aliasedBy());
            if (names.remove(alias) == false && fencePreparation == false) {
                return;
            }
            if (descriptors.update(current.index().withAliasedBy(names), current.generation()).isPresent()) {
                return;
            }
        }
        MetadataPlane.LOGGER.warn("could not remove stale alias hint [{}] from [{}]", alias, index);
    }

    /**
     * Reads an alias for mutation, briefly backing off while another writer prepares its hints.
     *
     * @param name the alias name
     * @return its published value and generation, or a still-busy reservation after the bounded wait
     * @throws IOException if the register cannot be read or the thread is interrupted
     */
    public DescriptorResolution resolveForAliasMutation(String name) throws IOException {
        final long started = System.nanoTime();
        long pause = 1_000_000L;
        while (true) {
            final var current = recoverAlias(name);
            if (current.alias() == null || current.alias().mutationPending() == false || System.nanoTime() - started >= 250_000_000L) {
                return current.alias() != null && current.alias().creationPending()
                    ? new DescriptorResolution(null, null, current.generation())
                    : current;
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new java.io.InterruptedIOException("interrupted waiting for alias mutation: " + name);
            }
            java.util.concurrent.locks.LockSupport.parkNanos(pause);
            pause = Math.min(64_000_000L, pause * 2L);
        }
    }

    /**
     * Removes an alias.
     *
     * <p>Nothing to clean up behind it: an alias owns no shards and no bytes, which is the difference
     * between deleting one and deleting an index.
     *
     * @param name the alias
     * @return true if there was one
     * @throws IOException if the delete fails
     */
    public boolean deleteAlias(String name) throws IOException {
        final DescriptorResolution resolved = plane.resolve(name);
        if (resolved.alias() == null) {
            return false;
        }
        return deleteAlias(resolved.alias(), resolved.generation());
    }

    /**
     * Removes an alias the caller has read, only if it has not changed since.
     *
     * <p>The unconditional form re-read its own generation and deleted whatever was there, so "the last
     * index was removed, delete the alias" deleted an index a concurrent add had just put in.
     *
     * @param alias the record as the caller read it
     * @param expectedGeneration the generation it was read at
     * @return true if this caller deleted it; false if it changed first
     * @throws IOException if the delete fails
     */
    public boolean deleteAlias(org.opensearch.serverless.cluster.AliasRecord alias, long expectedGeneration) throws IOException {
        if (alias.mutationPending()) {
            recoverAlias(alias.name());
            return false;
        }
        final boolean deleted = descriptors.deleteIfUnchanged(alias.name(), expectedGeneration, clock.getAsLong());
        if (deleted) {
            for (String index : alias.indices()) {
                removeAliasHint(index, alias.name(), false);
            }
        }
        return deleted;
    }
}
