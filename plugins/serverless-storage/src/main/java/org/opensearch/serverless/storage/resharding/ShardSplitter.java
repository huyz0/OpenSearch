/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;

import java.io.IOException;

/**
 * The control-plane half of resharding-by-copy (rfc-serverless-opensearch.md &sect;16 Phase 5):
 * splits a source shard's current published manifest into {@code numPartitions} target shard
 * identities, each a {@link ShardCloner#clone zero-copy clone} of the exact same source generation
 * plus a {@link ShardPartitionDescriptor} recording which partition it serves.
 *
 * <p><b>Deliberately just {@link ShardCloner#clone} plus one extra write, not a new mechanism</b>:
 * a split target and a plain clone are byte-for-byte the same "new shard identity referencing an
 * existing shard's bundles without copying them" primitive -- {@link ShardCloner#clone} already
 * has the source-generation pin correctness (formally verified, &sect;18.5's {@code CloneGc.tla})
 * that a split target needs exactly as much as a clone does, since both leave the source shard's
 * bundles referenced by a brand-new manifest that {@code GcSchedulerTask}'s own per-shard sweep
 * knows nothing about without a pin. The only thing a split target needs beyond a plain clone is
 * knowing <em>which slice</em> of the shared document space it's responsible for -- that's exactly
 * {@link ShardPartitionDescriptor}, and {@link PartitionFilteringDirectoryReader} is the only other
 * piece of new machinery this feature needed.
 *
 * <p><b>"logical-first, physical-later," and what's out of scope here</b>: every split target's
 * directory holds the pre-split shard's <em>entire</em> document set, not just its own partition --
 * {@link PartitionFilteringDirectoryReader} is what makes each target's reads still return only its
 * own slice. Physically rewriting each target's bundles down to just its own partition's documents
 * (dropping the doc-routing filter and its per-refresh cost once done) is real follow-up work this
 * class does not attempt -- see this package's own {@code package-info.java}.
 */
public final class ShardSplitter {

    private ShardSplitter() {}

    /**
     * Splits a source shard's current published manifest into one target shard identity as one
     * partition of {@code numPartitions}. Called once per target to split a source {@code
     * numPartitions} ways -- the same "one call per target" shape {@link ShardCloner#clone} already
     * has, rather than inventing a new batched multi-target request shape.
     *
     * @param sourceIndexUuid the index being split from; must already have a published manifest.
     * @param sourceShardId the shard number within {@code sourceIndexUuid}.
     * @param sourceManifestStore where the source shard's manifests live.
     * @param sourceShardStateStore where the source shard's head lives.
     * @param sourcePinRegistry the source shard's durable pin registry, protected via this call.
     * @param targetIndexUuid the brand-new index this split target creates; must not already have
     *                        a published head, same refusal {@link ShardCloner#clone} already gives.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     * @param targetManifestStore where the target shard's new manifest is written.
     * @param targetShardStateStore where the target shard's new head is published.
     * @param targetLineageStore where the target shard's clone lineage is recorded -- reused
     *                           unchanged from {@link ShardCloner}, since a split target's GC-safety
     *                           story (the source-generation pin, releasable via {@link
     *                           ShardCloner#deleteClone}) is identical to a plain clone's.
     * @param targetPartitionStore where the target shard's {@link ShardPartitionDescriptor} is written.
     * @param partitionIndex which of {@code numPartitions} partitions this target serves, in {@code [0, numPartitions)}.
     * @param numPartitions how many partitions the source shard's document space is being split into; must be {@code >= 2}.
     * @param nowMillis the target manifest's {@code createdAtMillis} -- passed in rather than read
     *                  internally so this class stays trivially deterministic to test.
     * @throws IOException under the same conditions {@link ShardCloner#clone} throws under, or if
     *                      {@code partitionIndex}/{@code numPartitions} form an invalid {@link ShardPartitionDescriptor}.
     */
    public static void split(
        String sourceIndexUuid,
        int sourceShardId,
        BlobContainerManifestStore sourceManifestStore,
        ShardStateStore sourceShardStateStore,
        DurablePinRegistry sourcePinRegistry,
        String targetIndexUuid,
        int targetShardId,
        BlobContainerManifestStore targetManifestStore,
        ShardStateStore targetShardStateStore,
        BlobContainerCloneLineageStore targetLineageStore,
        BlobContainerShardPartitionStore targetPartitionStore,
        int partitionIndex,
        int numPartitions,
        long nowMillis
    ) throws IOException {
        // Validate before any durable write -- a bad (partitionIndex, numPartitions) pair must
        // never leave a pin on the source or a half-written target behind, the same "fail before
        // touching anything" contract ShardCloner's own target-already-active check gives.
        ShardPartitionDescriptor descriptor = new ShardPartitionDescriptor(partitionIndex, numPartitions);

        ShardCloner.clone(
            sourceIndexUuid,
            sourceShardId,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            targetIndexUuid,
            targetShardId,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            nowMillis
        );
        targetPartitionStore.writeDescriptor(descriptor);
    }
}
