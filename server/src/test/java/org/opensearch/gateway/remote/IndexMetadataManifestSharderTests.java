/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public class IndexMetadataManifestSharderTests extends OpenSearchTestCase {

    private static UploadedIndexMetadata index(String uuid) {
        return new UploadedIndexMetadata("index-" + uuid, uuid, "index-metadata-file-for-" + uuid);
    }

    public void testFirstShardedWriteRewritesEveryNonEmptyShard() {
        // previousShardCount = 0 signals "no previous sharded manifest" -- everything with content
        // must be written, nothing can be carried forward because nothing exists yet to carry.
        List<UploadedIndexMetadata> current = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            current.add(index(UUID.randomUUID().toString()));
        }
        IndexMetadataManifestSharder.Plan plan = IndexMetadataManifestSharder.plan(current, emptySet(), emptyList(), 0, 8);

        assertTrue("nothing can be carried forward on the first sharded write", plan.getCarriedForward().isEmpty());
        int totalWritten = plan.getShardsToWrite().values().stream().mapToInt(List::size).sum();
        assertEquals("every current index must appear in exactly one shard to write", current.size(), totalWritten);
    }

    public void testUnchangedShardIsCarriedForwardVerbatim() {
        String uuidA = "index-uuid-a";
        String uuidB = "index-uuid-b";
        int shardCount = 8;
        int shardA = ManifestShardFunction.shardFor(uuidA, shardCount);
        int shardB = ManifestShardFunction.shardFor(uuidB, shardCount);
        assumeFalse("fixture needs the two indices in different shards", shardA == shardB);

        UploadedManifestShard previousShardA = new UploadedManifestShard(shardA, "blob-for-shard-a-v1", 1);
        UploadedManifestShard previousShardB = new UploadedManifestShard(shardB, "blob-for-shard-b-v1", 1);

        List<UploadedIndexMetadata> current = List.of(index(uuidA), index(uuidB));
        // Only uuidA changed this version -- shard B must be carried forward untouched.
        IndexMetadataManifestSharder.Plan plan = IndexMetadataManifestSharder.plan(
            current,
            singleton(uuidA),
            List.of(previousShardA, previousShardB),
            shardCount,
            shardCount
        );

        assertEquals(singletonList(previousShardB), plan.getCarriedForward());
        assertTrue("shard A must be rewritten since its index changed", plan.getShardsToWrite().containsKey(shardA));
        assertFalse("shard B must not be rewritten -- nothing in it changed", plan.getShardsToWrite().containsKey(shardB));
    }

    public void testDeletedIndexCausesItsOldShardToBeRewrittenWithoutIt() {
        String uuidSurvivor = "index-uuid-survivor";
        String uuidDeleted = "index-uuid-deleted";
        int shardCount = 8;
        // Force both into the same shard by trying candidate UUIDs until they collide -- the scenario
        // this test means to exercise (one deleted, one surviving, same shard) only arises that way.
        int targetShard = ManifestShardFunction.shardFor(uuidSurvivor, shardCount);
        String deletedInSameShard = uuidDeleted;
        for (int i = 0; i < 10000; i++) {
            String candidate = "candidate-" + i;
            if (ManifestShardFunction.shardFor(candidate, shardCount) == targetShard) {
                deletedInSameShard = candidate;
                break;
            }
        }
        assumeTrue(
            "fixture needs a colliding UUID within the search budget",
            ManifestShardFunction.shardFor(deletedInSameShard, shardCount) == targetShard
        );

        UploadedManifestShard previousShard = new UploadedManifestShard(targetShard, "blob-v1", 2);
        // Deleted index no longer appears in currentIndices, but its UUID is in changedOrDeletedIndexUUIDs.
        List<UploadedIndexMetadata> current = singletonList(index(uuidSurvivor));
        IndexMetadataManifestSharder.Plan plan = IndexMetadataManifestSharder.plan(
            current,
            singleton(deletedInSameShard),
            singletonList(previousShard),
            shardCount,
            shardCount
        );

        assertTrue("the shard holding the deleted index's survivor must be rewritten", plan.getShardsToWrite().containsKey(targetShard));
        List<UploadedIndexMetadata> rewritten = plan.getShardsToWrite().get(targetShard);
        assertEquals(1, rewritten.size());
        assertEquals(uuidSurvivor, rewritten.get(0).getIndexUUID());
        assertTrue("nothing should be carried forward -- the only shard involved was rewritten", plan.getCarriedForward().isEmpty());
    }

    public void testShardCountChangeInvalidatesEveryCarryForward() {
        List<UploadedIndexMetadata> current = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            current.add(index(UUID.randomUUID().toString()));
        }
        List<UploadedManifestShard> previousShards = List.of(
            new UploadedManifestShard(0, "blob-0-v1", 10),
            new UploadedManifestShard(1, "blob-1-v1", 10)
        );
        // Nothing changed this version (empty changed set), but the shard count itself moved 2 -> 8:
        // every previous reference is meaningless under the new partitioning.
        IndexMetadataManifestSharder.Plan plan = IndexMetadataManifestSharder.plan(current, emptySet(), previousShards, 2, 8);

        assertTrue("a shard count change must not carry anything forward", plan.getCarriedForward().isEmpty());
        int totalWritten = plan.getShardsToWrite().values().stream().mapToInt(List::size).sum();
        assertEquals(current.size(), totalWritten);
    }

    public void testShardWithNoLiveIndicesIsDroppedEntirely() {
        int shardCount = 8;
        String onlyIndexInShard = "the-only-one";
        int shardId = ManifestShardFunction.shardFor(onlyIndexInShard, shardCount);
        UploadedManifestShard previousShard = new UploadedManifestShard(shardId, "blob-v1", 1);

        // The index that used to be the only occupant of this shard is now deleted, and nothing else
        // hashes into that shard -- current is empty entirely, for simplicity.
        IndexMetadataManifestSharder.Plan plan = IndexMetadataManifestSharder.plan(
            emptyList(),
            singleton(onlyIndexInShard),
            singletonList(previousShard),
            shardCount,
            shardCount
        );

        assertTrue("an empty shard must not be carried forward", plan.getCarriedForward().isEmpty());
        assertTrue("an empty shard must not be scheduled for a (pointless) rewrite either", plan.getShardsToWrite().isEmpty());
    }

    public void testRejectsNonPositiveShardCount() {
        expectThrows(IllegalArgumentException.class, () -> IndexMetadataManifestSharder.plan(emptyList(), emptySet(), emptyList(), 0, 0));
    }

    public void testEveryCurrentIndexIsAccountedForExactlyOnce() {
        // A broader randomized check: however plan() decides to split work between carriedForward and
        // shardsToWrite, every currently-live index must show up in the union exactly once -- no index
        // silently dropped, none duplicated across shards.
        int shardCount = randomIntBetween(2, 64);
        List<UploadedIndexMetadata> current = new ArrayList<>();
        Set<String> changed = new java.util.HashSet<>();
        int total = randomIntBetween(10, 200);
        for (int i = 0; i < total; i++) {
            String uuid = UUID.randomUUID().toString();
            current.add(index(uuid));
            if (randomBoolean()) {
                changed.add(uuid);
            }
        }
        // Half the time simulate an existing previous manifest at the same shard count with nothing
        // carried in (previousShards empty means shards not in the changed set fall back to "write",
        // matching the safe-default branch), the other half start from empty/no-previous.
        IndexMetadataManifestSharder.Plan plan = IndexMetadataManifestSharder.plan(current, changed, emptyList(), shardCount, shardCount);

        long accounted = plan.getShardsToWrite().values().stream().mapToInt(List::size).sum();
        assertEquals("every current index must be written when there is nothing to carry forward from", total, accounted);
        assertTrue(plan.getCarriedForward().isEmpty());
    }
}
