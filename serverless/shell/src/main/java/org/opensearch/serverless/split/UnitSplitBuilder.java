/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.split;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.routing.UnitLayout;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.PruningDigest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Builds the two halves of a planned split from the old unit's last published commit, and publishes each
 * under its new unit id.
 *
 * <p>The old unit is read from the object store into a scratch directory, never through its owner, so
 * writes and searches on it carry on while this runs. Each half is a fresh Lucene index filled by
 * {@code addIndexes} over the old segments, each seen through a {@link HalfFilter}: postings and doc values
 * are copied, not rebuilt from source, which is what makes a split cheaper than a reindex. The halves keep
 * the old commit's user data (sequence numbers, history uuid), so a writer that later opens one sees the
 * same checkpoints the old unit had and replays the write log from there.
 *
 * <p><b>What this does not do.</b> It neither moves routing nor fences the old unit, and it does not apply
 * writes the old unit took after its last publish. The halves are published but unreferenced: a plan that
 * is dropped leaves them to garbage collection, and one that goes ahead catches them up and swaps the
 * descriptor separately. A caller that wants a complete split today must quiesce the old unit first, by
 * publishing it with no writes in flight.
 */
public final class UnitSplitBuilder {

    /**
     * The term the halves are first published at: the lowest a shard head can hold, so the first writer to
     * take a half publishes over this commit rather than being refused as older than it. Recorded with no
     * writer id, so that writer is not mistaken for a second node at the same term.
     */
    static final long BUILD_TERM = 1L;

    private UnitSplitBuilder() {}

    /**
     * What a build produced.
     *
     * @param split the plan it built
     * @param source the old unit's commit it built from
     * @param left the lower half's published commit
     * @param right the upper half's published commit
     * @param leftDocs documents the lower half holds
     * @param rightDocs documents the upper half holds
     * @param checkpoint the local checkpoint of the old unit's commit, which both halves' commits carry: a record past it
     *     is not in either half yet
     */
    public record Built(
        UnitLayout.Split split,
        CommitManifest source,
        CommitManifest left,
        CommitManifest right,
        long leftDocs,
        long rightDocs,
        long checkpoint
    ) {
    }

    /**
     * Builds and publishes both halves of a split.
     *
     * @param plane the metadata plane, for the units' published commits
     * @param descriptor the index, at the layout the split was planned from
     * @param split the plan, from {@code descriptor.layout().planSplit}
     * @param scratch an empty local directory this build may use and the caller deletes afterwards
     * @return what was built
     * @throws IOException if the old unit cannot be read or a half cannot be written or published
     * @throws IllegalStateException if the old unit has published nothing
     */
    public static Built build(MetadataPlane plane, IndexDescriptor descriptor, UnitLayout.Split split, Path scratch) throws IOException {
        if (descriptor.layout() == null || descriptor.layout().units().contains(split.retired()) == false) {
            throw new IllegalArgumentException("the split's " + split.retired() + " is not a unit of [" + descriptor.name() + "]");
        }
        final IndexDescriptor after = descriptor.withLayout(descriptor.layout().apply(split));
        final int oldId = split.retired().id();
        try (Directory sourceDir = FSDirectory.open(Files.createDirectories(scratch.resolve("unit-" + oldId)))) {
            final SegmentPublisher source = plane.segmentPublisher(descriptor.name(), descriptor.uuid(), oldId);
            final Optional<CommitManifest> commit = source.restoreInto(sourceDir, shardId(descriptor, oldId));
            if (commit.isEmpty()) {
                throw new IllegalStateException("unit " + oldId + " of [" + descriptor.name() + "] has published nothing to split");
            }
            try (DirectoryReader reader = DirectoryReader.open(sourceDir)) {
                final Map<String, String> userData = reader.getIndexCommit().getUserData();
                final Half left = buildHalf(plane, after, reader, userData, split.left(), scratch);
                final Half right = buildHalf(plane, after, reader, userData, split.right(), scratch);
                final String checkpoint = userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY);
                return new Built(
                    split,
                    commit.get(),
                    left.published,
                    right.published,
                    left.docs,
                    right.docs,
                    checkpoint == null ? SequenceNumbers.NO_OPS_PERFORMED : Long.parseLong(checkpoint)
                );
            }
        }
    }

    private record Half(CommitManifest published, long docs) {
    }

    private static Half buildHalf(
        MetadataPlane plane,
        IndexDescriptor after,
        DirectoryReader source,
        Map<String, String> userData,
        UnitLayout.Unit half,
        Path scratch
    ) throws IOException {
        final List<CodecReader> filtered = new ArrayList<>();
        long docs = 0;
        Sort sort = null;
        boolean blocks = false;
        for (LeafReaderContext leaf : source.leaves()) {
            final CodecReader segment = (CodecReader) leaf.reader();
            if (sort == null) {
                sort = segment.getMetaData().sort();
            }
            blocks |= segment.getFieldInfos().fieldInfo(Lucene.PARENT_FIELD) != null;
            final HalfFilter kept = HalfFilter.of(segment, half);
            docs += kept.kept();
            filtered.add(kept);
        }
        final IndexWriterConfig config = new IndexWriterConfig(Lucene.STANDARD_ANALYZER).setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setCommitOnClose(false);
        if (sort != null) {
            config.setIndexSort(sort);
        }
        if (blocks) {
            config.setParentField(Lucene.PARENT_FIELD);
        }
        final ShardId shardId = shardId(after, half.id());
        final Directory dir = FSDirectory.open(Files.createDirectories(scratch.resolve("unit-" + half.id())));
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            writer.addIndexes(filtered.toArray(new CodecReader[0]));
            writer.setLiveCommitData(userData.entrySet());
            writer.commit();
        }
        final IndexSettings settings = new IndexSettings(after.toIndexMetadata(Map.of()), Settings.EMPTY);
        final Store store = new Store(shardId, settings, dir, new ShardLock(shardId) {
            @Override
            protected void closeInternal() {}
        });
        try {
            // Widened before the publish, as a writer does: a half with no digest would be ruled out of
            // searches as empty. An empty digest rules nothing out until the half's first writer publishes
            // one of its own.
            plane.rollups().widen(after.name(), after.uuid(), after.numberOfShards(), half.id(), PruningDigest.EMPTY);
            final CommitManifest published = plane.segmentPublisher(after.name(), after.uuid(), half.id()).publish(store, BUILD_TERM, null);
            return new Half(published, docs);
        } finally {
            store.close();
        }
    }

    private static ShardId shardId(IndexDescriptor descriptor, int unit) {
        return new ShardId(descriptor.name(), descriptor.uuid(), unit);
    }
}
