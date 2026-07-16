/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Overlays an in-place split child's {@link ShardRange} onto a {@link DirectoryReader}'s live docs
 * -- the reader-side counterpart of {@link PartitionFilteringDirectoryReader}, for core's own
 * unbounded-hash-range split (dynamic-partitioning-plan.md's Task 20/21) rather than this plugin's
 * pre-existing equal-partition resharding-by-copy mechanism.
 *
 * <p>{@code ShardCloner.clone} (dynamic-partitioning-progress.md's Task 12/19) attaches a child to
 * its parent's data by manifest reference, not by physically copying only the child's share of the
 * documents -- so, exactly like {@link PartitionFilteringDirectoryReader}'s own javadoc explains for
 * the sibling mechanism, every document from the parent's full manifest is still physically present
 * in every child's directory until a physical bundle rewrite happens (not done by this increment).
 * Without this filter, every child would return every document, not a disjoint hash-range slice of
 * them -- exactly the "80 hits but 40 expected" symptom this class exists to fix.
 *
 * <p>Follows the same {@code FilterDirectoryReader}/{@code FilterLeafReader} shape as {@link
 * PartitionFilteringDirectoryReader} (see that class's own javadoc for the design rationale, which
 * applies unchanged here).
 */
public final class InPlaceSplitFilteringDirectoryReader extends FilterDirectoryReader {

    private final ShardRange range;

    /**
     * Wraps a reader so every leaf's live docs are additionally restricted to {@code range}.
     *
     * @param in the reader to wrap.
     * @param range the split child's own hash range.
     */
    public InPlaceSplitFilteringDirectoryReader(DirectoryReader in, ShardRange range) throws IOException {
        super(in, new RangeSubReaderWrapper(range));
        this.range = range;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new InPlaceSplitFilteringDirectoryReader(in, range);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // Unlike PartitionFilteringDirectoryReader (wrapped internally by the reader engine's own
        // reference manager, outside core's per-request IndexShard#wrapSearcher path), this class is
        // wired via IndexModule#setReaderWrapper, whose contract (see IndexShard#wrapSearcher)
        // requires the wrapped reader's cache helper to equal the original's -- delegating is also
        // simply correct here: this filter's bitset is a deterministic, unchanging (for this child's
        // whole lifetime) function of the wrapped reader's own live docs, so two lookups against the
        // same underlying reader generation always see the same filtered result.
        return in.getReaderCacheHelper();
    }

    private static final class RangeSubReaderWrapper extends SubReaderWrapper {
        private final ShardRange range;

        RangeSubReaderWrapper(ShardRange range) {
            this.range = range;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            try {
                return RangeFilteringLeafReader.wrap(reader, range);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** The per-leaf half: computes and overlays one segment's hash-range-membership {@link Bits}. */
    static final class RangeFilteringLeafReader extends FilterLeafReader {

        private final LeafReader wrapped;
        private final FixedBitSet liveDocs;
        private final int numDocs;

        private RangeFilteringLeafReader(LeafReader reader, FixedBitSet liveDocs, int numDocs) {
            super(reader);
            this.wrapped = reader;
            this.liveDocs = liveDocs;
            this.numDocs = numDocs;
        }

        static LeafReader wrap(LeafReader reader, ShardRange range) throws IOException {
            Bits existingLiveDocs = reader.getLiveDocs();
            FixedBitSet bits = new FixedBitSet(reader.maxDoc());
            int liveCount = 0;
            IdFieldVisitor visitor = new IdFieldVisitor();
            for (int docId = 0; docId < reader.maxDoc(); docId++) {
                if (existingLiveDocs != null && existingLiveDocs.get(docId) == false) {
                    continue; // already hard-deleted -- never a candidate for this range either
                }
                visitor.reset();
                reader.storedFields().document(docId, visitor);
                String id = visitor.id();
                if (id != null && InPlaceSplitPartitionFilter.matches(id, range)) {
                    bits.set(docId);
                    liveCount++;
                }
                // id == null excluded rather than included on the unexpected/defensive path, the
                // safe direction: it can only under- rather than over-include a child's documents.
            }
            return new RangeFilteringLeafReader(reader, bits, liveCount);
        }

        @Override
        public Bits getLiveDocs() {
            return liveDocs;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return wrapped.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }

    /** Reads only the stored {@code _id} field, decoding it back to its original string form. */
    private static final class IdFieldVisitor extends StoredFieldVisitor {
        private String id;

        void reset() {
            id = null;
        }

        String id() {
            return id;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            if (id != null) {
                return Status.STOP;
            }
            return IdFieldMapper.NAME.equals(fieldInfo.name) ? Status.YES : Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            id = Uid.decodeId(value);
        }
    }
}
