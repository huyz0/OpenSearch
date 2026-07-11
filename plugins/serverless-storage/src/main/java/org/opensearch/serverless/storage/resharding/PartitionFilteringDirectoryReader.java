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
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Overlays a {@link ShardPartitionDescriptor} onto a {@link DirectoryReader}'s live docs -- the
 * reader-side half of rfc-serverless-opensearch.md &sect;16 Phase 5's "resharding-by-copy" doc-routing
 * partition filter: until a split target's bundles are physically rewritten (never done by this
 * increment -- see {@link ShardSplitter}'s own javadoc), every document from the pre-split shard's
 * full manifest is still physically present in every split target's directory, so a split target
 * must filter its own reads down to just its assigned partition or every target would return every
 * document, not a disjoint slice of them.
 *
 * <p>Follows the exact same {@code FilterDirectoryReader}/{@code FilterLeafReader} shape Lucene's
 * own {@code SoftDeletesDirectoryReaderWrapper} uses to overlay soft-delete visibility onto live
 * docs -- computing a combined {@link Bits} (this reader's own live docs AND partition membership)
 * once per leaf, at wrap time, is the standard, correct way to change document visibility without
 * touching the underlying segment data. {@code doWrapDirectoryReader} means {@code
 * DirectoryReader#openIfChanged} on an already-wrapped reader automatically re-wraps the new
 * generation with this same filter -- {@code ReadOnlyEngine}'s own refresh path (via {@code
 * OpenSearchReaderManager}) needs no change at all for the filter to survive a refresh.
 *
 * <p><b>Cost, and why it's accepted</b>: computing each leaf's partition-membership bitset means
 * reading every live document's stored {@code _id} field once per reader open -- real, non-trivial
 * I/O the un-split read path doesn't pay. This is the RFC's own explicitly-accepted tradeoff for
 * staying "logical-first" rather than physically rewriting bundles at split time: a split target
 * is slower to open a fresh reader generation than an equivalent physically-rewritten shard would
 * be, but requires zero bundle-copy I/O at split time and zero change to any bundle-reading code
 * path.
 */
public final class PartitionFilteringDirectoryReader extends FilterDirectoryReader {

    private final ShardPartitionDescriptor descriptor;

    /**
     * Wraps a reader so every leaf's live docs are additionally restricted to {@code descriptor}'s partition.
     *
     * @param in the reader to wrap.
     * @param descriptor which partition (of how many) this reader should filter down to.
     */
    public PartitionFilteringDirectoryReader(DirectoryReader in, ShardPartitionDescriptor descriptor) throws IOException {
        super(in, new PartitionSubReaderWrapper(descriptor));
        this.descriptor = descriptor;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new PartitionFilteringDirectoryReader(in, descriptor);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // Visibility changes on every wrap (a fresh bitset is computed each time), so unlike the
        // wrapped reader's own core cache helper (still valid -- term/postings data hasn't
        // changed), no stable reader-level cache key exists here. Same choice
        // SoftDeletesDirectoryReaderWrapper documents for the identical reason.
        return null;
    }

    private static final class PartitionSubReaderWrapper extends SubReaderWrapper {
        private final ShardPartitionDescriptor descriptor;

        PartitionSubReaderWrapper(ShardPartitionDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            try {
                return PartitionFilteringLeafReader.wrap(reader, descriptor);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** The per-leaf half: computes and overlays one segment's partition-membership {@link Bits}. */
    static final class PartitionFilteringLeafReader extends FilterLeafReader {

        private final LeafReader wrapped;
        private final FixedBitSet liveDocs;
        private final int numDocs;

        private PartitionFilteringLeafReader(LeafReader reader, FixedBitSet liveDocs, int numDocs) {
            super(reader);
            this.wrapped = reader;
            this.liveDocs = liveDocs;
            this.numDocs = numDocs;
        }

        static LeafReader wrap(LeafReader reader, ShardPartitionDescriptor descriptor) throws IOException {
            Bits existingLiveDocs = reader.getLiveDocs();
            FixedBitSet bits = new FixedBitSet(reader.maxDoc());
            int liveCount = 0;
            IdFieldVisitor visitor = new IdFieldVisitor();
            for (int docId = 0; docId < reader.maxDoc(); docId++) {
                if (existingLiveDocs != null && existingLiveDocs.get(docId) == false) {
                    continue; // already hard-deleted -- never a candidate for this partition either
                }
                visitor.reset();
                reader.storedFields().document(docId, visitor);
                String id = visitor.id();
                if (id != null && RoutingPartitionFilter.matches(id, descriptor)) {
                    bits.set(docId);
                    liveCount++;
                }
                // id == null means this document has no stored _id (shouldn't happen for a real
                // OpenSearch-written segment, but excluding rather than including on the
                // unexpected/defensive path is the safe direction: it can only under- rather than
                // over-include a partition's documents.
            }
            return new PartitionFilteringLeafReader(reader, bits, liveCount);
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
