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
import java.util.function.Predicate;

/**
 * Shared machinery for {@link FilterDirectoryReader}s that overlay a document-id predicate onto a
 * reader's live docs, restricting each leaf's visible documents to just those whose stored {@code
 * _id} field matches the predicate. Both {@link InPlaceSplitFilteringDirectoryReader} and {@link
 * PartitionFilteringDirectoryReader} need exactly this shape -- recompute one leaf's live-doc {@link
 * Bits} by re-reading every live document's stored {@code _id} once, at wrap time, and combine that
 * with the predicate -- differing only in what the predicate itself is. Everything predicate-agnostic
 * (the {@code SubReaderWrapper}, the {@code FilterLeafReader}, and the stored-field visitor used to
 * recover each document's id) lives here once; subclasses supply only the predicate and their own
 * {@code doWrapDirectoryReader}/{@code getReaderCacheHelper} behavior, which differ per subclass (see
 * each subclass's own javadoc for why).
 */
abstract class AbstractIdFilteringDirectoryReader extends FilterDirectoryReader {

    /**
     * Wraps a reader so every leaf's live docs are additionally restricted to ids matching {@code idPredicate}.
     *
     * @param in the reader to wrap.
     * @param idPredicate returns {@code true} for a document's {@code _id} iff it should remain visible.
     */
    protected AbstractIdFilteringDirectoryReader(DirectoryReader in, Predicate<String> idPredicate) throws IOException {
        super(in, new IdFilteringSubReaderWrapper(idPredicate));
    }

    private static final class IdFilteringSubReaderWrapper extends SubReaderWrapper {
        private final Predicate<String> idPredicate;

        IdFilteringSubReaderWrapper(Predicate<String> idPredicate) {
            this.idPredicate = idPredicate;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            try {
                return IdFilteringLeafReader.wrap(reader, idPredicate);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** The per-leaf half: computes and overlays one segment's id-predicate-membership {@link Bits}. */
    static final class IdFilteringLeafReader extends FilterLeafReader {

        private final LeafReader wrapped;
        private final FixedBitSet liveDocs;
        private final int numDocs;

        private IdFilteringLeafReader(LeafReader reader, FixedBitSet liveDocs, int numDocs) {
            super(reader);
            this.wrapped = reader;
            this.liveDocs = liveDocs;
            this.numDocs = numDocs;
        }

        static LeafReader wrap(LeafReader reader, Predicate<String> idPredicate) throws IOException {
            Bits existingLiveDocs = reader.getLiveDocs();
            FixedBitSet bits = new FixedBitSet(reader.maxDoc());
            int liveCount = 0;
            IdFieldVisitor visitor = new IdFieldVisitor();
            for (int docId = 0; docId < reader.maxDoc(); docId++) {
                if (existingLiveDocs != null && existingLiveDocs.get(docId) == false) {
                    continue; // already hard-deleted -- never a candidate for this predicate either
                }
                visitor.reset();
                reader.storedFields().document(docId, visitor);
                String id = visitor.id();
                if (id != null && idPredicate.test(id)) {
                    bits.set(docId);
                    liveCount++;
                }
                // id == null excluded rather than included on the unexpected/defensive path, the
                // safe direction: it can only under- rather than over-include a matching document set.
            }
            return new IdFilteringLeafReader(reader, bits, liveCount);
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
