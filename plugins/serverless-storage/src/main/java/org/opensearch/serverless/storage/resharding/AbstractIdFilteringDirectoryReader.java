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
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.BiPredicate;

/**
 * Shared machinery for {@link FilterDirectoryReader}s that overlay a per-document predicate onto a
 * reader's live docs, restricting each leaf's visible documents to just those whose stored {@code
 * _id} (and, where relevant, {@code _routing}) match the predicate. Both {@link
 * InPlaceSplitFilteringDirectoryReader} and {@link PartitionFilteringDirectoryReader} need exactly
 * this shape -- recompute one leaf's live-doc {@link Bits} by re-reading every live document's stored
 * {@code _id}/{@code _routing} once, at wrap time, and combine that with the predicate -- differing
 * only in what the predicate itself is. Everything predicate-agnostic (the {@code SubReaderWrapper},
 * the {@code FilterLeafReader}, and the stored-field visitor used to recover each document's id and
 * routing) lives here once; subclasses supply only the predicate and their own {@code
 * doWrapDirectoryReader}/{@code getReaderCacheHelper} behavior, which differ per subclass (see each
 * subclass's own javadoc for why).
 *
 * <p>The predicate receives both {@code _id} and the document's stored {@code _routing} ({@code null}
 * when the document was indexed without a custom routing value), so an in-place split child can
 * reproduce core's real routing hash -- {@code effectiveRouting = routing != null ? routing : id} --
 * exactly as {@link org.opensearch.cluster.routing.OperationRouting#generateShardId} does. A
 * subclass that keys only off {@code _id} simply ignores the routing argument.
 */
abstract class AbstractIdFilteringDirectoryReader extends FilterDirectoryReader {

    /**
     * Wraps a reader so every leaf's live docs are additionally restricted to documents matching
     * {@code idRoutingPredicate}.
     *
     * @param in the reader to wrap.
     * @param idRoutingPredicate given a document's {@code _id} and its stored {@code _routing}
     *        ({@code null} if none), returns {@code true} iff the document should remain visible.
     */
    protected AbstractIdFilteringDirectoryReader(DirectoryReader in, BiPredicate<String, String> idRoutingPredicate)
        throws IOException {
        super(in, new IdFilteringSubReaderWrapper(idRoutingPredicate));
    }

    private static final class IdFilteringSubReaderWrapper extends SubReaderWrapper {
        private final BiPredicate<String, String> idRoutingPredicate;

        IdFilteringSubReaderWrapper(BiPredicate<String, String> idRoutingPredicate) {
            this.idRoutingPredicate = idRoutingPredicate;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            try {
                return IdFilteringLeafReader.wrap(reader, idRoutingPredicate);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** The per-leaf half: computes and overlays one segment's predicate-membership {@link Bits}. */
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

        static LeafReader wrap(LeafReader reader, BiPredicate<String, String> idRoutingPredicate) throws IOException {
            Bits existingLiveDocs = reader.getLiveDocs();
            FixedBitSet bits = new FixedBitSet(reader.maxDoc());
            int liveCount = 0;
            IdRoutingFieldVisitor visitor = new IdRoutingFieldVisitor();
            for (int docId = 0; docId < reader.maxDoc(); docId++) {
                if (existingLiveDocs != null && existingLiveDocs.get(docId) == false) {
                    continue; // already hard-deleted -- never a candidate for this predicate either
                }
                visitor.reset();
                reader.storedFields().document(docId, visitor);
                String id = visitor.id();
                if (id != null && idRoutingPredicate.test(id, visitor.routing())) {
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

    /**
     * Reads the stored {@code _id} (decoded back to its original string) and {@code _routing} fields.
     * {@code _routing} is only stored for documents indexed with a custom routing value (see {@link
     * RoutingFieldMapper}), so {@link #routing()} stays {@code null} for the common no-routing case --
     * exactly what {@code effectiveRouting = routing != null ? routing : id} then falls back to. This
     * mirrors core's own {@code ShardSplittingQuery.Visitor}: {@code _id} arrives via {@code
     * binaryField}, {@code _routing} via {@code stringField}.
     */
    private static final class IdRoutingFieldVisitor extends StoredFieldVisitor {
        private String id;
        private String routing;

        void reset() {
            id = null;
            routing = null;
        }

        String id() {
            return id;
        }

        String routing() {
            return routing;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            // Stop early once both fields are in hand; a document may legitimately have no _routing.
            if (id != null && routing != null) {
                return Status.STOP;
            }
            if (IdFieldMapper.NAME.equals(fieldInfo.name) || RoutingFieldMapper.NAME.equals(fieldInfo.name)) {
                return Status.YES;
            }
            return Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
                id = Uid.decodeId(value);
            }
        }

        @Override
        public void stringField(FieldInfo fieldInfo, String value) {
            if (RoutingFieldMapper.NAME.equals(fieldInfo.name)) {
                routing = value;
            }
        }
    }
}
