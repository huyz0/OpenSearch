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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;

/**
 * Shared machinery for {@link FilterDirectoryReader}s that overlay a per-document predicate onto a
 * reader's live docs, restricting each leaf's visible documents to just those whose stored {@code
 * _id} (and, where relevant, {@code _routing}) match the predicate. Both {@link
 * InPlaceSplitFilteringDirectoryReader} and {@link PartitionFilteringDirectoryReader} need exactly
 * this shape -- recompute one leaf's live-doc {@link Bits} from every document's stored {@code
 * _id}/{@code _routing} and combine that with the predicate -- differing only in what the predicate
 * itself is. Everything predicate-agnostic (the {@code SubReaderWrapper}, the {@code
 * FilterLeafReader}, the stored-field visitor used to recover each document's id and routing, the
 * nested-block handling and the per-segment membership cache) lives here once; subclasses supply
 * only the predicate, a value identifying it, and their own {@code doWrapDirectoryReader}/{@code
 * getReaderCacheHelper} behavior, which differ per subclass (see each subclass's own javadoc).
 *
 * <p>The predicate receives both {@code _id} and the document's stored {@code _routing} ({@code null}
 * when the document was indexed without a custom routing value), so an in-place split child can
 * reproduce core's real routing hash -- {@code effectiveRouting = routing != null ? routing : id} --
 * exactly as {@link org.opensearch.cluster.routing.OperationRouting#generateShardId} does. A
 * subclass that keys only off {@code _id} simply ignores the routing argument.
 *
 * <p><b>Why the membership bitset is cached per segment core (finding R-1).</b> {@link
 * InPlaceSplitFilteringDirectoryReader} is installed through {@code IndexModule#setReaderWrapper},
 * and core applies a reader wrapper inside {@code IndexShard#wrapSearcher}, which runs on <em>every
 * searcher acquisition</em> -- i.e. once per search request per shard, not once per reader
 * generation. Computing the bitset in the wrap path therefore used to mean an O(maxDoc)
 * stored-fields scan per query: on a reader shard whose directory is object-store backed, that
 * faults the whole {@code .fdt}/{@code .fdx} in from the object store for every single query. The
 * sibling {@link PartitionFilteringDirectoryReader} does not have that problem because the reader
 * engine's own reference manager wraps it once per refresh -- so this class now amortises the scan
 * itself, which fixes the wrapper-seam mechanism regardless of which seam a subclass is wired into.
 *
 * <p>The cached artefact is deliberately the <em>predicate-membership</em> bitset, which is a pure
 * function of the segment's immutable core (its stored fields and its doc-values) and of the
 * predicate -- it deliberately does <em>not</em> fold in live docs, which change from one reader
 * generation to the next as deletes land. Live docs are ANDed in on every wrap, which is a handful
 * of in-memory bit operations and no I/O at all. The cache is therefore keyed on the wrapped leaf's
 * {@code getCoreCacheHelper()} key -- the key Lucene itself defines as "valid for as long as this
 * segment's core data is unchanged" -- paired with a subclass-supplied {@code filterKey} so two
 * different filters over the same segment cannot collide. Entries are dropped by a core
 * {@code ClosedListener}, so the cache cannot outlive the segments it describes.
 *
 * <p><b>Why nested child documents are handled explicitly (finding R-8).</b> A nested child document
 * has its {@code _id} <em>indexed but not stored</em> ({@code IdFieldMapper.NESTED_FIELD_TYPE} calls
 * {@code setStored(false)}), so the stored-field visitor recovers {@code null} for it. Excluding
 * every {@code null}-id document -- which is what this class used to do, calling it "the safe
 * direction" -- silently cleared every nested child from live docs on every split shard: {@code
 * nested}/{@code ToParentBlockJoinQuery} queries returned zero hits, {@code numDocs()} under-reported,
 * and, far worse, {@link InPlaceSiblingMerger} wraps each child in this same filter before {@code
 * addIndexes}, so an in-place merge would have <em>physically and permanently</em> dropped them.
 * There is nothing safe about a direction that deletes data. The fix is core's own: identify parent
 * (root) documents, evaluate the predicate only on those, and propagate each parent's verdict across
 * its whole block -- exactly what {@code ShardSplittingQuery.markChildDocs} does. Parents are
 * identified the same way {@code Queries.newNonNestedFilter()} identifies them, by the presence of
 * {@code _primary_term} doc values, read here directly off the leaf rather than through an
 * {@code IndexSearcher} because we are already walking the segment document by document.
 */
abstract class AbstractIdFilteringDirectoryReader extends FilterDirectoryReader {

    /**
     * Per-segment-core membership bitsets, keyed first on the leaf's core cache key and then on the
     * subclass's filter identity. The outer entry is removed by a {@code ClosedListener} registered
     * on the same core cache helper, so the cache tracks live segments exactly and never pins a
     * closed one.
     */
    private static final Map<IndexReader.CacheKey, Map<Object, FixedBitSet>> MEMBERSHIP_CACHE = new ConcurrentHashMap<>();

    /**
     * How many times the O(maxDoc) stored-fields scan has actually run. Test-visible only: it is the
     * one observable that distinguishes "the bitset was cached" from "the bitset was recomputed",
     * which is the whole of finding R-1.
     */
    static final AtomicLong MEMBERSHIP_COMPUTATIONS = new AtomicLong();

    /**
     * Wraps a reader so every leaf's live docs are additionally restricted to documents matching
     * {@code idRoutingPredicate}.
     *
     * @param in the reader to wrap.
     * @param filterKey a value identifying this filter, used as the second half of the membership
     *        cache key. It must implement {@code equals}/{@code hashCode} by value and must be equal
     *        across two wraps that would compute the same bitset -- both current subclasses pass a
     *        {@code record}, which satisfies that by construction.
     * @param idRoutingPredicate given a document's {@code _id} and its stored {@code _routing}
     *        ({@code null} if none), returns {@code true} iff the document should remain visible.
     */
    protected AbstractIdFilteringDirectoryReader(DirectoryReader in, Object filterKey, BiPredicate<String, String> idRoutingPredicate)
        throws IOException {
        super(in, new IdFilteringSubReaderWrapper(Objects.requireNonNull(filterKey, "filterKey"), idRoutingPredicate));
    }

    private static final class IdFilteringSubReaderWrapper extends SubReaderWrapper {
        private final Object filterKey;
        private final BiPredicate<String, String> idRoutingPredicate;

        IdFilteringSubReaderWrapper(Object filterKey, BiPredicate<String, String> idRoutingPredicate) {
            this.filterKey = filterKey;
            this.idRoutingPredicate = idRoutingPredicate;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            try {
                return IdFilteringLeafReader.wrap(reader, filterKey, idRoutingPredicate);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** The per-leaf half: overlays one segment's predicate-membership {@link Bits} onto its live docs. */
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

        static LeafReader wrap(LeafReader reader, Object filterKey, BiPredicate<String, String> idRoutingPredicate) throws IOException {
            FixedBitSet membership = membershipFor(reader, filterKey, idRoutingPredicate);
            // Always hand the leaf reader its own copy: `membership` may be the cached instance, and
            // a FixedBitSet handed out as `Bits` must never be mutated by whoever received it.
            FixedBitSet visible = membership.clone();
            Bits existingLiveDocs = reader.getLiveDocs();
            if (existingLiveDocs != null) {
                // A hard-deleted document is never visible, whatever the predicate says about it.
                // This is the only part of the computation that depends on the reader generation,
                // and it is pure in-memory bit work -- no stored-field I/O.
                for (int docId = nextSetBit(visible, 0); docId != DocIdSetIterator.NO_MORE_DOCS; docId = nextSetBit(visible, docId + 1)) {
                    if (existingLiveDocs.get(docId) == false) {
                        visible.clear(docId);
                    }
                }
            }
            return new IdFilteringLeafReader(reader, visible, visible.cardinality());
        }

        /** {@code FixedBitSet#nextSetBit} throws past the end of the set; this saturates instead. */
        private static int nextSetBit(FixedBitSet bits, int from) {
            return from >= bits.length() ? DocIdSetIterator.NO_MORE_DOCS : bits.nextSetBit(from);
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
     * The cached, live-docs-independent half of the computation: which documents of this segment's
     * core the predicate admits. Falls back to computing without caching when the leaf exposes no
     * core cache helper (a synthetic or already-filtered reader), which is correct but not amortised.
     */
    private static FixedBitSet membershipFor(LeafReader reader, Object filterKey, BiPredicate<String, String> idRoutingPredicate)
        throws IOException {
        IndexReader.CacheHelper coreCacheHelper = reader.getCoreCacheHelper();
        if (coreCacheHelper == null) {
            return computeMembership(reader, idRoutingPredicate);
        }
        IndexReader.CacheKey coreKey = coreCacheHelper.getKey();
        Map<Object, FixedBitSet> byFilter = MEMBERSHIP_CACHE.get(coreKey);
        if (byFilter == null) {
            Map<Object, FixedBitSet> created = new ConcurrentHashMap<>();
            byFilter = MEMBERSHIP_CACHE.putIfAbsent(coreKey, created);
            if (byFilter == null) {
                byFilter = created;
                // Registered exactly once per core key, by whichever thread won the putIfAbsent.
                coreCacheHelper.addClosedListener(MEMBERSHIP_CACHE::remove);
            }
        }
        FixedBitSet cached = byFilter.get(filterKey);
        if (cached != null) {
            return cached;
        }
        FixedBitSet computed = computeMembership(reader, idRoutingPredicate);
        FixedBitSet raced = byFilter.putIfAbsent(filterKey, computed);
        return raced != null ? raced : computed;
    }

    /**
     * The O(maxDoc) stored-fields scan itself. Deliberately ignores live docs (see the class
     * javadoc): the result describes the segment's core, and the caller ANDs the generation's live
     * docs in afterwards.
     */
    private static FixedBitSet computeMembership(LeafReader reader, BiPredicate<String, String> idRoutingPredicate) throws IOException {
        MEMBERSHIP_COMPUTATIONS.incrementAndGet();
        final int maxDoc = reader.maxDoc();
        FixedBitSet membership = new FixedBitSet(maxDoc);
        FixedBitSet parentDocs = parentDocs(reader);
        IdRoutingFieldVisitor visitor = new IdRoutingFieldVisitor();
        StoredFields storedFields = reader.storedFields();
        for (int docId = 0; docId < maxDoc; docId++) {
            if (parentDocs != null && parentDocs.get(docId) == false) {
                // A nested child document. It carries no stored _id of its own and it is not
                // independently addressable: its visibility is entirely its parent's, and is set
                // below when the parent is admitted. Skipping it here is what stops it being
                // silently dropped for having a null id (finding R-8).
                continue;
            }
            visitor.reset();
            storedFields.document(docId, visitor);
            String id = visitor.id();
            if (id == null || idRoutingPredicate.test(id, visitor.routing()) == false) {
                continue;
            }
            membership.set(docId);
            if (parentDocs != null) {
                // Lucene indexes a nested block as [child, child, ..., parent], so this parent's
                // children are exactly the documents between the previous parent and this one.
                // Same walk as ShardSplittingQuery#markChildDocs, which exists for this same reason.
                int previousParent = parentDocs.prevSetBit(Math.max(0, docId - 1));
                for (int child = previousParent + 1; child < docId; child++) {
                    membership.set(child);
                }
            }
        }
        return membership;
    }

    /**
     * The segment's root (non-nested) documents, or {@code null} when the segment demonstrably has
     * no nested children and the caller can take the cheaper every-document path.
     *
     * <p>Root documents are the ones carrying {@code _primary_term} doc values -- the same test
     * {@code Queries#newNonNestedFilter} uses. Two deliberate {@code null} returns:
     * <ul>
     *   <li>the field is absent entirely, which means this is not an OpenSearch-written segment (a
     *       hand-built test fixture, say); we have no way to identify blocks and no reason to think
     *       there are any, so every document is treated as a root, exactly as before this change;</li>
     *   <li>every document carries it, i.e. there are no nested children in this segment at all --
     *       the overwhelmingly common case, and worth not paying the block bookkeeping for.</li>
     * </ul>
     */
    private static FixedBitSet parentDocs(LeafReader reader) throws IOException {
        NumericDocValues primaryTerm = reader.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        if (primaryTerm == null) {
            return null;
        }
        final int maxDoc = reader.maxDoc();
        FixedBitSet parents = new FixedBitSet(maxDoc);
        int rootCount = 0;
        for (int docId = primaryTerm.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = primaryTerm.nextDoc()) {
            parents.set(docId);
            rootCount++;
        }
        return rootCount == maxDoc ? null : parents;
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
