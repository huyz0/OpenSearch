/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.split;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.serverless.routing.RoutingHash;
import org.opensearch.serverless.routing.UnitLayout;

import java.io.IOException;

/**
 * One segment seen through a split half: only the live documents whose id hashes into that half.
 *
 * <p>Handed to {@code IndexWriter#addIndexes}, which copies postings, doc values and stored fields of the
 * documents this reports live without re-analysing anything. Soft-deleted documents (superseded versions
 * and tombstones) are left out as well: a half starts with no history to retain, because recovery here
 * replays the write log rather than reading Lucene's history.
 *
 * <p><b>Nested documents follow their parent.</b> A parent is indexed last in its block and only it stores
 * the id, so a document without one takes the decision of the next document that has one. Splitting a
 * block would leave children pointing at another parent.
 */
final class HalfFilter extends FilterCodecReader {

    private final FixedBitSet keep;
    private final int kept;

    private HalfFilter(CodecReader in, FixedBitSet keep) {
        super(in);
        this.keep = keep;
        this.kept = keep.cardinality();
    }

    /**
     * Returns the segment restricted to one half.
     *
     * @param segment a segment of the old unit
     * @param half the unit whose hash range to keep
     * @return the filtered segment
     * @throws IOException if the segment cannot be read
     */
    static HalfFilter of(CodecReader segment, UnitLayout.Unit half) throws IOException {
        final int maxDoc = segment.maxDoc();
        final FixedBitSet keep = new FixedBitSet(maxDoc);
        final Bits hardLive = segment.getLiveDocs();
        final FixedBitSet softDeleted = softDeleted(segment);
        final StoredFields stored = segment.storedFields();
        final IdVisitor visitor = new IdVisitor();
        int blockStart = 0;
        for (int doc = 0; doc < maxDoc; doc++) {
            visitor.id = null;
            stored.document(doc, visitor);
            if (visitor.id == null) {
                continue; // a nested child, or a no-op tombstone: decided with the next document that has an id
            }
            final boolean inHalf = half.covers(RoutingHash.of(visitor.id));
            for (int member = blockStart; member <= doc; member++) {
                final boolean live = (hardLive == null || hardLive.get(member))
                    && (softDeleted == null || softDeleted.get(member) == false);
                if (inHalf && live) {
                    keep.set(member);
                }
            }
            blockStart = doc + 1;
        }
        return new HalfFilter(segment, keep);
    }

    private static FixedBitSet softDeleted(CodecReader segment) throws IOException {
        final NumericDocValues values = segment.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
        if (values == null) {
            return null;
        }
        final FixedBitSet deleted = new FixedBitSet(segment.maxDoc());
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            deleted.set(doc);
        }
        return deleted;
    }

    /** Returns how many documents this half keeps from the segment. */
    int kept() {
        return kept;
    }

    @Override
    public Bits getLiveDocs() {
        return keep;
    }

    @Override
    public int numDocs() {
        return kept;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    /** Reads only the stored id, and stops there. */
    private static final class IdVisitor extends StoredFieldVisitor {
        String id;

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
                return Status.YES;
            }
            return id == null ? Status.NO : Status.STOP;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            id = Uid.decodeId(value);
        }
    }
}
