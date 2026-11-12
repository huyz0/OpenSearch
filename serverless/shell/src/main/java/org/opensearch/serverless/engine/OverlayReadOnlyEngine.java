/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.ReadOnlyEngine;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.serverless.store.WalRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

/**
 * A reader's engine that answers from its published commit and, on top of it, the acknowledged writes its writer has
 * pushed since: the tail overlay.
 *
 * <p><b>Why.</b> A reader serves a published commit, so a write is searchable on it only once its writer has published
 * a commit holding it: a refresh interval after it was acknowledged at the soonest. Every publish costs PUTs, so that
 * interval is also the price of freshness. The writer already has every acknowledged write in hand, and pushes it to
 * the shard's readers over transport after the acknowledgement; this engine indexes what it is pushed into a small
 * in-memory index and searches that together with the commit. A write is then searchable a network hop after its
 * acknowledgement, however long the shard waits to publish, at no object-store request.
 *
 * <p><b>How a search sees both.</b> The searcher is a {@link FilterDirectoryReader} over the commit whose leaves are
 * the commit's, with every document the overlay replaces or deletes hidden from its live documents, followed by the
 * overlay's own segments. The overlay holds the latest pushed version of each document only; an operation older than
 * what the commit holds for the same id -- one at or below the commit's local checkpoint, or one the commit has a later
 * sequence number for -- is dropped. Each change to the overlay is a new reader with its own cache key, so the request
 * cache cannot serve a result from before it.
 *
 * <p><b>What it may miss, and what it never does.</b> A lost push, a record whose document needs a field the reader's
 * mapping does not have yet, or an overlay past its size cap leaves the reader where the commit puts it: as fresh as
 * it was before overlays existed, never answering with a version older than the commit's. The commit catches up at the
 * writer's next publish. Operations are applied one at a time by sequence number, so a missed one never reorders the
 * ones around it.
 */
public final class OverlayReadOnlyEngine extends ReadOnlyEngine {

    private static final Logger LOG = LogManager.getLogger(OverlayReadOnlyEngine.class);

    /** Where an overlay finds the records pushed for its shard. */
    public interface Tail {
        /**
         * Starts keeping the records pushed for a shard, from above a local checkpoint; called when an engine opens.
         *
         * @param shardId the shard
         * @param localCheckpoint the opened commit's local checkpoint: records at or below it are dropped
         */
        void track(ShardId shardId, long localCheckpoint);

        /**
         * Stops keeping them for this engine; called when it closes.
         *
         * @param shardId the shard
         */
        void untrack(ShardId shardId);

        /**
         * Returns the ordinal the next record for the shard will take: higher than any it holds.
         *
         * @param shardId the shard
         * @return the ordinal
         */
        long nextOrdinal(ShardId shardId);

        /**
         * Returns the records held for a shard from an ordinal on, in ordinal order.
         *
         * @param shardId the shard
         * @param fromOrdinal the first ordinal wanted
         * @return ordinal to record
         */
        Map<Long, WalRecord> from(ShardId shardId, long fromOrdinal);

        /**
         * Waits, bounded, while the shard's writer is being asked for what it acknowledged past the commit, so a search
         * does not answer from the commit alone in the moment between the engine opening and the answer arriving.
         *
         * @param shardId the shard
         */
        default void awaitFetch(ShardId shardId) {}
    }

    /** How many documents an overlay holds before it takes no more and waits for the next commit. */
    public static final int MAX_DOCUMENTS = 50_000;

    private final ShardId shardId;
    private final Overlay overlay;

    /**
     * Opens a reader's engine with an overlay.
     *
     * @param config the engine configuration
     * @param tail where pushed records are found, or null for the commit alone
     * @param parser parses a record with the index's current mapping; returns null when the document needs a mapping
     *     update this node does not have
     */
    public OverlayReadOnlyEngine(EngineConfig config, Tail tail, Function<WalRecord, ParsedDocument> parser) {
        super(config, null, null, true, Function.identity(), false);
        this.shardId = config.getShardId();
        Overlay opened = null;
        try {
            opened = tail == null ? null : new Overlay(config, tail, parser, getProcessedLocalCheckpoint());
        } catch (IOException | RuntimeException e) {
            // Without an overlay the reader is what it always was: the commit.
            LOG.warn("could not open the tail overlay of {}; it serves its commit only", shardId, e);
        }
        this.overlay = opened;
    }

    @Override
    protected ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        if (overlay == null) {
            return super.getReferenceManager(scope);
        }
        overlay.tail.awaitFetch(shardId);
        overlay.refreshIfBehind();
        return overlay.manager;
    }

    /** @return how many documents the overlay holds, or -1 without one; for tests and stats */
    public int overlayDocuments() {
        return overlay == null ? -1 : overlay.seqById.size();
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (overlay != null) {
            overlay.close();
        }
        super.closeNoLock(reason, closedLatch);
    }

    /** Where a document of the commit is, and the sequence number it was written at. */
    private record Hit(int leaf, int doc, long seqNo) {
    }

    /** The in-memory index of pushed records, and the readers built from it over the commit. */
    private final class Overlay {
        final Tail tail;
        final Function<WalRecord, ParsedDocument> parser;
        final long localCheckpoint;
        final OpenSearchDirectoryReader base;
        final DirectoryReader baseInner;
        final List<LeafReaderContext> baseLeaves;
        final EngineConfig config;
        ByteBuffersDirectory directory;
        IndexWriter writer;
        final Map<String, Long> seqById = new HashMap<>();
        final FixedBitSet[] masks;
        final boolean[] maskShared;
        final Manager manager;
        DirectoryReader overlayReader;
        long applied;
        /**
         * The ordinal through which the reader handed out holds what was pushed: {@link #applied} as of the last refresh
         * whose reader has been swapped in. A refresh advances {@code applied} as it goes, before its reader is out, so a
         * search that compared against {@code applied} while another refresh ran took the old reader and missed records
         * that had arrived well before it.
         */
        volatile long visible;
        /** What {@link #applied} was when the running refresh finished applying; published once its reader is swapped in. */
        long appliedByRefresh;
        volatile boolean closed;

        Overlay(EngineConfig config, Tail tail, Function<WalRecord, ParsedDocument> parser, long localCheckpoint) throws IOException {
            this.config = config;
            this.tail = tail;
            this.parser = parser;
            this.localCheckpoint = localCheckpoint;
            this.base = OverlayReadOnlyEngine.super.getReferenceManager(SearcherScope.EXTERNAL).acquire();
            this.baseInner = base.getDelegate();
            this.baseLeaves = baseInner.leaves();
            this.masks = new FixedBitSet[baseLeaves.size()];
            this.maskShared = new boolean[baseLeaves.size()];
            // The base reader is held by the manager as its first reader: acquired above, released when replaced.
            this.manager = new Manager(base);
            tail.track(shardId, localCheckpoint);
            this.applied = 0L;
        }

        void refreshIfBehind() {
            if (closed || tail.nextOrdinal(shardId) <= visible) {
                return;
            }
            try {
                manager.maybeRefreshBlocking();
            } catch (IOException | RuntimeException e) {
                LOG.debug("could not refresh the tail overlay of {}", shardId, e);
            }
        }

        /** Applies what was pushed since the last refresh; returns the reader to search, or null if nothing changed. */
        synchronized OpenSearchDirectoryReader refresh() throws IOException {
            if (closed) {
                return null;
            }
            final Map<Long, WalRecord> pending = tail.from(shardId, applied);
            boolean changed = false;
            for (Map.Entry<Long, WalRecord> entry : pending.entrySet()) {
                applied = Math.max(applied, entry.getKey() + 1);
                changed |= apply(entry.getValue());
            }
            appliedByRefresh = applied;
            if (changed == false) {
                return null;
            }
            final DirectoryReader next = DirectoryReader.openIfChanged(overlayReader, writer);
            if (next != null) {
                overlayReader.decRef();
                overlayReader = next;
            }
            final FixedBitSet[] snapshot = masks.clone();
            java.util.Arrays.fill(maskShared, true);
            baseInner.incRef();
            overlayReader.incRef();
            boolean built = false;
            try {
                final OpenSearchDirectoryReader composite = OpenSearchDirectoryReader.wrap(
                    new Composite(baseInner, snapshot, overlayReader),
                    shardId
                );
                built = true;
                return composite;
            } finally {
                if (built == false) {
                    baseInner.decRef();
                    overlayReader.decRef();
                }
            }
        }

        /** The in-memory index, made when the first record is applied: most readers are never pushed anything. */
        private void ensureWriter() throws IOException {
            if (writer != null) {
                return;
            }
            final IndexWriterConfig iwc = new IndexWriterConfig(config.getAnalyzer()).setCodec(config.getCodec())
                .setSimilarity(config.getSimilarity())
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                .setCommitOnClose(false);
            directory = new ByteBuffersDirectory();
            writer = new IndexWriter(directory, iwc);
            overlayReader = DirectoryReader.open(writer);
        }

        private boolean apply(WalRecord record) throws IOException {
            final long seqNo = record.seqNo();
            if (record.hasSequenceIdentity() == false || seqNo <= localCheckpoint) {
                return false;
            }
            final Long had = seqById.get(record.id());
            if (had != null && had >= seqNo) {
                return false;
            }
            if (had == null && seqById.size() >= MAX_DOCUMENTS) {
                return false;
            }
            final BytesRef uid = Uid.encodeId(record.id());
            final Hit hit = had == null ? find(uid) : null;
            if (hit != null && hit.seqNo() >= seqNo) {
                // The commit already holds this document at a later operation than the one pushed.
                return false;
            }
            final ParsedDocument parsed;
            if (record.isDeletion()) {
                parsed = null;
            } else {
                try {
                    parsed = parser.apply(record);
                } catch (RuntimeException e) {
                    LOG.debug("could not parse a pushed record of {}; it waits for the commit", shardId, e);
                    return false;
                }
                if (parsed == null) {
                    return false;
                }
                parsed.updateSeqID(seqNo, record.primaryTerm());
                parsed.version().setLongValue(record.version());
            }
            ensureWriter();
            final Term term = new Term(IdFieldMapper.NAME, uid);
            if (parsed == null) {
                writer.deleteDocuments(term);
            } else {
                writer.updateDocuments(term, parsed.docs());
            }
            if (hit != null) {
                mask(hit);
            }
            seqById.put(record.id(), seqNo);
            return true;
        }

        private void mask(Hit hit) {
            FixedBitSet bits = masks[hit.leaf()];
            if (bits == null) {
                bits = new FixedBitSet(baseLeaves.get(hit.leaf()).reader().maxDoc());
                maskShared[hit.leaf()] = false;
            } else if (maskShared[hit.leaf()]) {
                // A reader already built shares it; this refresh writes its own copy.
                bits = bits.clone();
                maskShared[hit.leaf()] = false;
            }
            bits.set(hit.doc());
            masks[hit.leaf()] = bits;
        }

        /** The live document of the commit with an id, and its sequence number; null if the commit has none. */
        private Hit find(BytesRef uid) throws IOException {
            for (int i = 0; i < baseLeaves.size(); i++) {
                final LeafReader leaf = baseLeaves.get(i).reader();
                final Terms terms = leaf.terms(IdFieldMapper.NAME);
                if (terms == null) {
                    continue;
                }
                final TermsEnum te = terms.iterator();
                if (te.seekExact(uid) == false) {
                    continue;
                }
                final Bits live = leaf.getLiveDocs();
                final PostingsEnum postings = te.postings(null, PostingsEnum.NONE);
                for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                    if (live == null || live.get(doc)) {
                        final NumericDocValues seqNos = leaf.getNumericDocValues(SeqNoFieldMapper.NAME);
                        final long seqNo = seqNos != null && seqNos.advanceExact(doc) ? seqNos.longValue() : Long.MAX_VALUE;
                        return new Hit(i, doc, seqNo);
                    }
                }
            }
            return null;
        }

        void close() {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
            }
            try {
                manager.close();
            } catch (IOException | RuntimeException e) {
                LOG.debug("closing the tail overlay manager of {}", shardId, e);
            }
            try {
                if (writer != null) {
                    overlayReader.decRef();
                    writer.rollback();
                    directory.close();
                }
            } catch (IOException | RuntimeException e) {
                LOG.debug("closing the tail overlay of {}", shardId, e);
            } finally {
                tail.untrack(shardId);
            }
        }

        /** Hands out the commit-plus-overlay reader, rebuilt whenever the overlay has taken a record. */
        final class Manager extends ReferenceManager<OpenSearchDirectoryReader> {
            Manager(OpenSearchDirectoryReader first) {
                current = first;
            }

            @Override
            protected void decRef(OpenSearchDirectoryReader reference) throws IOException {
                reference.decRef();
            }

            @Override
            protected OpenSearchDirectoryReader refreshIfNeeded(OpenSearchDirectoryReader referenceToRefresh) throws IOException {
                return refresh();
            }

            @Override
            protected void afterMaybeRefresh() throws IOException {
                // After the swap: what this refresh applied is now in the reader searches acquire.
                synchronized (Overlay.this) {
                    visible = Math.max(visible, appliedByRefresh);
                }
            }

            @Override
            protected boolean tryIncRef(OpenSearchDirectoryReader reference) {
                return reference.tryIncRef();
            }

            @Override
            protected int getRefCount(OpenSearchDirectoryReader reference) {
                return reference.getRefCount();
            }
        }
    }

    /** The commit's leaves, less what the overlay replaces, then the overlay's own. */
    private static final class Composite extends FilterDirectoryReader {
        private final DirectoryReader overlay;

        Composite(DirectoryReader base, FixedBitSet[] masks, DirectoryReader overlay) throws IOException {
            super(base, new SubReaderWrapper() {
                @Override
                protected LeafReader[] wrap(List<? extends LeafReader> readers) {
                    final List<LeafReader> leaves = new ArrayList<>(readers.size() + overlay.leaves().size());
                    for (int i = 0; i < readers.size(); i++) {
                        leaves.add(masks[i] == null ? readers.get(i) : new Masked(readers.get(i), masks[i]));
                    }
                    for (LeafReaderContext context : overlay.leaves()) {
                        leaves.add(context.reader());
                    }
                    return leaves.toArray(new LeafReader[0]);
                }

                @Override
                public LeafReader wrap(LeafReader reader) {
                    throw new UnsupportedOperationException("leaves are wrapped as a list");
                }
            });
            this.overlay = overlay;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
            throw new UnsupportedOperationException("a tail overlay reader is rebuilt, not reopened");
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            // The commit is the engine's for life, so the overlay's state is what tells one of these readers from another.
            return overlay.getReaderCacheHelper();
        }

        @Override
        protected void doClose() throws IOException {
            // Released, not closed: the base and overlay readers are shared by every reader built over them.
            try {
                in.decRef();
            } finally {
                overlay.decRef();
            }
        }
    }

    /** A commit leaf with the documents the overlay replaces taken out of its live documents. */
    private static final class Masked extends FilterLeafReader {
        private final Bits live;
        private final int numDocs;

        Masked(LeafReader in, FixedBitSet masked) {
            super(in);
            final Bits base = in.getLiveDocs();
            this.live = new Bits() {
                @Override
                public boolean get(int index) {
                    return (base == null || base.get(index)) && masked.get(index) == false;
                }

                @Override
                public int length() {
                    return in.maxDoc();
                }
            };
            // Only live documents are ever masked.
            this.numDocs = in.numDocs() - masked.cardinality();
        }

        @Override
        public Bits getLiveDocs() {
            return live;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }
}
