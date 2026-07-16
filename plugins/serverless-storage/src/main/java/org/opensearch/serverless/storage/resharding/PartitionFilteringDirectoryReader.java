/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.lucene.index.DirectoryReader;

import java.io.IOException;

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
 * docs -- computing a combined {@link org.apache.lucene.util.Bits} (this reader's own live docs AND
 * partition membership) once per leaf, at wrap time, is the standard, correct way to change document
 * visibility without touching the underlying segment data. {@code doWrapDirectoryReader} means {@code
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
 *
 * <p>Shares its wrapping/live-doc-rebuilding machinery with {@link InPlaceSplitFilteringDirectoryReader}
 * via {@link AbstractIdFilteringDirectoryReader}; this subclass supplies only the {@link
 * RoutingPartitionFilter} predicate and this mechanism's own {@link #getReaderCacheHelper()} behavior.
 */
public final class PartitionFilteringDirectoryReader extends AbstractIdFilteringDirectoryReader {

    private final ShardPartitionDescriptor descriptor;

    /**
     * Wraps a reader so every leaf's live docs are additionally restricted to {@code descriptor}'s partition.
     *
     * @param in the reader to wrap.
     * @param descriptor which partition (of how many) this reader should filter down to.
     */
    public PartitionFilteringDirectoryReader(DirectoryReader in, ShardPartitionDescriptor descriptor) throws IOException {
        super(in, id -> RoutingPartitionFilter.matches(id, descriptor));
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
}
