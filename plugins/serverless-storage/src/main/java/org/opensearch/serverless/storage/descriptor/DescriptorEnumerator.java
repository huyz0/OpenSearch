/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.DescriptorUnavailableException;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Every live index name, read from the object store and nothing else.
 *
 * <h2>What this is for, which is not serving queries</h2>
 *
 * Invariant I2 says every local structure is reconstructible from the object store. For the name index
 * that is a claim, and this is what makes it true. Without a path from the store back to the full set of
 * names, the name index is not a cache that can be discarded, it is a second source of truth that has to
 * be protected, which is a different and much worse system.
 *
 * <p>It is not a query path and must not become one. S13 measured an in-memory prefix match at 3.2 ms for
 * sixty-five thousand hits; a LIST pages a thousand keys at a time behind a serial continuation token, so
 * a population in the millions is seconds to minutes. Use it to rebuild, to reconcile when drift is
 * suspected, and to answer nothing.
 *
 * <h2>Why the keys had to be prefix-preserving</h2>
 *
 * This only works because {@link BlobDescriptorBackend} stores descriptors under their own names rather
 * than under a hash. Hashing would spread writes, which the store already does adaptively on its own, and
 * would cost exactly this: with hashed keys there is no way to enumerate a name range, and no way to
 * split the work either.
 *
 * <p>Deleted names are absent for free, because T10 moved tombstones out from under the descriptor prefix.
 * A layout that left them in place would need a read per key to find out which names are live, turning a
 * listing into a full fetch of the population.
 */
public final class DescriptorEnumerator implements DescriptorPrefixBackend {

    private static final Logger logger = LogManager.getLogger(DescriptorEnumerator.class);

    /**
     * The alphabet a prefix is split over.
     *
     * <p>Index names are lowercase and may not begin with most punctuation, so this covers what a first
     * character can be. A name whose first character falls outside it is still found, by the sweep that
     * follows the split shards: correctness does not depend on the alphabet being complete, only
     * parallelism does.
     */
    private static final String SPLIT_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz";

    private final Function<BlobPath, BlobContainer> containers;
    private final BlobPath basePath;

    public DescriptorEnumerator(Function<BlobPath, BlobContainer> containers, BlobPath basePath) {
        this.containers = containers;
        this.basePath = basePath;
    }

    /** Every live name, in one serial pass. Fine for a small population and for tests. */
    public List<String> allNames() throws IOException {
        return new ArrayList<>(new TreeSet<>(descriptors().listBlobs().keySet()));
    }

    /**
     * Every live name, with the listing split across {@code executor} by first character.
     *
     * <p>A single prefix cannot be paged in parallel, because the continuation token is serial by
     * construction. What can be parallelised is the prefix space: one listing per starting character runs
     * independently, and at a hundred million names that is the difference between a recovery option and a
     * theoretical one.
     *
     * <p>The results are unioned through a sorted set rather than concatenated. Prefix ranges are disjoint
     * in principle, and relying on that would make a future alphabet change silently duplicate names, which
     * a rebuild would turn into a corrupted index rather than an error.
     */
    public List<String> allNamesInParallel(ExecutorService executor) throws IOException {
        BlobContainer descriptors = descriptors();
        List<Callable<Collection<String>>> shards = new ArrayList<>();
        for (char first : SPLIT_ALPHABET.toCharArray()) {
            String prefix = String.valueOf(first);
            shards.add(() -> descriptors.listBlobsByPrefix(prefix).keySet());
        }

        TreeSet<String> names = new TreeSet<>();
        try {
            for (Future<Collection<String>> future : executor.invokeAll(shards)) {
                names.addAll(future.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while enumerating descriptors", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IOException("could not enumerate descriptors", e.getCause());
        }

        // The split covers what a name can start with, and a name that starts with something else is a
        // name this would otherwise lose. A rebuild that quietly drops indices is the failure this whole
        // area is most careful about, so the remainder is swept rather than assumed empty.
        int found = names.size();
        for (String name : descriptors.listBlobs().keySet()) {
            if (SPLIT_ALPHABET.indexOf(name.charAt(0)) < 0) {
                names.add(name);
            }
        }
        if (names.size() != found) {
            logger.info("{} descriptor names fell outside the split alphabet and were swept", names.size() - found);
        }
        return new ArrayList<>(names);
    }

    /**
     * Expands a wildcard prefix from one bounded listing, which is what lets the descriptor system index go.
     *
     * <h4>Why a listing can serve this when it cannot serve enumeration</h4>
     *
     * The class javadoc above says a listing is not a query path, and that stays true of enumeration: paging
     * millions of keys behind a serial continuation token is seconds to minutes. What makes <em>this</em> a
     * query is the cap. {@code DescriptorGate.DEFAULT_WILDCARD_EXPANSION_LIMIT} refuses an expansion past a
     * hundred matches, so asking for {@code limit + 1} keys is a single request that stops as soon as it has
     * them. S3 seeks to the prefix rather than scanning and returns keys in lexicographic order, so this is
     * one {@code ListObjectsV2} with {@code maxKeys} set: the same cost class as a GET, not a walk.
     *
     * <p>Asking for one more than the cap is the whole of the over-limit detection, which is the trick the
     * system index used with {@code size(limit + 1)}. Getting {@code limit + 1} keys back proves there are
     * more than {@code limit} without counting them.
     *
     * <h4>What a key can answer, and why that is now everything</h4>
     *
     * A key carries a name and nothing else. The other two fields of a match used to need the descriptor
     * itself; both are now constants for a gated index rather than unknowns:
     *
     * <ul>
     *   <li><b>open</b> is always true, because {@code MetadataIndexStateService} refuses to close a gated
     *       index at all. There is no other state a live descriptor can be in.</li>
     *   <li><b>hidden</b> is always false, because {@code DescriptorRepresentable} refuses to gate a hidden
     *       index. That refusal exists precisely so this can come from a listing rather than from a GET per
     *       match, or from a second keyspace written on every create.</li>
     * </ul>
     *
     * <p>Deleted names are absent for free, for the reason the class javadoc already gives: tombstones live
     * under their own prefix, so this listing is exactly the set of live names.
     *
     * <h4>Absence and failure are different answers</h4>
     *
     * A missing container means no gated index has ever been created, which is complete and empty rather
     * than broken -- the blob equivalent of the {@code IndexNotFoundException} the system index treated the
     * same way. Anything else propagates, because this decides which indices a request touches and a
     * swallowed failure turns "the catalogue was unreadable" into "there are no such indices".
     */
    @Override
    public AbsentIndexDescriptorSuppliers.PrefixExpansion expandPrefix(String prefix, int limit) {
        final String safePrefix = prefix == null ? "" : prefix;
        List<BlobMetadata> found;
        try {
            found = descriptors().listBlobsByPrefixInSortedOrder(safePrefix, limit + 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
        } catch (NoSuchFileException | FileNotFoundException e) {
            return AbsentIndexDescriptorSuppliers.PrefixExpansion.of(List.of());
        } catch (IOException | RuntimeException e) {
            throw new DescriptorUnavailableException(safePrefix + "*", e);
        }
        if (found.size() > limit) {
            return AbsentIndexDescriptorSuppliers.PrefixExpansion.tooMany(limit);
        }
        List<AbsentIndexDescriptorSuppliers.PrefixMatch> matches = new ArrayList<>(found.size());
        for (BlobMetadata blob : found) {
            IndexDescriptor desc = AbsentIndexDescriptorSuppliers.supply(blob.name());
            boolean open = desc == null || desc.state() == IndexDescriptor.State.OPEN;
            boolean hidden = desc != null && desc.hidden();
            matches.add(new AbsentIndexDescriptorSuppliers.PrefixMatch(blob.name(), open, hidden));
        }
        return AbsentIndexDescriptorSuppliers.PrefixExpansion.of(matches);
    }

    /**
     * The descriptor space as a child container, which is the one addressing that works on both stores.
     *
     * <p>Not interchangeable with listing the base container under a {@code "descriptors/"} prefix. On an
     * object store a key is flat and the slash is just a character, so that would work; on a filesystem
     * repository the slash is a directory separator and the listing iterates one level, returning nothing at
     * all. S7 shipped exactly that mistake for tombstones and it passed review.
     */
    private BlobContainer descriptors() {
        return containers.apply(basePath.add(trimmed(BlobDescriptorBackend.DESCRIPTOR_PREFIX)));
    }

    private static String trimmed(String prefix) {
        return prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
    }
}
