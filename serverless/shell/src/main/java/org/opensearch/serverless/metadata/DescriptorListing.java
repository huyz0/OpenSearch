/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Lists the descriptors under the index-name registers, a bounded page at a time, skipping tombstones and aliases.
 * Split out of {@link DescriptorStore}, which keeps the public entry points.
 */
final class DescriptorListing {

    private final ConditionalBlobContainer container;

    DescriptorListing(BlobContainer container) {
        this.container = ConditionalBlobContainer.of(container);
    }

    /**
     * One bounded page of descriptors, in name order.
     *
     * <p><b>This is the only enumeration a deployment at target scale may use.</b> "List every index" is
     * not a slow operation at 100 million indices; it is not an operation at all -- the answer does not
     * fit in a response, cannot be consumed by a caller, and is stale before it finishes. So the API is
     * a cursor walk, and callers that want everything pay for everything, one page at a time, visibly.
     *
     * <p><b>Both costs are bounded by the page.</b> The listing resumes from the cursor through
     * {@link BlobContainer#listBlobsByPrefix(String, String, int)} -- S3's {@code StartAfter}, GCS's
     * {@code startOffset} -- so nothing before it is listed again, and the descriptor reads are one per name
     * on the page. A page of a thousand is one listing request on S3 plus the reads.
     *
     * <p><b>Not a snapshot.</b> An index created or deleted behind the cursor is not seen by the rest of the
     * walk; one created or deleted ahead of it is seen or not depending on when its page is read. Every index
     * that exists for the whole walk appears exactly once. A tombstone takes no row, so a page can be shorter
     * than the limit, and the last page can be empty when only tombstones followed the cursor.
     *
     * @param after return names strictly greater than this, or null to start at the beginning
     * @param limit maximum descriptors to return
     * @return the page
     * @throws IOException if listing or reading fails
     */
    public DescriptorPage listPage(String after, int limit) throws IOException {
        return listPage("", after, limit);
    }

    /**
     * One bounded page of the descriptors whose names start with a prefix, in name order.
     *
     * @param prefix the prefix, empty for every name
     * @param after return names strictly greater than this, or null to start at the prefix
     * @param limit maximum descriptors to return
     * @return the page
     * @throws IOException if listing or reading fails
     * @see #listPage(String, int)
     */
    public DescriptorPage listPage(String prefix, String after, int limit) throws IOException {
        return listPage(prefix, after, limit, DescriptorStore::readInSequence);
    }

    /**
     * One bounded page of the descriptors whose names start with a prefix, reading each listed batch's
     * descriptors through {@code reads}.
     *
     * @param prefix the prefix, empty for every name
     * @param after return names strictly greater than this, or null to start at the prefix
     * @param limit maximum descriptors to return
     * @param reads how the descriptor reads of a batch are run
     * @return the page
     * @throws IOException if listing or any read fails
     */
    public DescriptorPage listPage(String prefix, String after, int limit, DescriptorStore.Reads reads) throws IOException {
        Names.validatePrefix(prefix);
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
        final Map<String, IndexDescriptor> page = new LinkedHashMap<>();
        String cursor = after;
        while (true) {
            // One more than the page can take, so a full page knows whether anything follows it.
            final int want = (int) Math.min((long) limit - page.size() + 1, LIST_PAGE);
            final List<org.opensearch.common.blobstore.BlobMetadata> batch = container.listBlobsByPrefix(prefix, cursor, want);
            // Every name the page can still take is read at once; the one beyond it only proves there is more.
            final List<java.util.concurrent.Callable<Optional<IndexDescriptor>>> tasks = new ArrayList<>();
            for (int i = 0; i < Math.min(batch.size(), limit - page.size()); i++) {
                final String name = batch.get(i).name();
                tasks.add(() -> readLive(name));
            }
            final List<Optional<IndexDescriptor>> read;
            try {
                read = reads.runAll(tasks);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted reading a page of descriptors", e);
            }
            for (int i = 0; i < read.size(); i++) {
                cursor = batch.get(i).name();
                final Optional<IndexDescriptor> descriptor = read.get(i);
                if (descriptor == null) {
                    throw new IOException("could not read the descriptor of [" + cursor + "]");
                }
                descriptor.ifPresent(d -> page.put(d.name(), d));
            }
            if (page.size() >= limit && batch.size() > read.size()) {
                // There is more. Report a cursor rather than a total: knowing how many there are in
                // total is itself a full scan, and is the question this API refuses to answer.
                return new DescriptorPage(page, cursor);
            }
            if (batch.size() < want) {
                return new DescriptorPage(page, null);
            }
            // Otherwise tombstones left the page short, or it filled exactly at the end of a listing: list
            // on from the last name read, which re-lists any name this batch held but did not read.
        }
    }

    /** A descriptor if the name holds a live one; empty for a tombstone or a name deleted since it was listed. */
    Optional<IndexDescriptor> readLive(String name) throws IOException {
        final Optional<BlobRegister> register = container.readRegister(name);
        if (register.isEmpty() || Tombstones.isTombstone(register.get().value()) || DescriptorStore.isAlias(register.get().value())) {
            return Optional.empty();
        }
        try (InputStream in = register.get().value().streamInput()) {
            return Optional.of(IndexDescriptor.fromStream(in));
        }
    }

    /** The most names one listing request asks for: S3's own page size. */
    static final int LIST_PAGE = 1000;

    /**
     * Returns the names beginning with a prefix, up to a cap.
     *
     * <p><b>This is what makes an index pattern answerable without enumerating the deployment.</b> A request path
     * never enumerates the deployment, and that refusal was the reason {@code logs-*} was refused
     * too. It does not have to be: a prefix listing with a maximum key count costs one request per thousand
     * names up to the cap, each page resuming where the last stopped, so its cost is set by the cap rather
     * than by the population, and a deployment with a hundred million indices pays exactly what one with ten
     * pays.
     *
     * <p><b>The cap is asked for plus one, deliberately.</b> A listing that returns exactly the cap is
     * indistinguishable from one that was cut off there. Asking for one more is what makes "there are more
     * than this" a fact rather than a guess, and it costs one key.
     *
     * <p><b>Names, not descriptors.</b> Reading each one would turn a bounded listing back into work
     * proportional to what matched, and the caller reads them anyway to find out what it got — an index,
     * an alias, or a tombstone left by a deletion. Which of those it is belongs to the caller, not here.
     *
     * <p><b>Only live names count against the cap.</b> A deleted name leaves a tombstone for its
     * quarantine, and a tenant that creates and deletes a name a day accumulates hundreds under one
     * prefix; a cap that counted them refused {@code logs-*} with one live index beneath it. So when the
     * bounded listing overflows, the names sharing the prefix are read to tell live from deleted, and the
     * cap is applied to the live ones, a page at a time, stopping as soon as the live names pass it. That
     * read is proportional to the tombstones under the prefix — the cost the tombstone sweep exists to bound —
     * and happens only when the bounded listing alone could not answer. In that case the tombstones are also
     * filtered out of the answer, since they were read anyway.
     *
     * @param prefix the prefix, which may be empty to mean every name
     * @param cap the most names to return
     * @return the matching names, in lexicographic order
     * @throws TooManyMatchesException if more than {@code cap} live names match
     * @throws IOException if the listing fails
     */
    public List<String> namesWithPrefix(String prefix, int cap) throws IOException {
        Names.validatePrefix(prefix);
        if (cap < 1) {
            throw new IllegalArgumentException("cap must be positive, got " + cap);
        }
        final List<String> names = listUpToCap(prefix, cap);
        return names != null ? names : liveNamesOrRefuse(prefix, cap);
    }

    /** Up to cap names in one serial walk; null when there are more, for the caller to settle. */
    List<String> listUpToCap(String prefix, int cap) throws IOException {
        // Up to cap + 1 names, a thousand per listing, each resuming where the last stopped.
        final List<String> names = new ArrayList<>();
        String cursor = null;
        while (names.size() <= cap) {
            final int want = (int) Math.min((long) cap + 1 - names.size(), LIST_PAGE);
            final List<org.opensearch.common.blobstore.BlobMetadata> batch = container.listBlobsByPrefix(prefix, cursor, want);
            for (org.opensearch.common.blobstore.BlobMetadata blob : batch) {
                names.add(blob.name());
            }
            if (batch.size() < want) {
                return names;
            }
            cursor = batch.get(batch.size() - 1).name();
        }
        return null;
    }

    List<String> liveNamesOrRefuse(String prefix, int cap) throws IOException {
        String cursor;
        // More names than the cap, deleted ones included. Walk the prefix again, a page at a time, reading
        // each name to tell live from deleted and refusing once the live ones pass the cap: the reads are
        // bounded by the cap plus the tombstones among them, and the memory by the cap.
        final List<String> live = new ArrayList<>();
        cursor = null;
        while (true) {
            final List<org.opensearch.common.blobstore.BlobMetadata> batch = container.listBlobsByPrefix(prefix, cursor, LIST_PAGE);
            for (org.opensearch.common.blobstore.BlobMetadata blob : batch) {
                final Optional<BlobRegister> register = container.readRegister(blob.name());
                if (register.isEmpty() || Tombstones.isTombstone(register.get().value())) {
                    continue;
                }
                live.add(blob.name());
                if (live.size() > cap) {
                    throw new TooManyMatchesException(prefix, cap);
                }
            }
            if (batch.size() < LIST_PAGE) {
                return live;
            }
            cursor = batch.get(batch.size() - 1).name();
        }
    }

    /**
     * Lists every descriptor.
     *
     * <p><b>Do not put this on a request path.</b> It is O(population) by construction, and that
     * was measured: 2N+1 operations when it backed {@code truthFor}. It survives for offline
     * work — a sweep that already intends to visit everything — and {@link #listPage} is what an API
     * uses. A caller that reaches for this to answer a user is answering the wrong question.
     *
     * @return descriptors by index name
     * @throws IOException if listing or reading fails
     */
    public Map<String, IndexDescriptor> listAll() throws IOException {
        final Map<String, IndexDescriptor> descriptors = new LinkedHashMap<>();
        final List<String> names = new ArrayList<>(container.listBlobs().keySet());
        for (String blobName : names) {
            final Optional<BlobRegister> register = container.readRegister(blobName);
            if (register.isEmpty()) {
                continue;
            }
            if (Tombstones.isTombstone(register.get().value()) || DescriptorStore.isAlias(register.get().value())) {
                continue;
            }
            try (InputStream in = register.get().value().streamInput()) {
                final IndexDescriptor descriptor = IndexDescriptor.fromStream(in);
                descriptors.put(descriptor.name(), descriptor);
            }
        }
        return descriptors;
    }
}
