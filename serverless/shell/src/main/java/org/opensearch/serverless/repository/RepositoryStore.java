/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.repository;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The snapshot repositories registered in this deployment: one blob per repository, named for it.
 */
public final class RepositoryStore {

    /** Logs under the metadata plane's name, where these messages have always been. */
    private final org.apache.logging.log4j.Logger logger;

    /** Lists the snapshots a repository holds. */
    @FunctionalInterface
    public interface SnapshotLister {
        /**
         * Lists a repository's snapshots.
         *
         * @param repo the repository
         * @return its snapshots
         * @throws IOException if they cannot be listed
         */
        List<SnapshotRecord> list(String repo) throws IOException;
    }

    private final BlobContainer container;
    private final SnapshotLister snapshotsOf;

    /**
     * Creates the store over the repositories container.
     *
     * @param container where the repository records live
     * @param snapshotsOf lists a repository's snapshots, so one still holding any is not deleted
     * @param logger where unreadable records are reported
     */
    public RepositoryStore(BlobContainer container, SnapshotLister snapshotsOf, org.apache.logging.log4j.Logger logger) {
        this.container = container;
        this.snapshotsOf = snapshotsOf;
        this.logger = logger;
    }

    /**
     * Registers a repository, or updates one already registered: a namespace within this deployment's own
     * object store to take snapshots under.
     *
     * <p><b>An upsert, not a put-if-absent — deliberately, to match real OpenSearch's own
     * {@code PUT _snapshot/{repo}}.</b> A repository descriptor is unlike an {@link org.opensearch.serverless.cluster.IndexDescriptor} in
     * exactly this respect: re-registering the same name on real OpenSearch updates its settings rather
     * than refusing, and a client written against that API sends a repeat {@code PUT} expecting exactly
     * that. There is no concurrent-writer race this needs to arbitrate the way {@code MetadataPlane.createIndex} does —
     * a repository name colliding with another caller's is a coordination question for whoever is running
     * two callers with the same repository name, not one this store needs an answer to.
     *
     * @param descriptor the repository to register
     * @throws IOException if the write fails
     */
    public void putRepository(RepositoryDescriptor descriptor) throws IOException {
        final var bytes = descriptor.toBytes();
        container.writeBlob(descriptor.name(), bytes.streamInput(), bytes.length(), false);
    }

    /**
     * Reads a repository's record.
     *
     * @param name the repository
     * @return the record, or empty if not registered
     * @throws IOException if the read fails
     */
    public Optional<RepositoryDescriptor> describeRepository(String name) throws IOException {
        if (container.blobExists(name) == false) {
            return Optional.empty();
        }
        try (java.io.InputStream in = container.readBlob(name)) {
            return Optional.of(RepositoryDescriptor.fromStream(in));
        } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
            return Optional.empty();
        }
    }

    /**
     * Lists every registered repository.
     *
     * <p>Allowed where {@code /_serverless/indices} is refused for the same reason {@code _cluster/settings}
     * is: a repository is operator-registered, human-scale — a handful, curated by hand — not a population
     * that grows with what a caller writes. Real OpenSearch's own {@code GET _snapshot} with no name is
     * exactly this listing, unconditionally, which is the strongest evidence the scale assumption holds.
     *
     * @return every registered repository, skipping — and logging — any record that cannot be read
     * @throws IOException if listing fails
     */
    public List<RepositoryDescriptor> listRepositories() throws IOException {
        final List<RepositoryDescriptor> found = new ArrayList<>();
        for (String name : container.listBlobs().keySet()) {
            try (java.io.InputStream in = container.readBlob(name)) {
                found.add(RepositoryDescriptor.fromStream(in));
            } catch (Exception e) {
                logger.debug("Omitting an unreadable repository from the listing (error_type={})", e.getClass().getSimpleName());
            }
        }
        return found;
    }

    /**
     * Removes a repository, refusing while it still holds snapshots.
     *
     * <p>The refusal is what a real {@code _snapshot} API's own delete-repository endpoint always enforces
     * — a repository is a namespace for durable records, and deleting it out from under snapshots that
     * still name it would leave them with nowhere to describe or restore from.
     *
     * @param name the repository
     * @return true if it existed
     * @throws RepositoryInUseException if it still holds snapshots
     * @throws IOException if listing, the read, or the delete fails
     */
    public boolean deleteRepository(String name) throws IOException {
        if (container.blobExists(name) == false) {
            return false;
        }
        final var held = snapshotsOf.list(name);
        if (held.isEmpty() == false) {
            throw new RepositoryInUseException(name, held.size());
        }
        container.deleteBlobsIgnoringIfNotExists(java.util.List.of(name));
        return true;
    }
}
