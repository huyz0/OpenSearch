/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;

import java.io.IOException;
import java.util.Map;

/**
 * The inverse of {@code ObjectStoreCommitPublisher}: given a {@link CommitManifest}, fetches
 * every file it references from object storage (checksum-verified, per rfc-serverless-opensearch.md
 * &sect;6.2/&sect;6.3) and writes it into a target Lucene {@link Directory} so a plain Lucene
 * reader can open the resulting commit.
 *
 * <p>This is a full-materialization strategy: every referenced file is fetched in full before the
 * commit becomes readable, rather than serving reads lazily out of the bundle store with a block
 * cache. That is a deliberate, scoped starting point -- correct and immediately useful for reader
 * shards whose segment set is small enough to fetch upfront -- not the eventual lazy/block-cached
 * object-store-native {@code Directory} the RFC's read path targets, which is separate follow-on
 * work.
 */
public final class ObjectStoreCommitMaterializer {

    private final BundleFileReader bundleStore;

    public ObjectStoreCommitMaterializer(BundleFileReader bundleStore) {
        this.bundleStore = bundleStore;
    }

    /**
     * Fetches and writes every file in {@code manifest.files()} into {@code targetDirectory}, so
     * that {@code Lucene.readSegmentInfos(targetDirectory)} (and hence any plain Lucene
     * {@code DirectoryReader}) can open the commit afterward. Every file's bytes are verified
     * against the checksum recorded in the manifest as they're fetched, so a corrupt or truncated
     * transfer fails loudly here rather than surfacing as a confusing Lucene-level error later.
     */
    public void materialize(CommitManifest manifest, Directory targetDirectory) throws IOException {
        for (Map.Entry<String, FileReference> entry : manifest.files().entrySet()) {
            String fileName = entry.getKey();
            FileReference ref = entry.getValue();
            byte[] bytes = bundleStore.readFile(
                ref.bundleName(),
                new BundleFileEntry(fileName, ref.offset(), ref.length(), ref.checksum())
            );
            try (IndexOutput out = targetDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(bytes, bytes.length);
            }
        }
        targetDirectory.sync(manifest.files().keySet());
    }
}
