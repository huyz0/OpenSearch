/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleFileReader;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;

/**
 * The read-path half of zero-copy clone (rfc-serverless-opensearch.md &sect;4.6/&sect;14): a
 * cloned shard's manifest can reference bundles that physically live in the <em>source</em>
 * index's own blob container, not the cloned shard's own -- {@code primary}'s container is scoped
 * to the cloned shard's own sub-path (see {@code ServerlessStoragePlugin#blobContainerFor}), so a
 * bundle name inherited from the source at clone time is simply absent there. This reader tries
 * {@code primary} first and falls back to {@code fallback} (the source's own {@link
 * BundleFileReader}) only on {@link NoSuchFileException} -- i.e. only when the bundle is genuinely
 * missing from the primary container, never masking a different I/O failure by silently retrying
 * against the wrong shard's data.
 *
 * <p>Correctly handles the shard's post-clone future too: once the cloned shard starts writing its
 * own commits, its own manifests reference a mix of inherited (source) and newly-written (own)
 * bundles in the same file map -- this reader's per-call fallback (rather than an
 * all-or-nothing choice made once) resolves each file correctly regardless of which bundle it
 * actually lives in.
 */
public final class FallbackBundleFileReader implements BundleFileReader {

    private final BundleFileReader primary;
    private final BundleFileReader fallback;

    /**
     * Wraps a primary reader with a source fallback.
     *
     * @param primary the cloned shard's own reader, tried first.
     * @param fallback the clone source's reader, tried only when {@code primary} reports the bundle missing.
     */
    public FallbackBundleFileReader(BundleFileReader primary, BundleFileReader fallback) {
        this.primary = primary;
        this.fallback = fallback;
    }

    /**
     * Folds an ordered list of readers (own shard first, then each clone-lineage hop back to the
     * original) into one nested fallback chain -- e.g. {@code chain(List.of(a, b, c))} tries
     * {@code a}, then {@code b}, then {@code c}, matching {@link ShardCloner#resolveLineageChain}'s
     * read-priority ordering. Needed for a clone of a clone, where the immediate source's own
     * manifest can still reference bundles that only physically exist further back in the chain --
     * a single {@code primary}/{@code fallback} pair only covers one hop.
     *
     * @param readers at least one reader; {@code readers.get(0)} is tried first.
     */
    public static BundleFileReader chain(List<BundleFileReader> readers) {
        BundleFileReader result = readers.get(readers.size() - 1);
        for (int i = readers.size() - 2; i >= 0; i--) {
            result = new FallbackBundleFileReader(readers.get(i), result);
        }
        return result;
    }

    /**
     * @param bundleName the bundle to read from.
     * @param entry the file's location within that bundle.
     */
    @Override
    public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
        try {
            return primary.readFile(bundleName, entry);
        } catch (NoSuchFileException e) {
            return fallback.readFile(bundleName, entry);
        }
    }
}
