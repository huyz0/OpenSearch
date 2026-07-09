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

    public FallbackBundleFileReader(BundleFileReader primary, BundleFileReader fallback) {
        this.primary = primary;
        this.fallback = fallback;
    }

    @Override
    public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
        try {
            return primary.readFile(bundleName, entry);
        } catch (NoSuchFileException e) {
            return fallback.readFile(bundleName, entry);
        }
    }
}
