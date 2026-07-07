/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.ReadOnlyEngine;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.serverless.storage.manifest.CommitManifest;

import java.io.IOException;
import java.util.function.Function;

/**
 * Opens a reader-shard engine directly from a published {@link CommitManifest}
 * (rfc-serverless-opensearch.md &sect;5/&sect;6): never a full local copy recovered by peer
 * transfer, never promotable to a writer -- exactly the reader-shard role's contract.
 *
 * <p>This does not need to be its own {@code Engine} subclass: once {@link
 * ObjectStoreCommitMaterializer} has populated the engine's {@link EngineConfig#getStore()}
 * directory with the manifest's files, that directory holds an ordinary valid Lucene commit, and
 * {@link ReadOnlyEngine} already implements everything a read-only shard needs (search, get,
 * completion stats, segment listing, refusing writes) against exactly that. Reusing it here means
 * the object-store-specific work is confined to the one genuinely novel piece -- materializing a
 * manifest into a directory -- rather than re-deriving several thousand lines of tested read-path
 * behavior.
 *
 * <p>Refreshing to a newer manifest generation (reopening once a writer publishes a new commit,
 * per the notification path in rfc-serverless-metadata-plane.md &sect;5) is not implemented here:
 * each call to {@link #open} materializes and opens one fixed generation for the lifetime of the
 * returned engine, matching how {@link ReadOnlyEngine} itself is a one-shot immutable view. Making
 * a reader shard advance to new generations without a full engine reopen is separate, larger work.
 */
public final class ObjectStoreReaderEngine {

    private ObjectStoreReaderEngine() {}

    /**
     * Materializes {@code manifest} into {@code config.getStore().directory()} and opens a
     * {@link ReadOnlyEngine} against the result. Sequence-number and translog stats are taken
     * directly from the manifest rather than read back out of the materialized commit or (as
     * {@link ReadOnlyEngine} would otherwise try) an actual local translog -- a reader shard has
     * no local translog at all, so this avoids requiring one to exist just to report stats.
     */
    public static ReadOnlyEngine open(EngineConfig config, CommitManifest manifest, ObjectStoreCommitMaterializer materializer)
        throws IOException {
        materializer.materialize(manifest, config.getStore().directory());
        SeqNoStats seqNoStats = new SeqNoStats(
            manifest.maxSeqNo(),
            manifest.localCheckpoint(),
            config.getGlobalCheckpointSupplier().getAsLong()
        );
        return new ReadOnlyEngine(config, seqNoStats, new TranslogStats(), true, Function.identity(), false);
    }
}
