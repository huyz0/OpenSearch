/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.Function;

/**
 * Where a {@link CompactNameIndex} is parked so a restarting tier does not have to reconstruct itself.
 *
 * <h2>The checkpoint is an optimisation, and saying so is the point</h2>
 *
 * A16 chose checkpointing over streaming persistence because a rebuild's retained memory is near zero and
 * only allocation churn is high, which made the decision about start-up time: loading packed entries beats
 * replaying the population.
 *
 * <p>But the reason this can be an optimisation at all is that something else can always rebuild it.
 * `DescriptorEnumerator` is that something. Without a path from the object store back to the full name set,
 * a checkpoint stops being a cache of a derivable thing and becomes the only copy, which would make the
 * name index a second source of truth with its own durability problem. So a lost or unreadable checkpoint
 * is a slow start here, never a data loss, and this class is written to make that the obvious reading.
 *
 * <h2>Written under a generation, replaced rather than mutated</h2>
 *
 * Each checkpoint is its own object and the newest wins. Overwriting one key in place would leave a window
 * where a reader sees a half-written structure, and a half-understood name index is a cluster that cannot
 * find its own indices, which is what {@link NameIndexCheckpoint}'s version guard already refuses. Writing
 * a new object and letting readers pick the highest generation has no such window.
 */
public final class BlobNameIndexCheckpointStore {

    private static final Logger logger = LogManager.getLogger(BlobNameIndexCheckpointStore.class);

    /** Where checkpoints live, beside the descriptors they were derived from. */
    public static final String CHECKPOINT_PREFIX = "nameindex";

    private final Function<BlobPath, BlobContainer> containers;
    private final BlobPath basePath;

    public BlobNameIndexCheckpointStore(Function<BlobPath, BlobContainer> containers, BlobPath basePath) {
        this.containers = containers;
        this.basePath = basePath;
    }

    /**
     * Writes a checkpoint at {@code generation}, which must be higher than any already written.
     *
     * <p>The generation is supplied rather than derived from a listing, because deriving it would need a
     * read-then-write with a window two writers fit through, and the caller producing a checkpoint already
     * knows how far it has consumed.
     */
    public void write(CompactNameIndex index, long generation) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            NameIndexCheckpoint.write(index, out);
            BytesReference bytes = out.bytes();
            // failIfAlreadyExists, because two writers at one generation have disagreed about how far the
            // feed has been consumed, and silently keeping one of them would hide that.
            checkpoints().writeBlob(nameFor(generation), bytes.streamInput(), bytes.length(), true);
            logger.info("wrote a name index checkpoint at generation {} ({} bytes)", generation, bytes.length());
        }
    }

    /**
     * The newest readable checkpoint, or empty when there is none.
     *
     * <p>Empty is an ordinary outcome, not a failure: a tier starting for the first time has no checkpoint
     * and rebuilds from the store. An unreadable newest checkpoint is treated the same way rather than
     * falling back to an older one, because an older checkpoint plus a change-log position that assumes
     * the newer one would skip everything between them, which is worse than a slow rebuild.
     */
    public Optional<Loaded> readLatest() {
        try {
            BlobContainer checkpoints = checkpoints();
            long best = -1;
            for (String blobName : checkpoints.listBlobs().keySet()) {
                long generation = generationOf(blobName);
                if (generation > best) {
                    best = generation;
                }
            }
            if (best < 0) {
                return Optional.empty();
            }
            try (InputStream stream = checkpoints.readBlob(nameFor(best)); StreamInput in = StreamInput.wrap(stream.readAllBytes())) {
                return Optional.of(new Loaded(NameIndexCheckpoint.read(in), best));
            }
        } catch (IOException | RuntimeException e) {
            logger.warn("could not read a name index checkpoint; the tier will rebuild from the descriptor store", e);
            return Optional.empty();
        }
    }

    /** A loaded checkpoint and the generation it was written at, so a reader knows where to resume. */
    public record Loaded(CompactNameIndex index, long generation) {
    }

    private BlobContainer checkpoints() {
        return containers.apply(basePath.add(CHECKPOINT_PREFIX));
    }

    /** Zero-padded so lexicographic order is generation order, the same reason the change log pads buckets. */
    static String nameFor(long generation) {
        return String.format(java.util.Locale.ROOT, "checkpoint-%019d", generation);
    }

    private static long generationOf(String blobName) {
        try {
            return Long.parseLong(blobName.substring(blobName.lastIndexOf('-') + 1));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            // Something else living in this prefix is not a reason to fail a load. Ignoring it beats
            // guessing at a generation, which would resume the feed from the wrong place.
            return -1;
        }
    }
}
