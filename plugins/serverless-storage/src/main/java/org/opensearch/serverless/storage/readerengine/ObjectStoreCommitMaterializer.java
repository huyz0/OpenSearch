/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.zip.CRC32C;

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
 *
 * <p><b>Safe to call more than once against the same, already-populated {@code targetDirectory}</b>
 * -- e.g. {@code ObjectStoreReaderEngine}'s own refresh-to-newer-generation polling
 * (rfc-serverless-opensearch.md &sect;16 Phase 3) materializes a second, newer manifest into the
 * same directory a first {@code open()} call already materialized into. Two successive Lucene
 * commits from the same shard routinely reference the *same* unchanged segment files (Lucene only
 * ever writes a new segment for new data; it does not rewrite an untouched one just because a later
 * commit still references it) -- found by a real refresh test throwing {@code
 * FileAlreadyExistsException} on exactly this overlap, not assumed.
 *
 * <p><b>"Already present" is decided on content identity, never on length.</b> This method used to
 * trust any on-disk file whose length matched its manifest entry, justified on the grounds that a
 * same-length-wrong-bytes file would be caught by Lucene's own per-file checksum footer when the
 * segment was opened. That justification does not survive compaction. A compacted commit is a
 * <em>different, perfectly valid</em> Lucene commit that reuses file names: a real experiment
 * against Lucene 10.5 shows a compaction's {@code segments_1} is 155 bytes -- exactly the length of
 * the writer's own {@code segments_1}, with entirely different content -- and that successive
 * compactions recycle segment names ({@code _3}, then {@code _0}) so a compacted {@code _0.si} can
 * collide with the writer's original. Every one of those files verifies its own footer perfectly
 * well; they are simply the wrong commit's files. Under the old length check the fetch was skipped
 * outright and the reader kept serving pre-compaction data while reporting itself caught up. The
 * check is now against the manifest's full {@link FileReference} -- bundle, offset, length and
 * CRC32C -- and the common unchanged-segment path stays free because what this materializer itself
 * wrote is remembered per directory (see {@link #materializedByDirectory}); only a file this
 * process did not write is actually re-hashed, once.
 *
 * <p><b>The segments file is written last, and superseded ones are removed.</b> Two separate
 * problems, one fix each. First, {@code manifest.files()} iterates a {@code Map} whose order comes
 * from {@code SegmentInfos#files}, which is a plain {@code HashSet} -- so nothing guaranteed the
 * {@code segments_N} file was not written <em>first</em>, leaving a directory that advertises a
 * commit whose segment data has not arrived yet. Any refresh landing in that window (core's own 1&nbsp;s
 * scheduled refresh reaches this engine independently of the poll) sees a {@code NoSuchFileException}.
 * The commit-visibility ordering every storage system uses -- all data, fsync, then the pointer --
 * costs nothing here. Second, the directory is shared and additive across generations, and Lucene
 * resolves "the commit" as the highest-generation {@code segments_N} present; a compaction whose
 * merged commit is at a <em>lower</em> Lucene generation than a superseded writer commit still on
 * disk would therefore never become visible at all. Deleting every {@code segments_*} that is not
 * this manifest's makes the resolution unambiguous: the only commit in the directory is the one the
 * manifest names.
 */
public final class ObjectStoreCommitMaterializer {

    private static final Logger logger = LogManager.getLogger(ObjectStoreCommitMaterializer.class);

    /**
     * Above this size, a file is streamed from the bundle store straight into the target directory
     * instead of being fetched as one {@code byte[]}.
     *
     * <p>The all-at-once path allocates the same file three or four times over, concurrently: the
     * ranged read, the extract copy, a decrypt copy when encryption is on, and a retained copy in
     * the in-memory bundle cache -- none of it behind a circuit breaker. Worse, it has a hard
     * ceiling: {@code byte[]} cannot exceed {@code Integer.MAX_VALUE}, and {@code
     * BlobContainerBundleStore#readRange} refuses anything larger with an {@code IOException}. That
     * ceiling is reachable in the shipped configuration -- compaction's default target bundle size
     * permits a single merged segment whose {@code .fdt}/{@code .tim}/{@code .doc} crosses
     * 2&nbsp;GiB -- and once crossed, no reader can materialize the shard and no compaction can read
     * it either, because {@code LuceneMergeCompactionPublisher} materializes before it merges. The
     * shard is stuck with no recovery path.
     *
     * <p>The threshold exists rather than streaming unconditionally because streaming means this
     * method computes the CRC32C itself, duplicating the verification {@code
     * BundleFileReader#readFile} already does, and it bypasses the caching tiers that make the small
     * files (which is most of them, and the ones actually re-read) cheap. Large files are exactly
     * where the caches are least useful and the heap spike most dangerous, so that is where the
     * trade flips.
     */
    static final long STREAMING_THRESHOLD_BYTES = 16L * 1024 * 1024;

    /** Buffer size for the streaming path -- one page-friendly chunk, not sized to the file. */
    private static final int STREAM_BUFFER_BYTES = 1024 * 1024;

    private final BundleFileReader bundleStore;

    /**
     * What this materializer has itself written into each target directory, so the overwhelmingly
     * common "this segment file is unchanged since the last generation" case costs a map lookup
     * rather than a full re-read and re-hash of the file.
     *
     * <p>Keyed weakly by {@link Directory} identity, and not by shard: one materializer instance is
     * shared across every shard a node hosts, so a per-instance map keyed by file name alone would
     * confuse two shards' identically-named {@code segments_1} files with each other -- which is
     * precisely the class of bug this whole method exists to stop. Weak keys because a directory
     * outliving its shard is exactly what closing a shard is supposed to end.
     */
    private final Map<Directory, Map<String, FileReference>> materializedByDirectory = new WeakHashMap<>();

    /**
     * Creates a materializer backed by {@code bundleStore}.
     *
     * @param bundleStore reads and checksum-verifies files out of object storage
     */
    public ObjectStoreCommitMaterializer(BundleFileReader bundleStore) {
        this.bundleStore = bundleStore;
    }

    /**
     * Fetches and writes every file in {@code manifest.files()} not already present (with the exact
     * content the manifest records) in {@code targetDirectory}, so that {@code
     * Lucene.readSegmentInfos(targetDirectory)} -- and hence any plain Lucene {@code
     * DirectoryReader} -- opens exactly this manifest's commit afterward, and no other. Every
     * newly-fetched file's bytes are verified against the checksum recorded in the manifest, so a
     * corrupt or truncated transfer fails loudly here rather than surfacing as a confusing
     * Lucene-level error later.
     *
     * <p>See this class's own javadoc for why "already present" is decided on the manifest's full
     * {@link FileReference} rather than on length, why the segments file is written last, and why
     * superseded {@code segments_*} files are removed.
     *
     * @param manifest the commit manifest whose files should be present in {@code targetDirectory}
     * @param targetDirectory the Lucene directory to write missing files into
     * @throws IOException if fetching or writing a file fails, or a checksum mismatches
     */
    public void materialize(CommitManifest manifest, Directory targetDirectory) throws IOException {
        Map<String, FileReference> known = sidecarFor(targetDirectory);
        Set<String> alreadyPresent = new HashSet<>(Arrays.asList(targetDirectory.listAll()));
        String segmentsFileName = manifest.segmentsFileName();

        Set<String> newlyWritten = new HashSet<>();
        for (Map.Entry<String, FileReference> entry : manifest.files().entrySet()) {
            if (entry.getKey().equals(segmentsFileName)) {
                continue; // written last, below -- see this class's javadoc.
            }
            if (materializeOne(entry.getKey(), entry.getValue(), targetDirectory, alreadyPresent, known)) {
                newlyWritten.add(entry.getKey());
            }
        }
        if (newlyWritten.isEmpty() == false) {
            // Durable before anything points at it. A crash after this and before the segments file
            // lands leaves a directory whose newest commit is the previous generation -- correct and
            // retryable -- rather than one advertising a commit whose data may not have survived.
            targetDirectory.sync(newlyWritten);
        }

        FileReference segmentsRef = manifest.files().get(segmentsFileName);
        if (segmentsRef == null) {
            // CommitManifest's own constructor already rejects this, so reaching it would mean a
            // manifest built by something other than that constructor -- fail loudly rather than
            // materialize a directory with no commit pointer in it.
            throw new IOException(
                "manifest generation " + manifest.generation() + " does not list its own segments file " + segmentsFileName
            );
        }
        if (materializeOne(segmentsFileName, segmentsRef, targetDirectory, alreadyPresent, known)) {
            targetDirectory.sync(Set.of(segmentsFileName));
        }

        // Only after the intended commit is fully present and durable: everything else that looks
        // like a commit pointer goes, so "the highest-generation segments_N in this directory" and
        // "the commit this manifest names" can never be two different things again.
        deleteSupersededSegmentsFiles(targetDirectory, segmentsFileName);
    }

    /**
     * Writes one file if it is not already present with exactly the content {@code ref} describes.
     *
     * @return whether this call actually wrote the file (and so it needs syncing).
     */
    private boolean materializeOne(
        String fileName,
        FileReference ref,
        Directory targetDirectory,
        Set<String> alreadyPresent,
        Map<String, FileReference> known
    ) throws IOException {
        if (alreadyPresent.contains(fileName)) {
            if (isAlreadyExactlyThisContent(fileName, ref, targetDirectory, known)) {
                return false;
            }
            // Either a prior call's write into this exact file was interrupted before it could be
            // synced (a truncated leftover), or a newer manifest genuinely rebinds this name to
            // different bytes (a compaction's recycled segment name). Both mean the on-disk copy is
            // not what this manifest describes. deleteFile before createOutput: Lucene's
            // createOutput throws FileAlreadyExistsException rather than overwriting.
            targetDirectory.deleteFile(fileName);
            recordMaterialized(known, fileName, null);
        }
        fetchInto(fileName, ref, targetDirectory);
        recordMaterialized(known, fileName, ref);
        return true;
    }

    /**
     * Whether the file already on disk is byte-for-byte the content {@code ref} names.
     *
     * <p>Free in the common case: if this materializer wrote the file itself, the {@link
     * FileReference} it wrote is remembered and an {@code equals} settles it. Otherwise -- a
     * directory populated by a previous incarnation of this process, which is exactly the case a
     * restart produces -- the file is length-checked and then genuinely re-hashed once, and the
     * result remembered so it is never re-hashed again. Paying a local disk read once per file per
     * process is far cheaper than either re-downloading the shard or, as before, trusting a
     * same-length file that belongs to a different commit.
     */
    private boolean isAlreadyExactlyThisContent(
        String fileName,
        FileReference ref,
        Directory targetDirectory,
        Map<String, FileReference> known
    ) throws IOException {
        FileReference recorded;
        synchronized (known) {
            recorded = known.get(fileName);
        }
        if (recorded != null) {
            return recorded.equals(ref);
        }
        if (targetDirectory.fileLength(fileName) != ref.length()) {
            return false;
        }
        long onDisk;
        try (IndexInput in = targetDirectory.openInput(fileName, IOContext.READONCE)) {
            onDisk = crc32cOf(in, ref.length());
        }
        if (onDisk != ref.checksum()) {
            logger.info(
                "local copy of [{}] has the length the manifest expects but different content ({} vs {}); re-fetching",
                fileName,
                onDisk,
                ref.checksum()
            );
            return false;
        }
        recordMaterialized(known, fileName, ref);
        return true;
    }

    /** Fetches {@code ref}'s bytes and writes them into {@code targetDirectory} under {@code fileName}, verified. */
    private void fetchInto(String fileName, FileReference ref, Directory targetDirectory) throws IOException {
        BundleFileEntry entry = new BundleFileEntry(fileName, ref.offset(), ref.length(), ref.checksum());
        if (ref.length() <= STREAMING_THRESHOLD_BYTES) {
            // readFile verifies the CRC32C itself before returning, so nothing is re-hashed here.
            byte[] bytes = bundleStore.readFile(ref.bundleName(), entry);
            try (IndexOutput out = targetDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(bytes, bytes.length);
            }
            return;
        }
        // Streaming path: never holds the file in heap, and has no 2 GiB ceiling. openFile is
        // documented as unverified, so the checksum is computed here, incrementally, and compared
        // at the end -- the same guarantee readFile gives, without the allocation.
        boolean complete = false;
        try {
            CRC32C crc = new CRC32C();
            long written = 0;
            byte[] buffer = new byte[STREAM_BUFFER_BYTES];
            try (
                InputStream in = bundleStore.openFile(ref.bundleName(), entry);
                IndexOutput out = targetDirectory.createOutput(fileName, IOContext.DEFAULT)
            ) {
                while (written < ref.length()) {
                    int wanted = (int) Math.min(buffer.length, ref.length() - written);
                    int read = in.read(buffer, 0, wanted);
                    if (read < 0) {
                        throw new IOException(
                            "bundle ["
                                + ref.bundleName()
                                + "] ended after "
                                + written
                                + " bytes while streaming ["
                                + fileName
                                + "], which the manifest says is "
                                + ref.length()
                                + " bytes"
                        );
                    }
                    crc.update(buffer, 0, read);
                    out.writeBytes(buffer, 0, read);
                    written += read;
                }
            }
            if (crc.getValue() != ref.checksum()) {
                throw new IOException(
                    "checksum mismatch streaming ["
                        + fileName
                        + "] out of bundle ["
                        + ref.bundleName()
                        + "]: manifest says "
                        + ref.checksum()
                        + " but the bytes hash to "
                        + crc.getValue()
                );
            }
            complete = true;
        } finally {
            if (complete == false) {
                // Leave no partially-written file behind claiming to be this manifest's: the next
                // attempt's content check would have to re-hash it only to reject it, and a
                // half-file that happens to reach the right length would be indistinguishable from
                // a real one to anything less careful.
                try {
                    targetDirectory.deleteFile(fileName);
                } catch (IOException ignored) {
                    // Nothing to clean up, or the directory is already unusable; the original
                    // failure is the one worth reporting.
                }
            }
        }
    }

    /**
     * Removes every {@code segments_*} file in {@code targetDirectory} other than {@code keep}.
     *
     * <p>Lucene resolves "the commit" in a directory as the highest-generation segments file it
     * finds, and this directory accumulates one per generation the reader has ever materialized,
     * with no {@code IndexFileDeleter} to clean up after it (a reader shard has no {@code
     * IndexWriter}). That is what let a compaction -- whose merged commit is at Lucene generation 1
     * regardless of what generation its source was at -- be materialized, refreshed, and recorded as
     * the current generation while the reader went on serving the pre-compaction commit and
     * reporting itself caught up. Removing the superseded pointers is what makes the manifest, not
     * the accident of which generation number is numerically largest, decide what is open.
     *
     * <p>A file being deleted here is never one an open reader still needs: a {@code
     * DirectoryReader} reads its {@code SegmentInfos} once at open and never consults the segments
     * file again, and the segment data files those older commits reference are deliberately left
     * alone. Failures are logged, not thrown -- a directory that still has an extra pointer in it is
     * a problem for the <em>next</em> materialization to notice, not a reason to fail this one after
     * it has already succeeded.
     */
    private void deleteSupersededSegmentsFiles(Directory targetDirectory, String keep) throws IOException {
        for (String name : targetDirectory.listAll()) {
            // IndexFileNames.SEGMENTS is "segments"; PENDING_SEGMENTS is "pending_segments", which
            // only an IndexWriter mid-commit ever creates and which no reader directory should have.
            // Both are commit pointers, and neither may outlive the manifest this call just applied.
            boolean isCommitPointer = name.startsWith(IndexFileNames.SEGMENTS) || name.startsWith(IndexFileNames.PENDING_SEGMENTS);
            if (name.equals(keep) || isCommitPointer == false) {
                continue;
            }
            try {
                targetDirectory.deleteFile(name);
                logger.debug("removed superseded commit pointer [{}], keeping [{}]", name, keep);
            } catch (IOException removalFailure) {
                logger.warn("could not remove superseded commit pointer [" + name + "]", removalFailure);
            }
        }
    }

    /**
     * Deletes every file in {@code targetDirectory} that none of {@code retained} references.
     *
     * <p>A reader shard has no {@code IndexWriter} and therefore no {@code IndexFileDeleter}, so
     * without this nothing ever removes a segment file that has fallen out of the current manifest:
     * over a day at a one-second publication cadence a reader accumulates every superseded segment
     * the shard has ever had, on local disk that neither {@code DiskCacheSpaceGovernor} nor the
     * per-shard cache budget covers (both bound the bundle cache tree, not the shard's Lucene store).
     *
     * <p>Callers pass more than just the current manifest on purpose: a searcher acquired moments
     * before an advance still holds open the previous generation's segment readers. Retaining that
     * generation as well means this only ever deletes files no live searcher can be holding. Even
     * so, a deletion failure is logged rather than thrown -- on a platform where an open handle
     * blocks unlinking, the right outcome is "reclaim it next time", not "fail the shard".
     *
     * @param targetDirectory the reader's store directory to prune.
     * @param retained every manifest whose files must survive -- typically the current generation
     *                 and the one before it.
     * @return how many files were actually deleted.
     */
    public int pruneUnreferencedFiles(Directory targetDirectory, Collection<CommitManifest> retained) throws IOException {
        Set<String> keep = new HashSet<>();
        for (CommitManifest manifest : retained) {
            keep.addAll(manifest.files().keySet());
        }
        Map<String, FileReference> known = sidecarFor(targetDirectory);
        List<String> deleted = new ArrayList<>();
        for (String name : targetDirectory.listAll()) {
            if (keep.contains(name) || name.equals(IndexWriter.WRITE_LOCK_NAME)) {
                continue;
            }
            try {
                targetDirectory.deleteFile(name);
                deleted.add(name);
            } catch (IOException stillInUse) {
                logger.debug("could not prune superseded reader file [" + name + "], will retry after a later advance", stillInUse);
            }
        }
        if (deleted.isEmpty() == false) {
            synchronized (known) {
                known.keySet().removeAll(deleted);
            }
            logger.debug("pruned {} superseded files from the reader store directory", deleted.size());
        }
        return deleted.size();
    }

    private Map<String, FileReference> sidecarFor(Directory targetDirectory) {
        synchronized (materializedByDirectory) {
            return materializedByDirectory.computeIfAbsent(targetDirectory, unused -> new java.util.HashMap<>());
        }
    }

    private static void recordMaterialized(Map<String, FileReference> known, String fileName, FileReference ref) {
        synchronized (known) {
            if (ref == null) {
                known.remove(fileName);
            } else {
                known.put(fileName, ref);
            }
        }
    }

    private static long crc32cOf(IndexInput in, long length) throws IOException {
        CRC32C crc = new CRC32C();
        byte[] buffer = new byte[(int) Math.min(STREAM_BUFFER_BYTES, Math.max(1L, length))];
        long remaining = length;
        while (remaining > 0) {
            int chunk = (int) Math.min(buffer.length, remaining);
            in.readBytes(buffer, 0, chunk);
            crc.update(buffer, 0, chunk);
            remaining -= chunk;
        }
        return crc.getValue();
    }
}
