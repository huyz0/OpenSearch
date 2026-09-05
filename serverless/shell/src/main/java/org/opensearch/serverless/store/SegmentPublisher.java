/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Uploads a shard's committed segments to the object store, and restores them onto a node taking over.
 *
 * <p>This is what makes the object store the source of truth rather than a backup. Once a commit is
 * published, the node that wrote it holds nothing that cannot be reconstructed elsewhere, which is the
 * property the whole architecture is for: node loss recovers by re-opening from the object store rather
 * than by copying between peers.
 *
 * <p><b>Fencing (section 9.6, R9).</b> A writer holding term T writes segment blobs under
 * {@code t=T/}, and the manifest register refuses a publish from a term older than the one it already
 * holds. Both halves are needed and they do different jobs. The manifest CAS stops a stale writer from
 * <em>being believed</em>; the term-scoped prefix stops it from <em>overwriting</em> anything, because
 * the blob names it computes cannot collide with a live writer's. A zombie that resumes after a long
 * pause therefore writes bytes nobody reads, rather than corrupting a file another node is serving.
 */
public final class SegmentPublisher {

    /** The register naming the currently published commit. */
    public static final String MANIFEST = "manifest";

    private final BlobStore blobStore;
    private final BlobPath shardBase;
    private final BlobContainer container;

    /** This instance's own last publish: the register need not be read to swap over what this wrote. */
    private volatile long lastPublishedGeneration = BlobRegister.ABSENT_GENERATION;
    private volatile CommitManifest lastPublished;

    /**
     * Creates a publisher for one shard.
     *
     * @param blobStore the backing store
     * @param shardBase the shard's base path
     */
    public SegmentPublisher(BlobStore blobStore, BlobPath shardBase) {
        this.blobStore = blobStore;
        this.shardBase = shardBase;
        this.container = blobStore.blobContainer(shardBase);
    }

    /**
     * Returns the container-path segment a writer at the given term writes under.
     *
     * <p>A container rather than a name prefix, for the same reason {@code RegisterMap} uses
     * {@link BlobPath} segments: hierarchy in the blob store is the container, and
     * {@code FsBlobContainer} resolves names flatly against one directory. A blob named
     * {@code "t=1/_0.cfe"} does not create {@code t=1/}; it fails to write. Learned twice.
     *
     * @param term the writer's term
     * @return the path segment
     */
    public static String termSegment(long term) {
        return "t=" + term;
    }

    /**
     * Publishes the shard's current commit.
     *
     * <p>Files already named by the previous manifest are not re-uploaded: a failover inherits them at
     * whatever blob they already occupy. Only files this commit added are written, under this writer's
     * term prefix.
     *
     * @param store the shard's store, already flushed to a commit
     * @param term the publishing writer's term
     * @return the manifest as published
     * @throws IOException if upload fails
     * @throws StaleWriterException if a newer term has already published
     */
    public CommitManifest publish(Store store, long term) throws IOException {
        return publish(store, term, null);
    }

    /**
     * Publishes the shard's current commit, recording which node did it.
     *
     * @param store the shard's store, already flushed to a commit
     * @param term the publishing writer's term
     * @param writerId the publishing node, recorded in the manifest and checked against the one already
     *     there; null to skip both
     * @return the manifest as published
     * @throws IOException if upload fails
     * @throws StaleWriterException if a newer term has already published, or another node has published at
     *     this one
     */
    public CommitManifest publish(Store store, long term, String writerId) throws IOException {
        return publish(store, null, term, writerId);
    }

    /**
     * Publishes one specific commit of the shard, which the caller has pinned.
     *
     * <p><b>Why the commit is passed in.</b> Reading "the latest commit" and then opening its files is a
     * check-then-act against Lucene's deletion policy: a periodic flush landing in between commits a newer
     * generation, and the files only the older one referenced are deleted out from under the upload, which
     * then fails on a file that was there a moment ago. A caller that holds the commit through
     * {@code IndexShard#acquireLastIndexCommit} keeps the policy's hands off it until the upload is done.
     *
     * @param store the shard's store, already flushed to a commit
     * @param commit the commit to publish, pinned by the caller; null to publish whatever is latest
     * @param term the publishing writer's term
     * @param writerId the publishing node, or null to skip the writer check
     * @return the manifest as published
     * @throws IOException if upload fails
     * @throws StaleWriterException if a newer term has already published, or another node has published at
     *     this one
     */
    public CommitManifest publish(Store store, org.apache.lucene.index.IndexCommit commit, long term, String writerId) throws IOException {
        // What this instance published last, if it has: the swap below is conditioned on that generation,
        // and a swap that fails re-reads. Reading before every publish was one register read per publish
        // per shard, paid to learn what this writer already knew.
        final CommitManifest remembered = lastPublished;
        final long rememberedGeneration = lastPublishedGeneration;
        final Optional<BlobRegister> existingRegister;
        final CommitManifest existing;
        if (remembered != null && rememberedGeneration != BlobRegister.ABSENT_GENERATION) {
            existingRegister = Optional.empty();
            existing = remembered;
        } else {
            existingRegister = container.readRegister(MANIFEST);
            existing = existingRegister.isPresent() ? parse(existingRegister.get()) : null;
        }
        if (existing != null && existing.term() > term) {
            // A zombie: paused past its lease, someone else took the shard and published. Its bytes are
            // already inert because of the term prefix; this stops it from claiming they are current.
            throw new StaleWriterException(term, existing.term());
        }
        if (existing != null
            && existing.term() == term
            && existing.writer() != null
            && writerId != null
            && existing.writer().equals(writerId) == false) {
            // Two nodes holding one term. That cannot happen while the shard-head's compare-and-swap
            // behaves, and if it ever does not, this is the difference between a loud refusal and a commit
            // silently assembled from two nodes' segments: the inherit-by-name step below would take this
            // writer's file names, find them already published by the other one, and skip the upload -- so
            // the surviving manifest would name segments that never existed together.
            throw new ForeignWriterException(term, existing.writer(), writerId);
        }

        final Map<String, String> published = new LinkedHashMap<>();
        final Map<String, Long> lengths = new LinkedHashMap<>();
        final Map<String, String> inherited = existing == null ? Map.of() : existing.files();
        final Directory directory = store.directory();

        store.incRef();
        try {
            final Store.MetadataSnapshot snapshot = commit == null ? store.getMetadata() : store.getMetadata(commit);
            for (String fileName : snapshot.asMap().keySet()) {
                final String alreadyAt = inherited.get(fileName);
                if (alreadyAt != null) {
                    // Segment files are immutable once written; a name that is already published has the
                    // same bytes. Re-uploading would make failover cost the size of the shard.
                    published.put(fileName, alreadyAt);
                    final Long knownLength = existing.lengthOf(fileName);
                    if (knownLength != null) {
                        lengths.put(fileName, knownLength);
                    }
                    continue;
                }
                final String termDir = termSegment(term);
                try (IndexInput input = directory.openInput(fileName, IOContext.READONCE)) {
                    final long length = input.length();
                    try (InputStream stream = new IndexInputStream(input, length)) {
                        blobStore.blobContainer(shardBase.add(termDir)).writeBlob(fileName, stream, length, false);
                    }
                    lengths.put(fileName, length);
                }
                // The manifest records which term's container a file lives in, not a full path, so a
                // failover can inherit files without moving them.
                published.put(fileName, termDir);
            }
        } finally {
            store.decRef();
        }

        final CommitManifest manifest = new CommitManifest(term, published, writerId, lengths);
        final long expected = remembered != null && rememberedGeneration != BlobRegister.ABSENT_GENERATION
            ? rememberedGeneration
            : existingRegister.map(BlobRegister::generation).orElse(BlobRegister.ABSENT_GENERATION);
        final BlobRegisterCasResult result = container.compareAndSwapRegister(MANIFEST, expected, manifest.toBytes());
        if (result.applied()) {
            lastPublishedGeneration = result.currentGeneration();
            lastPublished = manifest;
        } else {
            lastPublishedGeneration = BlobRegister.ABSENT_GENERATION;
            lastPublished = null;
        }
        if (result.applied() == false && remembered != null) {
            // The register moved under what this instance remembered -- a repair, a restore, a manifest
            // rewritten by hand. Once, the way every publish used to begin: read it and publish over it.
            return publish(store, commit, term, writerId);
        }
        if (result.applied() == false) {
            // Who moved it. A newer term is the fence this exception exists for. The same term and the
            // same writer is this node's own concurrent publish -- two of them used to race here, the loser
            // was treated as a zombie, and the shard was closed while its head still named this node. The
            // other publish's manifest is a valid commit of ours, so it is the answer.
            final Optional<BlobRegister> now = container.readRegister(MANIFEST);
            if (now.isPresent()) {
                final CommitManifest current = parse(now.get());
                if (current.term() == term && writerId != null && writerId.equals(current.writer())) {
                    return current;
                }
            }
            throw new StaleWriterException(term, -1L);
        }
        return manifest;
    }

    /**
     * Reads the currently published manifest.
     *
     * @return the manifest, or empty if nothing has been published for this shard
     * @throws IOException if the register cannot be read
     */
    public Optional<CommitManifest> readManifest() throws IOException {
        final Optional<BlobRegister> register = container.readRegister(MANIFEST);
        if (register.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(parse(register.get()));
    }

    /**
     * Restores the published commit into a directory, whole, so a shard can open from local disk alone.
     *
     * <p>Not on the activation path any more: a writer opens the way a reader does, on a directory that
     * reads published segments block by block, so its cold start is proportional to what it touches rather
     * than to the shard. This survives for a caller that genuinely wants a complete local copy.
     *
     * <p>Called before the shard is recovered, and the reason recovery must then use
     * {@code ExistingStoreRecoverySource}: {@code EMPTY_STORE} calls {@code Store#createEmpty}, which
     * would delete exactly what this just wrote.
     *
     * @param target the shard's directory
     * @param shardId for error messages
     * @return the manifest restored, or empty if nothing was published
     * @throws IOException if download fails
     */
    public Optional<CommitManifest> restoreInto(Directory target, ShardId shardId) throws IOException {
        final Optional<CommitManifest> manifest = readManifest();
        if (manifest.isEmpty() || manifest.get().files().isEmpty()) {
            return Optional.empty();
        }
        for (Map.Entry<String, String> file : manifest.get().files().entrySet()) {
            // Local disk is a cache, and a node re-opening onto a newer commit finds files from an earlier
            // one still there. The segment counter is carried in the commit, so a successor that restored
            // this node's last published commit mints the same next segment name this node did for a
            // segment it never published: a same-named file is not necessarily the same bytes. Overwrite
            // rather than trust the name.
            try {
                target.deleteFile(file.getKey());
            } catch (java.io.IOException ignored) {
                // absent, which is the normal case on a cold node
            }
            final BlobContainer source = blobStore.blobContainer(shardBase.add(file.getValue()));
            try (InputStream in = source.readBlob(file.getKey()); IndexOutput out = target.createOutput(file.getKey(), IOContext.DEFAULT)) {
                final byte[] buffer = new byte[8192];
                int read;
                while ((read = in.read(buffer)) > 0) {
                    out.writeBytes(buffer, read);
                }
            } catch (org.apache.lucene.store.AlreadyClosedException e) {
                throw new IOException("directory closed while restoring " + shardId, e);
            }
        }
        return manifest;
    }

    private CommitManifest parse(BlobRegister register) throws IOException {
        try (InputStream in = register.value().streamInput()) {
            return CommitManifest.fromStream(in);
        }
    }

    /**
     * Adapts a Lucene {@link IndexInput} to an {@link InputStream} for blob upload.
     *
     * <p><b>Mark and reset are supported, and have to be.</b> The S3 client refuses a stream it cannot
     * rewind — "No mark support on inputStream breaks the S3 SDK's ability to retry requests" — because a
     * request it cannot replay is a request it cannot retry, and an upload that fails once would then
     * fail permanently. A filesystem never asks, which is why this was invisible until the shell was
     * pointed at an object store: publishing a segment was impossible on S3 and worked perfectly on disk.
     *
     * <p>Implemented against the underlying seek rather than by wrapping in a {@code BufferedInputStream},
     * whose mark is bounded by a read-ahead limit. Segment files are routinely larger than any limit worth
     * buffering, and a mark that silently expires part-way through a large upload would turn a retryable
     * failure into a corrupt one.
     */
    private static final class IndexInputStream extends InputStream {

        private final IndexInput input;
        private final long length;
        private final long origin;
        private long position;
        private long mark = -1L;

        IndexInputStream(IndexInput input, long length) {
            this.input = input;
            this.length = length;
            // Where this stream started, which is not necessarily where the IndexInput did.
            this.origin = input.getFilePointer();
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public synchronized void mark(int readLimit) {
            // readLimit ignored on purpose: the source is seekable, so there is no read-ahead buffer to
            // outgrow and no honest reason to invalidate a mark.
            mark = position;
        }

        @Override
        public synchronized void reset() throws IOException {
            if (mark < 0) {
                throw new IOException("reset without a mark");
            }
            input.seek(origin + mark);
            position = mark;
        }

        @Override
        public int available() {
            return (int) Math.min(Integer.MAX_VALUE, length - position);
        }

        @Override
        public int read() throws IOException {
            if (position >= length) {
                return -1;
            }
            position++;
            return input.readByte() & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (position >= length) {
                return -1;
            }
            final int toRead = (int) Math.min(len, length - position);
            input.readBytes(b, off, toRead);
            position += toRead;
            return toRead;
        }
    }
}
