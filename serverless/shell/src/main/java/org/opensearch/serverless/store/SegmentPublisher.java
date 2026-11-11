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
 * <p><b>Fencing.</b> A writer holding term T writes segment blobs under
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

    /** Whether a publish writes the files it adds as one pack rather than a blob each. */
    private volatile boolean packing;

    /** Where small segment blobs go instead of the shard's term container, or null to keep them there (the default). */
    private volatile NodePackWriter nodePacks;
    private volatile String nodePackIndexUuid;
    private volatile int nodePackShard = -1;
    private volatile long nodePackMaxBlobBytes;

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
     * Sets whether a publish writes the files it adds as one blob per segment rather than one blob per file.
     *
     * <p>A publish adds a refresh's new segment, or a merge's, and the commit point naming it. A segment's files are
     * immutable and die together, so one blob holds them as well as several: a publish of one segment then costs one
     * PUT for its files, not one per file -- four or more for every refresh, which at a one-second refresh was most
     * of a node's PUTs. The manifest records each file's range of its blob (see {@link FileLocation}); a file
     * already published keeps the location it has, in a segment blob or not.
     *
     * @param packing true to pack
     * @return this publisher
     */
    public SegmentPublisher packing(boolean packing) {
        this.packing = packing;
        return this;
    }

    /**
     * Sends segment blobs up to a size into this node's packs, shared with every other shard the node publishes at
     * about the same time, instead of writing each into this shard's term container.
     *
     * <p>Opt-in: without it, each segment stays one blob of its own under the shard. Only applies while
     * {@link #packing} is on, since a node pack holds segment blobs. Larger blobs, such as big merged segments, keep a
     * blob of their own, so no shard's large files sit in a pack beside other shards' small ones.
     *
     * @param writer the node's pack writer, or null to turn node packs off
     * @param indexUuid this shard's index, recorded in each pack's header
     * @param shard this shard's number
     * @param maxBlobBytes the largest segment blob that goes into a node pack
     * @return this publisher
     */
    public SegmentPublisher nodePacks(NodePackWriter writer, String indexUuid, int shard, long maxBlobBytes) {
        if (writer != null && (indexUuid == null || shard < 0 || maxBlobBytes <= 0L)) {
            throw new IllegalArgumentException("node packs need the shard's index, number and a size limit");
        }
        this.nodePackIndexUuid = indexUuid;
        this.nodePackShard = shard;
        this.nodePackMaxBlobBytes = maxBlobBytes;
        this.nodePacks = writer;
        return this;
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
        return publish(store, commit, term, writerId, PruningDigest.EMPTY);
    }

    /**
     * Publishes a commit with its pruning digest, which rides in the manifest register.
     *
     * @param store the shard's store, already flushed to a commit
     * @param commit the commit to publish, pinned by the caller; null to publish whatever is latest
     * @param term the publishing writer's term
     * @param writerId the publishing node, or null to skip the writer check
     * @param digest the commit's pruning digest
     * @return the manifest as published
     * @throws IOException if upload fails
     */
    public CommitManifest publish(Store store, org.apache.lucene.index.IndexCommit commit, long term, String writerId, PruningDigest digest)
        throws IOException {
        return publish(store, commit, term, writerId, digest, -1L);
    }

    /**
     * Publishes a commit with its pruning digest and the part of the write-ahead log it covers.
     *
     * @param store the shard's store, already flushed to a commit
     * @param commit the commit to publish, pinned by the caller; null to publish whatever is latest
     * @param term the publishing writer's term
     * @param writerId the publishing node, or null to skip the writer check
     * @param digest the commit's pruning digest
     * @param walOrdinal the highest log ordinal of {@code term} whose records the commit holds; negative for unknown
     * @return the manifest as published
     * @throws IOException if upload fails
     */
    public CommitManifest publish(
        Store store,
        org.apache.lucene.index.IndexCommit commit,
        long term,
        String writerId,
        PruningDigest digest,
        long walOrdinal
    ) throws IOException {
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
            final java.util.List<String> fresh = new java.util.ArrayList<>();
            for (String fileName : snapshot.asMap().keySet()) {
                if (inherited.containsKey(fileName) == false) {
                    fresh.add(fileName);
                }
            }
            if (packing) {
                writeSegmentBlobs(directory, termSegment(term), fresh, inherited.keySet(), published, lengths);
            }
            for (String fileName : snapshot.asMap().keySet()) {
                if (published.containsKey(fileName)) {
                    // Packed above.
                    continue;
                }
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

        final CommitManifest manifest = new CommitManifest(term, published, writerId, lengths, digest, walOrdinal);
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
            return publish(store, commit, term, writerId, digest, walOrdinal);
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
     * Writes the files a publish adds as one blob per segment, and records where each file is in its blob.
     *
     * <p>A segment's files are written together and die together, when a merge or a delete of every document removes
     * the segment from the commit, so one blob holds them with no bytes left dead inside it while the segment lives.
     * The blob is named after the segment -- {@code _5.seg}, or {@code _5-<id>.seg} for files a later publish adds to
     * a segment already published, such as its deletions -- so its name says what it holds.
     *
     * <p>The commit point, {@code segments_N}, belongs to no segment and changes on every publish. It rides in the
     * blob of the last segment the publish writes: a publish that adds one segment is one PUT, and what that leaves
     * dead is the old commit point, under a kilobyte, until the segment itself goes.
     *
     * <p>Each blob starts with a header listing its files' names, offsets and lengths, so it can be checked or its
     * files recovered without the manifest. Readers do not read it: the manifest records every file's range, and a
     * header read before every cold open would be a request each time. A file of no length stays out of every blob:
     * it has no range to name, and is published as before.
     *
     * <p>Every blob is written only if absent, so neither a retry nor a zombie of the same term can overwrite one a
     * manifest names.
     */
    private void writeSegmentBlobs(
        Directory directory,
        String termDir,
        java.util.List<String> fileNames,
        java.util.Set<String> alreadyPublished,
        Map<String, String> published,
        Map<String, Long> lengths
    ) throws IOException {
        final java.util.TreeMap<String, java.util.List<String>> bySegment = new java.util.TreeMap<>();
        final java.util.List<String> commitPoints = new java.util.ArrayList<>();
        for (String fileName : fileNames) {
            if (directory.fileLength(fileName) <= 0L) {
                continue;
            }
            if (fileName.startsWith(org.apache.lucene.index.IndexFileNames.SEGMENTS)) {
                commitPoints.add(fileName);
            } else {
                bySegment.computeIfAbsent(
                    org.apache.lucene.index.IndexFileNames.parseSegmentName(fileName),
                    ignored -> new java.util.ArrayList<>()
                ).add(fileName);
            }
        }
        if (bySegment.isEmpty()) {
            return;
        }
        final java.util.Set<String> publishedSegments = new java.util.HashSet<>();
        for (String name : alreadyPublished) {
            if (name.startsWith(org.apache.lucene.index.IndexFileNames.SEGMENTS) == false) {
                publishedSegments.add(org.apache.lucene.index.IndexFileNames.parseSegmentName(name));
            }
        }
        final String last = bySegment.lastKey();
        final NodePackWriter packs = nodePacks;
        final java.util.List<SegmentBlob> toNodePack = new java.util.ArrayList<>();
        for (Map.Entry<String, java.util.List<String>> segment : bySegment.entrySet()) {
            final java.util.List<String> files = new java.util.ArrayList<>(segment.getValue());
            if (segment.getKey().equals(last)) {
                files.addAll(commitPoints);
            }
            if (packs != null) {
                final String blobName = publishedSegments.contains(segment.getKey())
                    ? uniqueSegmentBlob(segment.getKey())
                    : segment.getKey() + ".seg";
                final SegmentBlob prepared = prepareSegmentBlob(directory, blobName, files);
                if (prepared.length() <= nodePackMaxBlobBytes) {
                    toNodePack.add(prepared);
                    continue;
                }
            }
            if (publishedSegments.contains(segment.getKey()) == false) {
                try {
                    writeSegmentBlob(directory, termDir, segment.getKey() + ".seg", files, published, lengths);
                    continue;
                } catch (java.nio.file.FileAlreadyExistsException taken) {
                    // An earlier attempt of this term, or a writer that reused the segment's name, left a blob by it.
                }
            }
            writeSegmentBlob(directory, termDir, uniqueSegmentBlob(segment.getKey()), files, published, lengths);
        }
        if (toNodePack.isEmpty() == false) {
            writeToNodePack(packs, directory, toNodePack, published, lengths);
        }
    }

    /** A segment blob laid out but not yet written: its name, header, files in blob order and their sizes. */
    private record SegmentBlob(String name, byte[] header, java.util.List<String> files, Map<String, Long> sizes, long length) {
    }

    private static SegmentBlob prepareSegmentBlob(Directory directory, String blob, java.util.List<String> fileNames) throws IOException {
        final Map<String, Long> sizes = new LinkedHashMap<>();
        for (String fileName : fileNames) {
            sizes.put(fileName, directory.fileLength(fileName));
        }
        final java.util.List<String> ordered = new java.util.ArrayList<>(fileNames);
        ordered.sort(java.util.Comparator.<String>comparingLong(sizes::get).thenComparing(java.util.Comparator.naturalOrder()));
        final byte[] header = segmentBlobHeader(ordered, sizes);
        long length = header.length;
        for (String fileName : ordered) {
            length += sizes.get(fileName);
        }
        return new SegmentBlob(blob, header, java.util.List.copyOf(ordered), sizes, length);
    }

    /**
     * Writes a publish's small segment blobs into the node's next pack, all into the same one, and records where each
     * file landed. Each block is the segment blob exactly as it would be on its own, so a file's range is the block's
     * offset plus the file's offset in the blob.
     */
    private void writeToNodePack(
        NodePackWriter packs,
        Directory directory,
        java.util.List<SegmentBlob> blobs,
        Map<String, String> published,
        Map<String, Long> lengths
    ) throws IOException {
        final java.util.List<NodePackStore.Part> parts = new java.util.ArrayList<>(blobs.size());
        for (SegmentBlob blob : blobs) {
            parts.add(
                new NodePackStore.Part(nodePackIndexUuid, nodePackShard, blob.name(), blob.length(), () -> openSegmentBlob(directory, blob))
            );
        }
        final java.util.List<NodePackWriter.Landed> landed = packs.add(parts);
        for (int i = 0; i < blobs.size(); i++) {
            final SegmentBlob blob = blobs.get(i);
            long offset = blob.header().length;
            for (String fileName : blob.files()) {
                final long size = blob.sizes().get(fileName);
                published.put(fileName, landed.get(i).fileAt(offset, size).encode());
                lengths.put(fileName, size);
                offset += size;
            }
        }
    }

    /** Opens a laid-out segment blob's bytes from the first, holding its files open until the stream is closed. */
    private static java.io.InputStream openSegmentBlob(Directory directory, SegmentBlob blob) throws IOException {
        final java.util.List<IndexInput> opened = new java.util.ArrayList<>();
        boolean success = false;
        try {
            final java.util.List<PackInputStream.Part> parts = new java.util.ArrayList<>();
            long offset = blob.header().length;
            for (String fileName : blob.files()) {
                final IndexInput input = directory.openInput(fileName, IOContext.READONCE);
                opened.add(input);
                if (input.length() != blob.sizes().get(fileName)) {
                    throw new IOException("[" + fileName + "] changed length while it was being published");
                }
                parts.add(new PackInputStream.Part(input, offset, input.length()));
                offset += input.length();
            }
            final PackInputStream stream = new PackInputStream(blob.header(), parts);
            success = true;
            return new java.io.FilterInputStream(stream) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        org.opensearch.common.util.io.IOUtils.close(opened);
                    }
                }
            };
        } finally {
            if (success == false) {
                org.opensearch.common.util.io.IOUtils.closeWhileHandlingException(opened);
            }
        }
    }

    /** A blob name for a segment that cannot take its plain name: its files are already published, or the name is taken. */
    private static String uniqueSegmentBlob(String segment) {
        return segment + "-" + org.opensearch.common.UUIDs.randomBase64UUID() + ".seg";
    }

    /** Writes one segment's blob: the header, then the files, smallest first so an open's first reads sit together. */
    private void writeSegmentBlob(
        Directory directory,
        String termDir,
        String blob,
        java.util.List<String> fileNames,
        Map<String, String> published,
        Map<String, Long> lengths
    ) throws IOException {
        final Map<String, Long> sizes = new LinkedHashMap<>();
        for (String fileName : fileNames) {
            sizes.put(fileName, directory.fileLength(fileName));
        }
        final java.util.List<String> ordered = new java.util.ArrayList<>(fileNames);
        ordered.sort(java.util.Comparator.<String>comparingLong(sizes::get).thenComparing(java.util.Comparator.naturalOrder()));
        final byte[] header = segmentBlobHeader(ordered, sizes);
        final java.util.List<IndexInput> opened = new java.util.ArrayList<>();
        try {
            final java.util.List<PackInputStream.Part> parts = new java.util.ArrayList<>();
            final Map<String, FileLocation> located = new LinkedHashMap<>();
            long offset = header.length;
            for (String fileName : ordered) {
                final IndexInput input = directory.openInput(fileName, IOContext.READONCE);
                opened.add(input);
                final long size = input.length();
                parts.add(new PackInputStream.Part(input, offset, size));
                located.put(fileName, FileLocation.packed(termDir, blob, offset, size));
                offset += size;
            }
            final PackInputStream stream = new PackInputStream(header, parts);
            blobStore.blobContainer(shardBase.add(termDir)).writeBlob(blob, stream, stream.length(), true);
            for (Map.Entry<String, FileLocation> file : located.entrySet()) {
                published.put(file.getKey(), file.getValue().encode());
                lengths.put(file.getKey(), file.getValue().length());
            }
        } finally {
            org.opensearch.common.util.io.IOUtils.close(opened);
        }
    }

    /**
     * A segment blob's header: {@code SEG1}, the length of what follows as four bytes, then JSON naming each file's
     * offset from the start of the blob and its length.
     */
    static byte[] segmentBlobHeader(java.util.List<String> ordered, Map<String, Long> sizes) throws IOException {
        // The offsets count the header itself, whose length depends on how many digits they take: settle it by
        // encoding until the length stops moving, which a second or third pass always does.
        byte[] encoded = new byte[0];
        for (int pass = 0; pass < 8; pass++) {
            long offset = 8L + encoded.length;
            try (org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                builder.startObject().field("version", 1).startObject("files");
                for (String fileName : ordered) {
                    builder.startArray(fileName).value(offset).value(sizes.get(fileName)).endArray();
                    offset += sizes.get(fileName);
                }
                builder.endObject().endObject();
                final byte[] next = org.opensearch.core.common.bytes.BytesReference.toBytes(
                    org.opensearch.core.common.bytes.BytesReference.bytes(builder)
                );
                if (next.length == encoded.length) {
                    final java.nio.ByteBuffer header = java.nio.ByteBuffer.allocate(8 + next.length);
                    header.put(SEGMENT_BLOB_MAGIC).putInt(next.length).put(next);
                    return header.array();
                }
                encoded = next;
            }
        }
        throw new IOException("a segment blob's header did not settle");
    }

    private static final byte[] SEGMENT_BLOB_MAGIC = { 'S', 'E', 'G', '1' };

    /**
     * Opens one published file's bytes, whether it is a blob of its own or a range of a pack.
     *
     * @param manifest the commit naming it
     * @param fileName the file
     * @return its bytes, from the first
     * @throws IOException if the commit does not name it or it cannot be read
     */
    public InputStream openFile(CommitManifest manifest, String fileName) throws IOException {
        return openFile(blobStore, shardBase, manifest, fileName);
    }

    /**
     * Opens one published file's bytes under a shard's base path.
     *
     * @param blobStore the store
     * @param shardBase the shard's base path
     * @param manifest the commit naming the file
     * @param fileName the file
     * @return its bytes, from the first
     * @throws IOException if the commit does not name it or it cannot be read
     */
    public static InputStream openFile(BlobStore blobStore, BlobPath shardBase, CommitManifest manifest, String fileName)
        throws IOException {
        final FileLocation location = manifest.location(fileName);
        if (location == null) {
            throw new java.io.FileNotFoundException(fileName + " is not in the commit");
        }
        final BlobContainer container = blobStore.blobContainer(location.containerPath(shardBase));
        return location.isPacked()
            ? container.readBlob(location.blob(), location.offset(), location.length())
            : container.readBlob(location.blob());
    }

    /**
     * The highest sequence number a published commit holds, read from its commit point.
     *
     * <p>For a commit published before manifests recorded how far into the log they reach: the log keeps each term's
     * highest record after a publish, so a listing cannot tell whether a record is covered, and a crashed fleet's
     * cleanup had to replay every such shard to find out. Reading the commit point answers it in a few small reads.
     *
     * @param manifest the published commit
     * @return its maximum sequence number, or empty if the commit does not record one
     * @throws IOException if the commit cannot be read
     */
    public java.util.OptionalLong committedMaxSeqNo(CommitManifest manifest) throws IOException {
        return committedUserData(manifest, org.opensearch.index.seqno.SequenceNumbers.MAX_SEQ_NO);
    }

    /**
     * The local checkpoint of a published commit, read from its commit point: every operation at or below it is in
     * the commit.
     *
     * <p>Unlike the commit's highest sequence number, which counts operations issued but still being indexed when the
     * commit was taken, this is a bound below which nothing is missing, so it is what a record is judged covered by.
     *
     * @param manifest the published commit
     * @return its local checkpoint, or empty if the commit does not record one
     * @throws IOException if the commit cannot be read
     */
    public java.util.OptionalLong committedLocalCheckpoint(CommitManifest manifest) throws IOException {
        return committedUserData(manifest, org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY);
    }

    /** A sequence number recorded in a published commit point's user data, under a key. */
    private java.util.OptionalLong committedUserData(CommitManifest manifest, String key) throws IOException {
        // Only the commit point and its segments' info files: a few small blobs, nothing of the segments themselves.
        try (Directory scratch = new org.apache.lucene.store.ByteBuffersDirectory()) {
            String commitFile = null;
            for (Map.Entry<String, String> file : manifest.files().entrySet()) {
                final String name = file.getKey();
                final boolean isCommit = name.startsWith(org.apache.lucene.index.IndexFileNames.SEGMENTS + "_");
                if (isCommit == false && name.endsWith(".si") == false) {
                    continue;
                }
                if (isCommit) {
                    commitFile = name;
                }
                try (InputStream in = openFile(manifest, name); IndexOutput out = scratch.createOutput(name, IOContext.DEFAULT)) {
                    final byte[] buffer = new byte[8192];
                    int read;
                    while ((read = in.read(buffer)) > 0) {
                        out.writeBytes(buffer, read);
                    }
                }
            }
            if (commitFile == null) {
                return java.util.OptionalLong.empty();
            }
            final String value = org.apache.lucene.index.SegmentInfos.readCommit(scratch, commitFile).getUserData().get(key);
            return value == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(Long.parseLong(value));
        }
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
            try (
                InputStream in = openFile(manifest.get(), file.getKey());
                IndexOutput out = target.createOutput(file.getKey(), IOContext.DEFAULT)
            ) {
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
