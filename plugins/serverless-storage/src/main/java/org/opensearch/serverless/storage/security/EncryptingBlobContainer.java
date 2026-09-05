/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security;

import org.opensearch.common.blobstore.BlobContainer;

import javax.crypto.SecretKey;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Encrypts blob content at rest, transparently to every caller that only knows {@link
 * BlobContainer} (rfc-serverless-opensearch.md's security/encryption gap): wrapping a real
 * container in this one, anywhere a {@link BlobContainer} is expected, is the entire integration
 * -- {@code BlobContainerBundleStore}, {@code BlobContainerManifestStore}, {@code
 * BlobContainerShardStateStore}, and the WAL chunk writer all work unchanged, exactly because they
 * were already written against the interface, not a concrete backend.
 *
 * <p>Ciphertext format: a fixed header ({@link BlockLayout}) followed by a sequence of
 * independently-encrypted, independently-authenticated fixed-size blocks -- see {@link
 * BlockLayout}'s javadoc for the exact byte layout. Each block is its own {@link AesGcmCipher}
 * envelope with a fresh random IV and its own GCM tag, so a corrupted or tampered block fails to
 * decrypt loudly rather than silently, exactly like the previous single-envelope-per-blob format
 * did -- the difference is granularity, not the authentication guarantee.
 *
 * <h2>Each block is bound to its own position (format version 2)</h2>
 *
 * <p>A GCM tag proves the bytes under it were not altered. On its own it proves nothing about
 * <em>where those bytes belong</em>, and until this change nothing else did either: every block was
 * a self-contained envelope under one node-wide key, so an attacker with object-store write access
 * -- the trust boundary rfc-serverless-opensearch.md &sect;12 says is shared -- could move block 7
 * of index A's bundle into index B's bundle, reorder blocks within one blob, or lower the header's
 * {@code totalPlaintextLength} and drop the tail, and every surviving block would still decrypt
 * <em>and still authenticate</em>. Nothing failed, because nothing had ever been asked.
 *
 * <p>Each block's tag now covers associated data (see {@link #associatedDataFor}) naming the index
 * UUID, shard, container path, blob name, format version, declared block size, declared total
 * length, and the block's own index. Moving a block anywhere -- another blob, another index,
 * another offset, or the same offset under a rewritten header -- changes at least one of those
 * fields, so the tag check fails. The header is authenticated as a side effect, at no cost: AAD is
 * authenticated but not encrypted, so the blob does not grow by a byte.
 *
 * <h2>Reading blobs written before this change, and how an operator migrates</h2>
 *
 * <p>A blob whose header says {@link BlockLayout#FORMAT_VERSION_LEGACY_NO_AAD version 1} is read
 * with no associated data, exactly as it was written. This is not a compatibility courtesy that
 * costs nothing: <b>a version 1 blob remains substitutable, reorderable and truncatable</b>, and it
 * stays that way for as long as it exists. There is no way to upgrade a blob in place, because
 * rewriting one requires the plaintext, which requires the key, which lives on the node -- not in
 * any offline tool this plugin ships.
 *
 * <p><b>Migrating.</b> A version 1 blob is replaced the moment its content is rewritten, so the
 * migration is the normal life cycle of the data, driven deliberately:
 * <ol>
 *   <li>Upgrade every node to a build containing this change. From that point every new bundle,
 *       manifest and cache entry is written at version 2.</li>
 *   <li>Force each serverless index's existing bundles to be rewritten. Compaction is the supported
 *       way ({@code POST /_plugins/_serverless/storage/{index_uuid}/{shard_id}/_compact}, or
 *       {@code serverless_storage.compaction.interval} left running until it has swept the shard);
 *       compaction reads the old bundles and publishes new ones, which are written at version 2.
 *       An index with no compactable history can instead be reindexed into a fresh index.</li>
 *   <li>Let {@code serverless_storage.gc.interval} run long enough to delete the superseded
 *       manifests and their now-unreferenced bundles. Until GC has reclaimed them the old version 1
 *       objects are still present in the bucket, and still readable by anyone with bucket access.</li>
 *   <li>Set {@code serverless_storage.encryption.require_authenticated_blocks: true} on every node
 *       and restart. From then on a version 1 blob is <em>refused</em> rather than read, so the
 *       guarantee this class advertises is actually enforced instead of merely usually true. Do
 *       this last: setting it before step 3 completes makes any surviving version 1 blob an
 *       unreadable shard, which is the correct outcome but not a pleasant surprise.</li>
 * </ol>
 * The setting defaults to {@code false} precisely so an upgrade does not turn existing data into an
 * outage; it is the operator's declaration that step 3 finished, and this class has no way to
 * verify that claim itself.
 *
 * <h2>Which key</h2>
 *
 * <p>Encryption and decryption both go through {@link EncryptionKeyProvider#currentKey(String)},
 * passing this container's own index UUID -- not the index-agnostic {@link
 * EncryptionKeyProvider#currentKey()} this class used to call, which silently defeated the whole
 * point of a per-index-aware provider. <b>This does not by itself give per-index key isolation</b>:
 * no node configuration can build a provider that returns different keys per index today (see
 * {@link PerIndexEncryptionKeyProvider}, which is reachable only from tests). It means the call
 * site is now correct, so wiring such a provider becomes a configuration change rather than a code
 * change. Until then, &sect;12's per-index key domain does not exist -- see this plugin's README,
 * "Security boundaries and known gaps."
 *
 * <p><b>Ranged reads are now genuinely partial</b>, closing what was previously a known, deliberate
 * tradeoff: {@link #readBlob(String, long, long)} ({@code BlobContainerBundleStore}'s mechanism for
 * fetching just one file's bytes out of a bundle) fetches a small header, then computes exactly
 * which blocks overlap the requested range and reads *only* that contiguous byte span from the
 * delegate -- never the whole blob -- before decrypting just those blocks and slicing out the exact
 * requested sub-range. A read that happens to need only one block only ever decrypts that one
 * block. This is why {@link org.opensearch.serverless.storage.format.LocalDiskCachingBundleStore}
 * still matters even now: it avoids the network fetch and the header round-trip entirely on a
 * cache hit, which this class's per-request block fetch does not.
 *
 * <p>{@code readRegister}/{@code compareAndSwapRegister} are deliberately passed through
 * unencrypted: shard-head/pin registers contain no document data, only node ids, terms, and
 * generation numbers, and encrypting them would need the same CAS-safe whole-value treatment
 * {@code BlobContainerShardStateStore} already gives their *plaintext* today; segment and WAL data
 * (this class's actual target) carry the sensitive payload.
 *
 * <p><b>Registers are also unauthenticated, which is the part that matters and was never written
 * down.</b> "Not encrypted" is a confidentiality statement and it is the defensible one -- a
 * generation number is not a secret. "Not authenticated" is an integrity statement and it is not
 * defensible in the same breath: a shard's head register is what says <em>which manifest generation
 * is current</em>, so a caller with bucket write access can point a shard at any manifest that
 * exists, including a stale one, and every subsequent read is of a correctly-decrypting, correctly-
 * authenticated, wrong commit. The term-fencing CAS protocol defends against concurrent writers
 * racing each other; it does not defend against a writer that is outside the protocol entirely.
 * Closing this needs a MAC over the register value keyed by the same {@link EncryptionKeyProvider},
 * which is a wire-format change to {@code BlobContainerShardStateStore}'s value encoding and is not
 * done. Recorded here rather than left implied.
 */
public final class EncryptingBlobContainer extends RegisterDelegatingBlobContainer {

    /** 64 KiB: large enough that most bundle files fit in one or two blocks, small enough that a ranged read of a single file doesn't drag in unrelated data. */
    private static final int DEFAULT_BLOCK_SIZE_BYTES = 64 * 1024;

    /** What a {@link BlockLayout#FORMAT_VERSION_LEGACY_NO_AAD} block is decrypted with: nothing, because nothing is what it was written with. */
    private static final byte[] NO_LEGACY_ASSOCIATED_DATA = new byte[0];

    private final EncryptionKeyProvider keyProvider;
    private final int blockSizeBytes;

    /**
     * The index this container's blobs belong to. Two jobs, and they are separate on purpose:
     * it selects the key ({@link EncryptionKeyProvider#currentKey(String)}) and it is the first
     * field of every block's associated data. Even under a provider that returns one key for every
     * index -- the only kind any node configuration can build today -- the second job alone is what
     * stops one index's blocks from decrypting inside another's blob.
     */
    private final String indexUuid;

    /**
     * The position of this container within its index, as a stable string: {@code <shardId>} for the
     * shard container itself, {@code <shardId>/<childName>/...} for anything reached through {@link
     * #wrapChild}.
     *
     * <p>Deliberately <em>not</em> {@code delegate.path()}. The delegate's path is absolute within
     * the repository, so it contains the configured {@code base_path} -- and an operator who
     * relocates a bucket prefix, or restores into a repository registered under a different base
     * path, would find every blob's AAD had changed and every shard unreadable. What a block needs
     * to be bound to is where it sits <em>inside its own index</em>, which is invariant under all of
     * that.
     */
    private final String containerScope;

    /**
     * Whether a blob at {@link BlockLayout#FORMAT_VERSION_LEGACY_NO_AAD} is refused instead of read.
     * See this class's javadoc, "how an operator migrates" -- this is step 4, and it is the only
     * thing that turns the version 2 guarantee from "true of everything written since the upgrade"
     * into "true of everything this node will read."
     */
    private final boolean requireAuthenticatedBlocks;

    /**
     * Wraps a delegate container with block-level AES/GCM encryption at the default block size.
     *
     * @param delegate the underlying blob container to encrypt reads/writes against.
     * @param keyProvider supplies the key used for every encrypt/decrypt call.
     * @param indexUuid the UUID of the index whose data lives in {@code delegate}; selects the key and binds every block.
     * @param shardId the shard within that index.
     */
    public EncryptingBlobContainer(BlobContainer delegate, EncryptionKeyProvider keyProvider, String indexUuid, int shardId) {
        this(delegate, keyProvider, indexUuid, shardId, false);
    }

    /**
     * Wraps a delegate container with block-level AES/GCM encryption at the default block size,
     * optionally refusing legacy unauthenticated blobs.
     *
     * @param delegate the underlying blob container to encrypt reads/writes against.
     * @param keyProvider supplies the key used for every encrypt/decrypt call.
     * @param indexUuid the UUID of the index whose data lives in {@code delegate}; selects the key and binds every block.
     * @param shardId the shard within that index.
     * @param requireAuthenticatedBlocks {@code true} to refuse blobs written before associated data existed.
     */
    public EncryptingBlobContainer(
        BlobContainer delegate,
        EncryptionKeyProvider keyProvider,
        String indexUuid,
        int shardId,
        boolean requireAuthenticatedBlocks
    ) {
        this(delegate, keyProvider, indexUuid, String.valueOf(shardId), requireAuthenticatedBlocks, DEFAULT_BLOCK_SIZE_BYTES);
    }

    /**
     * Wraps a delegate container with block-level AES/GCM encryption at a given block size.
     *
     * @param delegate the underlying blob container to encrypt reads/writes against.
     * @param keyProvider supplies the key used for every encrypt/decrypt call.
     * @param indexUuid the UUID of the index whose data lives in {@code delegate}.
     * @param shardId the shard within that index.
     * @param blockSizeBytes plaintext bytes per block; package-visible so tests can use a small value to exercise multi-block logic cheaply.
     */
    EncryptingBlobContainer(BlobContainer delegate, EncryptionKeyProvider keyProvider, String indexUuid, int shardId, int blockSizeBytes) {
        this(delegate, keyProvider, indexUuid, String.valueOf(shardId), false, blockSizeBytes);
    }

    private EncryptingBlobContainer(
        BlobContainer delegate,
        EncryptionKeyProvider keyProvider,
        String indexUuid,
        String containerScope,
        boolean requireAuthenticatedBlocks,
        int blockSizeBytes
    ) {
        super(delegate);
        this.keyProvider = keyProvider;
        this.indexUuid = indexUuid;
        this.containerScope = containerScope;
        this.requireAuthenticatedBlocks = requireAuthenticatedBlocks;
        this.blockSizeBytes = blockSizeBytes;
    }

    /**
     * A child container keeps this container's key domain but extends the scope by the child's own
     * name, so a blob called {@code x} in {@code <shard>/sub/} is bound differently from a blob
     * called {@code x} in {@code <shard>/}. Without that, {@code children()} would hand back
     * containers whose blocks were interchangeable with the parent's -- the same substitution this
     * class exists to prevent, reintroduced one directory down.
     *
     * <p>{@code FilterBlobContainer#children} does not pass the child's map key, so the name is
     * recovered from the child's own path, which is where the key came from in the first place.
     */
    @Override
    protected BlobContainer wrapChild(BlobContainer child) {
        String[] parts = child.path().toArray();
        String childName = parts.length == 0 ? "" : parts[parts.length - 1];
        return new EncryptingBlobContainer(
            child,
            keyProvider,
            indexUuid,
            containerScope + "/" + childName,
            requireAuthenticatedBlocks,
            blockSizeBytes
        );
    }

    /**
     * The bytes each block's GCM tag additionally covers: everything that says <em>where this block
     * belongs</em>, and nothing that is a secret (AAD is authenticated, not encrypted).
     *
     * <p>Every field earns its place by naming a substitution it makes impossible. {@code indexUuid}
     * and {@code containerScope} stop a block moving between indices or shards; {@code blobName}
     * stops it moving between blobs in one shard; {@code blockIndex} stops reordering within one
     * blob; {@code formatVersion}, {@code blockSizeBytes} and {@code totalPlaintextLength} are the
     * header's own fields, so including them means a rewritten header -- a truncation, or a forged
     * allocation size -- fails the first block it is used to read. The {@code "EBCv2"} prefix and
     * the {@code |} separators keep it unambiguous: without a separator, index {@code "ab"} + shard
     * {@code "1"} and index {@code "a"} + shard {@code "b1"} would produce identical AAD.
     *
     * <p>Encoded as UTF-8 text rather than a packed binary struct because it is never parsed, only
     * compared: nothing reads these bytes back, so the only property that matters is that the writer
     * and the reader build the same string from the same facts, and a readable one is far easier to
     * keep that way. It also means a failed tag can be diagnosed by printing it.
     */
    private byte[] associatedDataFor(String blobName, BlockLayout.Header header, int blockIndex) {
        StringBuilder aad = new StringBuilder(96);
        aad.append("EBCv2|")
            .append(indexUuid)
            .append('|')
            .append(containerScope)
            .append('|')
            .append(blobName)
            .append('|')
            .append(header.formatVersion())
            .append('|')
            .append(header.blockSizeBytes())
            .append('|')
            .append(header.totalPlaintextLength())
            .append('|')
            .append(blockIndex);
        return aad.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        // A single unranged fetch of the whole ciphertext object, not readHeader() followed by
        // decryptRange()'s own separate ranged fetch -- those two calls together already cover
        // the entire object (the header's own bytes, immediately followed by every block's own
        // ciphertext, with no gap -- see BlockLayout#blockDiskOffset(0, ...) == HEADER_SIZE_BYTES),
        // so doing them as two requests instead of one doubled the GET cost of every full-object
        // read for no benefit. This is the manifest-read hot path whenever encryption is enabled
        // (BlobContainerManifestStore#readBlob always uses this no-range overload), so the doubled
        // cost was real, not theoretical.
        byte[] wholeObject = readAllAndClose(delegate.readBlob(blobName));
        BlockLayout.Header header = BlockLayout.decodeHeader(Arrays.copyOfRange(wholeObject, 0, BlockLayout.HEADER_SIZE_BYTES));
        requireReadableFormat(blobName, header);
        byte[] blockCiphertext = Arrays.copyOfRange(wholeObject, BlockLayout.HEADER_SIZE_BYTES, wholeObject.length);
        int blockCount = BlockLayout.blockCount(header.blockSizeBytes(), header.totalPlaintextLength());
        return new ByteArrayInputStream(decryptBlockRange(blobName, blockCiphertext, header, 0, blockCount - 1));
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        // Rejected before the header read, not after: a negative length used to survive
        // Math.min (which has no floor at zero), reach decryptRange, and produce a negative block
        // index, a negative disk offset, and a delegate read at a negative position. Only internal
        // callers reach this overload today, so this was never exploitable -- but "no caller passes
        // a bad value" is an invariant held somewhere else, and this is the method that would break
        // if it stopped being true.
        if (length < 0) {
            throw new IOException("range length must be >= 0, got " + length);
        }
        BlockLayout.Header header = readHeader(blobName);
        requireReadableFormat(blobName, header);
        if (position < 0 || position > header.totalPlaintextLength()) {
            throw new IOException("range start " + position + " out of bounds for blob of length " + header.totalPlaintextLength());
        }
        long clampedLength = Math.min(length, header.totalPlaintextLength() - position);
        return new ByteArrayInputStream(decryptRange(blobName, header, position, clampedLength));
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        byte[] ciphertext = encryptBlocked(blobName, readAllAndClose(inputStream));
        delegate.writeBlob(blobName, new ByteArrayInputStream(ciphertext), ciphertext.length, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        byte[] ciphertext = encryptBlocked(blobName, readAllAndClose(inputStream));
        delegate.writeBlobAtomic(blobName, new ByteArrayInputStream(ciphertext), ciphertext.length, failIfAlreadyExists);
    }

    // The three writeBlob*WithMetadata overloads are not covered by writeBlob/writeBlobAtomic's own
    // overrides above -- FilterBlobContainer (this class's superclass's superclass) passes them
    // straight through to the delegate unchanged, which would write plaintext to the real object
    // store. Nothing in this plugin currently calls these (verified: zero callers under
    // plugins/serverless-storage/src/main/java), but leaving them unguarded would silently defeat
    // this class's entire purpose the moment something does -- so they encrypt exactly like
    // writeBlob/writeBlobAtomic, just forwarding the metadata/cryptoMetadata arguments unchanged.

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        java.util.Map<String, String> metadata
    ) throws IOException {
        byte[] ciphertext = encryptBlocked(blobName, readAllAndClose(inputStream));
        delegate.writeBlobWithMetadata(blobName, new ByteArrayInputStream(ciphertext), ciphertext.length, failIfAlreadyExists, metadata);
    }

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        java.util.Map<String, String> metadata,
        org.opensearch.cluster.metadata.CryptoMetadata cryptoMetadata
    ) throws IOException {
        byte[] ciphertext = encryptBlocked(blobName, readAllAndClose(inputStream));
        delegate.writeBlobWithMetadata(
            blobName,
            new ByteArrayInputStream(ciphertext),
            ciphertext.length,
            failIfAlreadyExists,
            metadata,
            cryptoMetadata
        );
    }

    @Override
    public void writeBlobAtomicWithMetadata(
        String blobName,
        InputStream inputStream,
        java.util.Map<String, String> metadata,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        byte[] ciphertext = encryptBlocked(blobName, readAllAndClose(inputStream));
        delegate.writeBlobAtomicWithMetadata(
            blobName,
            new ByteArrayInputStream(ciphertext),
            metadata,
            ciphertext.length,
            failIfAlreadyExists
        );
    }

    private static byte[] readAllAndClose(InputStream in) throws IOException {
        try (InputStream toClose = in) {
            return toClose.readAllBytes();
        }
    }

    /**
     * Refuses a blob written before associated data existed, when the operator has declared
     * migration complete. The message names the setting and the blob, because the only useful
     * response to it is either "finish migrating that shard" or "this node was switched over too
     * early" -- and an operator cannot tell which from a bare authentication failure.
     */
    private void requireReadableFormat(String blobName, BlockLayout.Header header) throws IOException {
        if (requireAuthenticatedBlocks && header.authenticatesAssociatedData() == false) {
            throw new IOException(
                "blob ["
                    + blobName
                    + "] in ["
                    + indexUuid
                    + "]["
                    + containerScope
                    + "] is at block-encrypted format version "
                    + header.formatVersion()
                    + ", which predates per-block associated data and is therefore substitutable; refusing it because "
                    + "serverless_storage.encryption.require_authenticated_blocks is set. Either rewrite this shard's "
                    + "bundles (compaction, then GC) or unset that node setting until migration completes."
            );
        }
    }

    private byte[] encryptBlocked(String blobName, byte[] plaintext) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // The header is built first and then handed to associatedDataFor unchanged, so the bytes
        // that go on disk and the bytes that go into every tag are provably the same three fields.
        // Recomputing them independently on the read side is what the AAD check actually verifies.
        BlockLayout.Header header = new BlockLayout.Header(blockSizeBytes, plaintext.length);
        out.write(BlockLayout.encodeHeader(header));
        int blockCount = BlockLayout.blockCount(blockSizeBytes, plaintext.length);
        SecretKey key = keyProvider.currentKey(indexUuid);
        for (int blockIndex = 0; blockIndex < blockCount; blockIndex++) {
            int start = blockIndex * blockSizeBytes;
            int len = BlockLayout.blockPlaintextLength(blockIndex, blockSizeBytes, plaintext.length);
            byte[] blockPlaintext = Arrays.copyOfRange(plaintext, start, start + len);
            out.write(AesGcmCipher.encrypt(blockPlaintext, key, associatedDataFor(blobName, header, blockIndex)));
        }
        return out.toByteArray();
    }

    private BlockLayout.Header readHeader(String blobName) throws IOException {
        byte[] headerBytes = readAllAndClose(delegate.readBlob(blobName, 0, BlockLayout.HEADER_SIZE_BYTES));
        return BlockLayout.decodeHeader(headerBytes);
    }

    /**
     * Fetches and decrypts exactly the blocks overlapping plaintext range {@code [position,
     * position + length)}, in one additional ranged read of the delegate (the blocks a range needs
     * are always contiguous on disk, so this never issues more than one fetch beyond the header
     * read), then slices out precisely the requested sub-range.
     */
    private byte[] decryptRange(String blobName, BlockLayout.Header header, long position, long length) throws IOException {
        if (length == 0) {
            return new byte[0];
        }
        int blockSize = header.blockSizeBytes();
        long totalLength = header.totalPlaintextLength();
        int firstBlock = BlockLayout.blockIndexFor(position, blockSize);
        int lastBlock = BlockLayout.blockIndexFor(position + length - 1, blockSize);

        long diskStart = BlockLayout.blockDiskOffset(firstBlock, blockSize);
        long diskEnd = BlockLayout.blockDiskOffset(lastBlock, blockSize) + BlockLayout.blockCiphertextLength(
            lastBlock,
            blockSize,
            totalLength
        );
        byte[] rawBlocks = readAllAndClose(delegate.readBlob(blobName, diskStart, diskEnd - diskStart));

        byte[] concatenated = decryptBlockRange(blobName, rawBlocks, header, firstBlock, lastBlock);
        int sliceStart = (int) (position - (long) firstBlock * blockSize);
        return Arrays.copyOfRange(concatenated, sliceStart, sliceStart + (int) length);
    }

    /**
     * Decrypts blocks {@code [firstBlock, lastBlock]} (inclusive) given their raw, contiguous
     * ciphertext bytes already in hand ({@code rawBlocks[0]} is the first byte of {@code
     * firstBlock}'s own ciphertext) -- shared by both {@link #readBlob(String)} (the whole object,
     * already fully fetched in one unranged call) and {@link #decryptRange} (just the blocks one
     * ranged sub-fetch needs). Neither caller does any additional I/O here.
     */
    private byte[] decryptBlockRange(String blobName, byte[] rawBlocks, BlockLayout.Header header, int firstBlock, int lastBlock)
        throws IOException {
        int blockSize = header.blockSizeBytes();
        long totalLength = header.totalPlaintextLength();
        ByteArrayOutputStream decryptedBlocks = new ByteArrayOutputStream();
        int cursor = 0;
        SecretKey key = keyProvider.currentKey(indexUuid);
        // Version 1 blobs were written with no associated data at all, so they must be read with
        // none: passing the version 2 AAD would fail every tag and turn "old but intact data" into
        // "unreadable shard." The version came from the header, which at version 1 is exactly the
        // unauthenticated field this whole change exists to stop trusting -- which is why the
        // migration in this class's javadoc ends with a setting that stops accepting the answer.
        boolean withAad = header.authenticatesAssociatedData();
        for (int blockIndex = firstBlock; blockIndex <= lastBlock; blockIndex++) {
            int cipherLen = BlockLayout.blockCiphertextLength(blockIndex, blockSize, totalLength);
            if (cursor + cipherLen > rawBlocks.length) {
                // Reachable only from a header that overstates the data actually present, i.e. a
                // truncated or forged blob. Arrays.copyOfRange would zero-pad silently and hand the
                // padding to GCM, which fails -- correct, but with a message about authentication
                // that says nothing about the real cause.
                throw new IOException(
                    "blob ["
                        + blobName
                        + "] declares block "
                        + blockIndex
                        + " of "
                        + cipherLen
                        + " ciphertext bytes, but only "
                        + (rawBlocks.length - cursor)
                        + " bytes remain; the blob is truncated or its header does not describe it"
                );
            }
            byte[] blockCiphertext = Arrays.copyOfRange(rawBlocks, cursor, cursor + cipherLen);
            byte[] aad = withAad ? associatedDataFor(blobName, header, blockIndex) : NO_LEGACY_ASSOCIATED_DATA;
            decryptedBlocks.write(AesGcmCipher.decrypt(blockCiphertext, key, aad));
            cursor += cipherLen;
        }
        return decryptedBlocks.toByteArray();
    }
}
