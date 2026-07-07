/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.shardstate.fs;

import org.opensearch.common.io.Channels;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reference {@link ShardStateStore} implementation backed by real, local-filesystem atomicity
 * rather than an object store's native conditional writes. This exists so the shard-head CAS
 * protocol (rfc-serverless-metadata-plane.md &sect;4/&sect;7) can be correctness-tested
 * (including real concurrent-activation races, see {@code FsShardStateStoreTests}) without any
 * cloud dependency; a production deployment would use an implementation backed by S3 If-Match /
 * GCS generation preconditions / Azure ETag If-Match instead, behind the same interface.
 *
 * <p>Concurrency within one {@code FsShardStateStore} instance is guarded by an in-process lock
 * per shard, not {@link FileChannel#lock()}: the JDK's file-lock guarantee is explicitly
 * inter-process only, and a second overlapping {@code lock()} call from a different
 * {@link FileChannel} in the <em>same</em> JVM throws {@code OverlappingFileLockException}
 * rather than blocking. Real cross-process safety in this reference implementation is therefore
 * limited to a single JVM (adequate for tests exercising the CAS protocol's threading
 * correctness); production implementations get real cross-process atomicity from the object
 * store's native conditional write instead.
 *
 * <p>One file per shard, at {@code <baseDir>/<indexUuid>/<shardId>.head}. The file's first 8
 * bytes are a monotonic version counter; the remainder is the {@link ShardHead}'s
 * {@code Writeable} serialization. A zero-length (or absent) file means the shard has never been
 * activated, matching the store's lazy-head-creation semantics.
 */
public final class FsShardStateStore implements ShardStateStore {

    private final Path baseDir;
    private final ConcurrentHashMap<Path, ReentrantLock> locksByHeadFile = new ConcurrentHashMap<>();

    public FsShardStateStore(Path baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public Optional<VersionedShardHead> get(String indexUuid, int shardId) throws IOException {
        Path headFile = headFilePath(indexUuid, shardId);
        if (Files.exists(headFile) == false) {
            return Optional.empty();
        }
        ReentrantLock lock = lockFor(headFile);
        lock.lock();
        try (FileChannel channel = FileChannel.open(headFile, StandardOpenOption.READ)) {
            return readCurrent(channel);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CasResult compareAndSet(String indexUuid, int shardId, Optional<Long> expectedVersion, ShardHead newHead) throws IOException {
        Path headFile = headFilePath(indexUuid, shardId);
        Files.createDirectories(headFile.getParent());

        ReentrantLock lock = lockFor(headFile);
        lock.lock();
        try (
            FileChannel channel = FileChannel.open(headFile, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
        ) {
            Optional<VersionedShardHead> current = readCurrent(channel);

            if (expectedVersion.isEmpty() && current.isPresent()) {
                return CasResult.VERSION_CONFLICT;
            }
            if (expectedVersion.isPresent() && (current.isEmpty() || !expectedVersion.get().equals(current.get().version()))) {
                return CasResult.VERSION_CONFLICT;
            }

            long newVersion = current.map(VersionedShardHead::version).orElse(0L) + 1;
            writeNew(channel, newVersion, newHead);
            return CasResult.SUCCESS;
        } finally {
            lock.unlock();
        }
    }

    private ReentrantLock lockFor(Path headFile) {
        return locksByHeadFile.computeIfAbsent(headFile, p -> new ReentrantLock());
    }

    private Optional<VersionedShardHead> readCurrent(FileChannel channel) throws IOException {
        long size = channel.size();
        if (size == 0) {
            return Optional.empty();
        }
        byte[] bytes = Channels.readFromFileChannel(channel, 0, (int) size);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long version = buffer.getLong();
        byte[] headBytes = new byte[buffer.remaining()];
        buffer.get(headBytes);
        ShardHead head = new ShardHead(StreamInput.wrap(headBytes));
        return Optional.of(new VersionedShardHead(head, version));
    }

    private void writeNew(FileChannel channel, long newVersion, ShardHead newHead) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        newHead.writeTo(out);
        byte[] headBytes = BytesReference.toBytes(out.bytes());

        ByteBuffer buffer = ByteBuffer.allocate(8 + headBytes.length);
        buffer.putLong(newVersion);
        buffer.put(headBytes);
        buffer.flip();

        channel.truncate(0);
        Channels.writeToChannel(buffer.array(), channel, 0);
        channel.force(true);
    }

    private Path headFilePath(String indexUuid, int shardId) {
        return baseDir.resolve(indexUuid).resolve(shardId + ".head");
    }
}
