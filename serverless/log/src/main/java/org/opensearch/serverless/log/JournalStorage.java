/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.server.raftlog.segmented.OpenSearchLogRecovery;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Fail-closed guard around Ratis's permissive RECOVER-on-empty-directory behavior. */
final class JournalStorage implements AutoCloseable {
    // Strong references preserve OS locks even when a closed journal becomes unreachable.
    // A voter host normally opens one volume. The hard cap bounds failed-startup quarantine.
    private static final List<JournalStorage> PROCESS_VOLUMES = new ArrayList<>();
    private volatile boolean processOwned;
    private static final String BINDING = "journal-binding";
    private static final String INITIALIZED = "journal-initialized";
    private static final String FLOOR = "journal-floor";
    private final UUID deployment;
    private final Map<UUID, JournalDurableFloor> floors = new ConcurrentHashMap<>();
    private final JournalDiskIdentity.Lease lease;
    private final byte[] binding;
    private final Set<UUID> groups;

    private JournalStorage(JournalDiskIdentity.Lease lease, byte[] binding, Set<UUID> groups, JournalConfiguration configuration) {
        this.lease = lease;
        this.deployment = configuration.deploymentId();
        this.binding = binding;
        this.groups = Set.copyOf(groups);
    }

    static JournalStorage open(JournalConfiguration configuration) throws IOException {
        final var lease = JournalDiskIdentity.acquire(configuration.directory());
        JournalStorage storage = null;
        try {
            if (lease.id.equals(configuration.localVoterId()) == false) {
                throw new IOException("journal volume identity differs from authorized voter membership");
            }
            final byte[] binding = binding(configuration);
            final Set<UUID> groups = new HashSet<>();
            configuration.groups().forEach(group -> groups.add(group.id()));
            storage = new JournalStorage(lease, binding, groups, configuration);
            if (configuration.bootstrap()) {
                try (var entries = Files.list(lease.directory)) {
                    if (entries.anyMatch(path -> Set.of("voter-identity", "voter-lock").contains(path.getFileName().toString()) == false)) {
                        throw new IOException("bootstrap requires a blank, unbound journal volume");
                    }
                }
                storage.publish(lease.directory.resolve(BINDING));
            } else {
                storage.requireBinding(lease.directory.resolve(BINDING));
                for (UUID group : groups) {
                    final Path directory = storage.groupDirectory(group);
                    requireDirectory(directory);
                    requireDirectory(directory.resolve("current"));
                    requireRegular(directory.resolve("current/raft-meta"));
                    requireRegular(directory.resolve("current/raft-meta.conf"));
                    storage.requireBinding(directory.resolve(INITIALIZED));
                    final var floor = JournalDurableFloor.open(directory.resolve(FLOOR), configuration.deploymentId(), lease.id, group);
                    storage.floors.put(group, floor);
                    final long allowance = 4L * configuration.limits().maximumPendingRequests() + 32;
                    final long retained = configuration.limits().maximumRetainedEntries();
                    final long maximumFiles = retained > Long.MAX_VALUE - allowance ? Long.MAX_VALUE : retained + allowance;
                    final long observed = OpenSearchLogRecovery.verifyAndForce(
                        directory.resolve("current"),
                        floor.index(),
                        configuration.limits().queueBytes(),
                        (long) configuration.limits().segmentBytes() + configuration.limits().queueBytes() + 1024,
                        maximumFiles
                    );
                    floor.persist(observed);
                }
                final var allowed = new HashSet<>(Set.of("voter-identity", "voter-lock", BINDING));
                groups.forEach(group -> allowed.add(group.toString()));
                try (var entries = Files.list(lease.directory)) {
                    if (entries.anyMatch(path -> allowed.contains(path.getFileName().toString()) == false)) {
                        throw new IOException("journal contains unassigned or incomplete storage");
                    }
                }
            }
            return storage;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                if (storage == null) {
                    lease.close();
                } else {
                    storage.close();
                }
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    void retainUntilProcessExit() throws IOException {
        synchronized (PROCESS_VOLUMES) {
            if (processOwned) {
                return;
            }
            if (PROCESS_VOLUMES.size() >= 128) {
                throw new IOException("process journal volume limit reached; restart this JVM before opening more volumes");
            }
            PROCESS_VOLUMES.add(this);
            processOwned = true;
        }
    }

    boolean isProcessOwned() {
        return processOwned;
    }

    Path directory() {
        return lease.directory;
    }

    Path groupDirectory(UUID group) throws IOException {
        if (groups.contains(group) == false) {
            throw new IOException("unassigned physical group");
        }
        return directory().resolve(group.toString());
    }

    synchronized void initialize(UUID group) throws IOException {
        final Path directory = groupDirectory(group);
        requireDirectory(directory);
        requireDirectory(directory.resolve("current"));
        requireDirectory(directory.resolve("sm"));
        requireRegular(directory.resolve("current/raft-meta"));
        if (floors.containsKey(group) == false) {
            floors.put(group, JournalDurableFloor.create(directory.resolve(FLOOR), deployment, lease.id, group));
        }
        JournalDiskIdentity.forceDirectory(directory.resolve("current"));
        JournalDiskIdentity.forceDirectory(directory.resolve("sm"));
        JournalDiskIdentity.forceDirectory(directory);
        JournalDiskIdentity.forceDirectory(directory());
        final Path marker = directory.resolve(INITIALIZED);
        if (Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) {
            requireBinding(marker);
        } else {
            publish(marker);
        }
    }

    void flush(UUID group) throws IOException {
        JournalDiskIdentity.forceDirectory(groupDirectory(group).resolve("current"));
    }

    void recordFlushed(UUID group, long index) throws IOException {
        final var floor = floors.get(group);
        if (floor == null) {
            throw new IOException("journal durable boundary was not initialized");
        }
        floor.persist(index);
    }

    void beforeTruncate(UUID group, long firstIndex) throws IOException {
        if (firstIndex < 0) {
            throw new IOException("invalid journal truncation boundary");
        }
        recordFlushed(group, firstIndex - 1);
    }

    private void requireBinding(Path path) throws IOException {
        requireRegular(path);
        final byte[] actual;
        try (var input = Files.newInputStream(path)) {
            actual = input.readNBytes(binding.length + 1);
        }
        if (Arrays.equals(actual, binding) == false) {
            throw new IOException("journal storage binding is corrupt or differs from authorized placement");
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.force(true);
        }
        JournalDiskIdentity.forceDirectory(path.getParent());
    }

    private void publish(Path destination) throws IOException {
        final Path temporary = Files.createTempFile(destination.getParent(), "journal-binding-", ".tmp");
        try {
            try (var channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                final var bytes = ByteBuffer.wrap(binding);
                while (bytes.hasRemaining()) {
                    channel.write(bytes);
                }
                channel.force(true);
            }
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE);
            JournalDiskIdentity.forceDirectory(destination.getParent());
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static byte[] binding(JournalConfiguration configuration) throws IOException {
        final var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(3);
            out.writeUTF(configuration.deploymentId().toString());
            out.writeUTF(configuration.localVoterId().toString());
            for (var group : configuration.groups()
                .stream()
                .sorted(java.util.Comparator.comparing(group -> group.id().toString()))
                .toList()) {
                out.writeUTF(group.id().toString());
                for (var peer : group.peers().stream().sorted(java.util.Comparator.comparing(peer -> peer.id().toString())).toList()) {
                    out.writeUTF(peer.id().toString());
                    out.writeUTF(peer.host());
                    out.writeInt(peer.port());
                    out.writeUTF(peer.availabilityZone());
                }
            }
        }
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static void requireRegular(Path path) throws IOException {
        if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) == false) {
            throw new IOException("required journal file is missing or not regular: " + path);
        }
    }

    private static void requireDirectory(Path path) throws IOException {
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) == false) {
            throw new IOException("required journal directory is missing or not a directory: " + path);
        }
    }

    @Override
    public void close() throws IOException {
        if (processOwned) {
            throw new IOException("native journal volume remains leased until this JVM exits");
        }
        IOException failedClose = null;
        for (var floor : floors.values()) {
            try {
                floor.close();
            } catch (IOException failure) {
                if (failedClose == null) {
                    failedClose = failure;
                } else {
                    failedClose.addSuppressed(failure);
                }
            }
        }
        try {
            lease.close();
        } catch (IOException failure) {
            if (failedClose == null) {
                failedClose = failure;
            } else {
                failedClose.addSuppressed(failure);
            }
        }
        if (failedClose != null) {
            throw failedClose;
        }
    }
}
