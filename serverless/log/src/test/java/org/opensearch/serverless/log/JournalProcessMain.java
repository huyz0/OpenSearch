/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Bounded binary control channel for a real journal JVM; never a production endpoint. */
public final class JournalProcessMain {
    static final int READY = 0x4a525459;
    static final int LEADER = 1;
    static final int APPEND = 2;
    static final int SCAN = 3;
    static final int CLOSE = 4;
    static final int STALL = 5;
    static final int STALLED = 6;
    static final int CLOSE_STAY = 7;
    static final int ACTIVATE = 8;
    static final int IO_OPEN = 9;
    static final int ACTIVATION_COORDINATE = 10;
    static final int APPEND_BATCH = 11;
    static final int PROGRESS = 12;
    static final int FAIL_STORAGE = 13;
    static final int STORAGE_FAILED = 14;
    static final int ADVANCE = 15;
    static final int RECEIPT = 16;
    static final int RELEASE = 17;
    static final int VOTERS = 18;
    static final int BACKGROUND_START = 30;
    static final int BACKGROUND_STATUS = 31;
    static final int BACKGROUND_STOP = 32;
    static final int VOTE = 19;

    static final int FRAME_LIMIT = 17 * 1024;

    private JournalProcessMain() {}

    @SuppressForbidden(reason = "Child fixture stdout carries the bounded binary control protocol; all logs go to stderr")
    private static BufferedOutputStream protocolOutput() {
        return new BufferedOutputStream(System.out);
    }

    /**
     * Runs one voter process from a trusted fixture's placement arguments.
     * @param arguments volume, namespace, local UUID, bootstrap, group UUID CSV, three peer triples, and optional activation/membership CSVs
     * @throws Exception on startup or broken control channel
     */
    public static void main(String[] arguments) throws Exception {
        if (arguments.length < 14 || arguments.length > 17) {
            throw new IllegalArgumentException(
                "expected volume, namespace, identity, bootstrap, groups, three peer triples, and optional activation/membership CSVs"
            );
        }
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        for (int i = 5; i < 14; i += 3) {
            peers.add(
                new JournalConfiguration.Peer(
                    UUID.fromString(arguments[i]),
                    "127.0.0.1",
                    Integer.parseInt(arguments[i + 1]),
                    arguments[i + 2]
                )
            );
        }
        final var groups = new ArrayList<JournalConfiguration.Group>();
        for (String id : arguments[4].split(",")) {
            groups.add(new JournalConfiguration.Group(UUID.fromString(id), peers));
        }
        final var limits = new JournalConfiguration.Limits(
            4,
            16384,
            32,
            1024 * 1024,
            1024 * 1024,
            2 * 1024 * 1024,
            0,
            Long.getLong("tests.journal.retained", 10_000L),
            1
        );
        final var originalJoins = new HashMap<UUID, JournalMembershipRecord>();
        if (arguments.length == 17 && arguments[16].isEmpty() == false) {
            for (String encoded : arguments[16].split(",")) {
                if (originalJoins.size() >= 4 || encoded.length() > 2 * JournalMembershipRecord.MAX_BYTES) {
                    throw new IOException("invalid fixture learner reservations");
                }
                final var record = JournalMembershipRecord.read(
                    new java.io.ByteArrayInputStream(java.util.Base64.getDecoder().decode(encoded))
                );
                if (originalJoins.putIfAbsent(record.voters().id(), record) != null) {
                    throw new IOException("duplicate fixture learner reservation");
                }
            }
        }
        final UUID local = UUID.fromString(arguments[2]);
        final var joiningPeer = originalJoins.values()
            .stream()
            .flatMap(record -> record.pending().target().peers().stream())
            .filter(peer -> peer.id().equals(local))
            .findFirst()
            .orElse(null);
        final var configuration = new JournalConfiguration(
            UUID.fromString(arguments[1]),
            UUID.fromString(arguments[2]),
            Path.of(arguments[0]),
            groups,
            Boolean.parseBoolean(arguments[3]),
            limits,
            joiningPeer
        );
        final var keys = KeyStore.getInstance("PKCS12");
        try (var input = JournalProcessMain.class.getResourceAsStream("tls-test-keystore.p12")) {
            keys.load(input, "testonly".toCharArray());
        }
        final var keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyFactory.init(keys, "testonly".toCharArray());
        final var trust = KeyStore.getInstance("PKCS12");
        try (var input = JournalProcessMain.class.getResourceAsStream("tls-test-truststore.p12")) {
            trust.load(input, "testonly".toCharArray());
        }
        final var trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trust);
        final var requiredActivations = new HashMap<UUID, LogCheckpoint>();
        if (arguments.length >= 15 && arguments[14].isEmpty() == false) {
            for (String value : arguments[14].split(",")) {
                final String[] fields = value.split(":");
                if (fields.length != 3 || requiredActivations.size() >= 4) {
                    throw new IOException("invalid fixture activation requirements");
                }
                final UUID group = UUID.fromString(fields[0]);
                requiredActivations.put(
                    group,
                    new LogCheckpoint(
                        JournalGroupRecord.controlIdentity(configuration.deploymentId(), group),
                        Long.parseLong(fields[1]),
                        Long.parseLong(fields[2])
                    )
                );
            }
        }
        final var memberships = new HashMap<UUID, JournalMembershipRecord>();
        if (arguments.length >= 16 && arguments[15].isEmpty() == false) {
            for (String encoded : arguments[15].split(",")) {
                if (memberships.size() >= 4 || encoded.length() > 2 * JournalMembershipRecord.MAX_BYTES) {
                    throw new IOException("invalid fixture membership requirements");
                }
                final var record = JournalMembershipRecord.read(
                    new java.io.ByteArrayInputStream(java.util.Base64.getDecoder().decode(encoded))
                );
                if (memberships.putIfAbsent(record.voters().id(), record) != null) {
                    throw new IOException("duplicate fixture membership requirements");
                }
            }
        }
        final var failStorage = new AtomicBoolean();
        final var storageFailed = new AtomicBoolean();
        final var stall = new AtomicBoolean();
        final var entered = new AtomicBoolean();
        final var release = new CountDownLatch(1);
        final RatisJournal.DiskSpaceProbe probe = () -> {
            if (failStorage.get() && Thread.currentThread().getName().endsWith("-SegmentedRaftLogWorker")) {
                storageFailed.set(true);
                throw new IOException("injected journal volume failure before log force");
            }
            if (stall.get() && Thread.currentThread().getName().endsWith("-SegmentedRaftLogWorker")) {
                entered.set(true);
                boolean interrupted = false;
                while (true) {
                    try {
                        release.await();
                        break;
                    } catch (InterruptedException expected) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return Long.MAX_VALUE;
        };
        try (
            var journal = memberships.isEmpty()
                && org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.journal.assigned", "false")) == false
                    ? RatisJournal.open(
                        configuration,
                        (X509KeyManager) keyFactory.getKeyManagers()[0],
                        (X509TrustManager) trustFactory.getTrustManagers()[0],
                        probe,
                        requiredActivations
                    )
                    : RatisJournal.openAssigned(
                        configuration,
                        (X509KeyManager) keyFactory.getKeyManagers()[0],
                        (X509TrustManager) trustFactory.getTrustManagers()[0],
                        probe,
                        memberships,
                        originalJoins
                    );
            var writes = new BackgroundWrites(journal);

            var input = new DataInputStream(new BufferedInputStream(System.in));
            var output = new DataOutputStream(protocolOutput())
        ) {
            output.writeInt(READY);
            output.flush();
            FileChannel pendingChannel = null;
            while (true) {
                final int command;
                try {
                    command = input.readUnsignedByte();
                } catch (EOFException done) {
                    break;
                }
                try {
                    switch (command) {
                        case ADVANCE -> {
                            final var intent = JournalMembershipRecord.read(new java.io.ByteArrayInputStream(readFrame(input)));
                            journal.advanceReplacement(intent).toCompletableFuture().get(10, TimeUnit.SECONDS);
                            output.writeByte(0);
                        }
                        case RECEIPT -> {
                            final var intent = JournalMembershipRecord.read(new java.io.ByteArrayInputStream(readFrame(input)));
                            final var receipt = journal.replacementReceipt(intent);
                            output.writeByte(0);
                            output.writeBoolean(receipt.isPresent());
                            if (receipt.isPresent()) {
                                writeFrame(output, receipt.get().encode());
                            }
                        }
                        case VOTERS -> {
                            final UUID group = groups.get(input.readInt()).id();
                            final var division = ((RaftServer) field(journal, "server")).getDivision(RaftGroupId.valueOf(group));
                            final var voters = division.getRaftConf().getCurrentPeers();
                            output.writeByte(0);
                            output.writeInt(voters.size());
                            for (var voter : voters) {
                                output.writeUTF(voter.getId().toString());
                            }
                        }
                        case RELEASE -> {
                            release.countDown();
                            output.writeByte(0);
                        }
                        case BACKGROUND_START -> {
                            final UUID group = groups.get(input.readInt()).id();
                            writes.start(group, OperationEnvelope.decode(readFrame(input), 16384));
                            output.writeByte(0);
                        }
                        case BACKGROUND_STATUS -> {
                            output.writeByte(0);
                            output.writeBoolean(writes.running.get());
                            output.writeInt(writes.outstanding.get());
                            output.writeLong(writes.submitted.get());
                            output.writeLong(writes.acknowledged.get());
                        }
                        case BACKGROUND_STOP -> {
                            writes.stopping.set(true);
                            output.writeByte(0);
                        }
                        case LEADER -> {
                            final boolean ready = journal.isLeaderReady(groups.get(input.readInt()).id());
                            output.writeByte(0);
                            output.writeBoolean(ready);
                        }
                        case APPEND -> {
                            final UUID group = groups.get(input.readInt()).id();
                            final var operation = OperationEnvelope.decode(readFrame(input), 16384);
                            final var checkpoint = journal.append(group, operation).toCompletableFuture().get(10, TimeUnit.SECONDS);
                            output.writeByte(0);
                            writeCheckpoint(output, checkpoint);
                        }
                        case APPEND_BATCH -> {
                            final UUID group = groups.get(input.readInt()).id();
                            final int count = input.readInt();
                            if (count < 1 || count > 24) {
                                throw new IOException("invalid fixture proposal batch size");
                            }
                            final var operation = OperationEnvelope.decode(readFrame(input), 16384);
                            final var pending = new ArrayList<CompletableFuture<LogCheckpoint>>();
                            for (int i = 0; i < count; i++) {
                                pending.add(journal.append(group, operation).toCompletableFuture());
                            }
                            CompletableFuture.allOf(pending.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
                            output.writeByte(0);
                        }
                        case VOTE -> {
                            final var group = groups.get(input.readInt());
                            final long term = input.readLong();
                            final int choice = input.readInt();
                            if (choice < 0 || choice > 1) {
                                throw new IOException("invalid fixture vote candidate");
                            }
                            final var candidate = group.peers()
                                .stream()
                                .filter(peer -> peer.id().equals(configuration.localVoterId()) == false)
                                .skip(choice)
                                .findFirst()
                                .orElseThrow();
                            final var request = org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto.newBuilder()
                                .setServerRequest(
                                    org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto.newBuilder()
                                        .setRequestorId(
                                            org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFromUtf8(
                                                candidate.id().toString()
                                            )
                                        )
                                        .setReplyId(
                                            org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFromUtf8(
                                                configuration.localVoterId().toString()
                                            )
                                        )
                                        .setRaftGroupId(
                                            org.apache.ratis.util.ProtoUtils.toRaftGroupIdProtoBuilder(RaftGroupId.valueOf(group.id()))
                                        )
                                        .setCallId(0)
                                )
                                .setCandidateTerm(term)
                                .setCandidateLastEntry(
                                    org.apache.ratis.proto.RaftProtos.TermIndexProto.newBuilder().setTerm(0).setIndex(-1)
                                )
                                .setPreVote(false)
                                .build();
                            final var reply = ((RaftServer) field(journal, "server")).requestVote(request);
                            output.writeByte(0);
                            output.writeBoolean(reply.getServerReply().getSuccess());
                        }
                        case PROGRESS -> {
                            final UUID group = groups.get(input.readInt()).id();
                            final var log = ((RaftServer) field(journal, "server")).getDivision(RaftGroupId.valueOf(group)).getRaftLog();
                            output.writeByte(0);
                            output.writeLong(log.getFlushIndex());
                            output.writeLong(log.getLastCommittedIndex());
                            output.writeLong(
                                ((RaftServer) field(journal, "server")).getDivision(RaftGroupId.valueOf(group)).getInfo().getCurrentTerm()
                            );
                        }
                        case SCAN -> {
                            final UUID group = groups.get(input.readInt()).id();
                            final long after = input.readLong();
                            final int count = input.readInt();
                            if (count < 0 || count > 128) {
                                throw new IOException("invalid fixture selection size");
                            }
                            final var selected = new HashSet<ShardLogId>();
                            for (int i = 0; i < count; i++) {
                                try (var frame = new InputStreamStreamInput(new ByteArrayInputStream(readFrame(input)))) {
                                    selected.add(ShardLogId.readFrom(frame));
                                }
                            }
                            final var batch = journal.scan(group, after, selected, 4096, 65536);
                            output.writeByte(0);
                            output.writeLong(batch.throughIndex());
                            output.writeLong(batch.committedIndex());
                            output.writeInt(batch.operations().size());
                            for (var item : batch.operations()) {
                                writeCheckpoint(output, item.checkpoint());
                                writeFrame(output, item.operation().encode(16384));
                            }
                        }
                        case ACTIVATION_COORDINATE -> {
                            final var activation = journal.groupActivation(groups.get(input.readInt()).id());
                            output.writeByte(0);
                            writeCheckpoint(output, activation.checkpoint());
                        }
                        case ACTIVATE -> {
                            journal.activateGroup(groups.get(input.readInt()).id()).toCompletableFuture().get(20, TimeUnit.SECONDS);
                            output.writeByte(0);
                        }
                        case FAIL_STORAGE -> {
                            failStorage.set(true);
                            output.writeByte(0);
                        }
                        case STORAGE_FAILED -> {
                            output.writeByte(0);
                            output.writeBoolean(storageFailed.get());
                        }
                        case STALL -> {
                            stall.set(true);
                            output.writeByte(0);
                        }
                        case STALLED -> {
                            final boolean blocked = entered.get();
                            if (blocked && pendingChannel == null) {
                                pendingChannel = logChannel(journal, groups.getFirst().id());
                            }
                            output.writeByte(0);
                            output.writeBoolean(blocked);
                        }
                        case IO_OPEN -> {
                            if (pendingChannel == null) {
                                throw new IOException("fixture has not captured a stalled log channel");
                            }
                            output.writeByte(0);
                            output.writeBoolean(pendingChannel.isOpen());
                        }
                        case CLOSE, CLOSE_STAY -> {
                            journal.close();
                            output.writeByte(0);
                            output.flush();
                            if (command == CLOSE) {
                                return;
                            }
                        }
                        default -> throw new IOException("unknown fixture command");
                    }
                } catch (Exception failure) {
                    output.writeByte(1);
                    final String message = failure.toString();
                    output.writeUTF(message.substring(0, Math.min(2048, message.length())));
                }
                output.flush();
            }
        }
    }

    private static final class BackgroundWrites implements AutoCloseable {
        private final RatisJournal journal;
        private final AtomicBoolean stopping = new AtomicBoolean();
        private final AtomicBoolean running = new AtomicBoolean();
        private final java.util.concurrent.atomic.AtomicInteger outstanding = new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicLong submitted = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong acknowledged = new java.util.concurrent.atomic.AtomicLong();
        private Thread worker;

        private BackgroundWrites(RatisJournal journal) {
            this.journal = journal;
        }

        private void start(UUID group, OperationEnvelope operation) throws IOException {
            if (worker != null) {
                throw new IOException("fixture background writer already started");
            }
            running.set(true);
            worker = org.apache.ratis.util.Daemon.newBuilder()
                .setName("journal-fixture-background-writes")
                .setRunnable(() -> run(group, operation))
                .build();
            worker.start();
        }

        private void run(UUID group, OperationEnvelope operation) {
            final var pending = new ArrayList<CompletableFuture<?>>();
            try {
                while (stopping.get() == false && journal.isLeaderReady(group)) {
                    pending.removeIf(CompletableFuture::isDone);
                    if (pending.size() < 24) {
                        outstanding.incrementAndGet();
                        try {
                            final var future = journal.append(group, operation).toCompletableFuture();
                            submitted.incrementAndGet();
                            pending.add(future);
                            future.whenComplete((checkpoint, failure) -> {
                                if (failure == null) {
                                    acknowledged.incrementAndGet();
                                }
                                outstanding.decrementAndGet();
                            });
                        } catch (IOException | RuntimeException rejected) {
                            outstanding.decrementAndGet();
                            break;
                        }
                    } else {
                        try {
                            CompletableFuture.anyOf(pending.toArray(CompletableFuture[]::new)).get(100, TimeUnit.MILLISECONDS);
                        } catch (java.util.concurrent.TimeoutException awaitingCompletion) {
                            // Bounded polling of actual accepted work; never cancel its persistence.
                        } catch (java.util.concurrent.ExecutionException ambiguous) {
                            break;
                        }
                    }
                }
            } catch (IOException unavailable) {
                // Expected when this member loses leadership or is removed.
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                running.set(false);
            }
        }

        @Override
        public void close() throws InterruptedException {
            stopping.set(true);
            if (worker != null) {
                worker.interrupt();
                worker.join(TimeUnit.SECONDS.toMillis(2));
            }
        }
    }

    private static FileChannel logChannel(RatisJournal journal, UUID group) throws ReflectiveOperationException, IOException {
        final var server = (RaftServer) field(journal, "server");
        final var worker = field(server.getDivision(RaftGroupId.valueOf(group)).getRaftLog(), "fileLogWorker");
        return (FileChannel) field(field(field(worker, "out"), "out"), "fileChannel");
    }

    @SuppressForbidden(reason = "Fault fixture inspects the pinned SDK private log channel without expanding its production API")
    private static Object field(Object owner, String name) throws ReflectiveOperationException {
        final var field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    static byte[] readFrame(DataInputStream input) throws IOException {
        final int length = input.readInt();
        if (length < 0 || length > FRAME_LIMIT) {
            throw new IOException("invalid fixture frame size");
        }
        final byte[] frame = input.readNBytes(length);
        if (frame.length != length) {
            throw new EOFException("truncated fixture frame");
        }
        return frame;
    }

    static void writeFrame(DataOutputStream output, byte[] frame) throws IOException {
        if (frame.length > FRAME_LIMIT) {
            throw new IOException("fixture frame too large");
        }
        output.writeInt(frame.length);
        output.write(frame);
    }

    private static void writeCheckpoint(DataOutputStream output, LogCheckpoint checkpoint) throws IOException {
        final var bytes = new ByteArrayOutputStream();
        try (var encoded = new OutputStreamStreamOutput(bytes)) {
            checkpoint.writeTo(encoded);
        }
        writeFrame(output, bytes.toByteArray());
    }
}
