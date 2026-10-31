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
    static final int FRAME_LIMIT = 17 * 1024;

    private JournalProcessMain() {}

    @SuppressForbidden(reason = "Child fixture stdout carries the bounded binary control protocol; all logs go to stderr")
    private static BufferedOutputStream protocolOutput() {
        return new BufferedOutputStream(System.out);
    }

    /**
     * Runs one voter process from a trusted fixture's placement arguments.
     * @param arguments volume, namespace, local UUID, bootstrap, group UUID CSV, three peer triples, and optional activation CSV
     * @throws Exception on startup or broken control channel
     */
    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 14 && arguments.length != 15) {
            throw new IllegalArgumentException(
                "expected volume, namespace, identity, bootstrap, groups, three peer triples, and optional activation CSV"
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
        final var limits = new JournalConfiguration.Limits(4, 16384, 32, 1024 * 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        final var configuration = new JournalConfiguration(
            UUID.fromString(arguments[1]),
            UUID.fromString(arguments[2]),
            Path.of(arguments[0]),
            groups,
            Boolean.parseBoolean(arguments[3]),
            limits
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
        if (arguments.length == 15 && arguments[14].isEmpty() == false) {
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
        final var stall = new AtomicBoolean();
        final var entered = new AtomicBoolean();
        final var release = new CountDownLatch(1);
        try (
            var journal = RatisJournal.open(
                configuration,
                (X509KeyManager) keyFactory.getKeyManagers()[0],
                (X509TrustManager) trustFactory.getTrustManagers()[0],
                () -> {
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
                },
                requiredActivations
            );
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
                        case PROGRESS -> {
                            final UUID group = groups.get(input.readInt()).id();
                            final var log = ((RaftServer) field(journal, "server")).getDivision(RaftGroupId.valueOf(group)).getRaftLog();
                            output.writeByte(0);
                            output.writeLong(log.getFlushIndex());
                            output.writeLong(log.getLastCommittedIndex());
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
