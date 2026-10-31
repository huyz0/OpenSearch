/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/** A finite control queue and deadlines around a real voter process. */
final class JournalProcess implements AutoCloseable {
    private final Process process;
    private final DataInputStream input;
    private final DataOutputStream output;
    private final ExecutorService commands;
    private final Path log;

    private JournalProcess(Process process, Path log) {
        this.process = process;
        this.log = log;
        input = new DataInputStream(new BufferedInputStream(process.getInputStream()));
        output = new DataOutputStream(new BufferedOutputStream(process.getOutputStream()));
        commands = OpenSearchExecutors.newFixed(
            "journal-process-control",
            1,
            1,
            OpenSearchExecutors.daemonThreadFactory("journal-process-control"),
            new ThreadContext(Settings.EMPTY)
        );
    }

    static JournalProcess start(JournalConfiguration configuration, Path log) throws IOException {
        return start(configuration, log, Map.of());
    }

    static JournalProcess start(JournalConfiguration configuration, Path log, Map<UUID, LogCheckpoint> requiredActivations)
        throws IOException {
        final List<String> arguments = new ArrayList<>();
        arguments.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        arguments.add("-Xms64m");
        arguments.add("-Xmx256m");
        arguments.add("--enable-native-access=ALL-UNNAMED");
        arguments.add("-Dlog4j2.configurationFile=" + JournalProcessMain.class.getResource("journal-process-log4j2.xml"));
        arguments.add("-cp");
        arguments.add(System.getProperty("tests.journal.classpath"));
        arguments.add(JournalProcessMain.class.getName());
        arguments.add(configuration.directory().toString());
        arguments.add(configuration.deploymentId().toString());
        arguments.add(configuration.localVoterId().toString());
        arguments.add(Boolean.toString(configuration.bootstrap()));
        arguments.add(configuration.groups().stream().map(group -> group.id().toString()).collect(Collectors.joining(",")));
        for (var peer : configuration.groups().getFirst().peers()) {
            arguments.add(peer.id().toString());
            arguments.add(Integer.toString(peer.port()));
            arguments.add(peer.availabilityZone());
        }
        arguments.add(
            requiredActivations.entrySet()
                .stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue().index() + ":" + entry.getValue().entryTerm())
                .collect(Collectors.joining(","))
        );
        final var fixture = new JournalProcess(startProcess(arguments, log), log);
        try {
            fixture.execute(() -> {
                if (fixture.input.readInt() != JournalProcessMain.READY) {
                    throw new IOException("invalid voter startup response");
                }
                return null;
            });
            return fixture;
        } catch (IOException | RuntimeException | Error failure) {
            fixture.close();
            throw failure;
        }
    }

    @SuppressForbidden(reason = "ProcessBuilder.redirectError requires java.io.File for this child-process fixture")
    private static Process startProcess(List<String> arguments, Path log) throws IOException {
        return new ProcessBuilder(arguments).redirectError(log.toFile()).start();
    }

    boolean leader(int group) throws IOException {
        return exchange(JournalProcessMain.LEADER, out -> out.writeInt(group), DataInputStream::readBoolean);
    }

    LogCheckpoint append(int group, OperationEnvelope operation) throws IOException {
        return exchange(JournalProcessMain.APPEND, out -> {
            out.writeInt(group);
            JournalProcessMain.writeFrame(out, operation.encode(16384));
        }, JournalProcess::readCheckpoint);
    }

    void appendBatch(int group, OperationEnvelope operation, int count) throws IOException {
        exchange(JournalProcessMain.APPEND_BATCH, out -> {
            out.writeInt(group);
            out.writeInt(count);
            JournalProcessMain.writeFrame(out, operation.encode(16384));
        }, in -> null);
    }

    record Progress(long flushed, long committed) {
    }

    Progress progress(int group) throws IOException {
        return exchange(JournalProcessMain.PROGRESS, out -> out.writeInt(group), in -> new Progress(in.readLong(), in.readLong()));
    }

    RatisJournal.ScanBatch scan(int group, long after, Set<ShardLogId> selection) throws IOException {
        return exchange(JournalProcessMain.SCAN, out -> {
            out.writeInt(group);
            out.writeLong(after);
            out.writeInt(selection.size());
            for (var id : selection) {
                final var bytes = new ByteArrayOutputStream();
                try (var encoded = new OutputStreamStreamOutput(bytes)) {
                    id.writeTo(encoded);
                }
                JournalProcessMain.writeFrame(out, bytes.toByteArray());
            }
        }, in -> {
            final long through = in.readLong();
            final long committed = in.readLong();
            final int count = in.readInt();
            if (count < 0 || count > 4096) {
                throw new IOException("invalid fixture replay count");
            }
            final var operations = new ArrayList<RatisJournal.CommittedOperation>();
            int bytes = 0;
            for (int i = 0; i < count; i++) {
                final var checkpoint = readCheckpoint(in);
                final byte[] frame = JournalProcessMain.readFrame(in);
                bytes += frame.length;
                if (bytes > 65536) {
                    throw new IOException("fixture replay exceeds byte budget");
                }
                operations.add(new RatisJournal.CommittedOperation(OperationEnvelope.decode(frame, 16384), checkpoint));
            }
            return new RatisJournal.ScanBatch(operations, through, committed);
        });
    }

    LogCheckpoint activation(int group) throws IOException {
        return exchange(JournalProcessMain.ACTIVATION_COORDINATE, out -> out.writeInt(group), JournalProcess::readCheckpoint);
    }

    void activate(int group) throws IOException {
        exchange(JournalProcessMain.ACTIVATE, out -> out.writeInt(group), in -> null);
    }

    void stall() throws IOException {
        exchange(JournalProcessMain.STALL, out -> {}, in -> null);
    }

    boolean stalled() throws IOException {
        return exchange(JournalProcessMain.STALLED, out -> {}, DataInputStream::readBoolean);
    }

    boolean ioResourcesOpen() throws IOException {
        return exchange(JournalProcessMain.IO_OPEN, out -> {}, DataInputStream::readBoolean);
    }

    void stopButKeepProcessAlive() throws IOException {
        exchange(JournalProcessMain.CLOSE_STAY, out -> {}, in -> null);
    }

    void stop() throws IOException {
        exchange(JournalProcessMain.CLOSE, out -> {}, in -> null);
        try {
            if (process.waitFor(15, TimeUnit.SECONDS) == false) {
                throw new IOException("voter did not exit after shutdown; log: " + log);
            }
            if (process.exitValue() != 0) {
                throw new IOException("voter shutdown failed; log: " + log);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } finally {
            close();
        }
    }

    boolean alive() {
        return process.isAlive();
    }

    private <T> T exchange(int command, Writer writer, Reader<T> reader) throws IOException {
        return execute(() -> {
            output.writeByte(command);
            writer.write(output);
            output.flush();
            final int status = input.readUnsignedByte();
            if (status != 0) {
                throw new IOException("voter command failed: " + input.readUTF() + "; log: " + log);
            }
            return reader.read(input);
        });
    }

    private <T> T execute(Action<T> action) throws IOException {
        try {
            return commands.submit(action::run).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted voter control call; log: " + log, e);
        } catch (ExecutionException e) {
            throw new IOException("voter control failed; log: " + log, e.getCause());
        } catch (TimeoutException e) {
            close();
            throw new IOException("voter control timed out; log: " + log, e);
        }
    }

    private static LogCheckpoint readCheckpoint(DataInputStream input) throws IOException {
        try (var encoded = new InputStreamStreamInput(new ByteArrayInputStream(JournalProcessMain.readFrame(input)))) {
            return LogCheckpoint.readFrom(encoded);
        }
    }

    @Override
    public void close() {
        process.destroyForcibly();
        try {
            if (process.waitFor(10, TimeUnit.SECONDS) == false) {
                throw new IllegalStateException("voter process did not terminate; log: " + log);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while killing fixture voter", e);
        } finally {
            commands.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream output) throws IOException;
    }

    @FunctionalInterface
    private interface Reader<T> {
        T read(DataInputStream input) throws IOException;
    }

    @FunctionalInterface
    private interface Action<T> {
        T run() throws IOException;
    }
}
