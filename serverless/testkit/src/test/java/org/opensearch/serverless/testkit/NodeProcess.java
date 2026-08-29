/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.serverless.shell.ServerlessBootstrap;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A serverless node in its own JVM.
 *
 * <p>Everything up to now ran nodes as objects in the test's own process. That is enough to test logic
 * and not enough to test the things this project actually claims: that a node which <em>dies</em> —
 * without unwinding, without releasing anything, without its shutdown hook running — is recovered from
 * by another node. {@code close()} is a cooperative shutdown and proves the opposite of what a crash
 * does. Only a real process can be killed.
 *
 * <p>Startup is synchronised on the readiness line the bootstrap prints, not on a sleep: the HTTP port
 * is 0, so the parent cannot know the address until the child says so, and polling a port that is not
 * open yet is indistinguishable from polling one that never will be.
 *
 * <p>Output is drained on a thread whether or not anyone reads it. A child whose stdout pipe fills up
 * blocks forever, and the symptom is a test that hangs rather than one that fails.
 */
final class NodeProcess implements Closeable {

    private final String name;
    private final Process process;
    private final String httpAddress;
    private final String nodeId;
    private final List<String> output;
    private final Thread drain;

    private NodeProcess(String name, Process process, String httpAddress, String nodeId, List<String> output, Thread drain) {
        this.name = name;
        this.process = process;
        this.httpAddress = httpAddress;
        this.nodeId = nodeId;
        this.output = output;
        this.drain = drain;
    }

    /**
     * Forks a node and waits until it reports itself ready.
     *
     * @param name the node name
     * @param store the shared object store directory
     * @param home this node's private path.home
     * @param extra any additional settings
     * @return the running process
     * @throws Exception if the node does not start
     */
    static NodeProcess start(String name, Path store, Path home, Map<String, String> extra) throws Exception {
        final List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        // Not java.class.path: see the comment on processTest in build.gradle. A child forked onto the
        // test classpath dies inside NodeEnvironment because Randomness sees randomizedtesting and asks
        // for a test context that main() cannot have.
        final String nodeClasspath = System.getProperty("serverless.node.classpath");
        if (nodeClasspath == null) {
            throw new IllegalStateException("serverless.node.classpath is not set; run these under the processTest task");
        }
        command.add(nodeClasspath);
        // The child is an OpenSearch node, so it needs what an OpenSearch node needs. Inheriting the
        // test JVM's module flags rather than guessing them keeps this working when they change.
        command.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
        command.add("--add-opens=java.base/java.util=ALL-UNNAMED");
        command.add("-Dio.netty.noUnsafe=true");
        command.add("-Dio.netty.noKeySetOptimization=true");
        command.add("-Dio.netty.recycler.maxCapacityPerThread=0");
        command.add("-Dopensearch.set.netty.runtime.available.processors=false");

        final Map<String, String> settings = new java.util.LinkedHashMap<>();
        settings.put("node.name", name);
        settings.put("cluster.name", "serverless-contention");
        settings.put("path.home", home.toString());
        settings.put("network.host", "127.0.0.1");
        settings.put("http.port", "0");
        settings.put("transport.port", "0");
        settings.put("serverless.roles", "ingest");
        settings.put(ServerlessBootstrap.STORE_PATH, store.toString());
        final Path readyFile = home.resolve("ready");
        settings.put(ServerlessBootstrap.READY_FILE, readyFile.toString());
        settings.putAll(extra);
        settings.forEach((k, v) -> command.add("-D" + k + "=" + v));

        command.add(ServerlessBootstrap.class.getName());

        final Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        final List<String> output = java.util.Collections.synchronizedList(new ArrayList<>());

        // Drained on a thread whether or not anyone reads it. A child whose stdout pipe fills up blocks
        // forever, and the symptom is a test that hangs rather than one that fails.
        final Thread drain = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.add(line);
                }
            } catch (Exception e) {
                // The stream closing is how a dead child looks from here; not an error.
            }
        }, "drain-" + name);
        drain.setDaemon(true);
        drain.start();

        String readyLine = null;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            if (java.nio.file.Files.exists(readyFile)) {
                final String contents = java.nio.file.Files.readString(readyFile).strip();
                if (contents.startsWith(ServerlessBootstrap.READY_MARKER)) {
                    readyLine = contents;
                    break;
                }
            }
            if (process.isAlive() == false) {
                // Fail with the child's own output rather than with a timeout that says nothing about why.
                throw new IllegalStateException("node " + name + " exited before readiness; output was:\n" + String.join("\n", output));
            }
            Thread.sleep(100);
        }
        if (readyLine == null) {
            process.destroyForcibly();
            throw new IllegalStateException("node " + name + " did not start; output was:\n" + String.join("\n", output));
        }

        String http = null;
        String id = null;
        for (String token : readyLine.split("\\s+")) {
            if (token.startsWith("http=")) {
                http = token.substring("http=".length());
            } else if (token.startsWith("node=")) {
                id = token.substring("node=".length());
            }
        }
        return new NodeProcess(name, process, http, id, output, drain);
    }

    /**
     * Returns this node's bound HTTP address, as {@code host:port}.
     *
     * @return the address
     */
    String http() {
        return httpAddress;
    }

    /**
     * Returns this node's id.
     *
     * @return the node id
     */
    String nodeId() {
        return nodeId;
    }

    /**
     * Returns this node's name.
     *
     * @return the name
     */
    String name() {
        return name;
    }

    /**
     * Reports whether the process is still running.
     *
     * @return true if alive
     */
    boolean alive() {
        return process.isAlive();
    }

    /**
     * Kills the node without letting it clean up — the crash case.
     *
     * <p>No shutdown hook runs, so no lease is released and no final publish happens. Everything the
     * survivors do afterwards has to be driven by the lease expiring, which is the point.
     *
     * @throws InterruptedException if interrupted while waiting for it to die
     */
    void killHard() throws InterruptedException {
        process.destroyForcibly();
        assert process.waitFor(30, TimeUnit.SECONDS) : "node " + name + " would not die";
    }

    /**
     * Freezes the process with SIGSTOP — the only way to make a real zombie.
     *
     * <p>A killed-and-restarted node is not a zombie: it comes back empty, wins a fresh compare-and-swap
     * and owns the shard legitimately at a higher term, so it never exercises fencing at all. A
     * <em>stopped</em> node is the dangerous case. It is not dead, it holds a lease it cannot renew, and
     * when it resumes it still believes everything it believed before — including that it owns a shard
     * that has since moved and been written to by somebody else.
     *
     * @throws Exception if the signal cannot be sent
     */
    void pause() throws Exception {
        signal("STOP");
    }

    /**
     * Thaws a paused process with SIGCONT. It wakes up believing nothing has changed.
     *
     * @throws Exception if the signal cannot be sent
     */
    void resume() throws Exception {
        signal("CONT");
    }

    private void signal(String name) throws Exception {
        final Process kill = new ProcessBuilder("kill", "-" + name, String.valueOf(process.pid())).redirectErrorStream(true).start();
        if (kill.waitFor(30, TimeUnit.SECONDS) == false || kill.exitValue() != 0) {
            throw new IllegalStateException("could not send SIG" + name + " to " + this.name);
        }
    }

    /**
     * Returns everything the process has printed, for diagnosing a failure.
     *
     * @return the output lines
     */
    List<String> output() {
        return List.copyOf(output);
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (process.waitFor(30, TimeUnit.SECONDS) == false) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
        drain.interrupt();
    }
}
