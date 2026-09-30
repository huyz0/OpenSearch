/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import java.io.Closeable;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A fleet's clients: writes spread over the active indices with a trickle to dormant ones, wide searches, gets,
 * and a create/delete churn -- every write recorded in the ledger with what it was told.
 *
 * <p>Every request goes to a node chosen at random among those running, as a load balancer in front of a fleet
 * would send it; the node forwards what it does not own. A write is never retried under the same id: a retry is
 * a new attempt with a new id, so each document found later answers for exactly one attempt.
 */
final class FleetLoad implements Closeable {

    /** What the load is aimed at, and how hard. */
    record Shape(int activeIndices, int population, double dormantFraction, double churnFraction, int churnNames, double writesPerSecond,
        double churnOpsPerSecond) {
    }

    /** One finished request, for the latency and error-rate windows. */
    record Sample(String kind, long endedNanos, long latencyNanos, FleetLedger.Outcome outcome) {
    }

    private static final Pattern SEQ_NO = Pattern.compile("\"_seq_no\":(\\d+)");
    private static final Pattern TERM = Pattern.compile("\"_primary_term\":(\\d+)");
    private static final Pattern ERROR_TYPE = Pattern.compile("\"type\":\"([a-z_]+)\"");

    private final Supplier<List<NodeProcess>> nodes;
    private final FleetLedger ledger;
    private final HttpClient http;
    private volatile Shape shape;
    private final AtomicBoolean running = new AtomicBoolean();
    private final List<Thread> threads = new ArrayList<>();
    private final ConcurrentLinkedQueue<Sample> samples = new ConcurrentLinkedQueue<>();
    private final Set<String> churnLive = ConcurrentHashMap.newKeySet();
    private final List<FleetLedger.Write> ackedForGets = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong ids = new AtomicLong();
    private final String runId;

    /**
     * How many writes may be outstanding at once. The load is open-loop: writes are sent on schedule whatever the
     * fleet is doing, as independent clients would send them, rather than by a fixed set of threads each waiting
     * for its last answer -- with which one sick node that holds requests for a timeout stalled every writer, and
     * a scenario measured the harness. Past this bound a write is shed by the client and counted as such.
     */
    static final int MAX_IN_FLIGHT = 1024;

    /**
     * A generator per thread. The load's threads are not test threads, so the framework's randomness is not
     * reachable from them; each gets its own seeded generator instead.
     */
    private static final java.util.concurrent.atomic.AtomicLong SEEDS = new java.util.concurrent.atomic.AtomicLong(0x5EEDL);
    private static final ThreadLocal<java.util.Random> RANDOM = ThreadLocal.withInitial(
        () -> new java.util.Random(SEEDS.getAndIncrement())
    );

    private final java.util.concurrent.Semaphore inFlight = new java.util.concurrent.Semaphore(MAX_IN_FLIGHT);
    private final java.util.concurrent.ExecutorService senders = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicLong shed = new AtomicLong();

    FleetLoad(Supplier<List<NodeProcess>> nodes, FleetLedger ledger, Shape shape, String runId) {
        this.nodes = nodes;
        this.ledger = ledger;
        this.shape = shape;
        this.runId = runId;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    void reshape(Shape shape) {
        this.shape = shape;
    }

    static String populationName(int i) {
        return String.format(Locale.ROOT, "logs-%07d", i);
    }

    static String churnName(int i) {
        return String.format(Locale.ROOT, "churn-%03d", i);
    }

    synchronized void start() {
        if (running.getAndSet(true)) {
            return;
        }
        threads.clear();
        threads.add(spawn("fleet-writes", this::writeLoop));
        threads.add(spawn("fleet-search", this::searchLoop));
        threads.add(spawn("fleet-get", this::getLoop));
        threads.add(spawn("fleet-churn", this::churnLoop));
    }

    synchronized void stop() throws InterruptedException {
        running.set(false);
        for (Thread thread : threads) {
            thread.join(TimeUnit.MINUTES.toMillis(2));
        }
        threads.clear();
        // And every write already sent, so a check that follows sees each of them with its answer.
        if (inFlight.tryAcquire(MAX_IN_FLIGHT, 3, TimeUnit.MINUTES)) {
            inFlight.release(MAX_IN_FLIGHT);
        }
    }

    /** Writes the client could not send because {@link #MAX_IN_FLIGHT} were already outstanding. */
    long shed() {
        return shed.get();
    }

    private Thread spawn(String name, Runnable body) {
        final Thread thread = new Thread(body, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Nodes a load balancer's health check has taken out of rotation: running, but no longer sent requests. A
     * frozen process accepts connections and answers nothing, and without this every client thread ends up
     * parked on it for a full timeout -- which measures the harness, not the fleet.
     */
    private final java.util.Set<String> outOfRotation = java.util.concurrent.ConcurrentHashMap.newKeySet();

    void takeOutOfRotation(String node) {
        outOfRotation.add(node);
    }

    void putBackInRotation(String node) {
        outOfRotation.remove(node);
    }

    /** The nodes requests are sent to now. */
    List<NodeProcess> inRotation() {
        final List<NodeProcess> live = new java.util.ArrayList<>(nodes.get());
        live.removeIf(node -> outOfRotation.contains(node.name()));
        return live.isEmpty() ? nodes.get() : live;
    }

    private NodeProcess anyNode() {
        final List<NodeProcess> live = inRotation();
        return live.get(RANDOM.get().nextInt(live.size()));
    }

    private void writeLoop() {
        long next = System.nanoTime();
        while (running.get()) {
            final Shape s = shape;
            // On a schedule rather than after each answer; see MAX_IN_FLIGHT.
            next += (long) (TimeUnit.SECONDS.toNanos(1) / Math.max(0.1, s.writesPerSecond()));
            final long wait = next - System.nanoTime();
            if (wait > 0) {
                LockSupport.parkNanos(wait);
            } else if (wait < -TimeUnit.SECONDS.toNanos(1)) {
                // Fell a second behind -- a stalled scheduler, not a slow fleet: do not burst to catch up.
                next = System.nanoTime();
            }
            final String index = pickIndex(s);
            if (inFlight.tryAcquire() == false) {
                shed.incrementAndGet();
                continue;
            }
            try {
                senders.execute(() -> {
                    try {
                        write(anyNode(), index);
                    } finally {
                        inFlight.release();
                    }
                });
            } catch (java.util.concurrent.RejectedExecutionException e) {
                inFlight.release();
                return;
            }
        }
    }

    private static String pickIndex(Shape s) {
        final double roll = RANDOM.get().nextDouble();
        if (roll < s.churnFraction()) {
            return churnName(RANDOM.get().nextInt(s.churnNames()));
        }
        if (roll < s.churnFraction() + s.dormantFraction() && s.population() > s.activeIndices()) {
            return populationName(s.activeIndices() + RANDOM.get().nextInt(s.population() - s.activeIndices()));
        }
        return populationName(RANDOM.get().nextInt(Math.max(1, s.activeIndices())));
    }

    /**
     * One write attempt, recorded whatever happens.
     *
     * @param node the node to send it to
     * @param index the index
     * @return the ledger entry
     */
    FleetLedger.Write write(NodeProcess node, String index) {
        final String id = runId + "-" + ids.incrementAndGet();
        final String body = "{\"@timestamp\":" + System.currentTimeMillis() + ",\"n\":" + ids.get() + ",\"msg\":\"fleet\"}";
        final long startedAt = System.nanoTime();
        FleetLedger.Write write;
        try {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://" + node.http() + "/" + index + "/_doc/" + id))
                    .timeout(Duration.ofSeconds(60))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            final long endedAt = System.nanoTime();
            final int status = response.statusCode();
            final FleetLedger.Outcome outcome = status >= 200 && status < 300 ? FleetLedger.Outcome.ACKED
                : status >= 400 && status < 500 && status != 408 ? FleetLedger.Outcome.REFUSED
                : FleetLedger.Outcome.UNKNOWN;
            write = new FleetLedger.Write(
                index,
                id,
                node.name(),
                startedAt,
                endedAt,
                status,
                first(SEQ_NO, response.body()),
                first(TERM, response.body()),
                outcome,
                outcome == FleetLedger.Outcome.ACKED ? null : errorType(response.body())
            );
        } catch (ConnectException | HttpConnectTimeoutException e) {
            // Never left the client: nothing can have been applied.
            write = new FleetLedger.Write(
                index,
                id,
                node.name(),
                startedAt,
                System.nanoTime(),
                -1,
                -1,
                -1,
                FleetLedger.Outcome.REFUSED,
                "unsent"
            );
        } catch (Exception e) {
            write = new FleetLedger.Write(
                index,
                id,
                node.name(),
                startedAt,
                System.nanoTime(),
                -2,
                -1,
                -1,
                FleetLedger.Outcome.UNKNOWN,
                e.getClass().getSimpleName()
            );
        }
        ledger.record(write);
        samples.add(new Sample("write", write.endedNanos(), write.endedNanos() - write.startedNanos(), write.outcome()));
        if (write.outcome() == FleetLedger.Outcome.ACKED && index.startsWith("churn-") == false) {
            ackedForGets.add(write);
        }
        return write;
    }

    private void searchLoop() {
        while (running.get()) {
            final long now = System.currentTimeMillis();
            final String body = "{\"size\":10,\"track_total_hits\":true,\"query\":{\"range\":{\"@timestamp\":{\"gte\":"
                + (now - 60_000L)
                + "}}}}";
            timed("search", () -> request(anyNode(), "POST", "/logs-*/_search?allow_partial_activation=true", body));
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
        }
    }

    private void getLoop() {
        while (running.get()) {
            final FleetLedger.Write pick;
            synchronized (ackedForGets) {
                pick = ackedForGets.isEmpty() ? null : ackedForGets.get(RANDOM.get().nextInt(ackedForGets.size()));
            }
            if (pick != null) {
                timed("get", () -> request(anyNode(), "GET", "/" + pick.index() + "/_doc/" + pick.id(), null));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
    }

    private void churnLoop() {
        while (running.get()) {
            final Shape s = shape;
            if (s.churnOpsPerSecond() <= 0 || s.churnNames() <= 0) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
                continue;
            }
            final long startedAt = System.nanoTime();
            final String name = churnName(RANDOM.get().nextInt(s.churnNames()));
            if (churnLive.contains(name)) {
                final int status = timed("delete", () -> request(anyNode(), "DELETE", "/" + name, null));
                final long endedAt = System.nanoTime();
                ledger.recordDelete(new FleetLedger.Delete(name, startedAt, endedAt, status >= 200 && status < 300));
                if (status >= 200 && status < 300 || status == 404) {
                    churnLive.remove(name);
                }
            } else {
                final int status = timed(
                    "create",
                    () -> request(
                        anyNode(),
                        "PUT",
                        "/" + name,
                        "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},"
                            + "\"n\":{\"type\":\"long\"},\"msg\":{\"type\":\"text\"}}}}"
                    )
                );
                if (status >= 200 && status < 300 || status == 400) {
                    churnLive.add(name);
                }
            }
            final long pace = (long) (TimeUnit.SECONDS.toNanos(1) / s.churnOpsPerSecond());
            final long spent = System.nanoTime() - startedAt;
            if (spent < pace) {
                LockSupport.parkNanos(pace - spent);
            }
        }
    }

    @FunctionalInterface
    private interface Call {
        int send() throws Exception;
    }

    private int timed(String kind, Call call) {
        final long startedAt = System.nanoTime();
        int status;
        FleetLedger.Outcome outcome;
        try {
            status = call.send();
            outcome = status >= 200 && status < 300 ? FleetLedger.Outcome.ACKED
                : status >= 400 && status < 500 ? FleetLedger.Outcome.REFUSED
                : FleetLedger.Outcome.UNKNOWN;
        } catch (Exception e) {
            status = -1;
            outcome = FleetLedger.Outcome.UNKNOWN;
        }
        final long endedAt = System.nanoTime();
        samples.add(new Sample(kind, endedAt, endedAt - startedAt, outcome));
        return status;
    }

    int request(NodeProcess node, String method, String path, String body) throws Exception {
        final HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://" + node.http() + path))
            .timeout(Duration.ofSeconds(60))
            .header("Content-Type", "application/json");
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return http.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    String requestBody(NodeProcess node, String method, String path, String body) throws Exception {
        final HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://" + node.http() + path))
            .timeout(Duration.ofSeconds(120))
            .header("Content-Type", "application/json");
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        final HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (response.statusCode() >= 300) {
            throw new IllegalStateException(method + " " + path + " answered " + response.statusCode() + ": " + response.body());
        }
        return response.body();
    }

    /** Latency and outcomes of one kind of request that ended within a window. */
    record Window(String kind, long count, long acked, long refused, long unknown, long p50Millis, long p99Millis) {
        double errorRate() {
            return count == 0 ? 0d : (double) (refused + unknown) / count;
        }

        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "%s: %d (acked %d, refused %d, unknown %d, error rate %.2f%%), p50 %dms, p99 %dms",
                kind,
                count,
                acked,
                refused,
                unknown,
                errorRate() * 100,
                p50Millis,
                p99Millis
            );
        }
    }

    Window window(String kind, long fromNanos, long toNanos) {
        final List<Long> latencies = new ArrayList<>();
        long acked = 0;
        long refused = 0;
        long unknown = 0;
        for (Sample sample : samples) {
            if (sample.kind().equals(kind) == false || sample.endedNanos() < fromNanos || sample.endedNanos() > toNanos) {
                continue;
            }
            switch (sample.outcome()) {
                case ACKED -> {
                    acked++;
                    latencies.add(sample.latencyNanos());
                }
                case REFUSED -> refused++;
                case UNKNOWN -> unknown++;
            }
        }
        Collections.sort(latencies);
        return new Window(kind, acked + refused + unknown, acked, refused, unknown, percentile(latencies, 50), percentile(latencies, 99));
    }

    Map<String, Long> refusalReasons(long fromNanos, long toNanos) {
        final Map<String, Long> reasons = new TreeMap<>();
        for (FleetLedger.Write write : ledger.writes()) {
            if (write.endedNanos() >= fromNanos && write.endedNanos() <= toNanos && write.outcome() != FleetLedger.Outcome.ACKED) {
                reasons.merge(write.outcome() + ":" + write.status() + ":" + write.reason(), 1L, Long::sum);
            }
        }
        return reasons;
    }

    private static long percentile(List<Long> sorted, int p) {
        if (sorted.isEmpty()) {
            return 0;
        }
        return TimeUnit.NANOSECONDS.toMillis(sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(p / 100.0 * sorted.size()) - 1)));
    }

    private static long first(Pattern pattern, String body) {
        final Matcher m = pattern.matcher(body);
        return m.find() ? Long.parseLong(m.group(1)) : -1L;
    }

    private static String errorType(String body) {
        final Matcher m = ERROR_TYPE.matcher(body);
        return m.find() ? m.group(1) : null;
    }

    @Override
    public void close() {
        running.set(false);
        senders.shutdown();
    }
}
