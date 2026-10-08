/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.store.ObjectStores;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A fleet of real shell processes on an S3 API, under load, through seven failures -- judged by a ledger of
 * every write a client attempted.
 *
 * <p>Run by {@code :serverless:testkit:fleetTest}, parameterised entirely by system properties so a real bucket
 * can be pointed at it unchanged:
 * <ul>
 *   <li>{@code tests.fleet.endpoint}, {@code tests.fleet.access_key}, {@code tests.fleet.secret_key}, {@code
 *   tests.fleet.region}, {@code tests.fleet.path_style} -- the store; skipped when the endpoint is unreachable;</li>
 *   <li>{@code tests.fleet.nodes} (3), {@code tests.fleet.heap} (2g), {@code tests.fleet.max_shards} (100) -- the
 *   fleet;</li>
 *   <li>{@code tests.fleet.population} (2,000), {@code tests.fleet.active} (per node, 50) -- the deployment;</li>
 *   <li>{@code tests.fleet.writes_per_second} (100), sent open-loop, and {@code tests.fleet.churn_per_second} (5)
 *   -- the load;</li>
 *   <li>{@code tests.fleet.scenario_seconds} (60), {@code tests.fleet.scenarios} (all of steady, kill, pause,
 *   partition, slowdown, restart, storm), {@code tests.fleet.ttl_millis} (the production 30,000).</li>
 * </ul>
 * Every node reaches the store through a {@link FaultProxy} of its own, which is how one node is partitioned
 * from the store while its peers and clients can still reach it, and how the fleet's request mix is counted.
 * The results -- a markdown table and the ledger itself -- go to {@code build/fleet-results/<run>}.
 *
 * <p>The first finding of a lost acknowledged write stops the run there: later scenarios would only bury it.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@TimeoutSuite(millis = 24 * TimeUnits.HOUR)
public class ServerlessFleetScaleTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"n\":{\"type\":\"long\"},"
        + "\"msg\":{\"type\":\"text\"}}}";

    @SuppressForbidden(reason = "the harness's own -Dtests.fleet.setting.* overrides, read once at start")
    private static Map<String, String> settingOverrides() {
        final Map<String, String> overrides = new TreeMap<>();
        for (String key : System.getProperties().stringPropertyNames()) {
            if (key.startsWith("tests.fleet.setting.")) {
                overrides.put(key.substring("tests.fleet.setting.".length()), System.getProperty(key));
            }
        }
        return overrides;
    }

    /**
     * Log4j lines setting each logger named in tests.fleet.debug_loggers, comma-separated, to debug: the nodes'
     * own loggers are configured here, so a node setting cannot reach them.
     */
    private static String debugLoggers() {
        final StringBuilder lines = new StringBuilder();
        int i = 0;
        for (String name : prop("debug_loggers", "").split(",")) {
            if (name.isBlank() == false) {
                lines.append("logger.debug").append(i).append(".name = ").append(name.trim()).append('\n');
                lines.append("logger.debug").append(i).append(".level = debug").append('\n');
                i++;
            }
        }
        return lines.toString();
    }

    private static String prop(String key, String fallback) {
        return System.getProperty("tests.fleet." + key, fallback);
    }

    private static int intProp(String key, int fallback) {
        return Integer.parseInt(prop(key, Integer.toString(fallback)));
    }

    private final List<NodeProcess> fleet = new CopyOnWriteArrayList<>();
    private final List<FaultProxy> proxies = new ArrayList<>();
    private final List<Path> homes = new ArrayList<>();
    private final Map<String, String> nodeSettings = new LinkedHashMap<>();
    private URI storeEndpoint;
    private String storeAccess;
    private String storeSecret;
    private final StringBuilder report = new StringBuilder();
    private FleetLedger ledger;
    private boolean keepBucket;
    private FleetLoad load;
    private MetadataPlane plane;
    private Path results;
    private long ttlMillis;

    public void testAFleetUnderLoadAndFailureLosesNoAcknowledgedWrite() throws Exception {
        final String endpoint = prop("endpoint", "http://127.0.0.1:1");
        assumeTrue("no S3-compatible endpoint at " + endpoint, reachable(endpoint));
        final String access = prop("access_key", "rustfsadmin");
        final String secret = prop("secret_key", "rustfsadmin");
        final int nodes = intProp("nodes", 3);
        final int population = intProp("population", 2_000);
        final int activePerNode = intProp("active", 50);
        final int seconds = intProp("scenario_seconds", 60);
        ttlMillis = Long.parseLong(prop("ttl_millis", Long.toString(ServerlessBootstrap.DEFAULT_LEASE_TTL_MILLIS)));
        final List<String> scenarios = List.of(prop("scenarios", "steady,kill,pause,partition,slowdown,restart,storm").split(","));
        final String runId = "run" + System.currentTimeMillis();
        final String bucket = prop("bucket", "fleet-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT));
        results = Path.of(System.getProperty("user.dir")).resolve("build").resolve("fleet-results").resolve(runId);
        Files.createDirectories(results);

        final BlobStore direct = org.opensearch.repositories.s3.MinioBlobStores.create(endpoint, access, secret, bucket, createTempDir());
        plane = new MetadataPlane(direct, BlobPath.cleanPath(), System::currentTimeMillis, ttlMillis);

        header(nodes, population, activePerNode, seconds, endpoint, bucket);

        // The deployment: the population's descriptors, written straight to the store. A bucket kept from an earlier
        // run that already holds the whole population is reused as it is: creating a million descriptors takes a
        // quarter of an hour, and doing it again would only fail a million times.
        if (plane.describe(FleetLoad.populationName(population - 1)).isPresent()) {
            line("Population: " + population + " indices, reused from the bucket.\n");
        } else {
            final long populatingSince = System.nanoTime();
            populate(population);
            final long populatingSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - populatingSince);
            line("Population: " + population + " indices created in " + populatingSeconds + " s.\n");
        }

        // The fleet, each node behind its own proxy.
        final URI store = URI.create(endpoint);
        nodeSettings.put(ObjectStores.TYPE, "s3");
        nodeSettings.put(ObjectStores.BUCKET, bucket);
        nodeSettings.put(ObjectStores.PATH_STYLE, prop("path_style", "true"));
        nodeSettings.put(ObjectStores.REGION, prop("region", "us-east-1"));
        nodeSettings.put(ServerlessBootstrap.PROVIDER_ACK, "true");
        nodeSettings.put(ServerlessBootstrap.LEASE_TTL, Long.toString(ttlMillis));
        nodeSettings.put(ServerlessBootstrap.MAX_SHARDS, prop("max_shards", "100"));
        nodeSettings.put("serverless.roles", "ingest,search");
        // Any node setting, as tests.fleet.setting.<name>: what an A/B run changes without a code change.
        nodeSettings.putAll(settingOverrides());
        storeEndpoint = store;
        storeAccess = access;
        storeSecret = secret;
        for (int i = 0; i < nodes; i++) {
            addNodeSlot(i);
        }
        for (int i = 0; i < nodes; i++) {
            fleet.add(startNode(i));
        }

        ledger = new FleetLedger(results.resolve("ledger.csv"));
        final FleetLoad.Shape steady = new FleetLoad.Shape(
            Math.min(population, activePerNode * nodes),
            population,
            0.05,
            0.05,
            50,
            Double.parseDouble(prop("writes_per_second", "100")),
            Double.parseDouble(prop("churn_per_second", "5"))
        );
        load = new FleetLoad(() -> List.copyOf(fleet), ledger, steady, runId);
        final Thread sampler = startActivationSampler();
        try {
            load.start();
            // Warm: the active set taken, before anything is measured. Longer on a bucket a crashed run left behind,
            // whose every active shard names a dead node and is taken with a replay of its whole log.
            final int warmup = Integer.parseInt(prop("warmup_seconds", Integer.toString(Math.min(60, seconds))));
            line("- warm-up: " + warmup + " s of load before the first scenario");
            sleepSeconds(warmup);
            for (String scenario : scenarios) {
                final long started = System.nanoTime();
                final int ledgerMark = ledger.writes().size();
                final long shedMark = load.shed();
                final Map<String, Long> termsBefore = activeTerms(steady.activeIndices());
                final Map<String, Long> phasesBefore = activationPhases();
                line("\n## " + scenario + "\n");
                switch (scenario.trim()) {
                    case "steady" -> steady(seconds);
                    case "kill" -> kill(seconds);
                    case "pause" -> pause(seconds);
                    case "partition" -> partition(seconds);
                    case "slowdown" -> slowdown(seconds);
                    case "restart" -> rollingRestart(seconds);
                    case "storm" -> storm(seconds, steady);
                    case "drain" -> drain();
                    case "scaleout" -> scaleOut(seconds);
                    default -> throw new IllegalArgumentException("unknown scenario " + scenario);
                }
                final long ended = System.nanoTime();
                line("- writes: " + load.window("write", started, ended));
                // And without the first minute. Between scenarios the load stops for the read-back and the idle
                // measurement, long enough for the working set to be released; the minute after it resumes re-takes
                // every hot shard at once, and folded into one figure it read as the steady state's p99.
                if (ended - started > TimeUnit.SECONDS.toNanos(120)) {
                    line("- writes after the first minute: " + load.window("write", started + TimeUnit.SECONDS.toNanos(60), ended));
                }
                line("- searches: " + load.window("search", started, ended));
                line("- gets: " + load.window("get", started, ended));
                line("- churn: " + load.window("create", started, ended) + "; " + load.window("delete", started, ended));
                line("- not acknowledged, by reason: " + load.refusalReasons(started, ended));
                line("- writes shed by the client, " + FleetLoad.MAX_IN_FLIGHT + " already in flight: " + (load.shed() - shedMark));
                line("- store requests per node: " + storeCounts());
                line("- " + activationPhaseAverages(phasesBefore, activationPhases()));
                line("- " + unprunable());
                final Map<String, Long> termsAfter = activeTerms(steady.activeIndices());
                line("- ownership churn on the active set during the scenario: " + termChurn(termsBefore, termsAfter));
                final FleetLedger.Verdict verdict = verify(ledgerMark);
                line(
                    "- ownership churn on the active set during the read-back: "
                        + termChurn(termsAfter, activeTerms(steady.activeIndices()))
                );
                line("- **ledger: " + verdict + "**");
                writeReport();
                if (verdict.clean() == false) {
                    forensics(verdict);
                    keepBucket = true;
                    fail(
                        "scenario "
                            + scenario
                            + (verdict.lost().isEmpty() && verdict.refusedButVisible().isEmpty()
                                ? " was INCONCLUSIVE: the store could not be read to settle every write, which is not a loss: "
                                : " failed its ledger check: ")
                            + verdict
                            + "; see "
                            + results
                    );
                }
            }
        } finally {
            sampler.interrupt();
            load.close();
            load.stop();
            ledger.flush();
            writeReport();
            for (NodeProcess node : fleet) {
                node.close();
            }
            for (FaultProxy proxy : proxies) {
                proxy.close();
            }
            for (NodeProcess node : fleet) {
                Files.write(results.resolve(node.name() + ".log"), node.output(), StandardCharsets.UTF_8);
            }
            // The nodes' local data is a cache of the store, gigabytes of it at scale, and it sits under the project's
            // build directory where every file-tree task would walk it. The logs, the ledger and the report stay.
            for (Path home : homes) {
                org.opensearch.common.util.io.IOUtils.rm(home.resolve("data"));
            }
            if (keepBucket == false && Boolean.parseBoolean(prop("keep_bucket", "false")) == false) {
                org.opensearch.repositories.s3.MinioBlobStores.deleteBucket(endpoint, access, secret, bucket, createTempDir());
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ scenarios

    /** The load alone, then the load stopped: a node-hour of background, per node. */
    private void steady(int seconds) throws Exception {
        resetStoreCounts();
        final Map<String, Long> refusedBefore = perNodeField("write_backpressure", "refused");
        final long started = System.nanoTime();
        sleepSeconds(seconds);
        final double elapsed = (System.nanoTime() - started) / 1e9;
        final Map<String, Long> total = totalStoreCounts();
        line("- store requests per second under load, fleet-wide, by type: " + perSecond(total, elapsed));
        // Where the write limiter settled with nothing injected: its limit, the append time it is reacting to, and
        // how many writes it refused under this load.
        final Map<String, Long> limits = perNodeField("write_backpressure", "limit");
        final Map<String, Long> appendMillis = perNodeField("write_backpressure", "append_millis");
        final Map<String, Long> refusedAfter = perNodeField("write_backpressure", "refused");
        final Map<String, String> limiter = new TreeMap<>();
        long shed = 0;
        for (String node : limits.keySet()) {
            final long refused = refusedAfter.getOrDefault(node, 0L) - refusedBefore.getOrDefault(node, 0L);
            shed += refused;
            limiter.put(node, "limit " + limits.get(node) + ", append " + appendMillis.get(node) + " ms, refused " + refused);
        }
        line("- write limiter at the end of the load, no throttling injected: " + limiter + "; " + shed + " writes refused in all");

        load.stop();
        // Past the client's timeout before counting: a write the load sent last is still being appended, forwarded
        // and published for up to that long, and a window opened at once counted that tail as background.
        sleepSeconds(IDLE_SETTLE_SECONDS);
        final int idle = Math.max(180, seconds / 2);
        final Map<String, Integer> heldBefore = heldShardsPerNode();
        final Map<String, Long> janitorBefore = perNodeField("janitor", "store_requests");
        resetStoreCounts();
        final long idleStarted = System.nanoTime();
        sleepSeconds(idle);
        final double idleElapsed = (System.nanoTime() - idleStarted) / 1e9;
        final Map<String, Integer> heldAfter = heldShardsPerNode();
        final Map<String, Long> janitorAfter = perNodeField("janitor", "store_requests");
        // The figure the idle-cost target is about: nodes that held the same shards throughout, so no release or
        // activation is in it, and without the janitor's own requests. The rest is reported beside it.
        double stableRequests = 0;
        double stableJanitor = 0;
        double stableHeld = 0;
        final List<String> stableNodes = new ArrayList<>();
        double movingRequests = 0;
        int movedShards = 0;
        long janitorTotal = 0;
        for (int i = 0; i < proxies.size(); i++) {
            final String name = fleet.get(i).name();
            final long requests = proxies.get(i).counts().values().stream().mapToLong(Long::longValue).sum();
            final long janitor = janitorAfter.getOrDefault(name, 0L) - janitorBefore.getOrDefault(name, 0L);
            janitorTotal += janitor;
            final int before = heldBefore.getOrDefault(name, 0);
            final int after = heldAfter.getOrDefault(name, 0);
            if (Math.abs(after - before) <= Math.max(2, before / 50)) {
                stableNodes.add(name);
                stableRequests += requests;
                stableJanitor += janitor;
                stableHeld += (before + after) / 2d;
            } else {
                movingRequests += requests;
                movedShards += Math.abs(after - before);
            }
        }
        line(
            String.format(
                Locale.ROOT,
                "- idle cost on nodes that held their shards (%s): %.1f requests per held shard-hour, %.1f without the janitor's own"
                    + " (%.0f shards per node); nodes that released or took shards: %.0f requests per node-hour over %d shards moved;"
                    + " janitor: %.0f requests per node-hour",
                stableNodes,
                stableHeld == 0 ? 0d : stableRequests * 3600 / idleElapsed / stableHeld,
                stableHeld == 0 ? 0d : (stableRequests - stableJanitor) * 3600 / idleElapsed / stableHeld,
                stableNodes.isEmpty() ? 0d : stableHeld / stableNodes.size(),
                proxies.size() == stableNodes.size() ? 0d : movingRequests * 3600 / idleElapsed / (proxies.size() - stableNodes.size()),
                movedShards,
                janitorTotal * 3600 / idleElapsed / proxies.size()
            )
        );
        final Map<String, String> perNodeHour = new TreeMap<>();
        long fleetRequests = 0;
        double fleetHeld = 0;
        for (int i = 0; i < proxies.size(); i++) {
            final long sum = proxies.get(i).counts().values().stream().mapToLong(Long::longValue).sum();
            final String name = fleet.get(i).name();
            final double held = (heldBefore.getOrDefault(name, 0) + heldAfter.getOrDefault(name, 0)) / 2d;
            fleetRequests += sum;
            fleetHeld += held;
            perNodeHour.put(
                name,
                String.format(Locale.ROOT, "%.0f (holding %d->%d)", sum * 3600 / idleElapsed, heldBefore.get(name), heldAfter.get(name))
            );
        }
        line(
            "- background with the load stopped, requests per node-hour: "
                + perNodeHour
                + " (over "
                + idle
                + " s idle, after "
                + IDLE_SETTLE_SECONDS
                + " s to drain)"
        );
        line(
            String.format(
                Locale.ROOT,
                "- idle cost: %.0f requests per node-hour on average, %.1f per held shard-hour (%.0f shards held per node)",
                fleetRequests * 3600 / idleElapsed / proxies.size(),
                fleetHeld == 0 ? 0d : fleetRequests * 3600 / idleElapsed / fleetHeld,
                fleetHeld / proxies.size()
            )
        );
        // And what it is spent on, averaged over the nodes: the line items an idle-cost change has to move.
        final Map<String, Long> byArea = new TreeMap<>();
        for (FaultProxy proxy : proxies) {
            proxy.countsByArea().forEach((area, n) -> byArea.merge(area, n, Long::sum));
        }
        final Map<String, String> perNodeHourByArea = new TreeMap<>();
        byArea.forEach(
            (area, n) -> perNodeHourByArea.put(area, String.format(Locale.ROOT, "%.0f", n * 3600 / idleElapsed / proxies.size()))
        );
        line("- idle requests per node-hour by operation and area: " + perNodeHourByArea);
        wideSearchProbe();
        load.start();
    }

    /**
     * Wide searches over the whole population, one at a time with nothing else running, each twice per node: the
     * first is cold, the second warm.
     *
     * <p>Three shapes. A range no index can hold resolves a million names and rules every one out, which is the
     * cost of resolution alone. The load's own query -- the last minute -- is what a dashboard sends. A narrower
     * pattern shows how the cost scales with the names matched.
     */
    private void wideSearchProbe() {
        final long now = System.currentTimeMillis();
        final Map<String, String[]> shapes = new LinkedHashMap<>();
        shapes.put("logs-* nothing in range", new String[] { "/logs-*/_search", rangeFrom(now + TimeUnit.DAYS.toMillis(30)) });
        shapes.put("logs-* last minute", new String[] { "/logs-*/_search?allow_partial_activation=true", rangeFrom(now - 60_000L) });
        shapes.put(
            "logs-00000* last minute",
            new String[] { "/logs-00000*/_search?allow_partial_activation=true", rangeFrom(now - 60_000L) }
        );
        try (
            java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(5)).build()
        ) {
            shapes.forEach((shape, request) -> {
                final List<String> outcomes = new ArrayList<>();
                for (int i = 0; i < Math.min(3, fleet.size()); i++) {
                    final NodeProcess node = fleet.get(i);
                    for (String pass : new String[] { "cold", "warm" }) {
                        final long started = System.nanoTime();
                        String outcome;
                        try {
                            final var response = client.send(
                                java.net.http.HttpRequest.newBuilder()
                                    .uri(URI.create("http://" + node.http() + request[0]))
                                    .header("Content-Type", "application/json")
                                    .timeout(java.time.Duration.ofSeconds(120))
                                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(request[1]))
                                    .build(),
                                java.net.http.HttpResponse.BodyHandlers.ofString()
                            );
                            outcome = response.statusCode() + " " + searchSummary(response.body());
                        } catch (Exception e) {
                            outcome = e.getClass().getSimpleName();
                        }
                        outcomes.add(
                            String.format(
                                Locale.ROOT,
                                "%s %s %d ms: %s",
                                node.name(),
                                pass,
                                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started),
                                outcome
                            )
                        );
                    }
                }
                line("- wide search, " + shape + ": " + String.join("; ", outcomes));
            });
        }
    }

    private static final Pattern CANDIDATES = Pattern.compile("could match this query \\((\\d+)\\)");

    /**
     * How many indices a search for a range no index can hold still cannot rule out: those whose rollup marks a
     * writer, live or left behind by a crash, plus any refused for another reason. The janitor drives it down.
     */
    private String unprunable() {
        final String answer = nextMonthSearch();
        final long count = unprunableCount(answer);
        return "logs-* for next month: "
            + (count >= 0 ? count + " indices could not be ruled out" : searchSummary(answer))
            + (CANDIDATES.matcher(answer).find() ? ", refused at the cap" : ", answered under the cap");
    }

    private String nextMonthSearch() {
        final String body = rangeFrom(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(30));
        try {
            return load.requestBody(load.inRotation().get(0), "POST", "/logs-*/_search", body);
        } catch (Exception e) {
            // A refusal at the cap is the usual answer, and it names the count.
            return String.valueOf(e.getMessage());
        }
    }

    private static final Pattern SHARDS_SKIPPED = Pattern.compile("\"_shards\"\\s*:\\s*\\{[^}]*\"skipped\"\\s*:\\s*(\\d+)");

    /**
     * The indices a next-month search could not rule out: the count a refusal at the cap names, or, once it fits under
     * the cap, the shards it searched rather than skipped (one shard an index here). -1 if neither can be read.
     */
    private static long unprunableCount(String answer) {
        final Matcher refused = CANDIDATES.matcher(answer);
        if (refused.find()) {
            return Long.parseLong(refused.group(1));
        }
        final Matcher total = SHARDS_TOTAL.matcher(answer);
        if (total.find() == false) {
            return -1L;
        }
        final Matcher skipped = SHARDS_SKIPPED.matcher(answer);
        return Long.parseLong(total.group(1)) - (skipped.find() ? Long.parseLong(skipped.group(1)) : 0L);
    }

    /**
     * Runs the load until the janitor's backlog is drained: the next-month search's unprunable count every two minutes,
     * stopping when the janitor has done next to nothing three intervals running, or at tests.fleet.drain_max_seconds.
     * Reports the series and what the janitor's passes cost while draining and in the last, flat interval.
     */
    private void drain() throws Exception {
        final long maxSeconds = Long.parseLong(prop("drain_max_seconds", "7200"));
        final long started = System.nanoTime();
        final long janitorStart = fleetSum("janitor", "store_requests");
        final List<String> series = new ArrayList<>();
        final List<Long> counts = new ArrayList<>();
        long lastJanitor = janitorStart;
        long lastWork = janitorWork();
        long lastAt = started;
        double lastIntervalPerNodeHour = 0;
        boolean flat = false;
        int idleIntervals = 0;
        while (true) {
            final long count = unprunableCount(nextMonthSearch());
            final long now = System.nanoTime();
            final long janitor = fleetSum("janitor", "store_requests");
            final long work = janitorWork();
            if (now > lastAt) {
                lastIntervalPerNodeHour = (janitor - lastJanitor) * 3600e9 / (now - lastAt) / Math.max(1, fleet.size());
            }
            final long workInInterval = work - lastWork;
            lastJanitor = janitor;
            lastWork = work;
            lastAt = now;
            counts.add(count);
            series.add(TimeUnit.NANOSECONDS.toMinutes(now - started) + "m:" + count + "/" + workInInterval);
            // Flat means the janitor has run out of work, not that the count paused: a first run stopped at a lull and
            // the count fell by a third over the scenarios after it.
            // Under load the count never settles to a percent -- it includes every shard held at the moment, and those
            // turn over -- so drained is the janitor finding nothing to do three intervals running.
            idleIntervals = workInInterval <= Math.max(5L, fleet.size()) ? idleIntervals + 1 : 0;
            if (idleIntervals >= 3) {
                flat = true;
                break;
            }
            if (TimeUnit.NANOSECONDS.toSeconds(now - started) >= maxSeconds) {
                break;
            }
            sleepSeconds(120);
        }
        final double hours = (System.nanoTime() - started) / 3600e9;
        line(
            "- janitor backlog (logs-* next month: indices not ruled out / janitor actions in the interval) over time: "
                + series
                + (flat ? ", flat" : ", NOT flat at the limit")
        );
        line(
            String.format(
                Locale.ROOT,
                "- janitor while draining: %.0f store requests per node-hour over %.1f h; in the last interval %.0f; %d released, %d replays,"
                    + " %d stale claims forgotten in all",
                (lastJanitor - janitorStart) / hours / Math.max(1, fleet.size()),
                hours,
                lastIntervalPerNodeHour,
                fleetSum("janitor", "released"),
                fleetSum("janitor", "replays"),
                fleetSum("janitor", "claims_forgotten")
            )
        );
    }

    /** The head term of each active index's shard, read from the store. */
    private Map<String, Long> activeTerms(int active) {
        final Map<String, Long> terms = new java.util.concurrent.ConcurrentHashMap<>();
        final ExecutorService readers = Executors.newFixedThreadPool(16);
        try {
            final List<Future<?>> pending = new ArrayList<>();
            for (int i = 0; i < active; i++) {
                final String index = FleetLoad.populationName(i);
                pending.add(readers.submit(() -> {
                    plane.heads().read(index, 0).ifPresent(head -> terms.put(index, head.term()));
                    return null;
                }));
            }
            for (Future<?> f : pending) {
                try {
                    f.get();
                } catch (Exception e) {
                    // One unreadable head is one index missing from the figure.
                }
            }
        } finally {
            readers.shutdown();
        }
        return terms;
    }

    /**
     * How far the active shards' terms moved: every move is an ownership change, so a hot shard whose term races is
     * changing hands rather than serving.
     */
    private static String termChurn(Map<String, Long> before, Map<String, Long> after) {
        final List<Long> moved = new ArrayList<>();
        before.forEach((index, term) -> {
            final Long now = after.get(index);
            if (now != null) {
                moved.add(now - term);
            }
        });
        if (moved.isEmpty()) {
            return "no heads read";
        }
        Collections.sort(moved);
        final long total = moved.stream().mapToLong(Long::longValue).sum();
        return String.format(
            Locale.ROOT,
            "%d term changes over %d shards; per shard p50 %d, p99 %d, max %d",
            total,
            moved.size(),
            moved.get(moved.size() / 2),
            moved.get(Math.min(moved.size() - 1, (int) Math.ceil(moved.size() * 0.99) - 1)),
            moved.get(moved.size() - 1)
        );
    }

    private static String rangeFrom(long gteMillis) {
        return "{\"size\":10,\"track_total_hits\":true,\"query\":{\"range\":{\"@timestamp\":{\"gte\":" + gteMillis + "}}}}";
    }

    private static final Pattern SHARDS_TOTAL = Pattern.compile("\"_shards\"\\s*:\\s*\\{\\s*\"total\"\\s*:\\s*(\\d+)");
    private static final Pattern HITS_TOTAL = Pattern.compile("\"total\"\\s*:\\s*\\{\\s*\"value\"\\s*:\\s*(\\d+)");
    private static final Pattern REASON = Pattern.compile("\"reason\"\\s*:\\s*\"([^\"]{0,240})");

    /** Shards searched and hits for an answer, or the reason for a refusal, in a line. */
    private static String searchSummary(String body) {
        final Matcher shards = SHARDS_TOTAL.matcher(body);
        final Matcher hits = HITS_TOTAL.matcher(body);
        if (shards.find()) {
            return "shards " + shards.group(1) + ", hits " + (hits.find() ? hits.group(1) : "?");
        }
        final Matcher reason = REASON.matcher(body);
        return reason.find() ? reason.group(1) : body.substring(0, Math.min(240, body.length()));
    }

    /** kill -9 of the node holding the most shards, writes continuing; how long until each of its shards takes writes again. */
    private void kill(int seconds) throws Exception {
        final int victim = victimNode();
        final NodeProcess node = fleet.get(victim);
        final List<String> owned = shardsOwnedBy(node.nodeId());
        line("- killing " + node.name() + ", which owned " + owned.size() + " shards; lease TTL " + ttlMillis + " ms");
        final long leaseExpiresAt = plane.membership().read(node.nodeId()).map(l -> l.expiresAtMillis()).orElse(-1L);
        final long killedAtMillis = System.currentTimeMillis();
        final long killedAt = System.nanoTime();
        fleet.remove(node);
        final Map<String, Long> sourcesBefore = new TreeMap<>();
        for (String source : org.opensearch.serverless.reconcile.BackgroundReconciler.ACTIVATION_SOURCES) {
            sourcesBefore.put(source, fleetSum("by_source", source));
        }
        final long routedBefore = fleetSum("capacity", "takeover_writes_routed");
        final long leftBefore = fleetSum("capacity", "takeover_doubts_left");
        node.killHard();
        acquiredAfter.clear();
        // Each survivor's activation queue while it takes the dead node's shards: how deep, and how long the longest wait.
        final Map<String, long[]> queues = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.concurrent.atomic.AtomicBoolean watching = new java.util.concurrent.atomic.AtomicBoolean(true);
        final Thread watcher = new Thread(() -> {
            try (
                java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                    .connectTimeout(java.time.Duration.ofSeconds(5))
                    .build()
            ) {
                while (watching.get()) {
                    for (NodeProcess survivor : List.copyOf(fleet)) {
                        final String body = statsOf(client, survivor);
                        if (body != null) {
                            final Map<String, Long> q = objectFields(body, "activation_queue");
                            queues.merge(
                                survivor.name(),
                                new long[] { q.getOrDefault("queued", 0L), q.getOrDefault("max_wait_millis", 0L) },
                                (a, b) -> new long[] { Math.max(a[0], b[0]), Math.max(a[1], b[1]) }
                            );
                        }
                    }
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
                }
            }
        }, "kill-queue-watcher");
        watcher.setDaemon(true);
        watcher.start();
        final Map<String, Long> takeover;
        try {
            takeover = probeUntilWritable(owned, killedAt, Math.max(seconds, (int) (6 * ttlMillis / 1000)), node.nodeId());
        } finally {
            watching.set(false);
            watcher.join(30_000);
        }
        final StringBuilder depth = new StringBuilder("- survivors' activation queues during the takeover, deepest / longest wait:");
        new TreeMap<>(queues).forEach(
            (name, q) -> depth.append(' ').append(name).append('=').append(q[0]).append('/').append(q[1]).append("ms")
        );
        line(depth.toString());
        final StringBuilder bySource = new StringBuilder("- activations asked for across the survivors during the takeover, by source:");
        for (String source : org.opensearch.serverless.reconcile.BackgroundReconciler.ACTIVATION_SOURCES) {
            bySource.append(' ').append(source).append('=').append(fleetSum("by_source", source) - sourcesBefore.get(source));
        }
        line(bySource.toString());
        line(
            "- routed to the member taking a dead member's shard: "
                + (fleetSum("capacity", "takeover_writes_routed") - routedBefore)
                + " writes; doubts left to it: "
                + (fleetSum("capacity", "takeover_doubts_left") - leftBefore)
        );
        line("- takeover, kill to first acknowledged write on each of its shards: " + summarise(takeover, owned.size()));
        line(
            "- where the time went: the lease ran out "
                + (leaseExpiresAt < 0 ? "?" : Long.toString(leaseExpiresAt - killedAtMillis))
                + " ms after the kill; a survivor held the head: "
                + summarise(acquiredAfter, owned.size())
                + "; head to first acknowledged write: "
                + summarise(gaps(acquiredAfter, takeover), owned.size())
        );
        if (leaseExpiresAt >= 0 && takeover.isEmpty() == false) {
            // The figure runs compare on: how fast the survivors made the dead node's shards writable once they could
            // take them at all, which neither the lease nor how many shards it held decides.
            final long expiredAfter = leaseExpiresAt - killedAtMillis;
            final List<Long> sorted = new ArrayList<>(takeover.values());
            java.util.Collections.sort(sorted);
            final long half = sorted.get(sorted.size() / 2) - expiredAfter;
            final long all = sorted.get(sorted.size() - 1) - expiredAfter;
            line(
                String.format(
                    Locale.ROOT,
                    "- takeover throughput after the lease ran out: %.1f shards/s to half writable (%d ms), %.1f shards/s to all (%d ms)",
                    (sorted.size() / 2.0) / Math.max(1L, half) * 1000.0,
                    half,
                    sorted.size() / (double) Math.max(1L, all) * 1000.0,
                    all
                )
            );
        }
        line("- writes from the kill until every shard was back: " + load.window("write", killedAt, killedAt + maxOr(takeover, 0L)));
        sleepSeconds(Math.max(0, seconds - (int) TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - killedAt)));
        fleet.add(victim, startNode(victim));
    }

    /** How long a load balancer takes to stop routing to a node that no longer answers. */
    private static final int HEALTH_CHECK_SECONDS = 5;

    /** A process frozen past its lease, then thawed: it must not acknowledge anything it no longer owns. */
    private void pause(int seconds) throws Exception {
        final int victim = victimNode();
        final NodeProcess node = fleet.get(victim);
        final List<String> owned = shardsOwnedBy(node.nodeId());
        final long pauseMillis = 2 * ttlMillis;
        line("- pausing " + node.name() + " (" + owned.size() + " shards) for " + pauseMillis + " ms, twice the lease");
        final long pausedAt = System.nanoTime();
        node.pause();
        final Map<String, Long> takeover;
        try {
            // A load balancer notices a node that stops answering its health check within a few seconds and
            // stops sending it requests; until then clients keep arriving at the frozen process.
            sleepSeconds(HEALTH_CHECK_SECONDS);
            load.takeOutOfRotation(node.name());
            takeover = probeUntilWritable(owned, pausedAt, (int) (pauseMillis / 1000), node.nodeId());
        } finally {
            node.resume();
            load.putBackInRotation(node.name());
        }
        line("- takeover while it was frozen: " + summarise(takeover, owned.size()));
        sleepSeconds(Math.max(0, seconds - (int) TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - pausedAt)));
    }

    /** One node cut off from the store, still reachable by clients and peers: its acks must stop, its lease lapse. */
    private void partition(int seconds) throws Exception {
        final int victim = victimNode();
        final NodeProcess node = fleet.get(victim);
        final List<String> owned = shardsOwnedBy(node.nodeId());
        final long partitionMillis = 2 * ttlMillis;
        line("- partitioning " + node.name() + " (" + owned.size() + " shards) from the store for " + partitionMillis + " ms");
        final long cutAt = System.nanoTime();
        proxies.get(victim).partition();
        final Map<String, Long> takeover;
        try {
            takeover = probeUntilWritable(owned, cutAt, (int) (partitionMillis / 1000), node.nodeId());
            final FleetLoad.Window viaVictim = windowFor(node.name(), cutAt + TimeUnit.MILLISECONDS.toNanos(ttlMillis), System.nanoTime());
            line("- writes sent to " + node.name() + " once its lease had run out: " + viaVictim);
        } finally {
            proxies.get(victim).heal();
        }
        line("- takeover of its shards by the rest: " + summarise(takeover, owned.size()));
        sleepSeconds(Math.max(0, seconds - (int) TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - cutAt)));
    }

    /** A minute of 503 SlowDown on half of every node's requests, and how long until writes recover. */
    private void slowdown(int seconds) throws Exception {
        final long baselineFrom = System.nanoTime();
        sleepSeconds(Math.min(30, seconds / 4));
        final FleetLoad.Window baseline = load.window("write", baselineFrom, System.nanoTime());
        final long burstAt = System.nanoTime();
        for (FaultProxy proxy : proxies) {
            proxy.setSlowdownFraction(0.5);
        }
        sleepSeconds(60);
        long injected = 0;
        for (FaultProxy proxy : proxies) {
            proxy.setSlowdownFraction(0);
            injected += proxy.injected();
        }
        final long healedAt = System.nanoTime();
        line("- baseline: " + baseline);
        line("- during the burst (" + injected + " SlowDowns injected): " + load.window("write", burstAt, healedAt));
        long recovered = -1;
        final long deadline = healedAt + TimeUnit.SECONDS.toNanos(Math.max(60, seconds));
        while (System.nanoTime() < deadline) {
            sleepSeconds(5);
            final long now = System.nanoTime();
            final FleetLoad.Window recent = load.window("write", now - TimeUnit.SECONDS.toNanos(10), now);
            if (recent.count() > 0
                && recent.errorRate() <= baseline.errorRate() + 0.01
                && recent.p99Millis() <= Math.max(1, 2 * baseline.p99Millis())) {
                recovered = TimeUnit.NANOSECONDS.toMillis(now - healedAt);
                break;
            }
        }
        line("- recovered to baseline " + (recovered < 0 ? "NOT within the window" : recovered + " ms after the burst ended"));
    }

    /** Every node killed and restarted in turn under load (Windows has no graceful signal, so each stop is a crash). */
    private void rollingRestart(int seconds) throws Exception {
        final long gap = Math.max(TimeUnit.SECONDS.toNanos(seconds) / Math.max(1, fleet.size()), TimeUnit.MILLISECONDS.toNanos(ttlMillis));
        for (int i = 0; i < homes.size(); i++) {
            final NodeProcess node = fleet.get(i);
            final long at = System.nanoTime();
            fleet.remove(node);
            node.killHard();
            fleet.add(i, startNode(i));
            line("- restarted " + node.name() + " in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - at) + " ms");
            LockSupport.parkNanos(gap);
        }
    }

    /** A node's proxy, home and keystore, before it is started. */
    private void addNodeSlot(int i) throws Exception {
        proxies.add(new FaultProxy("n" + i, storeEndpoint.getHost(), storeEndpoint.getPort()));
        final Path home = results.resolve("nodes").resolve("n" + i);
        Files.createDirectories(home);
        NodeProcess.writeKeystore(home, Map.of("s3.client.default.access_key", storeAccess, "s3.client.default.secret_key", storeSecret));
        homes.add(home);
    }

    /**
     * A node added while the fleet is at its cap: how long until it carries its share, and what write latency does
     * meanwhile. Each minute after the join reports the writes' p99 and every node's shards, and the fleet's capacity
     * counters say how the share moved -- handed off by full nodes, steered to the new one, or only refused.
     */
    private void scaleOut(int seconds) throws Exception {
        final long beforeFrom = System.nanoTime() - TimeUnit.SECONDS.toNanos(60);
        line("- the minute before: " + load.window("write", beforeFrom, System.nanoTime()) + "; held " + perNodeField("capacity", "held"));
        final Map<String, Long> countersBefore = capacityCounters();
        final int i = homes.size();
        final String name = "n" + i;
        addNodeSlot(i);
        final long joinedAt = System.nanoTime();
        fleet.add(startNode(i));
        line("- added " + name + " in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - joinedAt) + " ms");
        long halfShareAt = -1;
        long minuteFrom = System.nanoTime();
        while (System.nanoTime() - joinedAt < TimeUnit.SECONDS.toNanos(seconds)) {
            sleepSeconds(15);
            final Map<String, Long> held = perNodeField("capacity", "held");
            final long mine = held.getOrDefault(name, 0L);
            final double others = held.entrySet()
                .stream()
                .filter(e -> e.getKey().equals(name) == false)
                .mapToLong(Map.Entry::getValue)
                .average()
                .orElse(0);
            if (halfShareAt < 0 && others > 0 && mine >= others / 2) {
                halfShareAt = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - joinedAt);
            }
            if (System.nanoTime() - minuteFrom >= TimeUnit.SECONDS.toNanos(60)) {
                final long now = System.nanoTime();
                line(
                    "- minute "
                        + TimeUnit.NANOSECONDS.toMinutes(now - joinedAt)
                        + " after the join: "
                        + load.window("write", minuteFrom, now)
                        + "; held "
                        + held
                );
                minuteFrom = now;
            }
        }
        line(
            "- "
                + name
                + " held half the others' average "
                + (halfShareAt < 0 ? "NOT within the scenario" : halfShareAt + " ms after joining")
        );
        final Map<String, Long> countersAfter = capacityCounters();
        final Map<String, Long> moved = new TreeMap<>();
        for (Map.Entry<String, Long> each : countersAfter.entrySet()) {
            moved.put(each.getKey(), each.getValue() - countersBefore.getOrDefault(each.getKey(), 0L));
        }
        line("- capacity counters across the fleet during the scenario: " + moved);
    }

    private static final List<String> PHASES = List.of(
        "describe_millis",
        "acquire_millis",
        "mark_owned_millis",
        "open_millis",
        "opened",
        "view_millis"
    );

    /** Writer activations' time by phase, summed over the live nodes. */
    private Map<String, Long> activationPhases() {
        final Map<String, Long> totals = new TreeMap<>();
        for (String field : PHASES) {
            totals.put(field, fleetSum("activation_phases", field));
        }
        for (String step : org.opensearch.serverless.shard.ShardReconciler.WRITER_OPEN_PHASES) {
            totals.put("writer_open." + step, fleetSum("writer_open", step + "_millis"));
        }
        totals.put("writer_open.opened", fleetSum("writer_open", "opened"));
        totals.put("writer_open.log_records", fleetSum("writer_open", "log_records"));
        return totals;
    }

    /** The average writer activation, by phase, between two readings. Restarted nodes start their sums again. */
    private static String activationPhaseAverages(Map<String, Long> before, Map<String, Long> after) {
        final long opened = after.getOrDefault("opened", 0L) - before.getOrDefault("opened", 0L);
        if (opened <= 0) {
            return "writer activations: none opened";
        }
        final StringBuilder out = new StringBuilder("writer activations: " + opened + " opened; average ms by phase:");
        for (String field : PHASES.subList(0, 4)) {
            final long spent = Math.max(0L, after.getOrDefault(field, 0L) - before.getOrDefault(field, 0L));
            out.append(' ').append(field.replace("_millis", "")).append('=').append(spent / opened);
        }
        final long view = Math.max(0L, after.getOrDefault("view_millis", 0L) - before.getOrDefault("view_millis", 0L));
        out.append(" (of open, the local view ").append(view / opened).append(')');
        final long writers = after.getOrDefault("writer_open.opened", 0L) - before.getOrDefault("writer_open.opened", 0L);
        if (writers > 0) {
            out.append("; a writer's open by step, average ms over ").append(writers).append(':');
            for (String step : org.opensearch.serverless.shard.ShardReconciler.WRITER_OPEN_PHASES) {
                final String key = "writer_open." + step;
                out.append(' ')
                    .append(step)
                    .append('=')
                    .append(Math.max(0L, after.getOrDefault(key, 0L) - before.getOrDefault(key, 0L)) / writers);
            }
            final long records = after.getOrDefault("writer_open.log_records", 0L) - before.getOrDefault("writer_open.log_records", 0L);
            out.append(String.format(java.util.Locale.ROOT, "; log records read per open %.1f", (double) records / writers));
        }
        return out.toString();
    }

    /** The fleet's capacity counters, summed over the live nodes. */
    private Map<String, Long> capacityCounters() {
        final Map<String, Long> counters = new TreeMap<>();
        for (String field : List.of(
            "reader_opens_refused",
            "activations_refused",
            "evicted",
            "handed_off",
            "writes_steered",
            "takeovers_overcommitted"
        )) {
            counters.put(field, fleetSum("capacity", field));
        }
        return counters;
    }

    /** Deletes and recreates of the very names being written to: a write to a deleted index must be refused. */
    private void storm(int seconds, FleetLoad.Shape base) throws Exception {
        load.reshape(
            new FleetLoad.Shape(
                base.activeIndices(),
                base.population(),
                base.dormantFraction(),
                0.5,
                10,
                base.writesPerSecond(),
                Math.max(10, base.churnOpsPerSecond())
            )
        );
        try {
            sleepSeconds(seconds);
        } finally {
            load.reshape(base);
        }
    }

    // ------------------------------------------------------------------------------------------------ the ledger check

    /**
     * Stops the load, lets the fleet settle, and reads back every write recorded since {@code mark}: each acked one
     * must be there, each refused one must not. The load resumes afterwards.
     */
    private FleetLedger.Verdict verify(int mark) throws Exception {
        final Thread watchdog = new Thread(() -> {
            try {
                Thread.sleep(TimeUnit.MINUTES.toMillis(10));
                dumpNodes("verify-running-10-minutes");
            } catch (InterruptedException ignored) {}
        }, "fleet-verify-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();
        try {
            return verifyChecked(mark);
        } finally {
            watchdog.interrupt();
        }
    }

    /** Thread dumps and output so far of every node, for a check that is taking too long. */
    @SuppressForbidden(reason = "the harness's own stacks, for a check that has hung")
    private void dumpNodes(String why) {
        for (NodeProcess node : fleet) {
            try {
                final Process jcmd = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "jcmd").toString(),
                    Long.toString(node.pid()),
                    "Thread.print"
                ).redirectErrorStream(true).start();
                final byte[] dump = jcmd.getInputStream().readAllBytes();
                jcmd.waitFor(60, TimeUnit.SECONDS);
                Files.write(results.resolve(node.name() + "-" + why + ".threads.txt"), dump);
                Files.write(results.resolve(node.name() + "-" + why + ".log"), node.output(), StandardCharsets.UTF_8);
            } catch (Exception e) {
                logger.warn("could not dump " + node.name(), e);
            }
        }
        try {
            final Path harness = results.resolve("harness-" + why + ".threads.txt");
            final StringBuilder all = new StringBuilder();
            Thread.getAllStackTraces().forEach((thread, stack) -> {
                all.append(thread.getName()).append('\n');
                for (StackTraceElement frame : stack) {
                    all.append("    at ").append(frame).append('\n');
                }
            });
            Files.writeString(harness, all.toString(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.warn("could not dump the harness", e);
        }
        line("- dumped threads and logs (" + why + ") to " + results);
    }

    /** What the stale-read check found. */
    private record StaleCheck(int indices, int withoutWriter, int complete, int reportedBehind, int lagging, int inconclusive, List<
        String> violations, Map<String, Integer> notAnswering) {
        @Override
        public String toString() {
            return indices
                + " indices searched; "
                + withoutWriter
                + " with no live writer: "
                + complete
                + " complete, "
                + reportedBehind
                + " reported as not answering, "
                + violations.size()
                + " SILENTLY BEHIND; "
                + lagging
                + " with a live writer behind by the publish window; "
                + inconclusive
                + " changed owner during the check; not answering, by first reason: "
                + notAnswering;
        }
    }

    /**
     * Searches every index written so far and holds each one with no live writer to the guarantee: it answers with at
     * least every acknowledged document, or reports the shard as not answering. One that does neither served a
     * commit behind the log as though it were current.
     *
     * <p>A shard with a live writer is read from its last published commit by design, so falling short there is
     * the publish window and only counted. An owner that changes between the head read and the search makes the
     * answer inconclusive rather than wrong, and is counted as that.
     */
    private StaleCheck staleReadCheck() throws Exception {
        final Map<String, java.util.Set<String>> ackedIds = new TreeMap<>();
        for (FleetLedger.Write write : new ArrayList<>(ledger.writes())) {
            if (write.outcome() == FleetLedger.Outcome.ACKED && write.index().startsWith("logs-") && ledger.mustBePresent(write)) {
                ackedIds.computeIfAbsent(write.index(), k -> new java.util.HashSet<>()).add(write.id());
            }
        }
        final java.util.concurrent.atomic.AtomicInteger withoutWriter = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger complete = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger reportedBehind = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger lagging = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger inconclusive = new java.util.concurrent.atomic.AtomicInteger();
        final List<String> violations = java.util.Collections.synchronizedList(new ArrayList<>());
        final Map<String, java.util.concurrent.atomic.AtomicInteger> why = new java.util.concurrent.ConcurrentHashMap<>();
        final ExecutorService searchers = Executors.newFixedThreadPool(16);
        try {
            final List<Future<?>> pending = new ArrayList<>();
            for (Map.Entry<String, java.util.Set<String>> index : ackedIds.entrySet()) {
                pending.add(searchers.submit(() -> {
                    final var before = plane.heads().read(index.getKey(), 0);
                    final boolean writerLive = before.isPresent() && liveOwner(before.get().ownerNodeId());
                    final List<NodeProcess> nodes = load.inRotation();
                    final String body = load.requestBody(
                        nodes.get(randomIntBetween(0, nodes.size() - 1)),
                        "POST",
                        "/" + index.getKey() + "/_search?size=0&track_total_hits=true",
                        "{\"query\":{\"match_all\":{}}}"
                    );
                    final var after = plane.heads().read(index.getKey(), 0);
                    final long total = firstNumber(body, "value");
                    final long failed = firstNumber(body, "failed");
                    final int expected = index.getValue().size();
                    if (before.map(h -> h.term()).orElse(0L).equals(after.map(h -> h.term()).orElse(0L)) == false) {
                        inconclusive.incrementAndGet();
                    } else if (writerLive) {
                        if (total < expected) {
                            lagging.incrementAndGet();
                        }
                    } else {
                        withoutWriter.incrementAndGet();
                        if (failed > 0) {
                            reportedBehind.incrementAndGet();
                            why.computeIfAbsent(firstFailureType(body), k -> new java.util.concurrent.atomic.AtomicInteger())
                                .incrementAndGet();
                        } else if (total >= expected) {
                            complete.incrementAndGet();
                        } else {
                            violations.add(index.getKey() + ": " + total + " of " + expected + " acknowledged, no failure; head " + after);
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> each : pending) {
                try {
                    each.get();
                } catch (java.util.concurrent.ExecutionException e) {
                    // A search that failed outright said so; it is not a silent answer.
                    reportedBehind.incrementAndGet();
                    final String message = String.valueOf(e.getCause() == null ? e : e.getCause().getMessage());
                    why.computeIfAbsent("whole search: " + firstFailureType(message), k -> new java.util.concurrent.atomic.AtomicInteger())
                        .incrementAndGet();
                }
            }
        } finally {
            searchers.shutdownNow();
        }
        return new StaleCheck(
            ackedIds.size(),
            withoutWriter.get(),
            complete.get(),
            reportedBehind.get(),
            lagging.get(),
            inconclusive.get(),
            List.copyOf(violations),
            new TreeMap<>(why.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get())))
        );
    }

    /** The type of the first failure a search response or error names, its status if it names none. */
    private static String firstFailureType(String body) {
        final Matcher failure = Pattern.compile("\"failures\":\\[\\{.*?\"type\":\"([^\"]+)\"", Pattern.DOTALL).matcher(body);
        if (failure.find()) {
            return failure.group(1);
        }
        final Matcher type = Pattern.compile("\"type\":\"([^\"]+)\"").matcher(body);
        if (type.find()) {
            return type.group(1);
        }
        final Matcher status = Pattern.compile("\\b([45]\\d\\d)\\b").matcher(body);
        return status.find() ? "status " + status.group(1) : body.length() > 60 ? body.substring(0, 60) : body;
    }

    /** Whether a node holds an unexpired lease, read from the store. */
    private boolean liveOwner(String nodeId) throws java.io.IOException {
        if (nodeId == null) {
            return false;
        }
        return plane.membership().read(nodeId).map(lease -> lease.isExpiredAt(System.currentTimeMillis()) == false).orElse(false);
    }

    private static long firstNumber(String body, String field) {
        final Matcher m = Pattern.compile("\"" + field + "\":(-?\\d+)").matcher(body);
        return m.find() ? Long.parseLong(m.group(1)) : -1L;
    }

    /** Appends findings of one kind to the forensics file. */
    private void forensicsLines(String kind, List<String> findings) throws java.io.IOException {
        final StringBuilder out = new StringBuilder();
        for (String finding : findings) {
            out.append(kind).append(' ').append(finding).append('\n');
        }
        Files.writeString(
            results.resolve("forensics.txt"),
            out.toString(),
            StandardCharsets.UTF_8,
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.APPEND
        );
    }

    private FleetLedger.Verdict verifyChecked(int mark) throws Exception {
        load.stop();
        ledger.flush();
        // Before the read-back, which takes over every shard it finds behind and so would hide what is checked here.
        final StaleCheck stale = staleReadCheck();
        line("- stale-read check: " + stale);
        if (stale.violations().isEmpty() == false) {
            forensicsLines("STALE", stale.violations());
            keepBucket = true;
            fail(
                "a search answered from a commit behind acknowledged writes, with no writer and no failure: " + stale + "; see " + results
            );
        }
        final List<FleetLedger.Write> writes = new ArrayList<>(ledger.writes()).subList(mark, ledger.writes().size());
        long acked = 0;
        long refused = 0;
        long unknown = 0;
        long excused = 0;
        final Map<String, List<FleetLedger.Write>> mustExist = new HashMap<>();
        final Map<String, List<FleetLedger.Write>> mustNotExist = new HashMap<>();
        for (FleetLedger.Write write : writes) {
            switch (write.outcome()) {
                case ACKED -> {
                    acked++;
                    if (ledger.mustBePresent(write)) {
                        mustExist.computeIfAbsent(write.index(), k -> new ArrayList<>()).add(write);
                    } else {
                        excused++;
                    }
                }
                case REFUSED -> {
                    refused++;
                    mustNotExist.computeIfAbsent(write.index(), k -> new ArrayList<>()).add(write);
                }
                case UNKNOWN -> unknown++;
            }
        }
        // First every index once; then only those with something missing, each retried on its own budget. A
        // shard whose owner died is read from its published commit until someone takes it and replays its log,
        // so a retry writes to it first, which makes somebody take it.
        final Map<String, List<FleetLedger.Write>> missing = new java.util.concurrent.ConcurrentHashMap<>();
        // In parallel: a long scenario writes to tens of thousands of dormant indices, each read back by opening a reader,
        // and one at a time that was hours.
        final ExecutorService firstPass = Executors.newFixedThreadPool(16);
        try {
            final List<Future<?>> reads = new ArrayList<>();
            for (Map.Entry<String, List<FleetLedger.Write>> index : mustExist.entrySet()) {
                reads.add(firstPass.submit(() -> {
                    final List<FleetLedger.Write> absent = missingOf(index.getKey(), index.getValue(), true);
                    if (absent.isEmpty() == false) {
                        missing.put(index.getKey(), absent);
                    }
                    return null;
                }));
            }
            for (Future<?> read : reads) {
                read.get();
            }
        } finally {
            firstPass.shutdown();
        }
        final List<FleetLedger.Write> lost = java.util.Collections.synchronizedList(new ArrayList<>());
        final List<FleetLedger.Write> unreached = java.util.Collections.synchronizedList(new ArrayList<>());
        final List<String> lostEvidence = java.util.Collections.synchronizedList(new ArrayList<>());
        final Map<String, List<FleetLedger.Write>> unsettled = new java.util.concurrent.ConcurrentHashMap<>();
        if (missing.isEmpty() == false) {
            line("- first read-back found " + missing.size() + " indices with absent acknowledged writes; retrying each");
            for (Map.Entry<String, List<FleetLedger.Write>> index : missing.entrySet()) {
                final List<Long> seqNos = new ArrayList<>();
                final Map<String, List<Long>> failedByType = new java.util.TreeMap<>();
                for (FleetLedger.Write write : index.getValue()) {
                    final String failure = readFailures.get(write.id());
                    if (failure == null) {
                        seqNos.add(write.seqNo());
                    } else {
                        failedByType.computeIfAbsent(failure, k -> new ArrayList<>()).add(write.seqNo());
                    }
                }
                final var descriptor = plane.describe(index.getKey());
                line(
                    "  - "
                        + index.getKey()
                        + ": absent seqNos "
                        + seqNos
                        + (failedByType.isEmpty() ? "" : ", unreadable " + failedByType)
                        + " of "
                        + mustExist.get(index.getKey()).size()
                        + " acked; head "
                        + plane.heads().read(index.getKey(), 0)
                        + "; manifest "
                        + (descriptor.isPresent()
                            ? plane.segmentPublisher(index.getKey(), descriptor.get().uuid(), 0).readManifest()
                            : "no index")
                );
            }
            final ExecutorService retries = Executors.newFixedThreadPool(Math.min(16, missing.size()));
            try {
                final List<Future<?>> pending = new ArrayList<>();
                for (Map.Entry<String, List<FleetLedger.Write>> index : missing.entrySet()) {
                    pending.add(retries.submit(() -> {
                        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(4 * ttlMillis + 30_000L);
                        List<FleetLedger.Write> still = index.getValue();
                        while (still.isEmpty() == false && System.nanoTime() < deadline) {
                            load.write(coordinatorFor(index.getKey()), index.getKey());
                            sleepSeconds(2);
                            still = missingOf(index.getKey(), still, true);
                        }
                        if (still.isEmpty() == false) {
                            unsettled.put(index.getKey(), still);
                        }
                        return null;
                    }));
                }
                for (Future<?> f : pending) {
                    f.get();
                }
            } finally {
                retries.shutdown();
            }
        }
        // What the fleet would not hand over in time is decided from the store: the commit and the log a successor
        // would replay. A shard changing hands cannot hide a write from that, and one that cannot be read is
        // unreached -- inconclusive -- never lost. See DurabilityCheck.
        if (unsettled.isEmpty() == false) {
            line("- read-back could not reach " + unsettled.size() + " indices through the fleet; deciding them from the store");
            final DurabilityCheck check = new DurabilityCheck(plane, createTempDir());
            final ExecutorService checkers = Executors.newFixedThreadPool(Math.min(8, unsettled.size()));
            try {
                final List<Future<?>> pending = new ArrayList<>();
                for (Map.Entry<String, List<FleetLedger.Write>> index : unsettled.entrySet()) {
                    pending.add(checkers.submit(() -> {
                        final Map<String, FleetLedger.Write> byId = new LinkedHashMap<>();
                        for (FleetLedger.Write write : index.getValue()) {
                            byId.put(write.id(), write);
                        }
                        final var findings = check.check(index.getKey(), byId.keySet(), TimeUnit.MINUTES.toMillis(2));
                        for (DurabilityCheck.Finding finding : findings.values()) {
                            switch (finding.status()) {
                                case LOST -> {
                                    lost.add(byId.get(finding.id()));
                                    lostEvidence.add(finding.index() + "/" + finding.id() + ": " + finding.evidence());
                                }
                                case UNREACHED -> unreached.add(byId.get(finding.id()));
                                case VERIFIED -> {
                                }
                            }
                        }
                        return null;
                    }));
                }
                for (Future<?> f : pending) {
                    f.get();
                }
            } finally {
                checkers.shutdown();
            }
            if (lostEvidence.isEmpty() == false) {
                forensicsLines("LOST-EVIDENCE", lostEvidence);
            }
        }
        final List<FleetLedger.Write> refusedButVisible = new ArrayList<>();
        for (Map.Entry<String, List<FleetLedger.Write>> index : mustNotExist.entrySet()) {
            refusedButVisible.addAll(missingOf(index.getKey(), index.getValue(), false));
        }
        load.start();
        return new FleetLedger.Verdict(acked, acked - excused, refused, unknown, lost, refusedButVisible, excused, unreached);
    }

    /**
     * The node the read-back asks about an index, the same one each time and spread over the fleet by name.
     *
     * <p>A write to a shard nobody owns is taken by the node it was sent to, so the read-back sending every retry to
     * one node made that node take every shard being retried: twelve hundred of them, against a cap of four hundred,
     * and the rest went unread until the budget ran out.
     */
    private NodeProcess coordinatorFor(String index) {
        final List<NodeProcess> nodes = load.inRotation();
        return nodes.get(Math.floorMod(index.hashCode(), nodes.size()));
    }

    private static final Pattern DOC = Pattern.compile("\"_id\":\"([^\"]+)\"[^{}]*?\"found\":(true|false)");
    /** An item the node could not read -- not the same as one it read and did not find. */
    private static final Pattern FAILED = Pattern.compile(
        "\"_id\":\"([^\"]+)\",\"error\":\\{\"type\":\"([^\"]+)\",\"reason\":\"((?:[^\"\\\\]|\\\\.){0,160})"
    );
    /** Why the last read-back of an id failed rather than answered, for the diagnostics. */
    private final Map<String, String> readFailures = new java.util.concurrent.ConcurrentHashMap<>();

    /** The writes among {@code writes} that are absent (wantPresent) or present (otherwise), by multi-get. */
    private List<FleetLedger.Write> missingOf(String index, List<FleetLedger.Write> writes, boolean wantPresent) throws Exception {
        final List<FleetLedger.Write> odd = new ArrayList<>();
        for (int from = 0; from < writes.size(); from += 200) {
            final List<FleetLedger.Write> batch = writes.subList(from, Math.min(writes.size(), from + 200));
            final StringBuilder body = new StringBuilder("{\"ids\":[");
            for (int i = 0; i < batch.size(); i++) {
                body.append(i == 0 ? "" : ",").append('"').append(batch.get(i).id()).append('"');
            }
            body.append("]}");
            final Map<String, Boolean> found = new HashMap<>();
            String answer;
            try {
                answer = load.requestBody(coordinatorFor(index), "POST", "/" + index + "/_mget?_source=false", body.toString());
            } catch (IllegalStateException e) {
                // An index that no longer exists holds none of them.
                if (e.getMessage().contains("index_not_found")) {
                    answer = "";
                } else {
                    throw e;
                }
            }
            final Matcher m = DOC.matcher(answer);
            while (m.find()) {
                found.put(m.group(1), Boolean.parseBoolean(m.group(2)));
                readFailures.remove(m.group(1));
            }
            final Matcher failed = FAILED.matcher(answer);
            while (failed.find()) {
                readFailures.put(failed.group(1), failed.group(2) + ": " + failed.group(3));
            }
            for (FleetLedger.Write write : batch) {
                final boolean present = found.getOrDefault(write.id(), false);
                if (present != wantPresent) {
                    odd.add(write);
                }
            }
        }
        return odd;
    }

    /** What each lost write looks like from every node, and what the store says of its shard. */
    private void forensics(FleetLedger.Verdict verdict) throws Exception {
        final StringBuilder out = new StringBuilder();
        final java.util.Set<String> indices = new java.util.TreeSet<>();
        for (FleetLedger.Write write : verdict.lost()) {
            indices.add(write.index());
            out.append("LOST ").append(write).append('\n');
            for (NodeProcess node : fleet) {
                try {
                    out.append("  via ")
                        .append(node.name())
                        .append(": ")
                        .append(load.requestBody(node, "GET", "/" + write.index() + "/_doc/" + write.id(), null))
                        .append('\n');
                } catch (Exception e) {
                    out.append("  via ").append(node.name()).append(": ").append(e.getMessage()).append('\n');
                }
            }
        }
        for (FleetLedger.Write write : verdict.refusedButVisible()) {
            indices.add(write.index());
            out.append("REFUSED-BUT-VISIBLE ").append(write).append('\n');
        }
        for (String index : indices) {
            out.append("index ").append(index).append(": descriptor ").append(plane.describe(index)).append('\n');
            out.append("  head ").append(plane.heads().read(index, 0)).append('\n');
            plane.describe(index).ifPresent(d -> {
                try {
                    out.append("  manifest ").append(plane.segmentPublisher(index, d.uuid(), 0).readManifest()).append('\n');
                } catch (Exception e) {
                    out.append("  manifest unreadable: ").append(e).append('\n');
                }
            });
            try {
                out.append("  count via n0: ").append(load.requestBody(fleet.get(0), "GET", "/" + index + "/_count", null)).append('\n');
            } catch (Exception e) {
                out.append("  count failed: ").append(e.getMessage()).append('\n');
            }
        }
        Files.writeString(
            results.resolve("forensics.txt"),
            out.toString(),
            StandardCharsets.UTF_8,
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.APPEND
        );
        line("- forensics in " + results.resolve("forensics.txt"));
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private NodeProcess startNode(int i) throws Exception {
        final Map<String, String> settings = new LinkedHashMap<>(nodeSettings);
        settings.put(ObjectStores.ENDPOINT, proxies.get(i).endpoint());
        settings.put("cluster.name", "serverless-fleet");
        return NodeProcess.start(
            "n" + i,
            null,
            homes.get(i),
            settings,
            // The maximum only. Committing the whole heap at start made a node restarted mid-run, on a machine whose
            // memory the fleet and the store already had, page for longer than the harness would wait.
            List.of("-Xmx" + prop("heap", "2g"), "-Xms256m", "-Dlog4j.configurationFile=" + logConfig(homes.get(i)))
        );
    }

    /**
     * A log file per node, appended across restarts. The nodes have no logging configuration of their own, so
     * without this they print nothing below ERROR -- and the first wedged shard a fleet run found left no trail
     * of how it got there.
     */
    private Path logConfig(Path home) throws java.io.IOException {
        final Path config = home.resolve("config").resolve("log4j2-fleet.properties");
        final String file = home.resolve("node.log").toAbsolutePath().toString().replace('\\', '/');
        Files.writeString(
            config,
            String.join(
                "\n",
                "status = error",
                "appender.file.type = File",
                "appender.file.name = file",
                "appender.file.fileName = " + file,
                "appender.file.append = true",
                "appender.file.immediateFlush = true",
                "appender.file.layout.type = PatternLayout",
                "appender.file.layout.pattern = [%d{ISO8601}][%-5p][%c{1.}] [%t] %m%n%throwable",
                "rootLogger.level = warn",
                "rootLogger.appenderRef.file.ref = file",
                "logger.serverless.name = org.opensearch.serverless",
                "logger.serverless.level = " + prop("log_level", "info"),
                // A stack trace for every shard a search could not reach, every two seconds, for every node down:
                // tens of thousands of lines a minute in a failure scenario, burying what the log is kept for.
                "logger.fanout.name = org.opensearch.serverless.rest.Fanout",
                "logger.fanout.level = error",
                "logger.searchfanout.name = org.opensearch.serverless.rest.SearchFanout",
                "logger.searchfanout.level = error",
                debugLoggers(),
                ""
            ),
            StandardCharsets.UTF_8
        );
        return config.toAbsolutePath();
    }

    private void populate(int population) throws Exception {
        final ExecutorService creators = Executors.newFixedThreadPool(32);
        try {
            final List<Future<?>> pending = new ArrayList<>();
            final int perTask = 1_000;
            for (int from = 0; from < population; from += perTask) {
                final int start = from;
                pending.add(creators.submit(() -> {
                    for (int i = start; i < Math.min(population, start + perTask); i++) {
                        final String name = FleetLoad.populationName(i);
                        try {
                            plane.createIndex(new IndexDescriptor(name, "uuid-" + name, 1, MAPPING, null));
                        } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
                            // A re-run against a kept bucket.
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : pending) {
                f.get();
            }
        } finally {
            creators.shutdown();
            assertTrue(creators.awaitTermination(10, TimeUnit.MINUTES));
        }
    }

    /** Longer than the load's request timeout, so the last writes sent have been answered one way or the other. */
    private static final int IDLE_SETTLE_SECONDS = 75;

    private static final Pattern HELD_SHARD = Pattern.compile("\"kind\"\\s*:\\s*\"(writer|reader|frozen_view)\"");

    /** The columns activation.csv records per node, as object/field of the node's stats. */
    private static final String[][] SAMPLED = {
        { "activation_queue", "queued" },
        { "activation_queue", "running" },
        { "activation_queue", "done" },
        { "activation_queue", "wait_millis" },
        { "activation_queue", "run_millis" },
        { "activation_queue", "max_wait_millis" },
        { "activation_queue", "max_run_millis" },
        { "activation_queue", "limit" },
        { "capacity", "in_use" },
        { "capacity", "refused_last_minute" },
        { "fleet", "wanted_nodes" },
        { "fleet", "members" },
        { "jvm", "heap_used_bytes" },
        { "janitor", "passes" },
        { "janitor", "store_requests" },
        { "janitor", "examined" },
        { "janitor", "released" },
        { "janitor", "replays" },
        { "write_backpressure", "limit" },
        { "write_backpressure", "append_millis" },
        { "write_backpressure", "refused" } };

    private static final Pattern NUMBER_FIELD = Pattern.compile("\"([a-z_]+)\"\\s*:\\s*(-?\\d+)");

    /** The numeric fields of one object, by name, in a stats body; an object nested in it is not looked into. */
    static Map<String, Long> objectFields(String body, String object) {
        final Map<String, Long> fields = new HashMap<>();
        final int at = body.indexOf("\"" + object + "\"");
        if (at < 0) {
            return fields;
        }
        final int open = body.indexOf('{', at);
        final int close = body.indexOf('}', open);
        if (open < 0 || close < 0) {
            return fields;
        }
        final Matcher m = NUMBER_FIELD.matcher(body.substring(open, close));
        while (m.find()) {
            fields.putIfAbsent(m.group(1), Long.parseLong(m.group(2)));
        }
        return fields;
    }

    /** One node's stats body, or null if it did not answer. */
    private String statsOf(java.net.http.HttpClient client, NodeProcess node) {
        try {
            return client.send(
                java.net.http.HttpRequest.newBuilder()
                    .uri(URI.create("http://" + node.http() + "/_serverless/stats"))
                    .timeout(java.time.Duration.ofSeconds(10))
                    .GET()
                    .build(),
                java.net.http.HttpResponse.BodyHandlers.ofString()
            ).body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /** Everything the janitor has done across the fleet: shards released, replayed, and claims forgotten. */
    private long janitorWork() {
        return fleetSum("janitor", "released") + fleetSum("janitor", "replays") + fleetSum("janitor", "claims_forgotten");
    }

    /** One field of every live node's stats, by node name. */
    private Map<String, Long> perNodeField(String object, String field) {
        final Map<String, Long> values = new TreeMap<>();
        try (
            java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(5)).build()
        ) {
            for (NodeProcess node : List.copyOf(fleet)) {
                final String body = statsOf(client, node);
                if (body != null) {
                    final Long value = objectFields(body, object).get(field);
                    if (value != null) {
                        values.put(node.name(), value);
                    }
                }
            }
        }
        return values;
    }

    /** One field summed over the live nodes' stats, e.g. the janitor's store requests. */
    private long fleetSum(String object, String field) {
        long sum = 0;
        try (
            java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(5)).build()
        ) {
            for (NodeProcess node : List.copyOf(fleet)) {
                final String body = statsOf(client, node);
                if (body != null) {
                    sum += objectFields(body, object).getOrDefault(field, 0L);
                }
            }
        }
        return sum;
    }

    /**
     * Every node's activation queue, janitor, write limiter and held shards, every fifteen seconds, to activation.csv.
     */
    private Thread startActivationSampler() throws java.io.IOException {
        final Path csv = results.resolve("activation.csv");
        final StringBuilder header = new StringBuilder("epoch_ms,node,held");
        for (String[] column : SAMPLED) {
            header.append(',').append(column[0]).append('.').append(column[1]);
        }
        Files.writeString(csv, header.append('\n'));
        final Thread thread = new Thread(() -> {
            try (
                java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                    .connectTimeout(java.time.Duration.ofSeconds(5))
                    .build()
            ) {
                while (Thread.currentThread().isInterrupted() == false) {
                    final StringBuilder rows = new StringBuilder();
                    for (NodeProcess node : List.copyOf(fleet)) {
                        final String body = statsOf(client, node);
                        rows.append(System.currentTimeMillis()).append(',').append(node.name()).append(',');
                        if (body == null) {
                            rows.append(",".repeat(SAMPLED.length)).append('\n');
                            continue;
                        }
                        int held = 0;
                        final Matcher h = HELD_SHARD.matcher(body);
                        while (h.find()) {
                            held++;
                        }
                        rows.append(held);
                        for (String[] column : SAMPLED) {
                            final Long value = objectFields(body, column[0]).get(column[1]);
                            rows.append(',').append(value == null ? "" : value.toString());
                        }
                        rows.append('\n');
                    }
                    Files.writeString(csv, rows, StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND);
                    Thread.sleep(15_000L);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warn("the activation sampler stopped", e);
            }
        }, "fleet-activation-sampler");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /** How many shards each live node holds open, as it reports itself; a node that does not answer is left out. */
    private Map<String, Integer> heldShardsPerNode() {
        final Map<String, Integer> held = new TreeMap<>();
        try (
            java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(5)).build()
        ) {
            for (NodeProcess node : fleet) {
                try {
                    final var response = client.send(
                        java.net.http.HttpRequest.newBuilder()
                            .uri(URI.create("http://" + node.http() + "/_serverless/stats"))
                            .timeout(java.time.Duration.ofSeconds(30))
                            .GET()
                            .build(),
                        java.net.http.HttpResponse.BodyHandlers.ofString()
                    );
                    int count = 0;
                    final Matcher m = HELD_SHARD.matcher(response.body());
                    while (m.find()) {
                        count++;
                    }
                    held.put(node.name(), count);
                } catch (Exception e) {
                    line("- could not read held shards from " + node.name() + ": " + e);
                }
            }
        }
        return held;
    }

    /** Index names whose single shard's head names this node, among those the load has written to. */
    private List<String> shardsOwnedBy(String nodeId) throws Exception {
        final Set<String> touched = new java.util.TreeSet<>();
        for (FleetLedger.Write write : ledger.writes()) {
            if (write.outcome() == FleetLedger.Outcome.ACKED && write.index().startsWith("logs-")) {
                touched.add(write.index());
            }
        }
        final List<String> owned = Collections.synchronizedList(new ArrayList<>());
        final ExecutorService readers = Executors.newFixedThreadPool(16);
        try {
            final List<Future<?>> pending = new ArrayList<>();
            for (String index : touched) {
                pending.add(readers.submit(() -> {
                    final var head = plane.heads().read(index, 0);
                    if (head.isPresent() && nodeId.equals(head.get().ownerNodeId())) {
                        owned.add(index);
                    }
                    return null;
                }));
            }
            for (Future<?> f : pending) {
                f.get();
            }
        } finally {
            readers.shutdown();
        }
        return owned;
    }

    /**
     * The node a fault is aimed at: by default the one holding the median number of shards, so runs compare. Aimed at the
     * busiest, a kill took 282 shards in one run and 847 in the next, and recovery scaled with that rather than with
     * anything changed between them. {@code tests.fleet.victim=busiest} keeps the worst case available.
     */
    private int victimNode() throws Exception {
        if ("busiest".equals(prop("victim", "median"))) {
            return busiestNode();
        }
        final List<int[]> held = new ArrayList<>();
        for (int i = 0; i < fleet.size(); i++) {
            held.add(new int[] { i, shardsOwnedBy(fleet.get(i).nodeId()).size() });
        }
        held.sort(java.util.Comparator.comparingInt(h -> h[1]));
        final StringBuilder spread = new StringBuilder("- shards owned by node:");
        for (int[] h : held) {
            spread.append(' ').append(fleet.get(h[0]).name()).append('=').append(h[1]);
        }
        line(spread.append(" (lowest to highest)").toString());
        return held.get(held.size() / 2)[0];
    }

    private int busiestNode() throws Exception {
        int busiest = 0;
        int most = -1;
        for (int i = 0; i < fleet.size(); i++) {
            final int owned = shardsOwnedBy(fleet.get(i).nodeId()).size();
            if (owned > most) {
                most = owned;
                busiest = i;
            }
        }
        return busiest;
    }

    /** Writes to each index, through the live nodes, until each acknowledges one; per index, how long that took. */
    /** When a survivor first held each probed shard's head, in milliseconds from the fault; filled by the last probe. */
    private final Map<String, Long> acquiredAfter = new java.util.concurrent.ConcurrentHashMap<>();

    private static Map<String, Long> gaps(Map<String, Long> acquired, Map<String, Long> writable) {
        final Map<String, Long> gaps = new TreeMap<>();
        writable.forEach((index, at) -> {
            final Long held = acquired.get(index);
            if (held != null) {
                gaps.put(index, Math.max(0L, at - held));
            }
        });
        return gaps;
    }

    private Map<String, Long> probeUntilWritable(List<String> indices, long fromNanos, int budgetSeconds, String victimNodeId)
        throws Exception {
        final Map<String, Long> writableAfter = new java.util.concurrent.ConcurrentHashMap<>();
        final long deadline = fromNanos + TimeUnit.SECONDS.toNanos(Math.max(budgetSeconds, 1));
        acquiredAfter.clear();
        // Which survivor held each head, and when: once a second, every shard not yet seen held by someone else.
        final Thread headWatch = new Thread(() -> {
            while (System.nanoTime() < deadline && acquiredAfter.size() < indices.size()) {
                for (String index : indices) {
                    if (acquiredAfter.containsKey(index)) {
                        continue;
                    }
                    try {
                        final var head = plane.heads().read(index, 0);
                        if (head.isPresent()
                            && head.get().ownerNodeId() != null
                            && victimNodeId.equals(head.get().ownerNodeId()) == false) {
                            acquiredAfter.put(index, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fromNanos));
                        }
                    } catch (Exception e) {
                        // Read again next round.
                    }
                }
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
        }, "fleet-head-watch");
        headWatch.setDaemon(true);
        headWatch.start();
        // Every shard probed at once. A pool of sixteen probed the first sixteen and queued the rest, so a shard
        // writable at thirty seconds was measured when its probe got a thread -- the harness's queue, not the fleet.
        final ExecutorService probes = Executors.newVirtualThreadPerTaskExecutor();
        try {
            final List<Future<?>> pending = new ArrayList<>();
            for (String index : indices) {
                pending.add(probes.submit(() -> {
                    // Round the nodes in rotation by attempt: these are virtual threads, where the test framework's
                    // randomness is not reachable.
                    int attempt = Math.floorMod(index.hashCode(), 1 << 16);
                    while (System.nanoTime() < deadline) {
                        final List<NodeProcess> live = load.inRotation();
                        final FleetLedger.Write write = load.write(live.get(Math.floorMod(attempt++, live.size())), index);
                        if (write.outcome() == FleetLedger.Outcome.ACKED) {
                            writableAfter.put(index, TimeUnit.NANOSECONDS.toMillis(write.endedNanos() - fromNanos));
                            return null;
                        }
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(250));
                    }
                    return null;
                }));
            }
            for (Future<?> f : pending) {
                f.get();
            }
        } finally {
            probes.shutdown();
        }
        return writableAfter;
    }

    private static String summarise(Map<String, Long> millis, int of) {
        final List<Long> sorted = new ArrayList<>(millis.values());
        Collections.sort(sorted);
        if (sorted.isEmpty()) {
            return "none of " + of + " writable within the window";
        }
        return String.format(
            Locale.ROOT,
            "%d of %d writable; p50 %d ms, p99 %d ms, max %d ms",
            sorted.size(),
            of,
            sorted.get(sorted.size() / 2),
            sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(0.99 * sorted.size()) - 1)),
            sorted.get(sorted.size() - 1)
        );
    }

    private static long maxOr(Map<String, Long> millis, long fallback) {
        return TimeUnit.MILLISECONDS.toNanos(millis.values().stream().mapToLong(Long::longValue).max().orElse(fallback));
    }

    private FleetLoad.Window windowFor(String nodeName, long fromNanos, long toNanos) {
        long count = 0;
        long acked = 0;
        long refused = 0;
        long unknown = 0;
        for (FleetLedger.Write write : ledger.writes()) {
            if (write.node().equals(nodeName) && write.endedNanos() >= fromNanos && write.endedNanos() <= toNanos) {
                count++;
                switch (write.outcome()) {
                    case ACKED -> acked++;
                    case REFUSED -> refused++;
                    case UNKNOWN -> unknown++;
                }
            }
        }
        return new FleetLoad.Window("write via " + nodeName, count, acked, refused, unknown, 0, 0);
    }

    private void resetStoreCounts() {
        for (FaultProxy proxy : proxies) {
            proxy.resetCounts();
        }
    }

    private Map<String, Long> totalStoreCounts() {
        final Map<String, Long> total = new TreeMap<>();
        for (FaultProxy proxy : proxies) {
            proxy.counts().forEach((op, n) -> total.merge(op, n, Long::sum));
        }
        return total;
    }

    private String storeCounts() {
        final Map<String, Map<String, Long>> perNode = new TreeMap<>();
        for (int i = 0; i < proxies.size(); i++) {
            perNode.put("n" + i, proxies.get(i).counts());
        }
        return perNode.toString();
    }

    private static Map<String, String> perSecond(Map<String, Long> counts, double seconds) {
        final Map<String, String> rates = new TreeMap<>();
        counts.forEach((op, n) -> rates.put(op, String.format(Locale.ROOT, "%.1f", n / seconds)));
        return rates;
    }

    private void header(int nodes, int population, int activePerNode, int seconds, String endpoint, String bucket) {
        line("# Fleet run " + Instant.now());
        line("");
        line("- nodes " + nodes + ", heap " + prop("heap", "2g") + " each, shard cap " + prop("max_shards", "100") + " per node");
        line("- population " + population + " indices, active set " + Math.min(population, activePerNode * nodes));
        line(
            "- load: "
                + prop("writes_per_second", "100")
                + " writes/s, open-loop with up to "
                + FleetLoad.MAX_IN_FLIGHT
                + " in flight, 5% to dormant indices, 5% to churned names; churn "
                + prop("churn_per_second", "5")
                + " ops/s; a wide search every 2 s; gets every 100 ms"
        );
        line("- store " + endpoint + ", bucket " + bucket + "; lease TTL " + ttlMillis + " ms; each scenario " + seconds + " s");
        line(
            "- machine: "
                + Runtime.getRuntime().availableProcessors()
                + " hardware threads, "
                + (Runtime.getRuntime().maxMemory() >> 20)
                + " MB heap in the harness, "
                + System.getProperty("os.name")
        );
    }

    private void line(String text) {
        report.append(text).append('\n');
        logger.info("fleet: {}", text);
    }

    private void writeReport() throws Exception {
        Files.writeString(results.resolve("results.md"), report.toString(), StandardCharsets.UTF_8);
    }

    private static void sleepSeconds(long seconds) {
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(seconds));
    }

    private static boolean reachable(String endpoint) {
        try (
            java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(3)).build()
        ) {
            final java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/minio/health/live"))
                .timeout(java.time.Duration.ofSeconds(3))
                .GET()
                .build();
            return client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding()).statusCode() < 500;
        } catch (Exception e) {
            return false;
        }
    }
}
