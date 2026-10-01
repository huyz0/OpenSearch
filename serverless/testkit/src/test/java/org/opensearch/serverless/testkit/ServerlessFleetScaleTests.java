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
        for (int i = 0; i < nodes; i++) {
            proxies.add(new FaultProxy("n" + i, store.getHost(), store.getPort()));
            final Path home = results.resolve("nodes").resolve("n" + i);
            Files.createDirectories(home);
            NodeProcess.writeKeystore(home, Map.of("s3.client.default.access_key", access, "s3.client.default.secret_key", secret));
            homes.add(home);
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
                line("\n## " + scenario + "\n");
                switch (scenario.trim()) {
                    case "steady" -> steady(seconds);
                    case "kill" -> kill(seconds);
                    case "pause" -> pause(seconds);
                    case "partition" -> partition(seconds);
                    case "slowdown" -> slowdown(seconds);
                    case "restart" -> rollingRestart(seconds);
                    case "storm" -> storm(seconds, steady);
                    default -> throw new IllegalArgumentException("unknown scenario " + scenario);
                }
                final long ended = System.nanoTime();
                line("- writes: " + load.window("write", started, ended));
                line("- searches: " + load.window("search", started, ended));
                line("- gets: " + load.window("get", started, ended));
                line("- churn: " + load.window("create", started, ended) + "; " + load.window("delete", started, ended));
                line("- not acknowledged, by reason: " + load.refusalReasons(started, ended));
                line("- writes shed by the client, " + FleetLoad.MAX_IN_FLIGHT + " already in flight: " + (load.shed() - shedMark));
                line("- store requests per node: " + storeCounts());
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
                    fail("scenario " + scenario + " failed its ledger check: " + verdict + "; see " + results);
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
        final long started = System.nanoTime();
        sleepSeconds(seconds);
        final double elapsed = (System.nanoTime() - started) / 1e9;
        final Map<String, Long> total = totalStoreCounts();
        line("- store requests per second under load, fleet-wide, by type: " + perSecond(total, elapsed));

        load.stop();
        // Past the client's timeout before counting: a write the load sent last is still being appended, forwarded
        // and published for up to that long, and a window opened at once counted that tail as background.
        sleepSeconds(IDLE_SETTLE_SECONDS);
        final int idle = Math.max(180, seconds / 2);
        final Map<String, Integer> heldBefore = heldShardsPerNode();
        resetStoreCounts();
        final long idleStarted = System.nanoTime();
        sleepSeconds(idle);
        final double idleElapsed = (System.nanoTime() - idleStarted) / 1e9;
        final Map<String, Integer> heldAfter = heldShardsPerNode();
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
        final String body = rangeFrom(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(30));
        String answer;
        try {
            answer = load.requestBody(load.inRotation().get(0), "POST", "/logs-*/_search", body);
        } catch (Exception e) {
            // A refusal at the cap is the usual answer, and it names the count.
            answer = String.valueOf(e.getMessage());
        }
        final Matcher m = CANDIDATES.matcher(answer);
        return "logs-* for next month: " + (m.find() ? m.group(1) + " indices could not be ruled out" : searchSummary(answer));
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
        final int victim = busiestNode();
        final NodeProcess node = fleet.get(victim);
        final List<String> owned = shardsOwnedBy(node.nodeId());
        line("- killing " + node.name() + ", which owned " + owned.size() + " shards; lease TTL " + ttlMillis + " ms");
        final long leaseExpiresAt = plane.membership().read(node.nodeId()).map(l -> l.expiresAtMillis()).orElse(-1L);
        final long killedAtMillis = System.currentTimeMillis();
        final long killedAt = System.nanoTime();
        fleet.remove(node);
        node.killHard();
        acquiredAfter.clear();
        final Map<String, Long> takeover = probeUntilWritable(
            owned,
            killedAt,
            Math.max(seconds, (int) (6 * ttlMillis / 1000)),
            node.nodeId()
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
        line("- writes from the kill until every shard was back: " + load.window("write", killedAt, killedAt + maxOr(takeover, 0L)));
        sleepSeconds(Math.max(0, seconds - (int) TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - killedAt)));
        fleet.add(victim, startNode(victim));
    }

    /** How long a load balancer takes to stop routing to a node that no longer answers. */
    private static final int HEALTH_CHECK_SECONDS = 5;

    /** A process frozen past its lease, then thawed: it must not acknowledge anything it no longer owns. */
    private void pause(int seconds) throws Exception {
        final int victim = busiestNode();
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
        final int victim = busiestNode();
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
        String> violations) {
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
                + " changed owner during the check";
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
            List.copyOf(violations)
        );
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
        for (Map.Entry<String, List<FleetLedger.Write>> index : mustExist.entrySet()) {
            final List<FleetLedger.Write> absent = missingOf(index.getKey(), index.getValue(), true);
            if (absent.isEmpty() == false) {
                missing.put(index.getKey(), absent);
            }
        }
        final List<FleetLedger.Write> lost = java.util.Collections.synchronizedList(new ArrayList<>());
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
                        lost.addAll(still);
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
        final List<FleetLedger.Write> refusedButVisible = new ArrayList<>();
        for (Map.Entry<String, List<FleetLedger.Write>> index : mustNotExist.entrySet()) {
            refusedButVisible.addAll(missingOf(index.getKey(), index.getValue(), false));
        }
        load.start();
        return new FleetLedger.Verdict(acked, acked - excused, refused, unknown, lost, refusedButVisible, excused);
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
        Files.writeString(results.resolve("forensics.txt"), out.toString(), StandardCharsets.UTF_8);
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

    private static final Pattern QUEUE_FIELD = Pattern.compile(
        "\"(queued|running|done|wait_millis|run_millis|max_wait_millis|max_run_millis)\"\\s*:\\s*(\\d+)"
    );

    /**
     * Every node's activation queue and held shards, every fifteen seconds, to activation.csv: how long a take waited
     * for its turn, which a node's own counters are the only record of.
     */
    private Thread startActivationSampler() throws java.io.IOException {
        final Path csv = results.resolve("activation.csv");
        Files.writeString(csv, "epoch_ms,node,held,queued,running,done,wait_millis,run_millis,max_wait_millis,max_run_millis\n");
        final Thread thread = new Thread(() -> {
            try (
                java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                    .connectTimeout(java.time.Duration.ofSeconds(5))
                    .build()
            ) {
                while (Thread.currentThread().isInterrupted() == false) {
                    final StringBuilder rows = new StringBuilder();
                    for (NodeProcess node : List.copyOf(fleet)) {
                        try {
                            final String body = client.send(
                                java.net.http.HttpRequest.newBuilder()
                                    .uri(URI.create("http://" + node.http() + "/_serverless/stats"))
                                    .timeout(java.time.Duration.ofSeconds(10))
                                    .GET()
                                    .build(),
                                java.net.http.HttpResponse.BodyHandlers.ofString()
                            ).body();
                            final Map<String, String> fields = new HashMap<>();
                            final Matcher m = QUEUE_FIELD.matcher(body);
                            while (m.find()) {
                                fields.putIfAbsent(m.group(1), m.group(2));
                            }
                            int held = 0;
                            final Matcher h = HELD_SHARD.matcher(body);
                            while (h.find()) {
                                held++;
                            }
                            rows.append(System.currentTimeMillis()).append(',').append(node.name()).append(',').append(held);
                            for (String f : new String[] {
                                "queued",
                                "running",
                                "done",
                                "wait_millis",
                                "run_millis",
                                "max_wait_millis",
                                "max_run_millis" }) {
                                rows.append(',').append(fields.getOrDefault(f, ""));
                            }
                            rows.append('\n');
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        } catch (Exception e) {
                            rows.append(System.currentTimeMillis()).append(',').append(node.name()).append(",,,,,,,,\n");
                        }
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
