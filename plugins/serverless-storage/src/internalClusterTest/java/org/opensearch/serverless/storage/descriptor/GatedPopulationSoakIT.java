/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A soak in miniature: build a population on one machine and watch what it costs.
 *
 * <h2>What this is for</h2>
 *
 * Every scale claim on this branch is an extrapolation from small numbers plus an argument about shape. The
 * arguments are good and the numbers are tiny: the largest population any test builds is a few thousand
 * descriptors, and the cluster-state claim was measured over twenty-five creations. This runs the thing
 * instead, at whatever scale one machine allows, and reports what happened.
 *
 * <p>It is deliberately not precise. One JVM hosts every node, so heap is shared and per-node figures are
 * not separable; the store is a local filesystem unless pointed elsewhere, so a read costs microseconds
 * rather than the twenty to forty milliseconds a real GET does. Neither invalidates what it is looking for.
 * The costs this design lives or dies by are counts and bytes, and those do not change with the speed of the
 * medium underneath them.
 *
 * <h2>What it can show, and what it cannot</h2>
 *
 * It can show that cluster state stays flat across a population two orders of magnitude larger than anything
 * measured so far, roughly what an index costs in heap, and whether anything falls over between here and
 * there. Those are the questions unit tests structurally cannot answer, because they are about accumulation
 * and interaction rather than about one operation.
 *
 * <p>It cannot show real object-store latency, throttling, or genuine multi-node behaviour, and it cannot
 * reach a hundred million of anything. It is the difference between never having run the system and having
 * run it small, which is a larger step than the one from small to slightly larger.
 *
 * <h2>Running it</h2>
 *
 * Skipped unless asked for, because it is slow by design and nothing about it belongs in an ordinary build:
 *
 * <pre>
 * ./gradlew :plugins:serverless-storage:internalClusterTest \
 *     --tests '*GatedPopulationSoakIT*' -Dtests.soak=true -Dtests.soak.seconds=600
 * </pre>
 *
 * <p><b>Bounded by patience rather than by count</b>, which the first run taught. Asking for five hundred
 * indices ran past the suite timeout, and because the harness reported only at the end, the timeout threw
 * away every number it had gathered: a twenty minute run that measured nothing. Saying how long you are
 * willing to wait cannot fail that way. It creates for the budget, stops, and reports how far it got, and
 * how far it got is the measurement.
 *
 * <p>Progress goes to a file, named at the start of the run, and not to a logger. That is the second
 * lesson: gradle buffers a test's output until the test finishes, so anything written through the logger is
 * invisible for exactly as long as you would want to watch it. A run that has been going half an hour shows
 * nothing at all. The file is flushed on every line, so {@code tail -f} works while the run is in progress
 * and survives a kill.
 */
@com.carrotsearch.randomizedtesting.annotations.TimeoutSuite(millis = 4 * 60 * 60 * 1000)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GatedPopulationSoakIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /** How long the create phase runs before stopping and reporting, in seconds. */
    private static final int DEFAULT_BUDGET_SECONDS = 120;

    /** A ceiling, so a fast machine stops somewhere rather than running the budget out on a huge population. */
    private static final int MAX_INDICES = 1_000_000;

    /** How often progress reaches the file, so a killed run still leaves its numbers behind. */
    private static final int PROGRESS_EVERY = 25;

    /** Where progress is written. Overridable, because the default is only useful if you can find it. */
    private static final String PROGRESS_FILE = System.getProperty("tests.soak.progress", "/tmp/serverless-soak-progress.txt");

    private java.io.PrintWriter progress;

    /**
     * Opens the progress file and says where it is, on the one channel that is never buffered away: the
     * file itself. Failing to open it is not a reason to abandon the run, only to lose the live view.
     */
    private void openProgress() throws Exception {
        try {
            progress = new java.io.PrintWriter(new java.io.FileWriter(PROGRESS_FILE, java.nio.charset.StandardCharsets.UTF_8, false), true);
            note("soak starting, progress at " + PROGRESS_FILE);
        } catch (java.io.IOException e) {
            logger.warn("could not open the soak progress file at [{}]; the run continues without a live view", PROGRESS_FILE, e);
        }
    }

    /** One line, flushed, to both the file and the log. */
    private void note(String line) {
        if (progress != null) {
            progress.println(line);
        }
        logger.warn(line);
    }

    /** How many of the population to resolve in the read phase, as a fraction picked to look like real skew. */
    private static final int WORKING_SET_DIVISOR = 20;

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    public void testWhatAPopulationCosts() throws Exception {
        assumeTrue(
            "set -Dtests.soak=true to run the soak; it is slow by design",
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.soak", "false"))
        );
        long budgetNanos = TimeUnit.SECONDS.toNanos(Integer.getInteger("tests.soak.seconds", DEFAULT_BUDGET_SECONDS));
        int population = Integer.getInteger("tests.soak.indices", MAX_INDICES);
        openProgress();

        // Timed and reported separately, because the first runs spent most of their wall clock here and it
        // was impossible to tell cluster start-up from slow creation.
        long clusterNanos = System.nanoTime();
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);
        note(String.format(Locale.ROOT, "cluster up in %,d ms", (System.nanoTime() - clusterNanos) / 1_000_000));

        installBlobBackedDescriptorPlane();

        StringBuilder report = new StringBuilder(String.format(Locale.ROOT, "%ngated population soak, %,d indices%n", population));

        long versionBefore = clusterVersion();
        long bytesBefore = clusterStateBytes();
        long heapBefore = usedHeapBytes();

        long createdNanos = System.nanoTime();
        int created = createPopulation(population, budgetNanos, report);
        createdNanos = System.nanoTime() - createdNanos;

        long versionAfter = clusterVersion();
        long bytesAfter = clusterStateBytes();
        long heapAfter = usedHeapBytes();

        report.append(
            String.format(
                Locale.ROOT,
                "  created            %,10d of %,d%n"
                    + "  wall               %,10d ms   %,.0f per second%n"
                    + "  cluster versions   %,10d      %.4f per index%n"
                    + "  cluster bytes      %,10d      %.4f per index%n"
                    + "  heap delta         %,10d      %,d per index%n",
                created,
                population,
                createdNanos / 1_000_000,
                created / Math.max(1e-9, createdNanos / 1e9),
                versionAfter - versionBefore,
                (versionAfter - versionBefore) / (double) Math.max(1, created),
                bytesAfter - bytesBefore,
                (bytesAfter - bytesBefore) / (double) Math.max(1, created),
                heapAfter - heapBefore,
                (heapAfter - heapBefore) / Math.max(1, created)
            )
        );

        long resolveNanos = System.nanoTime();
        int resolved = resolveWorkingSet(created, budgetNanos, report);
        resolveNanos = System.nanoTime() - resolveNanos;
        report.append(
            String.format(
                Locale.ROOT,
                "  resolved           %,10d      %,d ms   %,.1f per second%n",
                resolved,
                resolveNanos / 1_000_000,
                resolved / Math.max(1e-9, resolveNanos / 1e9)
            )
        );

        long deleteNanos = System.nanoTime();
        int deleted = deleteSlice(created, budgetNanos);
        deleteNanos = System.nanoTime() - deleteNanos;
        report.append(
            String.format(
                Locale.ROOT,
                "  deleted            %,10d      %,d ms   %,.1f per second%n",
                deleted,
                deleteNanos / 1_000_000,
                deleted / Math.max(1e-9, deleteNanos / 1e9)
            )
        );
        report.append(
            String.format(
                Locale.ROOT,
                "  cluster bytes end  %,10d      %+,d against the start%n",
                clusterStateBytes(),
                clusterStateBytes() - bytesBefore
            )
        );

        note(report.toString());
        if (progress != null) {
            progress.close();
        }

        // Asserted rather than merely reported, because these are the claims the design rests on and they
        // hold at any population or the design does not work. Everything above is observation; these are the
        // two that would end it.
        assertEquals("a gated creation must still cost no publication at population", 0, versionAfter - versionBefore);
        assertTrue(
            "cluster state grew by "
                + (bytesAfter - bytesBefore)
                + " bytes over "
                + created
                + " gated creations, so "
                + "something is accumulating per index after all",
            bytesAfter - bytesBefore <= 0
        );
    }

    /**
     * Creates the population, reporting the first failure rather than only the count.
     *
     * <p>Stops at the first failure on purpose. This is looking for where the machine gives up, and the
     * interesting output is the number reached and the reason, not a total that quietly excludes them.
     *
     * <h4>Why concurrency is a knob rather than a constant</h4>
     *
     * With one request in flight this measures <em>latency</em> and calls it a rate. That is what the first
     * runs did, and 172.8 per second was really 5.8 ms per creation with a client that never asked for two
     * things at once. A latency figure cannot see a serialisation bottleneck at all: whether the work happens
     * on a thread of its own or queues behind every other creation in the cluster, one-at-a-time takes exactly
     * as long either way.
     *
     * <p>So the number that says whether admission was worth moving off the cluster manager's state update
     * thread is the one taken with several requests outstanding, and the number that stays comparable with
     * everything measured before it is the one taken with one. Both are available, and which was used is
     * printed with the result rather than left to be inferred.
     */
    private int createPopulation(int population, long budgetNanos, StringBuilder report) throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();

        int concurrency = Math.max(1, Integer.getInteger("tests.soak.concurrency", 1));
        note(String.format(Locale.ROOT, "creating with %,d request(s) in flight", concurrency));
        report.append(String.format(Locale.ROOT, "  concurrency        %,10d      requests in flight%n", concurrency));

        // Admission rather than a fixed pool, so the loop stays a loop and back pressure comes from the
        // cluster finishing work rather than from a queue this test would then be measuring.
        java.util.concurrent.Semaphore inFlight = new java.util.concurrent.Semaphore(concurrency);
        java.util.concurrent.atomic.AtomicInteger completed = new java.util.concurrent.atomic.AtomicInteger();
        // Counted apart from completions, because a rate computed over attempts and a population reported as
        // created are different numbers and conflating them would report indices that do not exist.
        java.util.concurrent.atomic.AtomicInteger succeeded = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<String> firstFailure = new java.util.concurrent.atomic.AtomicReference<>();

        long deadline = System.nanoTime() + budgetNanos;
        long started = System.nanoTime();
        int submitted = 0;
        for (int i = 0; i < population; i++) {
            if (System.nanoTime() > deadline || firstFailure.get() != null) {
                break;
            }
            try {
                inFlight.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            submitted++;
            final int ordinal = i;
            client().admin()
                .indices()
                .create(new CreateIndexRequest(tenant(ordinal)).settings(settings), new org.opensearch.core.action.ActionListener<>() {
                    @Override
                    public void onResponse(org.opensearch.action.admin.indices.create.CreateIndexResponse response) {
                        succeeded.incrementAndGet();
                        done();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        firstFailure.compareAndSet(null, e.getClass().getSimpleName() + ": " + e.getMessage());
                        done();
                    }

                    private void done() {
                        int now = completed.incrementAndGet();
                        inFlight.release();
                        if (now % PROGRESS_EVERY == 0) {
                            long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                            note(
                                String.format(
                                    Locale.ROOT,
                                    "created %,d in %,d ms, %.1f per second",
                                    now,
                                    elapsedMillis,
                                    now / Math.max(0.001, elapsedMillis / 1000.0)
                                )
                            );
                        }
                    }
                });
        }

        // Drained rather than abandoned, so the count reported is the count that actually landed and the
        // phases after this one do not race against creations still in flight.
        try {
            inFlight.acquire(concurrency);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (firstFailure.get() != null) {
            report.append(String.format(Locale.ROOT, "  STOPPED: %s%n", firstFailure.get()));
        } else if (submitted < population) {
            report.append(String.format(Locale.ROOT, "  budget spent at %,d indices%n", submitted));
        }
        return succeeded.get();
    }

    /**
     * Resolves a skewed slice of the population, which is what a real workload does to a cache.
     *
     * <p><b>Every failure is named, and that is the point of this method rather than a detail of it.</b> The
     * first version swallowed exceptions at {@code debug}, which the suite's WARN level discards, and
     * returned only a count. Under that arrangement a resolution failing after a thirty second
     * cluster-manager timeout is indistinguishable from one succeeding slowly: the phase simply took a long
     * time and reported a smaller number than expected. It ran for over an hour on a hundred and fifty five
     * names and nothing said why. What is swallowed here is not an incidental error, it is the measurement.
     *
     * <p>Bounded by a budget for the same reason the create phase is. A phase whose duration is set by a
     * timeout multiplied by a population is not slow, it is unbounded, and it takes every other number in the
     * run down with it when the suite timeout fires.
     */
    private int resolveWorkingSet(int created, long budgetNanos, StringBuilder report) throws Exception {
        int target = Math.max(1, created / WORKING_SET_DIVISOR);
        note(String.format(Locale.ROOT, "resolving a working set of %,d", target));
        Random random = new Random(42);
        int resolved = 0;
        int attempted = 0;
        long slowest = 0;
        String firstFailure = null;
        int failures = 0;
        long deadline = System.nanoTime() + budgetNanos;
        long started = System.nanoTime();
        for (int i = 0; i < target; i++) {
            if (System.nanoTime() > deadline) {
                note(String.format(Locale.ROOT, "resolve budget spent after %,d of %,d", attempted, target));
                break;
            }
            String name = tenant(random.nextInt(Math.max(1, created)));
            attempted++;
            long one = System.nanoTime();
            try {
                if (client().admin().indices().prepareGetSettings(name).get().getIndexToSettings().isEmpty() == false) {
                    resolved++;
                }
            } catch (Exception e) {
                failures++;
                if (firstFailure == null) {
                    firstFailure = e.getClass().getSimpleName() + ": " + e.getMessage();
                    note("first resolve failure on [" + name + "]: " + firstFailure);
                }
            }
            one = System.nanoTime() - one;
            slowest = Math.max(slowest, one);
            if (attempted % PROGRESS_EVERY == 0) {
                long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                note(
                    String.format(
                        Locale.ROOT,
                        "resolved %,d of %,d attempts in %,d ms, %.1f per second, slowest single %,d ms",
                        resolved,
                        attempted,
                        elapsedMillis,
                        attempted / Math.max(0.001, elapsedMillis / 1000.0),
                        slowest / 1_000_000
                    )
                );
            }
        }
        report.append(
            String.format(
                Locale.ROOT,
                "  resolve attempts   %,10d      %,d failed, slowest single %,d ms%n",
                attempted,
                failures,
                slowest / 1_000_000
            )
        );
        if (firstFailure != null) {
            report.append("  first failure      ").append(firstFailure).append(System.lineSeparator());
        }
        return resolved;
    }

    /**
     * Deletes a slice, so tombstones exist and the delete path is exercised at population.
     *
     * <p>Budgeted and failure-naming for the same reasons as {@link #resolveWorkingSet}: a phase that can
     * only end by running out of names cannot report anything if it is stopped, and a swallowed failure here
     * would make a delete that never happened look like a delete that was merely slow.
     */
    private int deleteSlice(int created, long budgetNanos) throws Exception {
        int target = Math.max(1, created / 10);
        note(String.format(Locale.ROOT, "deleting %,d", target));
        int deleted = 0;
        String firstFailure = null;
        long deadline = System.nanoTime() + budgetNanos;
        long started = System.nanoTime();
        for (int i = 0; i < target; i++) {
            if (System.nanoTime() > deadline) {
                note(String.format(Locale.ROOT, "delete budget spent after %,d of %,d", i, target));
                break;
            }
            try {
                client().admin().indices().delete(new DeleteIndexRequest(tenant(i))).actionGet();
                deleted++;
            } catch (Exception e) {
                if (firstFailure == null) {
                    firstFailure = e.getClass().getSimpleName() + ": " + e.getMessage();
                    note("first delete failure on [" + tenant(i) + "]: " + firstFailure);
                }
            }
            if ((i + 1) % PROGRESS_EVERY == 0) {
                long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                note(
                    String.format(
                        Locale.ROOT,
                        "deleted %,d of %,d in %,d ms, %.1f per second",
                        i + 1,
                        target,
                        elapsedMillis,
                        (i + 1) / Math.max(0.001, elapsedMillis / 1000.0)
                    )
                );
            }
        }
        return deleted;
    }

    private static String tenant(int i) throws Exception {
        return String.format(Locale.ROOT, "serverless_tenant-%08d", i);
    }

    private long clusterVersion() throws Exception {
        return client().admin().cluster().prepareState().get().getState().version();
    }

    private long clusterStateBytes() throws Exception {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            state.writeTo(out);
            return out.bytes().length();
        }
    }

    /**
     * Used heap after a collection, which is crude and is the right kind of crude here.
     *
     * <p>Every node shares this JVM, so this is the whole cluster's heap rather than one node's, and a
     * request for a collection is a request rather than a guarantee. What it can still show is the order of
     * magnitude of what an index costs to keep, which is the number that decides whether a working set fits.
     */
    private static long usedHeapBytes() throws Exception {
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
