/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * How many gated indices can one node hold open, and what closes them.
 *
 * <h2>Why this is the measurement that decides the design</h2>
 *
 * A hundred million indices at three to a hundred shards is three hundred million to ten billion shards.
 * No fleet holds that many open, so the whole architecture rests on one property: <b>resident shards track
 * the working set, not the population.</b> Every other number measured on this branch is subordinate to it.
 * Creation throughput, descriptor size, placement cost and cluster-state footprint all describe indices
 * that no node has opened; they say what it costs to have a hundred million indices exist, and say nothing
 * about what it costs to have used them.
 *
 * <p>Nothing has measured the used case. This does.
 *
 * <h2>What the code says before the measurement runs</h2>
 *
 * {@code IndicesClusterStateService.removeIndices} skips every gated index this node opened on demand:
 *
 * <pre>
 *   if (heldOnDemand(index)) { continue; }
 * </pre>
 *
 * and {@code heldOnDemand} is true for anything in {@code openedOnDemand} while a descriptor supplier is
 * registered. That set shrinks in exactly two places -- the sweep that finds a descriptor gone, meaning the
 * index was deleted, and node shutdown. Scale-to-zero does not shrink it: suspending a shard removes it from
 * the computed placement so new traffic stops arriving, and leaves the {@code IndexService} open.
 *
 * <p>So on the reading of the code there is no idle eviction, and residency tracks the number of distinct
 * tenants a node has ever served rather than the number it is serving. That is a hypothesis about behaviour,
 * and this class exists because a hypothesis about behaviour is exactly the kind of claim this branch has
 * repeatedly found to be wrong in one direction or the other. The idle phase below checks it rather than
 * asserting the reading was right.
 *
 * <h2>What it reports, and what those numbers are worth</h2>
 *
 * Cost per open index in heap, and the count reached before something failed. Both are honest only within
 * limits worth stating plainly:
 *
 * <ul>
 *   <li>Every node shares this JVM, so heap is the cluster's rather than one node's. With one data node and
 *       a cluster manager that holds no gated shard, the delta is dominated by the data node, and the
 *       number to trust is the <em>slope</em> -- bytes per additional open index -- not the intercept.</li>
 *   <li>The store is a local filesystem, so opening a shard costs microseconds where a real object store
 *       costs tens of milliseconds. That makes the count reached optimistic in time and unaffected in
 *       bytes, and bytes are what a residency ceiling is made of.</li>
 *   <li>A test JVM's heap is small. The count reached here is therefore not a production ceiling; it is a
 *       per-index cost that a production heap can be divided by, which is the number that generalises.</li>
 * </ul>
 *
 * <h2>Running it</h2>
 *
 * <pre>
 * ./gradlew :plugins:serverless-storage:internalClusterTest \
 *     --tests '*GatedResidencySoakIT*' -Dtests.residency=true -Dtests.residency.seconds=600
 * </pre>
 *
 * <p>Progress goes to a file for the reason {@code GatedPopulationSoakIT} learned it: gradle buffers a
 * test's output until the test ends, so a run that has been going twenty minutes shows nothing at all
 * through the logger, and a run killed by a timeout takes every number it gathered down with it.
 */
@com.carrotsearch.randomizedtesting.annotations.TimeoutSuite(millis = 4 * 60 * 60 * 1000)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
// HandleLimitFS caps a test JVM at 2,048 open file handles and throws java.nio.file.FileSystemException
// with the message "Too many open files" -- which reads exactly like the operating system refusing, and is
// not. The first run of this class stopped at 658 open indices for that reason and would have been reported
// as a residency ceiling. It is the harness. ExtrasFS is inherited from OpenSearchIntegTestCase and has to
// be repeated because naming any filesystem here replaces the superclass's list rather than adding to it.
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems({ "ExtrasFS", "HandleLimitFS" })
public class GatedResidencySoakIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /** How long the open phase runs before stopping and reporting, in seconds. */
    private static final int DEFAULT_BUDGET_SECONDS = 300;

    /** A ceiling, so a machine with room to spare still stops somewhere. */
    private static final int MAX_INDICES = 200_000;

    /** How often a sample is taken and written out. */
    private static final int SAMPLE_EVERY = 100;

    /**
     * The fraction of maximum heap at which the run stops on its own.
     *
     * <p>Stopping short of the wall rather than at it. An {@code OutOfMemoryError} inside a test JVM
     * hosting a cluster does not produce a clean measurement: it fires on whichever allocation happened to
     * be unlucky, which is usually not the one that matters, and it takes the reporting down with it. The
     * slope is what generalises anyway, and the slope is fully determined well before the wall.
     */
    private static final double HEAP_STOP_FRACTION = 0.85;

    private static final String PROGRESS_FILE = System.getProperty("tests.residency.progress", "/tmp/serverless-residency-progress.txt");

    private java.io.PrintWriter progress;

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
            // Without this a gated index has no routing table, so openComputedShardsOnDemand declines and
            // no shard is ever opened -- which would make this class measure the cost of not opening
            // anything and report a very large ceiling.
            .put(org.opensearch.serverless.storage.ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    public void testHowManyGatedIndicesANodeHoldsOpen() throws Exception {
        assumeTrue(
            "set -Dtests.residency=true to run the residency soak; it is slow by design",
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.residency", "false"))
        );
        long budgetNanos = TimeUnit.SECONDS.toNanos(Integer.getInteger("tests.residency.seconds", DEFAULT_BUDGET_SECONDS));
        int population = Integer.getInteger("tests.residency.indices", MAX_INDICES);
        openProgress();

        internalCluster().startClusterManagerOnlyNode();
        // One data node, so "open on this node" and "open anywhere" are the same number and the heap shared
        // across this JVM has one gated holder in it rather than several to apportion between.
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        IndicesService indices = internalCluster().getInstance(IndicesService.class, dataNode);

        Settings gated = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();

        long heapBefore = usedHeapBytes();
        long descriptorsBefore = openFileDescriptors();
        int openBefore = openIndexCount(indices);
        note(
            String.format(
                Locale.ROOT,
                "baseline: %,d open indices, %,d MiB heap used of %,d MiB max, %,d file descriptors of %s allowed",
                openBefore,
                heapBefore / (1024 * 1024),
                Runtime.getRuntime().maxMemory() / (1024 * 1024),
                descriptorsBefore,
                fileDescriptorLimit()
            )
        );
        // Tracked at every sample rather than read once at the end. The first run read it after the loop
        // had already broken, by which point the shards that failed had been closed and given their handles
        // back, so the number reported was the count after the collapse rather than the count that caused
        // it.
        long descriptorsPeak = descriptorsBefore;

        long deadline = System.nanoTime() + budgetNanos;
        long started = System.nanoTime();
        String stoppedBecause = "the population was exhausted";
        int opened = 0;
        long slowestOpenNanos = 0;

        for (int i = 0; i < population; i++) {
            if (System.nanoTime() > deadline) {
                stoppedBecause = "the budget was spent";
                break;
            }
            String name = tenant(i);
            long one = System.nanoTime();
            try {
                client().admin().indices().create(new CreateIndexRequest(name).settings(gated)).actionGet();
                // The write is what opens the shard. Creating a gated index touches no node: the descriptor
                // is written and nothing is told to build anything, which is the whole point of gating. So a
                // population built by creation alone would measure zero residency however large it grew.
                client().prepareIndex(name).setId("1").setSource("tenant", name).get();
            } catch (Exception e) {
                stoppedBecause = "opening [" + name + "] failed: " + e.getClass().getSimpleName() + ": " + e.getMessage();
                note("STOPPED at " + opened + " open: " + stoppedBecause);
                break;
            }
            slowestOpenNanos = Math.max(slowestOpenNanos, System.nanoTime() - one);
            opened++;

            if (opened % SAMPLE_EVERY == 0) {
                long heapNow = usedHeapBytes();
                int openNow = openIndexCount(indices);
                long descriptorsNow = openFileDescriptors();
                descriptorsPeak = Math.max(descriptorsPeak, descriptorsNow);
                long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
                note(
                    String.format(
                        Locale.ROOT,
                        "opened %,d in %,d ms (%.1f/s), %,d open on the node, heap %,d MiB, %,d B per open "
                            + "index, %,d file descriptors, %.1f per index",
                        opened,
                        elapsedMillis,
                        opened / Math.max(0.001, elapsedMillis / 1000.0),
                        openNow,
                        heapNow / (1024 * 1024),
                        (heapNow - heapBefore) / Math.max(1, opened),
                        descriptorsNow,
                        (descriptorsNow - descriptorsBefore) / (double) Math.max(1, opened)
                    )
                );
                if (heapNow > Runtime.getRuntime().maxMemory() * HEAP_STOP_FRACTION) {
                    stoppedBecause = String.format(
                        Locale.ROOT,
                        "heap reached %,d MiB, past %.0f%% of the %,d MiB maximum",
                        heapNow / (1024 * 1024),
                        HEAP_STOP_FRACTION * 100,
                        Runtime.getRuntime().maxMemory() / (1024 * 1024)
                    );
                    note("STOPPED at " + opened + " open: " + stoppedBecause);
                    break;
                }
            }
        }

        long openNanos = System.nanoTime() - started;
        long heapAfter = usedHeapBytes();
        long descriptorsAfter = openFileDescriptors();
        descriptorsPeak = Math.max(descriptorsPeak, descriptorsAfter);
        int openAfter = gatedOpenIndexCount(indices);
        long bytesPerIndex = (heapAfter - heapBefore) / Math.max(1, opened);

        // The idle phase. Nothing is written, nothing is deleted, and the question is only whether anything
        // gives a shard back. The wait is deliberately longer than the gated sweep interval, so a sweep that
        // was going to close something has had its chance.
        long idleSeconds = Integer.getInteger("tests.residency.idle.seconds", 90);
        note(String.format(Locale.ROOT, "idling %,d seconds to see whether anything closes", idleSeconds));
        Thread.sleep(TimeUnit.SECONDS.toMillis(idleSeconds));
        int openAfterIdle = gatedOpenIndexCount(indices);
        long heapAfterIdle = usedHeapBytes();

        StringBuilder report = new StringBuilder(String.format(Locale.ROOT, "%ngated residency soak%n"));
        report.append(
            String.format(
                Locale.ROOT,
                "  opened             %,10d      in %,d ms, %,.1f per second%n"
                    + "  slowest single     %,10d ms%n"
                    + "  stopped because    %s%n"
                    + "  gated open on node %,10d      was %,d at the baseline%n"
                    + "  heap delta         %,10d B    %,d B per open index%n"
                    + "  heap max           %,10d MiB%n"
                    + "  file descriptors   %,10d peak %+,d, %,.1f per open index, limit %s%n"
                    + "  after %,d s idle%n"
                    + "  open on the node   %,10d      %+,d against before the idle%n"
                    + "  heap               %,10d MiB  %+,d MiB%n",
                opened,
                openNanos / 1_000_000,
                opened / Math.max(1e-9, openNanos / 1e9),
                slowestOpenNanos / 1_000_000,
                stoppedBecause,
                openAfter,
                openBefore,
                heapAfter - heapBefore,
                bytesPerIndex,
                Runtime.getRuntime().maxMemory() / (1024 * 1024),
                descriptorsPeak,
                descriptorsPeak - descriptorsBefore,
                (descriptorsPeak - descriptorsBefore) / (double) Math.max(1, opened),
                fileDescriptorLimit(),
                idleSeconds,
                openAfterIdle,
                openAfterIdle - openAfter,
                heapAfterIdle / (1024 * 1024),
                (heapAfterIdle - heapAfter) / (1024 * 1024)
            )
        );

        // What the slope means at sizes a production node actually has. Reported rather than asserted,
        // because it is arithmetic on one measurement and presenting it as a guarantee would overstate it by
        // exactly the amount this class exists to stop overstating.
        if (bytesPerIndex > 0) {
            report.append(String.format(Locale.ROOT, "  projected ceiling, open gated indices per node%n"));
            for (int heapGib : new int[] { 8, 16, 31, 64 }) {
                long fit = (long) (heapGib * 1024L * 1024L * 1024L * 0.5 / bytesPerIndex);
                report.append(
                    String.format(Locale.ROOT, "    %,3d GiB heap    %,12d      at half the heap given to index residency%n", heapGib, fit)
                );
            }
        }

        note(report.toString());
        if (progress != null) {
            progress.close();
        }

        // The measurement's own validity. Every gated index written to must have been opened here, or the
        // bytes above are the cost of something other than residency and the whole report means nothing.
        assertEquals("every gated index this run wrote to must be open on the node, or the slope is not residency", opened, openAfter);

        // The property the design rests on, stated as the thing it would take to break it. This is expected
        // to hold today -- there is no idle eviction -- and a failure here is the good kind: it means
        // something now gives shards back, and the report above is what says how well.
        assertEquals(
            "an idle node released "
                + (openAfter - openAfterIdle)
                + " of "
                + openAfter
                + " open gated indices; if that "
                + "is a new eviction path, this expectation is what needs updating, and the numbers above are what it is worth",
            openAfter,
            openAfterIdle
        );
    }

    /** How many indices this node currently holds open, gated or not. */
    private static int openIndexCount(IndicesService indices) throws Exception {
        int count = 0;
        for (IndexService ignored : indices) {
            count++;
        }
        return count;
    }

    /**
     * How many of those are gated, which is the only count the slope can be attributed to.
     *
     * <p>Counting everything open is off by the indices this run causes but does not gate. The first write
     * to a gated index registers a dynamic mapping, which {@code IndexBackedMappingStore} records in
     * {@code .opensearch-index-mappings} -- an ordinary index, opened on this node like any other. One
     * extra, every run, and it is a real cost worth seeing rather than an artifact worth hiding: it is the
     * second system index this design has still not escaped.
     */
    private static int gatedOpenIndexCount(IndicesService indices) throws Exception {
        int count = 0;
        for (IndexService indexService : indices) {
            if (indexService.getIndexSettings().getSettings().getAsBoolean("index.serverless_storage.enabled", false)) {
                count++;
            }
        }
        return count;
    }

    /**
     * The limit on open file descriptors, as the JVM's own process sees it.
     *
     * <p>Read from the operating system bean rather than {@code /proc/self/limits}, which is what the first
     * version did and which reported "unknown" every run: the file is readable, and reading it is a file
     * permission the test security policy does not grant, so the attempt failed silently inside the catch.
     * The bean asks the same question through an interface that is already permitted.
     */
    private static String fileDescriptorLimit() throws Exception {
        java.lang.management.OperatingSystemMXBean bean = java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        try {
            java.lang.reflect.Method method = bean.getClass().getMethod("getMaxFileDescriptorCount");
            Object result = method.invoke(bean);
            if (result instanceof Number max) {
                return String.format(Locale.ROOT, "%,d", max.longValue());
            }
        } catch (Exception ignored) {}
        return "unknown";
    }

    /**
     * Open file descriptors for this JVM, or -1 where that cannot be counted.
     *
     * <p>Worth counting because heap is not the only thing a resident shard holds. A Lucene directory keeps
     * files open, and a per-process descriptor limit would be reached at a count that has nothing to do with
     * how much memory is left -- the kind of ceiling that is invisible in a heap measurement and obvious in
     * production.
     */
    private static long openFileDescriptors() throws Exception {
        try (var entries = java.nio.file.Files.list(java.nio.file.Path.of("/proc/self/fd"))) {
            return entries.count();
        } catch (Exception e) {
            return -1;
        }
    }

    private static String tenant(int i) throws Exception {
        return String.format(Locale.ROOT, "tenant-%08d", i);
    }

    /** Used heap after a requested collection. Crude in the same way, and for the same reason, as the population soak's. */
    private static long usedHeapBytes() throws Exception {
        System.gc();
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private void openProgress() throws Exception {
        try {
            progress = new java.io.PrintWriter(new java.io.FileWriter(PROGRESS_FILE, java.nio.charset.StandardCharsets.UTF_8, false), true);
            note("residency soak starting, progress at " + PROGRESS_FILE);
        } catch (java.io.IOException e) {
            logger.warn("could not open the residency progress file at [{}]; the run continues without a live view", PROGRESS_FILE, e);
        }
    }

    private void note(String line) {
        if (progress != null) {
            progress.println(line);
        }
        logger.warn(line);
    }
}
