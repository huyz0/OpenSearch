/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What a declared mapping costs a gated creation, and which part of it is the store.
 *
 * <p><b>Opt-in, like every other measurement here.</b> Run it with:
 *
 * <pre>
 *   ./gradlew :plugins:serverless-storage:internalClusterTest \
 *     --tests '*GatedMappedCreationCostIT*' -Dtests.mappingcost=true
 * </pre>
 *
 * <p>It shipped without the guard and broke the suite around it three times over, differently each run: a
 * cluster manager election timing out mid-creation, a residency test finding a stray index in cluster
 * state, a field refresher failing on shared state. Every one of them passed alone, and the suite was green
 * with this class removed. Deleting what it created and shrinking the population each fixed the run in
 * front of them and moved the failure somewhere else, which is the signal that the problem was the class
 * being there at all rather than its size. {@code GatedPopulationSoakIT} and {@code GatedResidencySoakIT}
 * were already behind their own flags for the same reason -- the convention existed and this did not
 * follow it.
 *
 * <h2>Why this is asked now</h2>
 *
 * The headline figure for this work is 10,505 gated creations per second, from which "100M indices in about
 * 2.6 hours" follows. That was measured on indices with no mapping, and until recently that was the only
 * kind of gated index there was: T11 carried create-time mappings, T13 found the carrying dropped every
 * field parameter, and T15 widened the store so object fields and parameters round-trip. An index declaring
 * a mapping is now the common gated index rather than a refused one.
 *
 * <p>Which makes an unmeasured claim load-bearing. The creation bypass that produced 10,505 -- skipping the
 * throwaway {@code IndexService} built inside {@code IndicesService.createIndexService}, which is
 * {@code synchronized} and was the single lock behind 122,222 blocking events -- declines on any non-empty
 * mapping, by the condition "it has a mapping to merge and validate". So the fast path and the mappings the
 * feature was widened to support are mutually exclusive, and every figure quoted for filling 100M indices
 * may describe a population nobody would create.
 *
 * <p>Every arm runs against one cluster in one run, with one variable between neighbouring arms. A ratio
 * from arms measured together is worth more than any of the absolute figures: throughput on a shared build
 * machine says as much about the machine as the code, and this box has been running at load average 45.
 *
 * <h2>T40: where the surviving cost actually is</h2>
 *
 * T20 removed the index-service lock and the ratio moved from about 14x to about 10x, leaving most of the
 * cost somewhere else -- and the somewhere else was named rather than measured: the mapping store's
 * blocking get-and-index per creation, against a shared five-shard index the unmapped arm never touches.
 * A named suspect is not a finding, and this is the third time in this area that the obvious candidate was
 * only part of the answer. (The "about a quarter" once attributed to the lock came from subtracting two
 * single-run ratios, and the repeat arm below shows that subtraction is worth less than it looks.)
 *
 * <p>So a third arm runs the same mapped creations with an in-memory {@code MappingGenerationStore.Store}
 * registered in place of the index-backed one. Everything upstream of the store is identical -- the same
 * declared mapping, the same parsing, the same descriptor write, the same read-then-swap protocol in
 * {@code MappingGenerationStore.updateMapping} -- so the differences are additive and each one names a
 * cause:
 *
 * <ul>
 *   <li>index-backed minus in-memory is what the index-backed store costs</li>
 *   <li>in-memory minus unmapped is everything else a declared mapping costs</li>
 * </ul>
 *
 * <p><b>"What the store costs" is the honest name for the first term, not "what the network costs".</b>
 * The in-memory double skips the two round trips, and it also skips building the indexing request, deriving
 * the field type counts, and checking that the mapping index exists. Those are small next to a round trip
 * and they are not zero, and nothing here separates them. The term is what removing the index-backed
 * implementation is worth, which is what a decision about it needs.
 *
 * <p>Both mapped arms assert that they swapped once per creation, because an arm that quietly stopped
 * reaching the store would price it at zero from one end or the other.
 *
 * <h2>What it asserts, and what it only reports</h2>
 *
 * Nothing asserts a duration or a ratio, for the reason {@code GatedCreationWithoutTemporaryIndexServiceIT}
 * gives: a timing assertion at these scales is a flake generator, and T20 has just finished removing one
 * that failed in two consecutive cycles. What is asserted is that the run measured what it claims to have
 * measured -- every arm created every index, and each mapped arm reached the store once per creation.
 *
 * <p>The throughput figures are logged. They are the finding, and the finding is a number rather than a
 * pass or a fail.
 */
public class GatedMappedCreationCostIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /**
     * Large enough that the arms are comparing steady-state creation rather than warm-up.
     *
     * <p>Chosen by getting it wrong: at 120 per arm the ratio read 13.6x where the same code read 8.5x at
     * 300. That was attributed to population size at the time. T40's repeat arm says it was warm-up -- the
     * same arm, same code, same run, measured three times apart depending only on where in the round it ran
     * -- so 300 is defensible as "long enough to amortise the fixed costs" and the 13.6-against-8.5
     * comparison should not be read as a finding about population.
     */
    private static final int INDICES_PER_ARM = 300;

    private static final int CONCURRENCY = 8;

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

    /**
     * Deletes everything this test made, before the gate comes down.
     *
     * <p>Not hygiene. Without it this class leaves its whole population -- 2,401 indices as it now stands --
     * behind in a shared test cluster and the rest of the suite fails around it: a cluster manager election
     * timing out mid-creation, a residency test finding a stray index in cluster state, a mapping write
     * hitting an index the framework had wiped.
     * All three passed alone and only failed after this class ran, which is what a leak looks like from the
     * outside: three unrelated failures, none of them where the problem is.
     *
     * <p>Ordered before the gate is uninstalled, because deleting a gated index needs the descriptor plane
     * that resolves its name.
     *
     * <p><b>By exact name, in chunks, and that is not a preference.</b> This used to delete by wildcard and
     * the wildcard never once matched: gated expansion is capped at 100 by
     * {@code serverless_storage.wildcard.max_expanded_indices}, every arm here is three times that, and the
     * refusal arrived as an exception this method caught and logged as "could not clean up". So the class
     * has been leaking its whole population every time it ran, while reading as though it cleaned up, and
     * the leak is the thing its own javadoc says breaks three unrelated tests. Found by reading the log of
     * a run that passed.
     */
    @After
    public void deleteWhatWasCreated() throws Exception {
        if (Boolean.getBoolean("tests.mappingcost") == false) {
            // The body was skipped, so nothing was created and sweeping would be about a hundred pointless
            // round trips. The uninstall still runs: it costs nothing, and this class used to run it
            // unconditionally, so anything that had come to depend on that would break silently here rather
            // than anywhere near the cause. Registry state leaking between classes is how this package's
            // failures usually present.
            DescriptorGate.uninstall();
            return;
        }
        List<String> survivors = new java.util.ArrayList<>();
        survivors.addAll(deleteByExactName("warmup", 1));
        for (String prefix : ARM_PREFIXES) {
            survivors.addAll(deleteByExactName(prefix, INDICES_PER_ARM));
        }
        DescriptorGate.uninstall();
        // Fails the test rather than warning. The leak this method exists to prevent was found by reading
        // the log of a run that passed, and a warning here would reproduce exactly that: the discovery
        // depends on someone reading a log they have no reason to open.
        assertEquals("indices survived the cleanup sweep, so this class is leaking into the shared cluster", List.of(), survivors);
    }

    /** Every prefix an arm creates under, so cleanup does not have to be kept in step with the rounds. */
    private static final List<String> ARM_PREFIXES = List.of(
        "mapped1",
        "memory1",
        "plain1",
        "repeat1",
        "mapped2",
        "memory2",
        "plain2",
        "repeat2"
    );

    /** How many names one delete request may carry. Explicit names are not subject to any expansion cap. */
    private static final int DELETE_CHUNK = 50;

    /**
     * Deletes {@code prefix-0} to {@code prefix-(count-1)}, in chunks, tolerating names that were never
     * created because an arm did not finish, and returns the names that survived.
     *
     * <p><b>Checking that it worked is the whole reason this method exists.</b> The wildcard version it
     * replaced reported success while deleting nothing, and a delete that resolves no name is also
     * acknowledged, so "no exception" says nothing either way. Re-resolving one name per chunk catches a
     * chunk that silently did nothing, which is the failure that actually happened.
     */
    private List<String> deleteByExactName(String prefix, int count) {
        List<String> survivors = new java.util.ArrayList<>();
        for (int from = 0; from < count; from += DELETE_CHUNK) {
            String[] names = new String[Math.min(DELETE_CHUNK, count - from)];
            for (int i = 0; i < names.length; i++) {
                names[i] = prefix + "-" + (from + i);
            }
            try {
                client().admin().indices().prepareDelete(names).setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
            } catch (Exception e) {
                logger.warn("could not delete [{}-{}] onwards", prefix, from, e);
            }
            String canary = names[names.length - 1];
            try {
                if (client().admin().indices().prepareExists(canary).get().isExists()) {
                    survivors.add(canary);
                }
            } catch (Exception e) {
                logger.warn("could not check whether [{}] survived deletion", canary, e);
            }
        }
        return survivors;
    }

    /** The one mapping every mapped arm declares, so the arms differ in the store and nothing else. */
    private static final Map<String, Object> DECLARED = Map.of("properties", Map.of("tenant", Map.of("type", "keyword")));

    /**
     * One round's four arms, as creations per second.
     *
     * <p>{@code indexBackedRepeat} is the same arm as {@code indexBacked} run at the end of the round, and
     * it exists because the two disagreeing is the only way to see that the round had not settled. Where the
     * two disagree, the attribution is quoted as the range they bracket rather than as a figure.
     */
    private record Round(double indexBacked, double inMemory, double unmapped, double indexBackedRepeat) {

        /** Per-creation service time. At concurrency C, an aggregate rate of R means C/R seconds each. */
        static double microsPer(double rate) {
            return CONCURRENCY * 1e6 / rate;
        }

        /** The store's share of what a declared mapping costs, taking the index-backed arm from one end. */
        double storeShare(double indexBackedRate) {
            double total = microsPer(indexBackedRate) - microsPer(unmapped);
            return total <= 0 ? Double.NaN : 100 * (microsPer(indexBackedRate) - microsPer(inMemory)) / total;
        }

        /** How far the round drifted, measured on the one arm that ran at both ends of it. */
        double drift() {
            return indexBackedRepeat / indexBacked;
        }
    }

    /**
     * The same three arms twice, attributed from the second round.
     *
     * <p>It ran once at first, expensive arm to cheap, on the reasoning that any drift would then work
     * against the finding. The drift turned out to be far too large for that to be a safe assumption:
     * repeating the index-backed arm at the end of a single round measured 448 creations per second in first
     * position and 1,335 in last, three times apart with the mapping and the store held constant. Whatever
     * that is -- JIT, the mapping index settling after its own creation, the descriptor plane warming -- it
     * is bigger than the effect being measured, and a subtraction between arms measured in different
     * positions was reporting mostly it.
     *
     * <p>So round one is warm-up and round two is the measurement, and every round repeats its index-backed
     * arm at the end. The repeat is the control: it is the same arm, the same store and the same mapping,
     * differing only in where in the round it ran, so the two figures bracket the drift instead of leaving
     * it as an argument about which direction the bias runs. The store's share is reported from both ends,
     * and the honest answer is the range, not the friendlier end of it.
     *
     * <p>One mapped creation runs before either round. The mapping index is created lazily by the first
     * write to it, and that one blocking create would otherwise be charged to the store as per-creation
     * cost. It is per-cluster cost. For the same reason the index-backed store is built once and re-registered
     * rather than rebuilt per round: {@code IndexBackedMappingStore} tracks "the mapping index exists" per
     * instance, so a fresh one puts a failing create at the head of the very arm being measured.
     */
    public void testWhatADeclaredMappingCostsAGatedCreation() throws Exception {
        assumeTrue(
            "set -Dtests.mappingcost=true to run this; it creates enough indices to disturb the tests "
                + "around it, which is why every measurement in this package is opt-in",
            Boolean.getBoolean("tests.mappingcost")
        );
        installBlobBackedDescriptorPlane();
        // In place of the identically-built store the gate just registered, so every index-backed arm runs
        // through the counter rather than only the ones this test re-registers.
        MappingGenerationStore.register(productionStore());

        createConcurrently("warmup", DECLARED, 1);
        // After the warm-up, because that is the write that creates the mapping index, and before any arm,
        // because a run against the wrong geometry should cost seconds rather than a full measurement.
        assertMappingIndexGeometry();

        InMemoryMappingStore inMemory = new InMemoryMappingStore();
        Round warmUp = measureRound("1", inMemory);
        Round measured = measureRound("2", inMemory);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT40 gated creation, %d indices per arm at concurrency %d, two rounds, "
                    + "mapping index at %d shards%n"
                    + "                                round 1 (warm-up)   round 2 (measured)%n"
                    + "  mapped, index-backed store  : %,10.0f per second  %,10.0f per second%n"
                    + "  mapped, in-memory store     : %,10.0f per second  %,10.0f per second%n"
                    + "  unmapped (control)          : %,10.0f per second  %,10.0f per second%n"
                    + "  mapped, index-backed, again : %,10.0f per second  %,10.0f per second%n"
                    + "  ratio, index-backed         : %9.2fx           %9.2fx%n"
                    + "  ratio, in-memory            : %9.2fx           %9.2fx%n"
                    + "  drift, first arm to repeat  : %9.2fx           %9.2fx%n"
                    + "%n"
                    + "  from round 2, the store's share of what a declared mapping costs a creation:%n"
                    + "    taking the index-backed arm from the front of the round : %.0f%%%n"
                    + "    taking its repeat from the end of the round             : %.0f%%%n",
                INDICES_PER_ARM,
                CONCURRENCY,
                mappingIndexShards(),
                warmUp.indexBacked(),
                measured.indexBacked(),
                warmUp.inMemory(),
                measured.inMemory(),
                warmUp.unmapped(),
                measured.unmapped(),
                warmUp.indexBackedRepeat(),
                measured.indexBackedRepeat(),
                warmUp.unmapped() / warmUp.indexBacked(),
                measured.unmapped() / measured.indexBacked(),
                warmUp.unmapped() / warmUp.inMemory(),
                measured.unmapped() / measured.inMemory(),
                warmUp.drift(),
                measured.drift(),
                measured.storeShare(measured.indexBacked()),
                measured.storeShare(measured.indexBackedRepeat())
            )
        );

        assertTrue(
            "every arm must have made progress, or this measured nothing",
            measured.indexBacked() > 0 && measured.inMemory() > 0 && measured.unmapped() > 0 && measured.indexBackedRepeat() > 0
        );
        // T41, asserted here rather than only in MappingGenerationStoreTests because this project has twice
        // shipped a fix that passed its own tests while never entering the path it claimed to fix. Nothing
        // in this test does anything but create, and a creation now swaps without reading first, so any read
        // at all means the creation path is still going through updateMapping.
        assertEquals(
            "a gated creation read a mapping that cannot exist, so it is not taking the create path",
            0L,
            productionStore().reads()
        );
    }

    /**
     * One round: index-backed, then in-memory, then the unmapped control, then index-backed again.
     *
     * <p>Each mapped arm is checked for having swapped exactly once per creation, and the two checks fail
     * for opposite reasons. The in-memory arm attributes the store's cost by not paying it, so a mapping
     * that stopped reaching {@code MappingGenerationStore} -- the exact defect T13 found -- would report the
     * store as free. The index-backed arm carries the cost being attributed, so the same silence there would
     * price an arm that never touched the store.
     */
    private Round measureRound(String round, InMemoryMappingStore inMemory) throws Exception {
        long storeSwapsBefore = productionStore().swaps();
        double indexBacked = createConcurrently("mapped" + round, DECLARED, INDICES_PER_ARM);
        assertEquals(
            "the index-backed arm of round " + round + " did not swap once per creation",
            INDICES_PER_ARM,
            productionStore().swaps() - storeSwapsBefore
        );

        double withoutStoreTraffic;
        long inMemorySwapsBefore = inMemory.swaps();
        MappingGenerationStore.register(inMemory);
        try {
            withoutStoreTraffic = createConcurrently("memory" + round, DECLARED, INDICES_PER_ARM);
        } finally {
            // Restored before the control arm runs, so the control is measured against the production
            // wiring rather than against a cluster still holding the test's double.
            MappingGenerationStore.register(productionStore());
        }
        assertEquals(
            "the in-memory arm of round " + round + " did not swap once per creation",
            INDICES_PER_ARM,
            inMemory.swaps() - inMemorySwapsBefore
        );

        double unmapped = createConcurrently("plain" + round, null, INDICES_PER_ARM);

        long repeatSwapsBefore = productionStore().swaps();
        double indexBackedAgain = createConcurrently("repeat" + round, DECLARED, INDICES_PER_ARM);
        assertEquals(
            "the repeat arm of round " + round + " did not swap once per creation",
            INDICES_PER_ARM,
            productionStore().swaps() - repeatSwapsBefore
        );
        return new Round(indexBacked, withoutStoreTraffic, unmapped, indexBackedAgain);
    }

    /**
     * The index-backed store, built once for the whole test.
     *
     * <p>Once, because {@code IndexBackedMappingStore} remembers per instance whether the mapping index
     * exists. A fresh instance per round would put up to {@code CONCURRENCY} blocking creates of
     * {@code .opensearch-index-mappings} at the head of the arm being measured, each one a cluster manager
     * round trip that fails with "already exists", charged to the store as if it were per-creation cost.
     */
    private synchronized CountingStore productionStore() {
        if (productionStore == null) {
            productionStore = new CountingStore(new IndexBackedMappingStore(client(), mappingIndexShards()));
        }
        return productionStore;
    }

    /**
     * The geometry this run gives the mapping index, from {@code -Dtests.mappingshards}.
     *
     * <p>T46 needs three geometries and cannot have them in one cluster: the index is created lazily by the
     * first write and never deleted, so a second store with a different count is told the index already
     * exists and then runs against the first one's geometry while the report claims the second. One run of
     * this class per count, each with its own cluster, is the only arrangement that measures what it says.
     */
    private static int mappingIndexShards() {
        return Integer.getInteger("tests.mappingshards", IndexBackedMappingStore.DEFAULT_SHARDS);
    }

    /**
     * Fails unless the mapping index really has the geometry this run asked for.
     *
     * <p>Every way of getting a second geometry wrong reports success: the create loses to an existing
     * index and returns normally, and a delete between arms leaves the store believing the index exists, so
     * the next write auto-creates it at the cluster default of one shard. Without this the harness would
     * print three numbers for one geometry and nothing would say so.
     */
    private void assertMappingIndexGeometry() {
        String actual = client().admin()
            .indices()
            .prepareGetSettings(IndexBackedMappingStore.MAPPING_INDEX)
            .get()
            .getSetting(IndexBackedMappingStore.MAPPING_INDEX, IndexMetadata.SETTING_NUMBER_OF_SHARDS);
        assertEquals(
            "this run asked for a mapping index of "
                + mappingIndexShards()
                + " shards and got "
                + actual
                + ", so it is about to measure a geometry it did not configure",
            String.valueOf(mappingIndexShards()),
            actual
        );
    }

    private volatile CountingStore productionStore;

    /** The index-backed store with a swap counter, so an arm that stopped reaching it cannot pass quietly. */
    private static final class CountingStore implements MappingGenerationStore.Store {

        private final MappingGenerationStore.Store delegate;
        private final java.util.concurrent.atomic.AtomicLong swaps = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong reads = new java.util.concurrent.atomic.AtomicLong();

        CountingStore(MappingGenerationStore.Store delegate) {
            this.delegate = delegate;
        }

        @Override
        public MappingGenerationStore.MappingGeneration read(String indexUuid) {
            reads.incrementAndGet();
            return delegate.read(indexUuid);
        }

        @Override
        public void delete(String indexUuid) {
            delegate.delete(indexUuid);
        }

        @Override
        public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
            swaps.incrementAndGet();
            return delegate.compareAndSwap(indexUuid, expectedGeneration, updated);
        }

        long swaps() {
            return swaps.get();
        }

        long reads() {
            return reads.get();
        }
    }

    /**
     * The same protocol as {@link IndexBackedMappingStore} with the network taken out.
     *
     * <p>Compare-and-swap on the generation, so a caller cannot tell it apart from the real store except by
     * how long it takes -- which is the whole measurement.
     */
    private static final class InMemoryMappingStore implements MappingGenerationStore.Store {

        private final java.util.concurrent.ConcurrentMap<String, MappingGenerationStore.MappingGeneration> byUuid =
            new java.util.concurrent.ConcurrentHashMap<>();
        private final AtomicInteger swaps = new AtomicInteger();

        @Override
        public MappingGenerationStore.MappingGeneration read(String indexUuid) {
            return byUuid.get(indexUuid);
        }

        @Override
        public void delete(String indexUuid) {
            byUuid.remove(indexUuid);
        }

        @Override
        public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
            swaps.incrementAndGet();
            if (expectedGeneration == 0) {
                return byUuid.putIfAbsent(indexUuid, updated) == null;
            }
            MappingGenerationStore.MappingGeneration current = byUuid.get(indexUuid);
            if (current == null || current.generation() != expectedGeneration) {
                return false;
            }
            return byUuid.replace(indexUuid, current, updated);
        }

        long swaps() {
            return swaps.get();
        }
    }

    /** Creates {@code count} gated indices concurrently, returning creations per second. */
    private double createConcurrently(String prefix, Map<String, Object> mapping, int count) throws Exception {
        CountDownLatch startTogether = new CountDownLatch(1);
        int workers = Math.min(CONCURRENCY, count);
        CountDownLatch finished = new CountDownLatch(workers);
        AtomicInteger created = new AtomicInteger();
        AtomicInteger next = new AtomicInteger();

        List<Thread> running = new java.util.ArrayList<>(workers);
        for (int worker = 0; worker < workers; worker++) {
            Thread thread = new Thread(() -> {
                try {
                    startTogether.await();
                    int index;
                    while ((index = next.getAndIncrement()) < count) {
                        CreateIndexRequest request = new CreateIndexRequest(prefix + "-" + index).settings(gated());
                        if (mapping != null) {
                            request.mapping(mapping);
                        }
                        client().admin().indices().create(request).actionGet();
                        created.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Logged rather than rethrown, because a worker that throws here would take the arm's
                    // rate with it. The count below is what turns a failure into a failed test.
                    logger.warn("creation failed in arm [{}]", prefix, e);
                } finally {
                    finished.countDown();
                }
            });
            running.add(thread);
            thread.start();
        }

        long startedAt = System.nanoTime();
        startTogether.countDown();
        boolean armFinished = finished.await(5, TimeUnit.MINUTES);
        double seconds = (System.nanoTime() - startedAt) / 1e9;
        if (armFinished == false) {
            // Stopped before failing, because these threads outlive the failure otherwise: cleanup would
            // sweep while they were still creating, DescriptorGate.uninstall would pull the plane out from
            // under them, and the survivors would be attributed to whichever suite ran next.
            for (Thread thread : running) {
                thread.interrupt();
            }
            for (Thread thread : running) {
                thread.join(TimeUnit.MINUTES.toMillis(1));
            }
            fail("arm [" + prefix + "] did not finish within five minutes");
        }

        // A partly failed arm used to be reported as a slow arm: the rate divided what completed by the
        // whole arm's wall clock, so 100 creations out of 300 read as a threefold slowdown, and the arm that
        // can fail for store reasons is exactly the one whose cost the finding rests on. An incomplete arm
        // is now a failed test rather than a quiet skew in the direction of the expected answer.
        assertEquals("arm [" + prefix + "] did not create every index; see the logged failures", count, created.get());
        return created.get() / seconds;
    }

    private static Settings gated() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
