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
 * <h2>T40, and why its arms are gone</h2>
 *
 * T20 removed the index-service lock for plainly typed mappings and the ratio moved from about 14x to about
 * 10x, leaving most of the cost somewhere else. T40 named the suspect and built an arm for it: the mapping
 * store's blocking get-and-index per creation, against a shared index the unmapped arm never touches,
 * isolated by running the same creations against an in-memory {@code MappingGenerationStore.Store}.
 *
 * <p><b>T58 removed that suspect from the creation path entirely.</b> A gated creation no longer calls
 * {@code MappingGenerationStore} at all -- the declared mapping rides the {@code IndexDescriptor} the
 * creation was already writing, in the same operation. So the two store arms became the same arm, and the
 * assertions that each had swapped once per creation could no longer hold: this harness has been unable to
 * run against the code it measures since T58, and being opt-in is why nothing noticed. Every creation figure
 * quoted for a mapped index dates from before that change.
 *
 * <h2>What the arms are now</h2>
 *
 * The difference between a mapped and an unmapped gated creation is no longer a store round trip. What is
 * left is mapping validation, a slightly larger descriptor blob, and -- for a mapped index only -- the
 * write-behind stats projection, which runs on GENERIC rather than on the creating thread. So the arms
 * split validation by mapping *shape*, which is what decides whether the index-service lock is taken:
 *
 * <ul>
 *   <li>plainly typed minus unmapped is what carrying a mapping costs when the registry can validate it</li>
 *   <li>one parameter added minus plainly typed is what the index-service lock costs, and nothing else
 *       differs between those two arms</li>
 * </ul>
 *
 * <p><b>What is deliberately not separated:</b> the stats projection rides along with every mapped arm. It
 * is asynchronous, so it does not sit in the creating thread's service time, but it is work on the same box
 * and at this concurrency that is not free. Separating it needs a fifth arm with the gate installed over a
 * non-projecting store, which is a fixture change rather than a knob this class has.
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
    private static final int INDICES_PER_ARM = Integer.getInteger("tests.mappingcost.perarm", 300);

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
        if (org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.mappingcost", "false")) == false) {
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
        "rich1",
        "plain1",
        "repeat1",
        "mapped2",
        "rich2",
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

    /**
     * The plainly typed mapping: every field's definition is exactly a type, so validation is a lookup in
     * an immutable registry and the creation keeps the bypass.
     */
    private static final Map<String, Object> DECLARED = Map.of("properties", Map.of("tenant", Map.of("type", "keyword")));

    /**
     * The same mapping with one parameter added, which is the whole difference between the arms.
     *
     * <p>{@code ignore_above} cannot be checked by a registry lookup -- the fast path's rule is that a
     * one-key definition has nothing else that could be wrong, and this has two -- so the creation builds a
     * throwaway {@code IndexService} inside {@code IndicesService}'s monitor. One parameter is the smallest
     * possible difference that crosses that boundary, which is what makes the subtraction between these two
     * arms a measurement of the lock rather than of mapping complexity.
     */
    private static final Map<String, Object> DECLARED_RICH = Map.of(
        "properties",
        Map.of("tenant", Map.of("type", "keyword", "ignore_above", 256))
    );

    /**
     * One round's four arms, as creations per second.
     *
     * <p>{@code indexBackedRepeat} is the same arm as {@code indexBacked} run at the end of the round, and
     * it exists because the two disagreeing is the only way to see that the round had not settled. Where the
     * two disagree, the attribution is quoted as the range they bracket rather than as a figure.
     */
    private record Round(double plainlyTyped, double richlyTyped, double unmapped, double plainlyTypedRepeat) {

        /** Per-creation service time. At concurrency C, an aggregate rate of R means C/R seconds each. */
        static double microsPer(double rate) {
            return CONCURRENCY * 1e6 / rate;
        }

        /** What the index-service lock costs a creation, in microseconds: the only difference between the two mapped arms. */
        double lockMicros() {
            return microsPer(richlyTyped) - microsPer(plainlyTyped);
        }

        /** What carrying a plainly typed mapping costs a creation once the lock is out of the way. */
        double mappingMicros(double plainlyTypedRate) {
            return microsPer(plainlyTypedRate) - microsPer(unmapped);
        }

        /** How far the round drifted, measured on the one arm that ran at both ends of it. */
        double drift() {
            return plainlyTypedRepeat / plainlyTyped;
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
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.mappingcost", "false"))
        );
        installBlobBackedDescriptorPlane();

        createConcurrently("warmup", DECLARED, 1);

        Round warmUp = measureRound("1");
        Round measured = measureRound("2");

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT60 gated creation, %d indices per arm at concurrency %d, two rounds%n"
                    + "                                round 1 (warm-up)   round 2 (measured)%n"
                    + "  mapped, plainly typed       : %,10.0f per second  %,10.0f per second%n"
                    + "  mapped, one parameter added : %,10.0f per second  %,10.0f per second%n"
                    + "  unmapped (control)          : %,10.0f per second  %,10.0f per second%n"
                    + "  mapped, plainly typed, again: %,10.0f per second  %,10.0f per second%n"
                    + "  ratio, plainly typed        : %9.2fx           %9.2fx%n"
                    + "  ratio, one parameter added  : %9.2fx           %9.2fx%n"
                    + "  drift, first arm to repeat  : %9.2fx           %9.2fx%n"
                    + "%n"
                    + "  from round 2, per creation at concurrency %d:%n"
                    + "    unmapped                                        : %,10.0f us%n"
                    + "    carrying a plainly typed mapping adds           : %,10.0f us  (repeat: %,.0f us)%n"
                    + "    needing the index-service lock adds on top      : %,10.0f us%n",
                INDICES_PER_ARM,
                CONCURRENCY,
                warmUp.plainlyTyped(),
                measured.plainlyTyped(),
                warmUp.richlyTyped(),
                measured.richlyTyped(),
                warmUp.unmapped(),
                measured.unmapped(),
                warmUp.plainlyTypedRepeat(),
                measured.plainlyTypedRepeat(),
                warmUp.unmapped() / warmUp.plainlyTyped(),
                measured.unmapped() / measured.plainlyTyped(),
                warmUp.unmapped() / warmUp.richlyTyped(),
                measured.unmapped() / measured.richlyTyped(),
                warmUp.drift(),
                measured.drift(),
                CONCURRENCY,
                Round.microsPer(measured.unmapped()),
                measured.mappingMicros(measured.plainlyTyped()),
                measured.mappingMicros(measured.plainlyTypedRepeat()),
                measured.lockMicros()
            )
        );

        assertTrue(
            "every arm must have made progress, or this measured nothing",
            measured.plainlyTyped() > 0 && measured.richlyTyped() > 0 && measured.unmapped() > 0 && measured.plainlyTypedRepeat() > 0
        );
    }

    /**
     * One round: plainly typed, richly typed, the unmapped control, then plainly typed again.
     *
     * <p>Each mapped arm is checked for having carried its fields into the descriptor, sampled at one index
     * per arm. That check replaces the swap counting this class did until T58, and it is the same guard
     * against the same failure: an arm whose mapping quietly stopped being carried would do less work than
     * the arm it is being compared with, and report the difference as the cost of something else.
     */
    private Round measureRound(String round) throws Exception {
        double plainlyTyped = createConcurrently("mapped" + round, DECLARED, INDICES_PER_ARM);
        assertFieldsReachedTheDescriptor("mapped" + round + "-0");

        double richlyTyped = createConcurrently("rich" + round, DECLARED_RICH, INDICES_PER_ARM);
        assertFieldsReachedTheDescriptor("rich" + round + "-0");

        double unmapped = createConcurrently("plain" + round, null, INDICES_PER_ARM);

        double plainlyTypedAgain = createConcurrently("repeat" + round, DECLARED, INDICES_PER_ARM);
        assertFieldsReachedTheDescriptor("repeat" + round + "-0");

        return new Round(plainlyTyped, richlyTyped, unmapped, plainlyTypedAgain);
    }

    /** The declared field is readable through the registered store, which since T58 means the descriptor. */
    private void assertFieldsReachedTheDescriptor(String index) {
        org.opensearch.cluster.metadata.IndexDescriptor descriptor = org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.supply(
            index
        );
        assertNotNull("[" + index + "] has no descriptor, so its arm did not create what it claims", descriptor);
        MappingGenerationStore.MappingGeneration mapping = MappingGenerationStore.currentMapping(descriptor.uuid());
        assertNotNull("[" + index + "] carried no mapping, so its arm was not doing the work being priced", mapping);
        assertEquals("keyword", MappingGenerationStore.typeOf(mapping.fields().get("tenant")));
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
