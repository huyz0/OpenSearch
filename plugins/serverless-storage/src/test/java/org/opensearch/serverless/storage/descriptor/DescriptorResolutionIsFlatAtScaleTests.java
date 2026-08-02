/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.serverless.storage.security.RequestCountingBlobContainer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;

/**
 * Resolving one name costs the same whether the store holds a hundred descriptors or a hundred million.
 *
 * <h2>Why this test exists, and what it replaces</h2>
 *
 * The whole design rests on one claim: per-index cost goes to zero rather than getting smaller. Everything
 * else, computed placement, the name index, gated creation, is downstream of it. Until now that claim was
 * evidenced by a suite of scale tests that built large populations and reported creation rate in indices per
 * second and point lookup in milliseconds, then asserted on the ratio between two population sizes.
 *
 * <p>Those were deleted, for a good reason and with a bad consequence. The reason: a wall-clock assertion on
 * shared hardware measures the machine, and they were the single largest source of flaky failures in this
 * branch. The consequence: they were also the only tests that ever built a population larger than a few
 * dozen, so deleting them left the central claim resting on 25 creations.
 *
 * <p>This restores the evidence in the form the earlier tests should have taken. Flatness is a claim about
 * <em>work per operation</em>, and work per operation is a count. A read that goes to the store costs one
 * round trip whether the store answers in ten microseconds or eighty milliseconds, and the count is the same
 * on a loaded build agent as on an idle laptop. So the assertion is not "the ratio of two timings is close to
 * one", which needs a tolerance and a quiet machine; it is "the two counts are equal", which needs neither.
 *
 * <h2>What is actually being falsified</h2>
 *
 * A design that scanned, listed, or consulted an index proportional to the population would show it here as a
 * count that grows with N. The flat prefix-preserving key layout means a point read is a single {@code GET}
 * of a known key, so the population is not consulted at all. That is the property, and a regression that
 * reintroduced a listing on the read path would fail this immediately rather than showing up as a latency
 * curve nobody runs.
 *
 * <p><b>Listings are counted separately, and that is what carries the argument.</b> Comparing two totals is
 * weaker than it looks against a filesystem fixture: {@code FsBlobContainer} answers any listing in a single
 * call however many blobs it returns, so a stray enumeration would cost one extra request at <em>both</em>
 * populations and the two totals would still match. A real object store pages at a thousand keys behind a
 * serial continuation token and bills per call, so the same enumeration would cost a hundred thousand
 * requests at 100M. A listing is the only operation whose cost grows with the population, which makes
 * "the read path issues zero listings" the honest statement of flatness rather than an equality between two
 * totals. Both are asserted; the listing count is the one that would still hold against S3.
 *
 * <p>Reads are taken through a second backend over the same container, so the cache is cold. That is the case
 * that matters for scale: a node joining a cluster that already holds a large population, whose first read of
 * a tenant must not be more expensive because other tenants exist.
 *
 * <p>The populations are modest by default so the suite can afford them, and the ratio between them is what
 * carries the argument rather than the absolute size. Raise both with
 * {@code -Dtests.descriptor.population=200000} to push it further; the assertions are ratios and equalities,
 * so they do not need retuning when it changes.
 */
public class DescriptorResolutionIsFlatAtScaleTests extends OpenSearchTestCase {

    /** The larger of the two populations. The smaller is this over {@link #FACTOR}. */
    private static final int POPULATION = Integer.getInteger("tests.descriptor.population", 4_000);

    /** How much bigger the large population is than the small one. */
    private static final int FACTOR = 20;

    private ObjectStoreRequestCounter counter;

    private BlobContainer countingContainer() throws Exception {
        counter = new ObjectStoreRequestCounter();
        return new RequestCountingBlobContainer(
            new FsBlobStore(1024, createTempDir(), false).blobContainer(BlobPath.cleanPath()),
            counter
        );
    }

    private static IndexDescriptor descriptor(String name) {
        return IndexDescriptor.from(
            IndexMetadata.builder(name)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                        .build()
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build()
        );
    }

    private static String tenant(int i) {
        return String.format(Locale.ROOT, "tenant-%08d", i);
    }

    /**
     * Builds {@code population} descriptors, then returns what one cold read of a name in the middle costs.
     *
     * <p>The read goes through a second backend over the same container so it cannot be answered from the
     * writer's cache, and the name is chosen from the middle of the population rather than the end so that a
     * structure with any ordering bias has nowhere favourable to hide it.
     */
    private long coldReadCostAtPopulation(int population) throws Exception {
        BlobContainer container = countingContainer();
        BlobDescriptorBackend writer = new BlobDescriptorBackend(container);
        for (int i = 0; i < population; i++) {
            writer.create(descriptor(tenant(i)));
        }

        BlobDescriptorBackend coldReader = new BlobDescriptorBackend(container);
        long before = counter.getCount();
        long listsBefore = counter.listCount();
        assertNotNull("the population must actually be there, or this is measuring an empty store", coldReader.get(tenant(population / 2)));
        assertEquals(
            "a point read must not enumerate the store. On a filesystem a stray listing is one cheap call; "
                + "against S3 it is one call per thousand keys, so at 100M it is the whole cost",
            0,
            counter.listCount() - listsBefore
        );
        return counter.getCount() - before;
    }

    /** Same, for a name that was never created. */
    private long coldMissCostAtPopulation(int population) throws Exception {
        BlobContainer container = countingContainer();
        BlobDescriptorBackend writer = new BlobDescriptorBackend(container);
        for (int i = 0; i < population; i++) {
            writer.create(descriptor(tenant(i)));
        }

        BlobDescriptorBackend coldReader = new BlobDescriptorBackend(container);
        long before = counter.getCount();
        long listsBefore = counter.listCount();
        assertNull(coldReader.get("tenant-absent"));
        assertEquals("confirming absence must not enumerate the tombstone space either", 0, counter.listCount() - listsBefore);
        return counter.getCount() - before;
    }

    /**
     * The claim itself: a point read costs one round trip, and the same one round trip, at both populations.
     */
    public void testAPointReadCostsOneRoundTripAtAnyPopulation() throws Exception {
        int small = POPULATION / FACTOR;

        long atSmall = coldReadCostAtPopulation(small);
        long atLarge = coldReadCostAtPopulation(POPULATION);

        logger.info("cold point read: {} round trips at {} descriptors, {} at {}", atSmall, small, atLarge, POPULATION);

        assertEquals("a cold point read is one GET of a known key", 1, atSmall);
        assertEquals(
            "a "
                + FACTOR
                + "x larger population must not make one read cost more. A count that grows with the "
                + "population means something on the read path is consulting the population",
            atSmall,
            atLarge
        );
    }

    /**
     * A miss is the most expensive descriptor operation, and it must also be flat.
     *
     * <p>Two round trips, live prefix then tombstone prefix, because "deleted" and "never existed" have
     * opposite safe answers. What matters at scale is that it stays two rather than becoming a scan of the
     * tombstone space as deletions accumulate.
     */
    public void testAMissCostsTwoRoundTripsAtAnyPopulation() throws Exception {
        int small = POPULATION / FACTOR;

        long atSmall = coldMissCostAtPopulation(small);
        long atLarge = coldMissCostAtPopulation(POPULATION);

        assertEquals("live prefix, then tombstone prefix", 2, atSmall);
        assertEquals("and still exactly two once the population is " + FACTOR + "x larger", atSmall, atLarge);
    }

    /**
     * Creation is one conditional write, and stays one however many names already exist.
     *
     * <p>This is the claim that takes the serialised cluster manager out of index creation, so a creation
     * whose cost depended on the population would defeat the purpose even if every read stayed flat.
     */
    public void testACreationCostsTheSameAtAnyPopulation() throws Exception {
        int small = POPULATION / FACTOR;

        long atSmall = creationCostAfterPopulating(small);
        long atLarge = creationCostAfterPopulating(POPULATION);

        assertEquals(
            "creating the " + POPULATION + "th index must cost what creating the " + small + "th did",
            atSmall,
            atLarge
        );
    }

    private long creationCostAfterPopulating(int population) throws Exception {
        BlobContainer container = countingContainer();
        BlobDescriptorBackend backend = new BlobDescriptorBackend(container);
        for (int i = 0; i < population; i++) {
            backend.create(descriptor(tenant(i)));
        }

        long before = counter.getCount();
        long listsBefore = counter.listCount();
        assertTrue(backend.create(descriptor("tenant-brand-new")));
        assertEquals("taking a name must not enumerate the names already taken", 0, counter.listCount() - listsBefore);
        return counter.getCount() - before;
    }
}
