/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.usage;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.bucket.BucketSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SearchAdmissionTests extends OpenSearchTestCase {

    private static Guardrails guardrails(Settings.Builder settings) {
        return new Guardrails(settings.build(), BucketSettings.GROUP::get, () -> null, () -> 7, System::currentTimeMillis);
    }

    private static Settings.Builder slots(int slots, long maxWaitMillis) {
        return Settings.builder()
            .put(UsageSettings.SEARCH_SLOTS.getKey(), slots)
            .put(UsageSettings.MAX_WAIT_MILLIS.getKey(), maxWaitMillis);
    }

    /** A search started on another thread, admitted or still waiting. */
    private static final class Pending {
        final CountDownLatch admitted = new CountDownLatch(1);
        final CountDownLatch end = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread thread;

        Pending(Guardrails guardrails, String index) {
            thread = new Thread(() -> {
                try (Releasable search = guardrails.admitSearch(List.of(index))) {
                    admitted.countDown();
                    end.await();
                } catch (Throwable t) {
                    failure.set(t);
                    admitted.countDown();
                }
            });
            thread.start();
        }

        boolean admittedWithin(long millis) throws InterruptedException {
            return admitted.await(millis, TimeUnit.MILLISECONDS);
        }

        void finish() throws InterruptedException {
            end.countDown();
            thread.join(10_000L);
            assertNull(failure.get());
        }
    }

    private static List<Releasable> hold(Guardrails guardrails, String index, int count) throws Exception {
        final List<Releasable> held = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            held.add(guardrails.admitSearch(List.of(index)));
        }
        return held;
    }

    public void testAQuietNodeHoldsNoTenantBack() throws Exception {
        final Guardrails guardrails = guardrails(slots(4, 5_000L));
        final long started = System.nanoTime();
        final List<Releasable> held = hold(guardrails, "a", 4);
        assertTrue("nothing waited", System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1));
        assertEquals(4, guardrails.searchesInFlight());
        held.forEach(Releasable::close);
        assertEquals(0, guardrails.searchesInFlight());
    }

    public void testOnASaturatedNodeATenantOverItsShareWaitsAndOthersGoAhead() throws Exception {
        final Guardrails guardrails = guardrails(slots(4, 10_000L));
        final List<Releasable> busy = hold(guardrails, "noisy", 4);
        final Pending noisy = new Pending(guardrails, "noisy");
        assertFalse("over its share of a full node", noisy.admittedWithin(200L));
        final Pending quiet = new Pending(guardrails, "quiet");
        assertTrue("under its share: goes ahead", quiet.admittedWithin(2_000L));
        busy.get(0).close();
        assertFalse("still as many searches as slots", noisy.admittedWithin(200L));
        busy.get(1).close();
        assertTrue("under the slots again", noisy.admittedWithin(2_000L));
        noisy.finish();
        quiet.finish();
        busy.subList(2, 4).forEach(Releasable::close);
        assertEquals(0, guardrails.searchesInFlight());
    }

    public void testAWaitEndsAtTheLongestWaitAndTheSearchRuns() throws Exception {
        final Guardrails guardrails = guardrails(slots(2, 300L));
        final List<Releasable> busy = hold(guardrails, "noisy", 2);
        final long started = System.nanoTime();
        try (Releasable late = guardrails.admitSearch(List.of("noisy"))) {
            final long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue("waited " + waited, waited >= 250L && waited < 5_000L);
            assertEquals(3, guardrails.searchesInFlight());
        }
        busy.forEach(Releasable::close);
    }

    public void testShareZeroTurnsTheFairShareOff() throws Exception {
        final Guardrails guardrails = guardrails(slots(2, 5_000L).put(UsageSettings.SEARCH_TENANT_SHARE.getKey(), 0.0));
        final long started = System.nanoTime();
        final List<Releasable> held = hold(guardrails, "a", 5);
        assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1));
        held.forEach(Releasable::close);
    }

    public void testATenantAtLevelTwoWaitsWhileHalfTheSlotsAreTaken() throws Exception {
        final Guardrails guardrails = guardrails(slots(4, 10_000L).put(UsageSettings.MODE.getKey(), "enforce"));
        guardrails.applyCluster(Settings.builder().put(UsageSettings.TENANT_LEVELS.getKey(), "low=2,one=1").build());
        final List<Releasable> busy = hold(guardrails, "other", 1);
        busy.addAll(hold(guardrails, "another", 1));
        final Pending low = new Pending(guardrails, "low");
        assertFalse("half the slots are taken", low.admittedWithin(200L));
        try (Releasable one = guardrails.admitSearch(List.of("one"))) {
            assertEquals("level one searches as before", 3, guardrails.searchesInFlight());
        }
        busy.get(0).close();
        assertTrue(low.admittedWithin(2_000L));
        low.finish();
        busy.get(1).close();
    }

    public void testObservingALevelTwoTenantIsNotSlowed() throws Exception {
        final Guardrails guardrails = guardrails(slots(4, 10_000L));
        guardrails.applyCluster(Settings.builder().put(UsageSettings.TENANT_LEVELS.getKey(), "low=2").build());
        final List<Releasable> busy = hold(guardrails, "other", 2);
        final long started = System.nanoTime();
        guardrails.admitSearch(List.of("low")).close();
        assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1));
        busy.forEach(Releasable::close);
    }

    public void testABucketGroupSharesOneShare() throws Exception {
        final Guardrails guardrails = guardrails(slots(2, 10_000L));
        final Settings member = Settings.builder().put(BucketSettings.GROUP.getKey(), "acme").build();
        guardrails.note("acme-a", member);
        guardrails.note("acme-b", member);
        final List<Releasable> busy = hold(guardrails, "acme-a", 2);
        final Pending sibling = new Pending(guardrails, "acme-b");
        assertFalse("its tenant already holds the node", sibling.admittedWithin(200L));
        busy.get(0).close();
        assertTrue(sibling.admittedWithin(2_000L));
        sibling.finish();
        busy.get(1).close();
    }
}
