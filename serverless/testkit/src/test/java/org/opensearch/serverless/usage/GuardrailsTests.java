/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.usage;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.bucket.BucketSettings;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.store.IndexUsage;
import org.opensearch.serverless.store.MeteredBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class GuardrailsTests extends OpenSearchTestCase {

    private final AtomicLong now = new AtomicLong(1_000_000L);
    private MeteredBlobStore store;
    private int written;

    private Guardrails guardrails(Settings settings) throws IOException {
        store = new MeteredBlobStore(new FsBlobStore(8192, createTempDir(), false));
        return new Guardrails(settings, BucketSettings.GROUP::get, () -> store.usage(), now::get);
    }

    /** Writes PUTs the store charges to an index: one cost unit each. */
    private void puts(String index, int count) throws IOException {
        final BlobContainer assignments = store.blobContainer(RegisterMap.assignments(BlobPath.cleanPath(), "n"));
        for (int i = 0; i < count; i++) {
            final String name = RegisterMap.assignmentBlob(index, written++);
            assignments.writeBlob(name, new ByteArrayInputStream(new byte[1]), 1, false);
        }
    }

    /** One PUT for each index, then the first tick, so the next tick has a rate. */
    private void baseline(Guardrails guardrails, String... indices) throws IOException {
        for (String index : indices) {
            puts(index, 1);
        }
        guardrails.tick();
    }

    private static Settings tier(String tier) {
        return Settings.builder().put(UsageSettings.INDEX_TIER.getKey(), tier).build();
    }

    /** Ten seconds on, writing {@code puts} PUTs for the index in them, then a tick. */
    private void tenSeconds(Guardrails guardrails, String index, int count) throws IOException {
        puts(index, count);
        now.addAndGet(Guardrails.TICK_MILLIS);
        guardrails.tick();
    }

    public void testOverForAMinuteRaisesALevelAndTwoMoreMinutesTheNext() throws IOException {
        final Guardrails guardrails = guardrails(Settings.EMPTY);
        guardrails.note("a", tier("small"));
        baseline(guardrails, "a");
        // 10 PUTs every 10 s: 60 a minute, three times the small tier's 20.
        final int[] levels = new int[40];
        for (int i = 0; i < levels.length; i++) {
            tenSeconds(guardrails, "a", 10);
            levels[i] = guardrails.level("a");
        }
        assertEquals("over since the first tick, at 10 s", 0, levels[5]);
        assertEquals("a minute over", 1, levels[6]);
        assertEquals(1, levels[17]);
        assertEquals("two more minutes", 2, levels[18]);
        assertEquals(2, levels[29]);
        assertEquals(3, levels[30]);
        assertEquals("three at most", 3, levels[39]);
        final Map<String, Object> line = guardrails.tenants().get("a");
        assertEquals(60.0, (Double) line.get("cost_per_minute"), 0.01);
        assertEquals(20L, line.get("budget_per_minute"));
        assertEquals("small", line.get("tier"));
    }

    public void testUnderForFiveMinutesLowersOneLevel() throws IOException {
        final Guardrails guardrails = guardrails(Settings.EMPTY);
        guardrails.note("a", tier("small"));
        baseline(guardrails, "a");
        for (int i = 0; i < 7; i++) {
            tenSeconds(guardrails, "a", 10);
        }
        assertEquals(1, guardrails.level("a"));
        // 2 PUTs every 10 s: 12 a minute, under 20.
        for (int i = 0; i < 30; i++) {
            tenSeconds(guardrails, "a", 2);
            assertEquals("still within five minutes, step " + i, 1, guardrails.level("a"));
        }
        tenSeconds(guardrails, "a", 2);
        assertEquals(0, guardrails.level("a"));
    }

    public void testABriefBurstDoesNotRaiseALevel() throws IOException {
        final Guardrails guardrails = guardrails(Settings.EMPTY);
        guardrails.note("a", tier("small"));
        baseline(guardrails, "a");
        for (int round = 0; round < 5; round++) {
            for (int i = 0; i < 5; i++) {
                tenSeconds(guardrails, "a", 50);
            }
            tenSeconds(guardrails, "a", 0);
        }
        assertEquals("never over for a whole minute", 0, guardrails.level("a"));
    }

    public void testABucketGroupIsOneTenantWithOneBudget() throws IOException {
        final Guardrails guardrails = guardrails(Settings.EMPTY);
        final Settings member = Settings.builder()
            .put(BucketSettings.GROUP.getKey(), "acme")
            .put(UsageSettings.INDEX_TIER.getKey(), "small")
            .build();
        guardrails.note("acme-logs", member);
        guardrails.note("acme-metrics", member);
        guardrails.note("other", tier("small"));
        baseline(guardrails, "acme-logs", "acme-metrics", "other");
        // 2 + 2 PUTs every 10 s: each index alone is under 20 a minute, the tenant at 24 is over.
        for (int i = 0; i < 7; i++) {
            puts("acme-logs", 2);
            puts("other", 2);
            tenSeconds(guardrails, "acme-metrics", 2);
        }
        assertEquals("acme", guardrails.info("acme-logs").tenant());
        assertEquals(1, guardrails.level("acme"));
        assertEquals(0, guardrails.level("other"));
        assertEquals(24.0, (Double) guardrails.tenants().get("acme").get("cost_per_minute"), 0.01);
    }

    public void testBudgetsComeFromTheTierOrTheIndexAndTheDefaultIsLarge() throws IOException {
        final Guardrails guardrails = guardrails(Settings.builder().put(UsageSettings.STANDARD_BUDGET.getKey(), 300L).build());
        guardrails.note("std", tier("standard"));
        guardrails.note("own", Settings.builder().put(UsageSettings.INDEX_BUDGET.getKey(), 7L).build());
        assertEquals(300L, guardrails.info("std").budgetPerMinute());
        assertEquals(7L, guardrails.info("own").budgetPerMinute());
        assertEquals(UsageSettings.Tier.LARGE, guardrails.info("own").tier());
        assertEquals("unseen: the node's default", 2_000L, guardrails.info("unseen").budgetPerMinute());
        assertEquals(UsageSettings.Mode.OBSERVE, guardrails.mode());
        final Settings changed = tier("small");
        guardrails.note("std", changed);
        assertEquals("new settings are read again", 20L, guardrails.info("std").budgetPerMinute());
    }

    public void testOffComputesNothingAndAnIndexOffIsLeftOut() throws IOException {
        final Guardrails off = guardrails(Settings.builder().put(UsageSettings.MODE.getKey(), "off").build());
        off.note("a", tier("small"));
        for (int i = 0; i < 10; i++) {
            tenSeconds(off, "a", 50);
        }
        assertEquals(0, off.level("a"));
        assertTrue(off.tenants().isEmpty());

        final Guardrails observing = guardrails(Settings.EMPTY);
        observing.note(
            "quiet",
            Settings.builder().put(UsageSettings.INDEX_TIER.getKey(), "small").put(UsageSettings.INDEX_MODE.getKey(), "off").build()
        );
        observing.tick();
        for (int i = 0; i < 10; i++) {
            tenSeconds(observing, "quiet", 50);
        }
        assertFalse(observing.tenants().containsKey("quiet"));
    }

    public void testNoUsageMeansNoLevels() {
        final Guardrails guardrails = new Guardrails(Settings.EMPTY, BucketSettings.GROUP::get, () -> null, now::get);
        guardrails.tick();
        guardrails.wrote("a", 5);
        guardrails.pushed("a", 5);
        guardrails.searching(List.of("a")).close();
        assertNull(guardrails.usage());
        assertTrue(guardrails.tenants().isEmpty());
    }

    public void testWritesSearchesAndPushesAreCounted() throws IOException {
        final Guardrails guardrails = guardrails(Settings.EMPTY);
        guardrails.wrote("a", 3);
        guardrails.wrote("a", 0);
        guardrails.pushed("a", 120);
        try (Releasable search = guardrails.searching(List.of("a", "b"))) {
            assertEquals("counted when it ends", 0L, store.usage().of("a").get(IndexUsage.Kind.QUERIES));
        }
        guardrails.searching(List.of()).close();
        assertEquals(3L, store.usage().of("a").get(IndexUsage.Kind.DOCS));
        assertEquals(120L, store.usage().of("a").get(IndexUsage.Kind.PUSH_BYTES));
        assertEquals(1L, store.usage().of("a").get(IndexUsage.Kind.QUERIES));
        assertEquals(1L, store.usage().of("b").get(IndexUsage.Kind.QUERIES));
        expectThrows(IllegalArgumentException.class, () -> store.usage().record("a", IndexUsage.Kind.BLOB_WRITE, 1L));
        expectThrows(IllegalArgumentException.class, () -> store.usage().record("a", IndexUsage.Kind.BYTES_READ, 1L));
    }

    public void testSettingsRefuseWhatIsNotAModeOrTier() {
        expectThrows(
            IllegalArgumentException.class,
            () -> UsageSettings.MODE.get(Settings.builder().put("serverless.cost.guardrails", "loud").build())
        );
        expectThrows(IllegalArgumentException.class, () -> UsageSettings.INDEX_TIER.get(tier("huge")));
        assertEquals(UsageSettings.Mode.ENFORCE, UsageSettings.Mode.parse(" Enforce "));
        assertEquals("", UsageSettings.INDEX_MODE.get(Settings.EMPTY));
        assertEquals(8, new UsageSettings().settings().size());
    }
}
