/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;

/**
 * T19. What index settings validation costs, and what a cache could remove.
 *
 * <p>S36 profiled gated creation cleanly and found the settings subsystem is the largest identified consumer
 * of the single cluster manager task thread, at roughly 20 percent: {@code Setting.get}, {@code getRaw},
 * {@code exists}, {@code AbstractScopedSettings}. Larger than the temporary {@code IndexService} everyone
 * guesses at.
 *
 * <p><b>Why a cache is plausible here specifically.</b> OpenSearch already batches index creations: the
 * executor in {@code MetadataCreateIndexService} receives a list of tasks and loops over them, so one state
 * rebuild and one publication serve the whole batch. What the loop cannot amortise is per-index work, since
 * every index has a different name. Settings are the exception. A bulk migration creates thousands of
 * indices with byte-identical settings, and validation is a pure function of them, so the same answer is
 * recomputed thousands of times.
 *
 * <p>This measures the ceiling before anything is built, which is the discipline S32 and S35 both paid for
 * by building first. If validation is cheap in absolute terms, a cache is not worth its invalidation
 * problem no matter what share of a profile it holds.
 *
 * <p>Two arms over the same settings:
 *
 * <ul>
 *   <li><b>validate</b>, what runs today, once per index creation</li>
 *   <li><b>cache hit</b>, an identity check standing in for a memo, which is the floor a perfect cache
 *       could reach</li>
 * </ul>
 *
 * <p><b>Measured, and the cache is not worth building:</b>
 *
 * <pre>
 *   validate                1,606 ns
 *   memo hit                  218 ns
 *   a perfect cache saves   1,388 ns per creation
 *   one creation costs  1,160,000 ns at 850 per second
 *   so validation is         0.14% of a creation
 * </pre>
 *
 * <p><b>The interesting part is why the profile disagreed.</b> S36 reported the settings subsystem at
 * roughly 20 percent of the cluster manager thread, and validation turns out to be 0.14 percent of a
 * creation. Both are true, and the gap is a flaw in how S36 attributed samples: it counted any stack
 * containing {@code Setting.get}, {@code getRaw} or {@code exists} as "settings subsystem", and those
 * frames appear throughout index creation rather than only inside validation. Reading settings while
 * constructing an {@code IndexService}, while building analyzers, and while resolving templates all land
 * in that bucket.
 *
 * <p>Attributing profile samples by "appears anywhere in the stack" over-counts any leaf that many parents
 * call. It is fine for locating a subsystem and wrong for sizing a fix, and S36's numbers should be read
 * that way. This test is what a number sized for a fix looks like: one function, called the way creation
 * calls it, timed on its own.
 */
public class IndexSettingsValidationCostTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 20_000;

    public void testWhatValidationCostsAndWhatACacheWouldSave() {
        IndexScopedSettings scopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;

        // What a serverless index carries, minus index.serverless_storage.enabled, which is registered by
        // the plugin rather than by IndexScopedSettings.DEFAULT_SCOPED_SETTINGS and so cannot be validated
        // here. That omission makes this an underestimate of the real per-creation cost rather than an
        // overestimate, which is the safe direction for a number used to decide whether to build a cache.
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, "some-index-uuid")
            .put("index.refresh_interval", "1s")
            .put("index.merge.policy.segments_per_tier", 4)
            .build();

        // Warmed with the same work as the measurement, since an under-warmed first arm is how S30 inverted
        // a whole curve.
        for (int i = 0; i < ITERATIONS; i++) {
            scopedSettings.validate(settings, true);
        }

        long startedAt = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            scopedSettings.validate(settings, true);
        }
        double validateNanos = (System.nanoTime() - startedAt) / (double) ITERATIONS;

        // The floor: what a memo hit costs, which is a hash and an equality check on the settings.
        Settings same = settings;
        int sink = 0;
        startedAt = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            sink += same.hashCode() ^ (same.equals(settings) ? 1 : 0);
        }
        double cacheHitNanos = (System.nanoTime() - startedAt) / (double) ITERATIONS;
        assertTrue("keep the sink alive so the loop is not optimised away", sink != Integer.MIN_VALUE);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT19 index settings validation, per call%n"
                    + "  validate (runs once per creation)   %,10.0f ns%n"
                    + "  memo hit (hash plus equals)         %,10.0f ns%n"
                    + "  a perfect cache removes             %,10.0f ns per creation%n"
                    + "  against a creation costing about 1,160,000 ns at 850 per second%n"
                    + "  so validation is %.2f%% of one creation%n",
                validateNanos,
                cacheHitNanos,
                validateNanos - cacheHitNanos,
                100.0 * validateNanos / 1_160_000.0
            )
        );

        assertTrue("both measurements must be non-zero, or this measured nothing", validateNanos > 0 && cacheHitNanos > 0);
    }
}
