/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scheduling;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

public class JitteredSchedulingTests extends OpenSearchTestCase {

    public void testJitteredIntervalIsNeverShorterThanTheConfiguredOne() {
        TimeValue interval = TimeValue.timeValueSeconds(60);
        for (int i = 0; i < 100; i++) {
            TimeValue jittered = JitteredScheduling.jitter(interval);
            assertTrue(
                "jittered interval must never be shorter than the configured one, got " + jittered,
                jittered.millis() >= interval.millis()
            );
        }
    }

    public void testJitteredIntervalNeverExceedsTheConfiguredJitterFraction() {
        TimeValue interval = TimeValue.timeValueSeconds(60);
        double jitterFraction = 0.2;
        long maxExtraMillis = Math.round(interval.millis() * jitterFraction);
        for (int i = 0; i < 100; i++) {
            TimeValue jittered = JitteredScheduling.jitter(interval, jitterFraction);
            long extra = jittered.millis() - interval.millis();
            assertTrue("jitter must never exceed the configured fraction, got extra=" + extra, extra <= maxExtraMillis);
        }
    }

    public void testDifferentCallsProduceGenuinelyDifferentValues() {
        // Not a hard guarantee (two draws could coincidentally match), but with a wide-enough
        // interval and many trials, real jitter must produce more than one distinct value --
        // proves this isn't secretly a no-op returning the same fixed offset every time.
        TimeValue interval = TimeValue.timeValueMinutes(10);
        java.util.Set<Long> distinctValues = new java.util.HashSet<>();
        for (int i = 0; i < 50; i++) {
            distinctValues.add(JitteredScheduling.jitter(interval).millis());
        }
        assertTrue("real jitter must produce more than one distinct value across 50 trials", distinctValues.size() > 1);
    }

    public void testZeroJitterFractionReturnsTheIntervalUnchanged() {
        TimeValue interval = TimeValue.timeValueSeconds(60);
        assertEquals(interval, JitteredScheduling.jitter(interval, 0.0));
    }

    public void testNonPositiveIntervalPassesThroughUnchanged() {
        // A disabled scheduler (interval <= 0, this plugin's own established convention for "off")
        // must never be turned into a real positive delay by jitter.
        assertEquals(TimeValue.MINUS_ONE, JitteredScheduling.jitter(TimeValue.MINUS_ONE));
    }

    public void testRejectsNegativeJitterFraction() {
        expectThrows(IllegalArgumentException.class, () -> JitteredScheduling.jitter(TimeValue.timeValueSeconds(60), -0.1));
    }
}
