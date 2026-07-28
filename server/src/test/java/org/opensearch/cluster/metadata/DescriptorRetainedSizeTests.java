/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.Version;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * T4b. Whether a descriptor's size varies enough to justify bounding a cache by bytes.
 *
 * <p>TiDB bounds its schema cache by bytes ({@code tidb_schema_cache_size}, 512 MB by default) rather than
 * by object count, and the TiDB study ranked adopting that second. This measures whether the reason
 * transfers before anything is built on it.
 *
 * <p><b>It may well not transfer.</b> A {@code TableInfo} grows with column count, so for TiDB an object
 * count says almost nothing about memory. An {@link IndexDescriptor} is a fixed shape of fourteen fields
 * whose only unbounded part is its alias list, so entry count may be a perfectly good proxy. Adopting a byte
 * budget because TiDB has one, without checking that, would be the same error as quoting S26's throughput
 * figure: correct about the other system, unchecked against this one.
 *
 * <p>The decision rule is stated before the numbers, so it cannot be fitted to them. A byte budget costs a
 * size estimate on every admission and makes the bound harder to reason about. It is worth that if a
 * plausible heavy descriptor is more than roughly five times a typical one, because at that ratio a cache
 * holding its full entry count can be several times the memory its bound implies. Below that, entry count
 * is the simpler bound and stays.
 *
 * <p>Note the interaction with T7. An index whose aliases carry filters or routing is no longer gated at
 * all, so the aliases reaching a descriptor are plain names. That narrows the variance this can see, which
 * is the honest framing: the heavy case here is many plain aliases, not one enormous filter.
 */
public class DescriptorRetainedSizeTests extends OpenSearchTestCase {

    /** Above this, an entry count stops predicting memory well enough to be the bound. */
    private static final double RATIO_JUSTIFYING_BYTE_BUDGET = 5.0;

    public void testHowMuchDescriptorSizeVaries() {
        long minimal = retainedSize(descriptor("i", 0, 8));
        long typical = retainedSize(descriptor("logs-application-prod-2026-07-29", 1, 36));
        long heavy = retainedSize(descriptor("logs-application-prod-2026-07-29", 20, 36));
        long extreme = retainedSize(descriptor("logs-application-prod-2026-07-29", 200, 36));

        double heavyRatio = heavy / (double) typical;
        double extremeRatio = extreme / (double) typical;

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT4b retained size of one descriptor%n"
                    + "  minimal (short name, no aliases)      %,8d bytes%n"
                    + "  typical (real name, one alias)        %,8d bytes%n"
                    + "  heavy   (real name, 20 aliases)       %,8d bytes   %.1fx typical%n"
                    + "  extreme (real name, 200 aliases)      %,8d bytes   %.1fx typical%n"
                    + "  a byte budget is justified above %.1fx%n",
                minimal,
                typical,
                heavy,
                heavyRatio,
                extreme,
                extremeRatio,
                RATIO_JUSTIFYING_BYTE_BUDGET
            )
        );

        assertTrue("every measurement must be non-zero, or this measured nothing", minimal > 0 && typical > 0 && heavy > 0);
        assertTrue("more aliases must cost more, or the estimator is not seeing the list", heavy > typical);
    }

    /**
     * The retained size, summed from parts the estimator will actually walk.
     *
     * <p>{@code RamUsageEstimator.sizeOfObject} refuses to traverse an {@link IndexDescriptor} and returns
     * its unknown-object default of 256 bytes for every input, which is how the first version of this test
     * reported that a two hundred alias descriptor was the same size as an empty one. The assertion that
     * more aliases must cost more is what caught it, and it stays for that reason.
     */
    private static long retainedSize(IndexDescriptor descriptor) {
        return RamUsageEstimator.shallowSizeOfInstance(IndexDescriptor.class) + RamUsageEstimator.sizeOf(descriptor.name())
            + RamUsageEstimator.sizeOf(descriptor.uuid()) + RamUsageEstimator.sizeOfCollection(descriptor.aliases());
    }

    private static IndexDescriptor descriptor(String name, int aliasCount, int aliasNameLength) {
        List<String> aliases = new ArrayList<>(aliasCount);
        for (int i = 0; i < aliasCount; i++) {
            StringBuilder alias = new StringBuilder(String.format(Locale.ROOT, "alias-%04d-", i));
            while (alias.length() < aliasNameLength) {
                alias.append('x');
            }
            aliases.add(alias.toString());
        }
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            aliases,
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            1_700_000_000_000L
        );
    }
}
