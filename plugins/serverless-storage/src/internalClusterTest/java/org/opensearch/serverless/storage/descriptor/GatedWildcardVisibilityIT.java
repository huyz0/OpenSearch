/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.Version;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.List;
import java.util.Locale;

/**
 * T25. What a wildcard does to a gated index, measured rather than reasoned about.
 *
 * <p>{@code IndexNameExpressionResolver.WildcardExpressionResolver.matches} reads
 * {@code metadata.getIndicesLookup()} and nothing else. All three of its branches do: match-all takes the
 * whole lookup, a suffix wildcard takes a {@code subMap} of it, and any other pattern filters it. A gated
 * index is by construction absent from that map, and {@code AbsentIndexDescriptorSuppliers} offers no
 * pattern seam, only exact-name {@code supply}, {@code exists} and a bounded {@code page}.
 *
 * <p>Reading that suggests the answer. This area has punished reading and concluding often enough that the
 * answer gets measured, and the thing worth measuring is not only <em>whether</em> a wildcard misses gated
 * indices but <em>how it reports the miss</em>. Those are different severities. A wildcard that raised
 * {@code IndexNotFoundException} would be a gap. A wildcard that returns an empty set is the failure this
 * whole area keeps producing: a confident wrong answer, where a tenant searching {@code tenant-*} is told
 * there are no matching indices rather than being told the question cannot be answered.
 *
 * <p>The controls carry the weight. An ordinary index under the identical pattern must resolve, or the
 * pattern was simply wrong; and an exact gated name must resolve, or the gate was never installed and this
 * measured nothing at all.
 *
 * <p>Resolver-level rather than end-to-end on purpose. The question is name resolution, and running it
 * against a deliberately empty {@link ClusterState} makes the descriptor store the only possible source of
 * an answer. An end-to-end search would fold in shard placement, wake and query execution, none of which is
 * being asked about here.
 */
public class GatedWildcardVisibilityIT extends OpenSearchIntegTestCase {

    private static final int TENANTS = 5;

    @After
    public void clearGate() {
        DescriptorGate.uninstall();
    }

    private IndexNameExpressionResolver resolver() {
        return new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
    }

    /** Installs the gate over a store holding {@link #TENANTS} gated tenants. */
    private void installWithTenants() {
        DescriptorStore store = new DescriptorStore(client(), 1);
        for (int i = 0; i < TENANTS; i++) {
            store.create(descriptor(tenant(i)));
        }
        DescriptorGate.install(
            store,
            new IndexBackedMappingStore(client()),
            new IndexBackedMappingStatsAggregator(client()),
            new StoreBackedFieldRefresher(),
            true
        );
    }

    /**
     * The guard. An exact gated name resolves, so anything the wildcard cases report is about the wildcard
     * rather than about a gate that was never installed.
     */
    public void testAnExactGatedNameResolves() {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        String[] resolved = resolver().concreteIndexNames(empty, IndicesOptions.strictExpandOpen(), tenant(0));

        assertEquals("the premise: the gate resolves an exact name", 1, resolved.length);
        assertEquals(tenant(0), resolved[0]);
    }

    /**
     * The question. A prefix wildcard over five gated tenants, against a cluster state that holds none of
     * them.
     *
     * <p>Reported rather than asserted at first, because the number and the failure mode are the finding.
     */
    @AwaitsFix(bugUrl = "WildcardExpressionResolver expands only over cluster state, so a gated index matches no pattern; see GATED_WILDCARD_DESIGN.md")
    public void testAPrefixWildcardOverGatedTenants() {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        String[] resolved = report("tenant-*", empty, IndicesOptions.lenientExpandOpen());

        assertEquals(
            "a prefix wildcard must find the gated tenants it names, or a tenant searching tenant-* is told "
                + "there are no matching indices when there are five",
            TENANTS,
            resolved.length
        );
    }

    /** Match-all, which takes a different branch: {@code resolveEmptyOrTrivialWildcard} over concrete names. */
    @AwaitsFix(bugUrl = "match-all resolves through Metadata.getConcreteAllIndices, which no gated index is in; see GATED_WILDCARD_DESIGN.md")
    public void testMatchAllOverGatedTenants() {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        String[] resolved = report("*", empty, IndicesOptions.lenientExpandOpen());

        assertEquals("match-all must find the gated tenants", TENANTS, resolved.length);
    }

    /**
     * Whether the miss is loud or silent, asked with the strictest options a caller can choose.
     *
     * <p>{@code allowNoIndices=false} is the one setting under which a caller has explicitly asked to be
     * told when a pattern matched nothing. If even that returns empty rather than raising, then no caller
     * can distinguish "no such tenants" from "cannot see these tenants", which is what makes this a
     * correctness problem rather than a missing feature.
     */
    @AwaitsFix(bugUrl = "no wildcard seam exists, so the miss cannot be distinguished from an empty match; see GATED_WILDCARD_DESIGN.md")
    public void testWhetherTheMissIsReportedAtAll() {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        IndicesOptions strict = IndicesOptions.fromOptions(false, false, true, false);

        String outcome;
        try {
            String[] resolved = resolver().concreteIndexNames(empty, strict, "tenant-*");
            outcome = resolved.length + " names, no error";
        } catch (Exception e) {
            outcome = e.getClass().getSimpleName();
        }
        logger.warn("T25: tenant-* with allowNoIndices=false over {} gated tenants gave [{}]", TENANTS, outcome);

        assertEquals(
            "with allowNoIndices=false a caller has asked to be told when a pattern matched nothing, so an "
                + "empty answer here means no caller can tell absence from invisibility",
            TENANTS + " names, no error",
            outcome
        );
    }

    /**
     * The control that proves the pattern itself is fine: the identical wildcard over ordinary indices with
     * cluster state entries resolves all of them.
     */
    public void testTheSameWildcardResolvesOrdinaryIndices() {
        for (int i = 0; i < TENANTS; i++) {
            createIndex(tenant(i));
        }

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        String[] resolved = resolver().concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), "tenant-*");

        assertEquals("the control: the same pattern over ordinary indices finds all of them", TENANTS, resolved.length);
    }

    private String[] report(String expression, ClusterState state, IndicesOptions options) {
        String[] resolved = resolver().concreteIndexNames(state, options, expression);
        logger.warn("T25: [{}] over {} gated tenants resolved {} names", expression, TENANTS, resolved.length);
        return resolved;
    }

    private static String tenant(int i) {
        return String.format(Locale.ROOT, "tenant-%03d", i);
    }

    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
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
