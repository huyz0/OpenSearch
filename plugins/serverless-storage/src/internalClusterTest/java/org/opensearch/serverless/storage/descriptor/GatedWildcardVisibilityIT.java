/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.UnsupportedWildcardException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
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
public class GatedWildcardVisibilityIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final int TENANTS = 5;

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    private IndexNameExpressionResolver resolver() throws Exception {
        return new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
    }

    /** Installs the gate over a store holding {@link #TENANTS} gated tenants. */
    private BlobDescriptorBackend installWithTenants() throws Exception {
        BlobDescriptorBackend store = installBlobBackedDescriptorPlane().points();
        for (int i = 0; i < TENANTS; i++) {
            store.create(descriptor(tenant(i)));
        }
        // Expansion is a search and searches are refresh-bound, per H18. Without this the tests below
        // measure an empty index, which looks identical to the defect they exist to catch.
        return store;
    }

    /**
     * The guard. An exact gated name resolves, so anything the wildcard cases report is about the wildcard
     * rather than about a gate that was never installed.
     */
    public void testAnExactGatedNameResolves() throws Exception {
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
    public void testAPrefixWildcardOverGatedTenants() throws Exception {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        String[] resolved = report("serverless_tenant-*", empty, IndicesOptions.lenientExpandOpen());

        assertEquals(
            "a prefix wildcard must find the gated tenants it names, or a tenant searching tenant-* is told "
                + "there are no matching indices when there are five",
            TENANTS,
            resolved.length
        );
    }

    /**
     * Match-all does not expand over gated indices, and this pins that as a decision rather than a gap.
     *
     * <p>It is the one place the contract gives something up instead of bounding it, and two attempts to
     * avoid that failed. Capping {@code _all} like any other prefix made cluster health fail once the
     * population passed the cap, and then hung the test cluster for twenty minutes, because the framework's
     * own consistency checks ask the same question. Restricting the expansion to explicitly written patterns
     * did not help, because health and node stats arrive here with an explicit {@code _all}: the resolver
     * cannot tell a caller enumerating the cluster from the cluster asking about itself.
     *
     * <p>So {@code *} keeps its cluster state meaning. A gated index is reached by prefix or by name, which
     * is consistent rather than population-dependent and cannot fail. The cost is that on a small cluster
     * {@code *} silently omits gated indices, which is a real wart and is why this test asserts the omission
     * out loud instead of leaving it to be rediscovered.
     */
    public void testMatchAllDeliberatelyDoesNotExpandOverGatedTenants() throws Exception {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        String[] resolved = report("*", empty, IndicesOptions.lenientExpandOpen());

        assertEquals("match-all means everything in cluster state, and a gated index is not in it", 0, resolved.length);
    }

    /**
     * Whether the miss is loud or silent, asked with the strictest options a caller can choose.
     *
     * <p>{@code allowNoIndices=false} is the one setting under which a caller has explicitly asked to be
     * told when a pattern matched nothing. If even that returns empty rather than raising, then no caller
     * can distinguish "no such tenants" from "cannot see these tenants", which is what makes this a
     * correctness problem rather than a missing feature.
     */
    public void testWhetherTheMissIsReportedAtAll() throws Exception {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        IndicesOptions strict = IndicesOptions.fromOptions(false, false, true, false);

        String outcome;
        try {
            String[] resolved = resolver().concreteIndexNames(empty, strict, "serverless_tenant-*");
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
     * A pattern that is not a prefix is refused rather than answered.
     *
     * <p>The refusal is the feature. {@code *-005} has no range to scan against a name-sorted structure, so
     * the alternatives are reading every name in the population or answering wrongly, and at a hundred
     * million the first is not an answer either. Refusing says which of the two the cluster is doing.
     */
    public void testALeadingWildcardIsRefusedRatherThanAnswered() throws Exception {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        UnsupportedWildcardException refused = expectThrows(
            UnsupportedWildcardException.class,
            () -> resolver().concreteIndexNames(empty, IndicesOptions.lenientExpandOpen(), "*-003")
        );
        assertTrue(
            "the message must say why, since the caller's only route forward is to rewrite the pattern: " + refused.getMessage(),
            refused.getMessage().contains("trailing wildcard")
        );
    }

    /** An embedded wildcard is refused for the same reason, and it is a separate branch of the check. */
    public void testAnEmbeddedWildcardIsRefused() throws Exception {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        expectThrows(
            UnsupportedWildcardException.class,
            () -> resolver().concreteIndexNames(empty, IndicesOptions.lenientExpandOpen(), "tenant*003")
        );
    }

    /**
     * A prefix matching more than the cap is refused, and the error names the cap.
     *
     * <p>The alternative, returning the first {@code limit} names, is the failure this area has now shipped
     * four measured times: an answer that is wrong and looks complete. A tenant given a hundred of their
     * five thousand indices has no way to tell.
     */
    public void testAPrefixOverTheCapIsRefusedRatherThanTruncated() throws Exception {
        installWithTenants();
        DescriptorGate.setWildcardExpansionLimit(TENANTS - 1);

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        UnsupportedWildcardException refused = expectThrows(
            UnsupportedWildcardException.class,
            () -> resolver().concreteIndexNames(empty, IndicesOptions.lenientExpandOpen(), "serverless_tenant-*")
        );
        logger.warn("T28: over-cap expansion reported [{}]", refused.getMessage());
        assertTrue(
            "the error must name the limit, or an operator cannot tell which setting to change",
            refused.getMessage().contains("[" + (TENANTS - 1) + "]")
        );
    }

    /** Exactly at the cap is answered, so the boundary is inclusive rather than off by one. */
    public void testAPrefixExactlyAtTheCapIsAnswered() throws Exception {
        installWithTenants();
        DescriptorGate.setWildcardExpansionLimit(TENANTS);

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        String[] resolved = resolver().concreteIndexNames(empty, IndicesOptions.lenientExpandOpen(), "serverless_tenant-*");

        assertEquals("a pattern matching exactly the cap is within it", TENANTS, resolved.length);
    }

    /**
     * A limit of zero refuses every wildcard, which is the "no wildcards at all" position.
     *
     * <p>Worth a test because it is a stated configuration rather than an accident of the arithmetic: the
     * narrowest form of the contract has to be reachable by setting a number, not by a different build.
     */
    public void testALimitOfZeroRefusesEveryWildcard() throws Exception {
        installWithTenants();
        DescriptorGate.setWildcardExpansionLimit(0);

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        expectThrows(
            UnsupportedWildcardException.class,
            () -> resolver().concreteIndexNames(empty, IndicesOptions.lenientExpandOpen(), "serverless_tenant-*")
        );
    }

    /** A prefix matching nothing is an empty answer rather than an error, as it is for ordinary indices. */
    public void testAPrefixMatchingNoGatedIndexIsEmptyRatherThanRefused() throws Exception {
        installWithTenants();

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        String[] resolved = resolver().concreteIndexNames(empty, IndicesOptions.lenientExpandOpen(), "nosuchprefix-*");

        assertEquals("matching nothing is a real answer, and differs from being unable to answer", 0, resolved.length);
    }

    /**
     * A deleted gated index does not match a wildcard.
     *
     * <p>H4 records a deletion as a tombstone rather than an absence, so the document is still there and
     * only its state says otherwise. T27 found listing had the same gap.
     */
    public void testATombstonedIndexDoesNotMatchAWildcard() throws Exception {
        BlobDescriptorBackend store = installWithTenants();
        store.put(descriptorWithState(tenant(0), IndexDescriptor.State.DELETED));

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        String[] resolved = resolver().concreteIndexNames(empty, IndicesOptions.lenientExpandOpen(), "serverless_tenant-*");

        assertEquals("a deleted index must not match", TENANTS - 1, resolved.length);
        for (String name : resolved) {
            assertNotEquals(tenant(0), name);
        }
    }

    /**
     * What an over-cap population does to the APIs that resolve {@code _all} without asking for it.
     *
     * <p>Refusing {@code *} is the stated contract and follows from treating it as a prefix expansion with
     * an empty prefix. The question this asks is what else that refusal reaches. Cluster health, node stats
     * and several internal paths resolve an empty index list, which is the same trivial-wildcard branch, and
     * an operator whose health API starts failing because the cluster grew past a hundred tenants would
     * reasonably call that a worse bug than the one being fixed.
     *
     * <p>Probed rather than reasoned about, because the answer depends on which of these APIs actually goes
     * through the resolver and which reads cluster state directly, and this area has repeatedly punished
     * reading the code and concluding.
     *
     * <p><b>The first version of the fix failed this, and not gently.</b> Cluster health did not merely
     * return an error: the test cluster then hung until the suite timed out at twenty minutes, because the
     * framework's own consistency checks ask the same question. Folding gated indices into an empty
     * expression list had turned a query limit into an outage. An empty list now keeps its cluster state
     * meaning, and only a pattern the caller actually wrote is capped.
     */
    public void testWhatAnOverCapPopulationDoesToClusterHealth() throws Exception {
        installWithTenants();
        DescriptorGate.setWildcardExpansionLimit(TENANTS - 1);

        String health;
        try {
            health = client().admin().cluster().prepareHealth().get().getStatus().toString();
        } catch (Exception e) {
            health = e.getClass().getSimpleName();
        }

        String catIndices;
        try {
            catIndices = String.valueOf(client().admin().indices().prepareStats().get().getIndices().size());
        } catch (Exception e) {
            catIndices = e.getClass().getSimpleName();
        }

        logger.warn("T28: with the population over the cap, cluster health gave [{}] and index stats gave [{}]", health, catIndices);

        assertFalse(
            "cluster health must keep working on a cluster that has outgrown its wildcard cap, or the cap "
                + "has turned a query limit into an outage: "
                + health,
            health.contains("Exception")
        );
    }

    /**
     * A wildcard costs one descriptor read, not two.
     *
     * <p>{@code innerResolve} asks {@code aliasOrIndexExists} about every expression before deciding whether
     * it is a pattern, and that seam consults the descriptor store on a miss. An index name cannot contain a
     * star, so for a wildcard that read can only miss, and it was one remote GET per wildcard request spent
     * asking whether an index is literally called {@code tenant-*}.
     *
     * <p>Harmless while H8a's seam existed and no wildcard reached it. A per-request cost the moment T28
     * made wildcards a normal path, which is why it is counted here rather than reasoned about.
     */
    public void testAWildcardDoesNotAlsoPayAPointLookup() throws Exception {
        BlobDescriptorBackend store = installWithTenants();

        // A prefix that matches nothing, which is what isolates the question. Measured against a matching
        // prefix the count was five, and five is correct: each resolved gated name needs a descriptor read
        // to carry its uuid, or the request cannot reach the shard. Those reads are the feature. The one
        // this is about is the extra lookup of the pattern itself, and only a zero-match prefix leaves it
        // alone in the count.
        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        long before = store.readCount();
        resolver().concreteIndexNames(empty, IndicesOptions.lenientExpandOpen(), "nosuchprefix-*");
        long reads = store.readCount() - before;

        logger.warn("T28: a wildcard matching nothing issued {} descriptor point reads", reads);
        assertEquals("a pattern is not a name, so it must not be looked up as one", 0, reads);
    }

    /**
     * An ordinary cluster is untouched, which is the property every seam in this area has to keep.
     *
     * <p>With no expander registered the wildcard resolves from cluster state exactly as it always has, and
     * a pattern the contract would refuse is not refused, because there is no gated population for the
     * contract to be about.
     */
    public void testWithoutTheGateALeadingWildcardStillWorks() throws Exception {
        for (int i = 0; i < TENANTS; i++) {
            createIndex(tenant(i));
        }
        // Deliberately not installed.

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        String[] resolved = resolver().concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), "*-003");

        assertEquals("a cluster that has never gated an index must resolve exactly as before", 1, resolved.length);
        assertEquals(tenant(3), resolved[0]);
    }

    /**
     * The control that proves the pattern itself is fine: the identical wildcard over ordinary indices with
     * cluster state entries resolves all of them.
     */
    public void testTheSameWildcardResolvesOrdinaryIndices() throws Exception {
        for (int i = 0; i < TENANTS; i++) {
            createIndex(tenant(i));
        }

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        String[] resolved = resolver().concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), "serverless_tenant-*");

        assertEquals("the control: the same pattern over ordinary indices finds all of them", TENANTS, resolved.length);
    }

    private String[] report(String expression, ClusterState state, IndicesOptions options) throws Exception {
        String[] resolved = resolver().concreteIndexNames(state, options, expression);
        logger.warn("T25: [{}] over {} gated tenants resolved {} names", expression, TENANTS, resolved.length);
        return resolved;
    }

    private static String tenant(int i) throws Exception {
        return String.format(Locale.ROOT, "serverless_tenant-%03d", i);
    }

    private static IndexDescriptor descriptorWithState(String name, IndexDescriptor.State state) throws Exception {
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            state,
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

    private static IndexDescriptor descriptor(String name) throws Exception {
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
