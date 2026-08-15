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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.DescriptorRepresentable;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.junit.After;

import java.util.List;

/**
 * T29. Whether an alias on a gated index resolves to anything, and what was done about it.
 *
 * <p>T7 closed the list of reasons an index must keep its cluster state entry, and a <em>plain</em> alias was
 * not one of them: a filter, routing, a write index declaration and a hidden flag were each refused, and an
 * alias carrying none of those was allowed through because {@code IndexDescriptor} has a field for the name.
 * That field is written, serialized, sized and round-tripped.
 *
 * <p>Nothing read it. {@code IndexDescriptor}'s own javadoc says state and aliases are carried "because
 * resolution needs them to answer without materializing", and resolution never asks: the exact-name seam is
 * a GET whose document id is the index name, so an alias name misses, and T28's prefix expansion matches on
 * the {@code name} field alone.
 *
 * <p>So the question is what a tenant gets after pointing an alias at a gated index and using it. If it is
 * an error, this is a missing feature. If it is an empty result, it is the same defect this area has now
 * shipped four times, and an alias is a worse place for it than most: an alias is what a client is told to
 * use precisely so the index behind it can change.
 *
 * <p><b>Measured.</b> Naming the alias resolved to zero indices with no error under lenient options and
 * raised {@code IndexNotFoundException} under strict ones, and a wildcard over the alias namespace found
 * nothing. So an alias on a gated index was silently dead.
 *
 * <p><b>What decided the repair.</b> The obvious fix is to resolve aliases by searching the {@code aliases}
 * field, and it was rejected after asking one more question: whether an alias can be <em>changed</em> on a
 * gated index. It cannot. Every alias operation is a cluster state update over metadata a gated index does
 * not have, and adding one does not fail cleanly, it times out with
 * {@code ClusterManagerNotDiscoveredException}. Resolution would therefore have served an alias that could
 * be set once at creation and never added, removed or repointed, which is the opposite of what an alias is
 * for, and with a refresh-bound visibility window on top.
 *
 * <p>So {@code DescriptorRepresentable} now refuses to gate any index that declares an alias, widening the
 * four alias rules T7 added into one. An aliased index keeps its cluster state entry and behaves exactly as
 * it always has, which is why the assertions below now check ordinary behaviour rather than gated behaviour.
 *
 * <p>Probed rather than argued, and the probe is what changed the answer: the resolution gap was the visible
 * problem and the mutation gap was the one that mattered.
 */
public class GatedAliasResolutionIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final String INDEX = "aliased-tenant";

    private static final String ALIAS = "serverless_tenant-alias";

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    private IndexNameExpressionResolver resolver() throws Exception {
        return new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
    }

    private BlobDescriptorBackend installWithAliasedIndex() throws Exception {
        BlobDescriptorBackend store = installBlobBackedDescriptorPlane().points();
        store.create(descriptor(INDEX, List.of(ALIAS)));
        return store;
    }

    /** The premise. The alias is on the descriptor, so anything below is about reading it, not writing it. */
    public void testTheAliasIsRecordedOnTheDescriptor() throws Exception {
        BlobDescriptorBackend store = installWithAliasedIndex();

        IndexDescriptor recorded = store.get(INDEX);
        assertNotNull(recorded);
        assertEquals("the premise: the descriptor carries the alias name", List.of(ALIAS), recorded.aliases());
    }

    /**
     * The rule. An index that declares an alias is not gatable, so it keeps its cluster state entry.
     *
     * <p>This is what makes the resolution gap unreachable rather than fixed: there is no such thing as a
     * gated index with an alias, so there is nothing for resolution to miss.
     */
    public void testAnIndexWithAnAliasIsNotGatable() throws Exception {
        IndexMetadata aliased = IndexMetadata.builder("with-alias")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .putAlias(AliasMetadata.builder("plain-alias"))
            .build();

        String reason = DescriptorRepresentable.whyNotRepresentable(aliased);
        logger.warn("T29: an index with a plain alias is not gatable because: {}", reason);

        assertNotNull("an index declaring an alias must keep its cluster state entry", reason);
        assertTrue("and the reason must name aliases, since an operator has to know which feature kept it", reason.contains("alias"));
    }

    /** The other half of the rule: an index without aliases is still gatable, so this did not gate nothing. */
    public void testAnIndexWithoutAliasesIsStillGatable() throws Exception {
        IndexMetadata plain = IndexMetadata.builder("no-alias")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();

        assertNull("the control: an ordinary index is still representable", DescriptorRepresentable.whyNotRepresentable(plain));
    }

    /** The control. The same alias on an ordinary index resolves, so the pattern of the test is sound. */
    public void testTheSameAliasOnAnOrdinaryIndexResolves() throws Exception {
        createIndex("ordinary-aliased");
        assertTrue(client().admin().indices().prepareAliases().addAlias("ordinary-aliased", "ordinary-alias").get().isAcknowledged());

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        String[] resolved = resolver().concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), "ordinary-alias");

        assertEquals("the control: an ordinary alias resolves to its index", 1, resolved.length);
        assertEquals("ordinary-aliased", resolved[0]);
    }

    private static IndexDescriptor descriptor(String name, List<String> aliases) throws Exception {
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
