/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The index-name resolver a plugin gets, which refuses rather than answering wrongly.
 *
 * <p><b>Why a plugin cannot have a working one here, and why that is architecture rather than a gap.</b>
 * Core's resolver turns {@code logs-*} or a concrete name into indices by consulting the {@code Metadata}
 * of a {@link ClusterState}. On a serverless node that {@code ClusterState} is a node-local materialised
 * view holding the indices <em>this node happens to have open</em> — never the deployment's index set,
 * which lives in the metadata plane and is deliberately not enumerable (&sect;6.3 refuses
 * {@code /_serverless/indices} for the same reason: walking a hundred million indices through a request
 * path is not an operation this system offers).
 *
 * <p>So a real resolver here would answer a wildcard with a subset and a concrete name this node has not
 * opened with {@code IndexNotFoundException}. Both are wrong answers that look like right ones, which is
 * exactly what D2 exists to prevent — and for a plugin evaluating privileges over an index pattern, a
 * subset is a security failure that fails open.
 *
 * <p><b>Passing {@code null} was worse.</b> Until now {@code createComponents} handed plugins a null here,
 * so a plugin that touched it got a {@code NullPointerException} from inside its own code with nothing to
 * say what was missing. A refusal that names the thing and says why is the same answer the REST surface
 * gives for an endpoint the shell has not implemented.
 *
 * <p><b>Three methods still work, because they are honest.</b> {@code resolveDateMathExpression} is string
 * arithmetic on a name and touches no cluster state; {@code isSystemIndexAccessAllowed} reads a thread
 * context header; {@code getExpressionResolvers} returns a list. Refusing those would be theatre.
 *
 * <p><b>Every other public method is overridden, and a test checks that this remains true.</b>
 * {@code ServerlessPluginCollaboratorTests} reflects over core's class and fails if any public instance
 * method is not declared here — otherwise a method added upstream would silently start returning a wrong
 * answer through inheritance, which is the failure mode this class exists to avoid.
 */
public final class RefusingIndexNameExpressionResolver extends IndexNameExpressionResolver {

    /**
     * Creates the resolver.
     *
     * @param threadContext the node thread context, which the superclass needs
     */
    public RefusingIndexNameExpressionResolver(ThreadContext threadContext) {
        super(threadContext);
    }

    private static UnsupportedOperationException refuse(String method) {
        return new UnsupportedOperationException(
            "IndexNameExpressionResolver."
                + method
                + " is not available in the serverless shell: a node's ClusterState holds only the indices "
                + "that node has open, so resolving a name or a pattern against it would answer with a "
                + "subset and call it the answer. Name indices explicitly, or look one up through the Client."
        );
    }

    @Override
    public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
        throw refuse("concreteIndexNames");
    }

    @Override
    public String[] concreteIndexNamesWithSystemIndexAccess(ClusterState state, IndicesRequest request) {
        throw refuse("concreteIndexNamesWithSystemIndexAccess");
    }

    @Override
    public ResolvedIndices.Local.Concrete concreteResolvedIndices(ClusterState state, IndicesRequest request) {
        throw refuse("concreteResolvedIndices");
    }

    @Override
    public Index[] concreteIndices(ClusterState state, IndicesRequest request) {
        throw refuse("concreteIndices");
    }

    @Override
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, String... indexExpressions) {
        throw refuse("concreteIndexNames");
    }

    @Override
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... expressions) {
        throw refuse("concreteIndexNames");
    }

    @Override
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, IndicesRequest request) {
        throw refuse("concreteIndexNames");
    }

    @Override
    public List<String> dataStreamNames(ClusterState state, IndicesOptions options, String... indexExpressions) {
        throw refuse("dataStreamNames");
    }

    @Override
    public ResolvedIndices.Local.Concrete concreteResolvedIndices(ClusterState state, IndicesOptions options, String... expressions) {
        throw refuse("concreteResolvedIndices");
    }

    @Override
    public ResolvedIndices.Local.Concrete concreteResolvedIndices(
        ClusterState state,
        IndicesOptions options,
        boolean includeDataStreams,
        String... indexExpressions
    ) {
        throw refuse("concreteResolvedIndices");
    }

    @Override
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, String... indexExpressions) {
        throw refuse("concreteIndices");
    }

    @Override
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... expressions) {
        throw refuse("concreteIndices");
    }

    @Override
    public ResolvedIndices.Local.Concrete concreteResolvedIndices(ClusterState state, IndicesRequest request, long startTime) {
        throw refuse("concreteResolvedIndices");
    }

    @Override
    public Index[] concreteIndices(ClusterState state, IndicesRequest request, long startTime) {
        throw refuse("concreteIndices");
    }

    @Override
    public Index concreteSingleIndex(ClusterState state, IndicesRequest request) {
        throw refuse("concreteSingleIndex");
    }

    @Override
    public Index concreteWriteIndex(ClusterState state, IndicesRequest request) {
        throw refuse("concreteWriteIndex");
    }

    @Override
    public Index concreteWriteIndex(ClusterState state, IndicesOptions options, String index, boolean allow, boolean includeDataStreams) {
        throw refuse("concreteWriteIndex");
    }

    @Override
    public boolean hasIndexAbstraction(String indexAbstraction, ClusterState state) {
        throw refuse("hasIndexAbstraction");
    }

    @Override
    public Set<String> resolveExpressions(ClusterState state, String... expressions) {
        throw refuse("resolveExpressions");
    }

    @Override
    public String[] filteringAliases(ClusterState state, String index, Set<String> resolvedExpressions) {
        throw refuse("filteringAliases");
    }

    @Override
    public String[] indexAliases(
        ClusterState state,
        String index,
        Predicate<AliasMetadata> requiredAlias,
        boolean skipIdentity,
        Set<String> resolvedExpressions
    ) {
        throw refuse("indexAliases");
    }

    @Override
    public Map<String, Set<String>> resolveSearchRouting(ClusterState state, String routing, String... expressions) {
        throw refuse("resolveSearchRouting");
    }

    @Override
    public Map<String, Set<String>> resolveSearchRoutingAllIndices(Metadata metadata, String routing) {
        throw refuse("resolveSearchRoutingAllIndices");
    }
}
