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
 * which lives in the metadata plane and is deliberately not enumerable (the shell refuses
 * {@code /_serverless/indices} for the same reason: walking a hundred million indices through a request
 * path is not an operation this system offers).
 *
 * <p>So a real resolver here would answer a wildcard with a subset and a concrete name this node has not
 * opened with {@code IndexNotFoundException}. Both are wrong answers that look like right ones, which is
 * exactly what the allowlist exists to prevent — and for a plugin evaluating privileges over an index pattern, a
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
        this(threadContext, name -> false);
    }

    private final java.util.function.Predicate<String> inView;

    /**
     * Creates the resolver, answering for the names a node's view holds in full.
     *
     * <p>The plugins' own indices and their aliases are in every node's view (see {@code PluginIndicesView}), so a name or
     * pattern that only a plugin's indices can match resolves against the view as core resolves it, and is the whole answer:
     * the anomaly-detection plugin resolves its result alias this way before writing a result. Any other name is refused as
     * before.
     *
     * @param threadContext the node thread context, which the superclass needs
     * @param inView whether a name or pattern can match only indices the view holds in full
     */
    public RefusingIndexNameExpressionResolver(ThreadContext threadContext, java.util.function.Predicate<String> inView) {
        this(threadContext, inView, name -> null);
    }

    private final java.util.function.Function<String, List<Index>> fromPlane;

    /**
     * Creates the resolver, also answering for names the metadata plane can resolve one at a time.
     *
     * <p>A plugin names tenant indices too: an anomaly detector checks its source index for blocks before every run. A
     * concrete name, an alias or a trailing wildcard is looked up in the metadata plane -- one descriptor read per name, a
     * capped prefix listing per wildcard -- so the answer is the deployment's, not the subset this node has open. Anything
     * the plane cannot answer that way ({@code _all}, exclusions, inner wildcards, data streams) is still refused.
     *
     * @param threadContext the node thread context, which the superclass needs
     * @param inView whether a name or pattern can match only indices the view holds in full
     * @param fromPlane the indices a name or trailing wildcard names in the metadata plane, empty if none, or null when
     *                  it cannot be answered there
     */
    public RefusingIndexNameExpressionResolver(
        ThreadContext threadContext,
        java.util.function.Predicate<String> inView,
        java.util.function.Function<String, List<Index>> fromPlane
    ) {
        super(threadContext);
        this.inView = inView;
        this.fromPlane = fromPlane;
    }

    /** The indices the expressions name, from the view or the metadata plane, or null when some cannot be answered. */
    private Index[] resolved(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... expressions) {
        if (expressions == null || expressions.length == 0) {
            return null;
        }
        final java.util.LinkedHashSet<Index> found = new java.util.LinkedHashSet<>();
        for (String expression : expressions) {
            if (expression == null || expression.isEmpty() || "_all".equals(expression) || expression.startsWith("-")) {
                return null;
            }
            if (inView.test(expression)) {
                found.addAll(List.of(super.concreteIndices(state, options, includeDataStreams, expression)));
                continue;
            }
            final String name = resolveDateMathExpression(expression);
            final boolean wildcard = name.endsWith("*");
            if (name.indexOf('*') != (wildcard ? name.length() - 1 : -1)) {
                return null;
            }
            final List<Index> named = fromPlane.apply(name);
            if (named == null) {
                return null;
            }
            if (named.isEmpty() && wildcard == false && options.ignoreUnavailable() == false) {
                throw new org.opensearch.index.IndexNotFoundException(name);
            }
            found.addAll(named);
        }
        if (found.isEmpty() && options.allowNoIndices() == false) {
            throw new org.opensearch.index.IndexNotFoundException(String.join(",", expressions));
        }
        return found.toArray(Index[]::new);
    }

    private static String[] names(Index[] indices) {
        return java.util.Arrays.stream(indices).map(Index::getName).toArray(String[]::new);
    }

    private boolean allInView(String... expressions) {
        if (expressions == null || expressions.length == 0) {
            return false;
        }
        for (String expression : expressions) {
            if (expression == null || expression.isEmpty() || "_all".equals(expression) || inView.test(expression) == false) {
                return false;
            }
        }
        return true;
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
        if (allInView(request.indices())) {
            return super.concreteIndexNames(state, request);
        }
        final Index[] found = resolved(state, request.indicesOptions(), request.includeDataStreams(), request.indices());
        if (found != null) {
            return names(found);
        }
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
        if (allInView(request.indices())) {
            return super.concreteIndices(state, request);
        }
        final Index[] found = resolved(state, request.indicesOptions(), request.includeDataStreams(), request.indices());
        if (found != null) {
            return found;
        }
        throw refuse("concreteIndices");
    }

    @Override
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, String... indexExpressions) {
        if (allInView(indexExpressions)) {
            return super.concreteIndexNames(state, options, indexExpressions);
        }
        final Index[] found = resolved(state, options, false, indexExpressions);
        if (found != null) {
            return names(found);
        }
        throw refuse("concreteIndexNames");
    }

    @Override
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... expressions) {
        if (allInView(expressions)) {
            return super.concreteIndexNames(state, options, includeDataStreams, expressions);
        }
        final Index[] found = resolved(state, options, includeDataStreams, expressions);
        if (found != null) {
            return names(found);
        }
        throw refuse("concreteIndexNames");
    }

    @Override
    public String[] concreteIndexNames(ClusterState state, IndicesOptions options, IndicesRequest request) {
        if (allInView(request.indices())) {
            return super.concreteIndexNames(state, options, request);
        }
        final Index[] found = resolved(state, options, request.includeDataStreams(), request.indices());
        if (found != null) {
            return names(found);
        }
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
        if (allInView(indexExpressions)) {
            return super.concreteIndices(state, options, indexExpressions);
        }
        final Index[] found = resolved(state, options, false, indexExpressions);
        if (found != null) {
            return found;
        }
        throw refuse("concreteIndices");
    }

    @Override
    public Index[] concreteIndices(ClusterState state, IndicesOptions options, boolean includeDataStreams, String... expressions) {
        if (allInView(expressions)) {
            return super.concreteIndices(state, options, includeDataStreams, expressions);
        }
        final Index[] found = resolved(state, options, includeDataStreams, expressions);
        if (found != null) {
            return found;
        }
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
        if (allInView(indexAbstraction)) {
            return super.hasIndexAbstraction(indexAbstraction, state);
        }
        if (indexAbstraction != null && indexAbstraction.contains("*") == false) {
            final List<Index> named = fromPlane.apply(resolveDateMathExpression(indexAbstraction));
            if (named != null) {
                return named.isEmpty() == false;
            }
        }
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
