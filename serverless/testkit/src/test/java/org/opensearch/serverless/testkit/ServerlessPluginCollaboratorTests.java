/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.serverless.shell.RefusingIndexNameExpressionResolver;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * What a plugin is actually handed, rather than what the signature promises.
 *
 * <p>{@code createComponents} takes eleven collaborators and the shell passed four of them as
 * {@code null}. A plugin that touched one got a {@code NullPointerException} raised inside its own code,
 * with nothing in it to say that the host had declined to provide something — the worst of both worlds,
 * since it is neither a working collaborator nor a refusal that explains itself.
 *
 * <p>Three are now real. The fourth cannot be, and is explicit about it: see
 * {@link RefusingIndexNameExpressionResolver} for why a resolver that answered would answer wrongly.
 *
 * <p><b>D5:</b> no object store is involved.
 */
public class ServerlessPluginCollaboratorTests extends OpenSearchTestCase {

    /** Captures every collaborator the host handed over, so the test can look at them. */
    public static final class CapturingPlugin extends Plugin {

        static final AtomicReference<Object[]> CAPTURED = new AtomicReference<>();

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            CAPTURED.set(
                new Object[] {
                    client,
                    clusterService,
                    threadPool,
                    resourceWatcherService,
                    scriptService,
                    xContentRegistry,
                    environment,
                    nodeEnvironment,
                    namedWriteableRegistry,
                    indexNameExpressionResolver,
                    repositoriesServiceSupplier }
            );
            return List.of(new Object());
        }
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-collaborators")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** Ten of the eleven are non-null, and the eleventh is a supplier that can at least be called. */
    public void testAPluginIsHandedRealCollaborators() throws Exception {
        CapturingPlugin.CAPTURED.set(null);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("collaborators"), List.of(new CapturingPlugin()))) {
            node.start();
            final Object[] got = CapturingPlugin.CAPTURED.get();
            assertNotNull("createComponents must have run", got);

            final String[] names = {
                "client",
                "clusterService",
                "threadPool",
                "resourceWatcherService",
                "scriptService",
                "xContentRegistry",
                "environment",
                "nodeEnvironment",
                "namedWriteableRegistry",
                "indexNameExpressionResolver",
                "repositoriesServiceSupplier" };
            for (int i = 0; i < names.length; i++) {
                assertNotNull("a plugin must not be handed a null [" + names[i] + "]", got[i]);
            }

            // The registry must be the one that can actually read what this node writes. An empty registry
            // is non-null and useless, which is the failure this assertion exists to catch.
            final NamedWriteableRegistry registry = (NamedWriteableRegistry) got[8];
            assertNotNull(
                "the registry a plugin gets must know the node's own query types",
                registry.getReader(org.opensearch.index.query.QueryBuilder.class, "match_all")
            );

            // The resolver is the shell's, not core's, and says so when used.
            assertTrue("the resolver must be the refusing one", got[9] instanceof RefusingIndexNameExpressionResolver);
        }
    }

    /**
     * A script service with no engines gives core's own error, not a null pointer.
     *
     * <p>Scripting is not offered by this shell. The useful way to say that to a plugin is the way a classic
     * node says it when the language's module is not installed, in the same words, from the same class.
     */
    public void testAPluginIsHandedAWorkingScriptService() throws Exception {
        CapturingPlugin.CAPTURED.set(null);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("collaborators-script"), List.of(new CapturingPlugin()))) {
            node.start();
            final ScriptService scripts = (ScriptService) CapturingPlugin.CAPTURED.get()[4];
            // Since M57 the service a plugin is handed has a real engine in it: modules:lang-painless is a
            // dependency and its engine is registered directly, the same way the transport is chosen. A
            // plugin that compiles a script gets a compiled script rather than an explanation.
            assertNotNull(
                "a plugin must be handed a script service that can actually compile",
                scripts.compile(
                    new org.opensearch.script.Script(ScriptType.INLINE, "painless", "1 + 1", Map.of()),
                    org.opensearch.script.FieldScript.CONTEXT
                )
            );

            // A language nothing registers is still refused by name rather than with a null pointer, which
            // is what this test was originally written to pin.
            final var failure = expectThrows(
                IllegalArgumentException.class,
                () -> scripts.compile(
                    new org.opensearch.script.Script(ScriptType.INLINE, "expression", "1 + 1", Map.of()),
                    org.opensearch.script.FieldScript.CONTEXT
                )
            );
            assertTrue("the refusal must name the missing language: " + failure.getMessage(), failure.getMessage().contains("expression"));
        }
    }

    /** Using the resolver refuses, and the refusal says what to do instead. */
    public void testResolvingAnIndexNameRefusesWithAReason() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("collaborators-resolve"))) {
            node.start();
            final IndexNameExpressionResolver resolver = node.indexNameExpressionResolver();
            final var failure = expectThrows(
                UnsupportedOperationException.class,
                () -> resolver.concreteIndexNames(
                    node.clusterService().state(),
                    org.opensearch.action.support.IndicesOptions.strictExpand(),
                    "logs-*"
                )
            );
            assertTrue(
                "the refusal must explain why, not merely refuse: " + failure.getMessage(),
                failure.getMessage().contains("only the indices that node has open")
            );

            // Date maths touches no cluster state, so refusing it would be theatre.
            assertEquals("plain-name", resolver.resolveDateMathExpression("plain-name"));
        }
    }

    /**
     * Every method of core's resolver that could answer wrongly is overridden here.
     *
     * <p>This is the test that keeps {@link RefusingIndexNameExpressionResolver} honest over time. Core
     * gains methods; a method it gains that this class does not override would be inherited, and the
     * inherited one resolves against a node-local {@code ClusterState} and returns a subset of the
     * deployment's indices while looking exactly like a correct answer. That is the precise failure the
     * class exists to prevent, so an upstream addition must break the build rather than quietly widen the
     * surface.
     */
    public void testEveryResolverMethodThatTouchesClusterStateIsOverridden() {
        // The three deliberate exceptions, each of which touches no cluster state.
        final List<String> honest = List.of("resolveDateMathExpression", "isSystemIndexAccessAllowed", "getExpressionResolvers");

        // What the shell's resolver actually declares, as opposed to what it inherits. Read from
        // getMethods() filtered by declaring class rather than from getDeclaredMethods(), which the
        // forbidden-apis policy refuses: the two agree exactly here, because an override cannot reduce
        // visibility, so every override of a public method is itself public and appears in getMethods().
        //
        // The distinction this test is built on survives the rewrite, and it is the only thing that
        // matters. A method merely *inherited* from core is declared by core, so it is absent from this
        // set and reported as missing -- which is the whole point. Asking getMethod() instead would have
        // found the inherited method and quietly reported nothing wrong.
        final Set<String> overridden = new HashSet<>();
        for (Method declared : RefusingIndexNameExpressionResolver.class.getMethods()) {
            if (declared.getDeclaringClass() == RefusingIndexNameExpressionResolver.class) {
                overridden.add(signatureOf(declared));
            }
        }

        final List<String> missing = new ArrayList<>();
        for (Method method : IndexNameExpressionResolver.class.getMethods()) {
            // getMethods() reaches inherited methods too -- Object's among them -- and core's own
            // declarations are the only ones this test speaks for.
            if (method.getDeclaringClass() != IndexNameExpressionResolver.class || Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            if (honest.contains(method.getName())) {
                continue;
            }
            if (overridden.contains(signatureOf(method)) == false) {
                missing.add(method.toString());
            }
        }
        assertTrue(
            "core's resolver has public methods the shell does not override, which would inherit an answer "
                + "computed over one node's open indices and present it as the deployment's: "
                + missing,
            missing.isEmpty()
        );
    }

    /** A method's name and parameter types, which is what "the same method" means for an override. */
    private static String signatureOf(Method method) {
        final StringBuilder signature = new StringBuilder(method.getName());
        for (Class<?> parameter : method.getParameterTypes()) {
            signature.append('|').append(parameter.getName());
        }
        return signature.toString();
    }
}
