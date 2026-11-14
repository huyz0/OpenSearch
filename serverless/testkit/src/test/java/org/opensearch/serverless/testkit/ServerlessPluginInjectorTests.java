/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.testkit.ServerlessPluginExtensionPointsTests.EchoRequest;
import org.opensearch.serverless.testkit.ServerlessPluginExtensionPointsTests.EchoResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Plugin objects are built by core's injector: what a plugin binds in its own Guice module, a {@code Provider}
 * parameter, and the lifecycle services a plugin names are all honoured, as on a classic node.
 */
public class ServerlessPluginInjectorTests extends OpenSearchTestCase {

    private static final AtomicReference<String> SERVICE_STATE = new AtomicReference<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SERVICE_STATE.set("none");
    }

    /** Built by the plugin and bound only in its Guice module, as the rule framework's registries are. */
    public static final class Registry {
        final String name;

        Registry(String name) {
            this.name = name;
        }
    }

    public static final class RegistryAction extends ActionType<EchoResponse> {
        static final RegistryAction INSTANCE = new RegistryAction();
        static final String NAME = "cluster:admin/serverless_testkit/registry";

        private RegistryAction() {
            super(NAME, EchoResponse::new);
        }
    }

    /** Takes the module-bound registry and a Provider, as the security plugin's configuration-update action does. */
    public static final class TransportRegistryAction extends HandledTransportAction<EchoRequest, EchoResponse> {
        private final Registry registry;
        private final Provider<ClusterService> clusterService;

        @Inject
        public TransportRegistryAction(
            TransportService transportService,
            ActionFilters actionFilters,
            Registry registry,
            Provider<ClusterService> clusterService,
            // Core's two registries, taken by type as index management's actions take them.
            org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry,
            org.opensearch.core.common.io.stream.NamedWriteableRegistry writeableRegistry
        ) {
            super(RegistryAction.NAME, transportService, actionFilters, EchoRequest::new);
            this.registry = registry;
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, EchoRequest request, ActionListener<EchoResponse> listener) {
            listener.onResponse(new EchoResponse(registry.name + "@" + clusterService.get().getClusterName().value()));
        }
    }

    /** A lifecycle service the plugin names and the node constructs, starts and stops. */
    public static final class Ticker extends AbstractLifecycleComponent {
        private final Registry registry;

        @Inject
        public Ticker(Registry registry) {
            this.registry = registry;
        }

        @Override
        protected void doStart() {
            SERVICE_STATE.set("started:" + registry.name);
        }

        @Override
        protected void doStop() {
            SERVICE_STATE.set("stopped");
        }

        @Override
        protected void doClose() {}
    }

    public static final class GuicePlugin extends Plugin implements ActionPlugin {
        private final Registry registry = new Registry("rules");

        @Override
        public Collection<Module> createGuiceModules() {
            return List.of(b -> b.bind(Registry.class).toInstance(registry));
        }

        @Override
        public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
            return List.of(Ticker.class);
        }

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(RegistryAction.INSTANCE, TransportRegistryAction.class));
        }
    }

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-injector")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(extra)
            .build();
    }

    private MetadataPlane plane() throws java.io.IOException {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), System::currentTimeMillis, 30_000L);
    }

    /** Module bindings, a Provider and a lifecycle service: the action runs and the service starts and stops. */
    public void testModuleBindingsProvidersAndLifecycleServicesWork() throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings("injector", Settings.EMPTY), List.of(new GuicePlugin()));
        try {
            node.startup().start();
            node.setMetadataPlane(plane());
            assertEquals("the plugin's lifecycle service must be started", "started:rules", SERVICE_STATE.get());
            final EchoResponse response = node.services().client().execute(RegistryAction.INSTANCE, new EchoRequest()).actionGet();
            assertEquals("rules@serverless-injector", response.clusterName);
        } finally {
            node.close();
        }
        assertEquals("closing the node must stop the plugin's lifecycle service", "stopped", SERVICE_STATE.get());
    }

    /** With the switch off, actions are resolved by constructor only, and a module-bound type cannot be found. */
    public void testTheSwitchFallsBackToConstructorResolution() throws Exception {
        try (
            ServerlessNode node = new ServerlessNode(
                nodeSettings("injector-off", Settings.builder().put("serverless.plugin_injector.enabled", false).build()),
                List.of(new GuicePlugin())
            )
        ) {
            final IllegalStateException failure = expectThrows(IllegalStateException.class, () -> node.startup().start());
            assertTrue("the missing type must be named: " + failure.getMessage(), failure.getMessage().contains("Registry"));
            assertEquals("no lifecycle service is started without the injector", "none", SERVICE_STATE.get());
        }
    }
}
