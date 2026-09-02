/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHeaderDefinition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * The plugins a node is running, without an injector.
 *
 * <p><b>R4, answered.</b> The risk register says the shell "implements the plugin contract without an
 * {@code Injector}", and until now nothing implemented it at all: {@code PluginsService} was constructed
 * with an empty list because {@code IndicesService} demands one, and no plugin was ever loaded. This is
 * the part that makes a plugin run.
 *
 * <p><b>Guice was never the hard part.</b> {@code createComponents} is a plain method returning a
 * collection of objects; classic OpenSearch binds those into an injector so that other components can ask
 * for them by type. The shell keeps them in a list and hands them back to whoever wants them. What a
 * plugin actually needs from the container — a {@code Client}, a thread pool, an environment — is passed
 * as arguments, and passing arguments is not a framework.
 *
 * <p><b>Three hooks, and each earns its place.</b> {@code createComponents} lets a plugin build its
 * state and reach the {@link ServerlessClient}; {@code getRestHandlers} lets it serve its own endpoints;
 * {@code getRestHandlerWrapper} lets it see every request before the handler does, which is where
 * authentication belongs and is exactly what the security plugin uses it for.
 *
 * <p><b>What is deliberately absent</b> is the action layer: no {@code TransportAction} registration, no
 * action-name routing, no {@code action/} package. That is §6.3's boundary, and honouring it is what keeps
 * this a plugin host rather than a second copy of the node it replaced.
 *
 * <p><b>{@code ActionFilter}s are the exception, and not a retreat from that.</b> A filter needs two things
 * — an action name and a request — and both can be produced where the shell does its work, without any of
 * the machinery above. {@link ActionGate} does exactly that, so a plugin's privilege evaluation runs
 * unmodified while the action layer stays unbuilt.
 */
public final class ServerlessPlugins {

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(ServerlessPlugins.class);

    private final List<Plugin> plugins;
    private final List<Object> components = new ArrayList<>();

    /**
     * Holds the plugins a node was given.
     *
     * @param plugins the loaded plugins, in load order
     */
    public ServerlessPlugins(List<Plugin> plugins) {
        this.plugins = List.copyOf(plugins);
    }

    /**
     * Returns the loaded plugins.
     *
     * @return the plugins
     */
    public List<Plugin> plugins() {
        return plugins;
    }

    /**
     * Returns whatever the plugins built, in the order they built it.
     *
     * <p>A list rather than a type-keyed registry, because nothing in the shell looks components up —
     * a plugin keeps its own references. This exists so that they are reachable at all, and so that a
     * plugin's component with a lifecycle can be closed.
     *
     * @return the components
     */
    public List<Object> components() {
        return List.copyOf(components);
    }

    /**
     * Lets each plugin build its state.
     *
     * <p>Called once, and before anything can reach the plugins. A plugin that throws here stops the node
     * rather than being skipped: a security plugin that failed to load its configuration and was quietly
     * ignored would leave a node serving without the thing it was installed to enforce.
     *
     * <p><b>The collaborators are real now.</b> Four of these used to be {@code null}, which meant a plugin
     * that touched one got a {@code NullPointerException} from inside its own code with nothing to say what
     * was missing. Three are genuine — a running {@code ResourceWatcherService}, a {@code ScriptService}
     * with no engines, and the node's {@code NamedWriteableRegistry}. The fourth cannot be genuine and is
     * therefore explicit: see {@link RefusingIndexNameExpressionResolver}. Only the
     * {@code RepositoriesService} supplier still yields null: that class is the classic repository-plugin
     * API surface — registering a repository <em>type</em>, hooking snapshot lifecycle events — and this
     * shell's own snapshot/restore surface (M45) does not use it, the same way {@code IndexShard} needs no
     * {@code ClusterService} to run. A plugin that implements a custom repository type still finds nothing
     * here; a caller taking or restoring a snapshot does not go through this at all.
     *
     * @param node the node the plugins are running in
     * @param environment the node environment
     * @throws Exception if any plugin fails to start
     */
    public void createComponents(ServerlessNode node, org.opensearch.env.Environment environment) throws Exception {
        for (Plugin plugin : plugins) {
            components.addAll(
                plugin.createComponents(
                    node.client(),
                    node.clusterService(),
                    node.threadPool(),
                    node.resourceWatcherService(),
                    node.scriptService(),
                    node.searchXContentRegistry(),
                    environment,
                    node.nodeEnvironment(),
                    node.namedWriteableRegistry(),
                    node.indexNameExpressionResolver(),
                    () -> null                              // RepositoriesService: snapshots are out of scope (R7)
                )
            );
            logger.info("started plugin {}", plugin.getClass().getName());
        }
    }

    /**
     * Collects the REST handlers the plugins want to serve.
     *
     * <p><b>This widens D2's allowlist, and does so knowingly.</b> The allowlist exists so that a classic
     * endpoint the shell has not implemented answers 501 rather than pretending; it is not a rule that the
     * surface may never grow. A plugin an operator chose to install adding its own routes is that operator
     * deciding, which is a different thing from the shell quietly accepting a request it cannot honour.
     *
     * @param settings the node settings
     * @param controller the REST controller
     * @param clusterSettings the cluster settings
     * @return the handlers to register
     */
    public List<RestHandler> restHandlers(Settings settings, RestController controller, ClusterSettings clusterSettings) {
        final List<RestHandler> handlers = new ArrayList<>();
        for (Plugin plugin : plugins) {
            if (plugin instanceof ActionPlugin actionPlugin) {
                handlers.addAll(
                    actionPlugin.getRestHandlers(
                        settings,
                        controller,
                        clusterSettings,
                        IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                        new SettingsFilter(List.of()),
                        null,
                        () -> null
                    )
                );
            }
        }
        return handlers;
    }

    /**
     * Composes the plugins' request wrappers into one.
     *
     * <p>This is the seam authentication lives on: a wrapper sees every request before its handler does,
     * can reject it, and can put an identity in the {@link ThreadContext} for the handler to find. It is
     * the same hook the OpenSearch security plugin uses, which is why it is worth having even though the
     * shell cannot host that plugin's authorization.
     *
     * <p>Refuses more than one, rather than picking an order. Two wrappers both claiming to authenticate
     * is a configuration mistake whose consequences depend on which ran first, and answering it with a
     * silent choice is how a system ends up enforcing the weaker of two policies.
     *
     * @param threadContext the node's thread context
     * @param headersToCopy headers the controller preserves
     * @return the wrapper, or null if no plugin offers one
     */
    public UnaryOperator<RestHandler> restHandlerWrapper(ThreadContext threadContext, Set<RestHeaderDefinition> headersToCopy) {
        UnaryOperator<RestHandler> found = null;
        Plugin owner = null;
        for (Plugin plugin : plugins) {
            if (plugin instanceof ActionPlugin actionPlugin) {
                final UnaryOperator<RestHandler> wrapper = actionPlugin.getRestHandlerWrapper(threadContext, headersToCopy);
                if (wrapper == null) {
                    continue;
                }
                if (found != null) {
                    throw new IllegalStateException(
                        "two plugins want to wrap every request -- "
                            + owner.getClass().getName()
                            + " and "
                            + plugin.getClass().getName()
                            + "; which one authenticates would depend on load order"
                    );
                }
                found = wrapper;
                owner = plugin;
            }
        }
        return found;
    }

    /**
     * Builds the identity service from whichever plugin provides one.
     *
     * <p>Core's default is {@code NoopIdentityPlugin}, whose subject reports {@code UNAUTHENTICATED} and
     * fails no checks — which is why plain OpenSearch is open without a security plugin, and why the shell
     * is too. Naming it here makes that a visible default rather than an absence.
     *
     * @param settings the node settings
     * @param threadPool the node thread pool
     * @return the identity service
     */
    public org.opensearch.identity.IdentityService identityService(Settings settings, org.opensearch.threadpool.ThreadPool threadPool) {
        final List<IdentityPlugin> identityPlugins = new ArrayList<>();
        for (Plugin plugin : plugins) {
            if (plugin instanceof IdentityPlugin identityPlugin) {
                identityPlugins.add(identityPlugin);
            }
        }
        return new org.opensearch.identity.IdentityService(settings, threadPool, identityPlugins);
    }

    /**
     * Returns every loaded plugin of a given type.
     *
     * <p><b>This rather than {@code PluginsService#filterPlugins}, and the difference is not cosmetic.</b>
     * Core's loader knows the plugins it loaded from disk and cannot be told about one handed to the node's
     * constructor. Filtering through it meant a plugin's extension points were honoured or ignored
     * depending on how the plugin arrived — an installed {@code AnalysisPlugin} contributed its analyzers
     * and the same class passed to a test did not. One list, one set of answers.
     *
     * @param type the plugin interface
     * @param <T> the plugin interface
     * @return the plugins implementing it, in load order
     */
    public <T> List<T> filter(Class<T> type) {
        final List<T> matching = new ArrayList<>();
        for (Plugin plugin : plugins) {
            if (type.isInstance(plugin)) {
                matching.add(type.cast(plugin));
            }
        }
        return matching;
    }

    /**
     * Layers every plugin's {@code additionalSettings} under the settings an operator supplied.
     *
     * <p>Core's {@code PluginsService#updatedSettings} does this for the plugins it loaded; this does it for
     * all of them, by the same rules. Two plugins contributing the same key is an error rather than a
     * silent winner — which key wins would otherwise depend on load order, and load order is not something
     * an operator chose.
     *
     * <p>The supplied settings go on top, so a plugin can offer a default and never override a decision.
     *
     * @param supplied the settings the node was constructed with
     * @return the merged settings
     */
    public Settings settingsWith(Settings supplied) {
        final java.util.Map<String, String> contributedBy = new java.util.HashMap<>();
        final Settings.Builder merged = Settings.builder();
        for (Plugin plugin : plugins) {
            final Settings additional = plugin.additionalSettings();
            for (String key : additional.keySet()) {
                final String earlier = contributedBy.put(key, plugin.getClass().getName());
                if (earlier != null) {
                    throw new IllegalArgumentException(
                        "two plugins both contribute the setting ["
                            + key
                            + "] -- "
                            + earlier
                            + " and "
                            + plugin.getClass().getName()
                            + "; which one applies would depend on load order"
                    );
                }
            }
            merged.put(additional);
        }
        return merged.put(supplied).build();
    }

    /**
     * Tells the plugins the node is serving.
     *
     * <p><b>Not the same event as {@code createComponents}, and plugins rely on the difference.</b> A
     * plugin builds its state when components are created and cannot use the node yet; {@code onNodeStarted}
     * is where it does the work that needs a running node — the OpenSearch security plugin initialises its
     * configuration repository here, which is to say it reads its own index. Never calling it leaves such a
     * plugin loaded, constructed, and never actually started.
     *
     * <p>A failure is logged rather than fatal, unlike {@code createComponents}. By this point the node is
     * serving: taking it down would turn a plugin's late-initialisation problem into an outage, and the
     * plugin had its chance to refuse loudly earlier.
     *
     * @param localNode the node's identity, now that the transport has bound
     */
    public void onNodeStarted(org.opensearch.cluster.node.DiscoveryNode localNode) {
        for (Plugin plugin : plugins) {
            if (plugin instanceof org.opensearch.plugins.ClusterPlugin clusterPlugin) {
                try {
                    clusterPlugin.onNodeStarted(localNode);
                } catch (Exception e) {
                    logger.error("plugin " + plugin.getClass().getName() + " failed on node start", e);
                }
            }
        }
    }

    /**
     * Collects the plugins' action filters, which is where authorization lives.
     *
     * @return the filters, in the order the plugins were loaded
     */
    public List<org.opensearch.action.support.ActionFilter> actionFilters() {
        final List<org.opensearch.action.support.ActionFilter> filters = new ArrayList<>();
        for (Plugin plugin : plugins) {
            if (plugin instanceof ActionPlugin actionPlugin) {
                filters.addAll(actionPlugin.getActionFilters());
            }
        }
        return filters;
    }

    /**
     * Collects the indices the plugins declare as their own.
     *
     * <p>Read from {@code SystemIndexPlugin#getSystemIndexDescriptors}, which is how every OpenSearch
     * plugin already says this, so a plugin does not have to know it is running on the shell. What the
     * shell does with the answer is refuse to route to them at all — see
     * {@link org.opensearch.serverless.rest.SystemIndices}.
     *
     * @param settings the node settings, which core's hook takes
     * @return the declared index patterns
     */
    public List<String> systemIndexPatterns(Settings settings) {
        final List<String> patterns = new ArrayList<>();
        for (Plugin plugin : plugins) {
            if (plugin instanceof org.opensearch.plugins.SystemIndexPlugin systemIndexPlugin) {
                for (org.opensearch.indices.SystemIndexDescriptor descriptor : systemIndexPlugin.getSystemIndexDescriptors(settings)) {
                    patterns.add(descriptor.getIndexPattern());
                }
            }
        }
        return patterns;
    }

    /**
     * Closes every plugin, reporting failures rather than letting one stop the rest.
     *
     * @param plugins the plugins to close
     */
    public static void closeAll(Collection<Plugin> plugins) {
        for (Plugin plugin : plugins) {
            try {
                plugin.close();
            } catch (Exception e) {
                logger.warn("plugin " + plugin.getClass().getName() + " failed to close", e);
            }
        }
    }
}
