/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.inject.Binding;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.Key;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.inject.spi.Element;
import org.opensearch.common.inject.spi.Elements;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Core's injector, built for what plugins bring and nothing else.
 *
 * <p><b>Why the shell needs one after all.</b> Constructor resolution ({@link PluginActions}) covers an action
 * whose every parameter is a node service or one of its plugin's components. Real plugins go further, in the
 * three ways a classic node's injector allows:
 *
 * <ul>
 *   <li>a plugin's {@code createGuiceModules()} binds objects that exist nowhere else: the rule framework's
 *       persistence and routing registries are bound only there, and its actions take them;</li>
 *   <li>an action takes a {@code Provider<T>} rather than a {@code T}: the security plugin's
 *       configuration-update action does, so the configuration repository can be built after it;</li>
 *   <li>a plugin names {@code getGuiceServiceClasses()}: lifecycle services the node constructs, starts and
 *       stops for it.</li>
 * </ul>
 *
 * <p>Each of these is Guice behaviour, and reimplementing it by reflection would be a second, subtly different
 * container. So this builds the real one, with core's {@link ModulesBuilder}, from the same inputs a classic
 * node uses: the plugins' modules, the node services, every plugin component bound by its own class, and every
 * plugin action bound as an eager singleton -- the binding core's {@code ActionModule} makes, which is what
 * keeps an action injected into another from being built twice and registering its transport handler twice.
 *
 * <p><b>What it is not.</b> It is not the node's container. The shell's own objects are wired by hand and
 * never asked for from here; only plugin objects are built by it, after the plugins have created their
 * components.
 *
 * <p><b>How services are bound.</b> Core binds each service by a hand-listed type. The shell's services are
 * often subclasses of what plugins ask for (its client is an anonymous {@code NodeClient}), so each service is
 * bound by its class and every supertype, the first service to claim a type keeping it -- the same
 * most-specific-first rule constructor resolution uses. Types from the JDK and marker interfaces every
 * service shares are never bound by supertype: binding {@code Closeable} to whichever service came first would
 * be a guess. A type a plugin module binds itself is left to that module, so a plugin's own binding is never
 * a duplicate-binding failure caused by the shell.
 */
final class PluginInjector {

    /**
     * Whether plugin objects are built by core's injector: on by default.
     *
     * <p>Off falls back to constructor resolution only, which cannot run a plugin that binds Guice modules,
     * takes a {@code Provider}, or names lifecycle services. With no plugin installed the two are identical.
     */
    static final String ENABLED = "serverless.plugin_injector.enabled";

    private final Injector injector;
    private final Set<Class<?>> bound;
    private final List<LifecycleComponent> services;

    private PluginInjector(Injector injector, Set<Class<?>> bound, List<LifecycleComponent> services) {
        this.injector = injector;
        this.bound = bound;
        this.services = services;
    }

    static boolean enabled(Settings settings) {
        return settings.getAsBoolean(ENABLED, true);
    }

    /**
     * Builds the injector.
     *
     * @param plugins every installed plugin
     * @param services the node services, most specific first
     * @param components every plugin component
     * @return the injector, with the plugins' lifecycle services constructed but not started
     * @throws IllegalStateException naming every binding that cannot be satisfied
     */
    static PluginInjector build(List<Plugin> plugins, List<Object> services, Collection<Object> components) {
        final List<Module> pluginModules = new ArrayList<>();
        final List<Class<? extends LifecycleComponent>> serviceClasses = new ArrayList<>();
        final Set<Class<?>> actions = new LinkedHashSet<>();
        for (Plugin plugin : plugins) {
            pluginModules.addAll(plugin.createGuiceModules());
            serviceClasses.addAll(plugin.getGuiceServiceClasses());
            if (plugin instanceof ActionPlugin actionPlugin) {
                for (ActionPlugin.ActionHandler<?, ?> handler : actionPlugin.getActions()) {
                    if (constructible(handler.getTransportAction())) {
                        actions.add(handler.getTransportAction());
                    }
                    for (Class<?> support : handler.getSupportTransportActions()) {
                        if (constructible(support)) {
                            actions.add(support);
                        }
                    }
                }
            }
        }
        final Set<Key<?>> claimed = new HashSet<>();
        for (Element element : Elements.getElements(pluginModules)) {
            if (element instanceof Binding<?> binding) {
                claimed.add(binding.getKey());
            }
        }

        final Set<Class<?>> bound = new LinkedHashSet<>();
        final ModulesBuilder modules = new ModulesBuilder();
        pluginModules.forEach(modules::add);
        modules.add(b -> {
            // Components by their own class, as core does: a plugin asking for another plugin's component
            // gets it only by naming that exact class, never because some object happened to implement an
            // interface it asked for.
            for (Object component : components) {
                if (component != null && claim(component.getClass(), claimed, bound)) {
                    bindInstance(b, component.getClass(), component);
                }
            }
            for (Object service : services) {
                for (Class<?> type : supertypes(service.getClass())) {
                    if (claim(type, claimed, bound)) {
                        bindInstance(b, type, service);
                    }
                }
            }
            for (Class<?> action : actions) {
                if (claim(action, claimed, bound)) {
                    b.bind(action).asEagerSingleton();
                }
            }
            for (Class<? extends LifecycleComponent> service : serviceClasses) {
                if (claim(service, claimed, bound)) {
                    b.bind(service).asEagerSingleton();
                }
            }
        });
        final Injector injector;
        try {
            injector = modules.createInjector();
        } catch (RuntimeException e) {
            // Guice's own message lists every unsatisfied dependency with the class that needed it; keep it
            // whole, so a plugin with several gaps is diagnosed in one start.
            throw new IllegalStateException("the plugins' objects cannot be built: " + e.getMessage(), e);
        }
        bound.addAll(claimed.stream().<Class<?>>map(k -> k.getTypeLiteral().getRawType()).toList());
        final List<LifecycleComponent> lifecycle = new ArrayList<>();
        for (Class<? extends LifecycleComponent> service : serviceClasses) {
            lifecycle.add(injector.getInstance(service));
        }
        return new PluginInjector(injector, Set.copyOf(bound), List.copyOf(lifecycle));
    }

    /** Whether the injector built this class, so asking for it does not construct a second one. */
    boolean has(Class<?> type) {
        return bound.contains(type);
    }

    <T> T instance(Class<T> type) {
        return injector.getInstance(type);
    }

    /** The plugins' lifecycle services, in the order they were declared. */
    List<LifecycleComponent> services() {
        return services;
    }

    /** Whether Guice can construct the class: an {@code @Inject} constructor, or a public no-argument one. */
    static boolean constructible(Class<?> type) {
        if (type.isInterface() || Modifier.isAbstract(type.getModifiers())) {
            return false;
        }
        for (Constructor<?> constructor : type.getConstructors()) {
            // Only core's annotation: its injector does not read javax or jakarta ones, which constructor
            // resolution still honours.
            if (constructor.isAnnotationPresent(org.opensearch.common.inject.Inject.class)) {
                return true;
            }
        }
        for (Constructor<?> constructor : type.getConstructors()) {
            if (constructor.getParameterCount() == 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean claim(Class<?> type, Set<Key<?>> claimed, Set<Class<?>> bound) {
        return claimed.contains(Key.get(type)) == false && bound.add(type);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void bindInstance(org.opensearch.common.inject.Binder binder, Class type, Object instance) {
        binder.bind(type).toInstance(instance);
    }

    /** The class and its supertypes that a plugin might ask for by type. */
    static List<Class<?>> supertypes(Class<?> type) {
        final Set<Class<?>> seen = new LinkedHashSet<>();
        final List<Class<?>> queue = new ArrayList<>();
        queue.add(type);
        for (int i = 0; i < queue.size(); i++) {
            final Class<?> next = queue.get(i);
            if (next == null || bindable(next) == false || seen.add(next) == false) {
                continue;
            }
            queue.add(next.getSuperclass());
            queue.addAll(List.of(next.getInterfaces()));
        }
        // The class itself is bound even when anonymous: nothing can name it, but nothing is harmed either.
        return List.copyOf(seen);
    }

    private static boolean bindable(Class<?> type) {
        final String name = type.getName();
        return name.startsWith("java.") == false
            && name.startsWith("javax.") == false
            && name.startsWith("jdk.") == false
            && name.startsWith("org.opensearch.common.lifecycle.") == false
            && name.startsWith("org.opensearch.core.xcontent.") == false
            // The stream package's marker interfaces only: its registry is a service plugins take by type.
            && (type.isInterface() == false || name.startsWith("org.opensearch.core.common.io.stream.") == false)
            && name.equals("org.opensearch.common.lease.Releasable") == false;
    }
}
