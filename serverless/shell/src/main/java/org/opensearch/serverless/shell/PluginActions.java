/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.TransportAction;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A plugin's own actions, built without Guice.
 *
 * <p><b>The gap this closes.</b> A plugin that ships a {@code TransportAction} — the OpenSearch security
 * plugin's configuration-update API is the case that matters — could not run one here. The shell's client
 * implements an allowlist of core's actions and refused everything else by name, so a plugin's REST handler
 * calling {@code client.execute(itsOwnAction, request)} was told the shell does not implement it. That is
 * not a small corner: an action is how a plugin does anything that is not a document operation.
 *
 * <p><b>Why this is not an action layer.</b> The shell declines to build one, and this does not build one
 * either. There is no registry of core's actions, no dispatch table for the shell's own work, no wrapping
 * of index or search. This holds only what plugins brought, and the shell's client asks it after its own
 * allowlist has not matched.
 *
 * <p><b>Constructor resolution, not injection.</b> Core binds these with Guice; plugins run here
 * without it. So each action is built by picking the constructor whose parameters can all be supplied from
 * what this node has — its services, and whatever <em>that</em> plugin itself returned from
 * {@code createComponents}. Never another plugin's components: the aggregate list was once offered to every
 * action, and a constructor parameter typed to some other plugin's private class would have been handed
 * that plugin's object by reflection.
 * That is a small, legible rule with two properties worth stating:
 *
 * <ul>
 *   <li><b>It refuses rather than guesses.</b> A parameter nothing can supply fails the node's start with
 *       the action and the parameter named. A plugin whose action is silently absent would be discovered
 *       by a caller getting an unexplained error much later.</li>
 *   <li><b>Ambiguity is refused too.</b> Two constructors of the same arity both satisfiable means the
 *       choice would depend on reflection order, which is not a decision anybody made.</li>
 * </ul>
 *
 * <p><b>What a plugin action gets for free.</b> {@link TransportAction#execute} runs the node's
 * {@code ActionFilters} itself, so an action built here goes through the same filters everything else
 * does — a plugin cannot reach past authorization by shipping its own action. And a
 * {@code HandledTransportAction} registers its own transport handler as it is constructed, so the action
 * is reachable node to node exactly as it would be on a classic node.
 */
public final class PluginActions {

    private final Map<String, TransportAction<? extends ActionRequest, ? extends ActionResponse>> byName;

    private PluginActions(Map<String, TransportAction<? extends ActionRequest, ? extends ActionResponse>> byName) {
        this.byName = Map.copyOf(byName);
    }

    /** An empty registry, for a node with no plugin actions. */
    public static PluginActions none() {
        return new PluginActions(Map.of());
    }

    /**
     * Builds every action the plugins declared.
     *
     * @param plugins the action plugins
     * @param available what a constructor may be given, most specific first, offered to every plugin alike
     * @return the registry
     * @throws IllegalStateException if an action cannot be built, naming what was missing
     */
    public static PluginActions build(List<ActionPlugin> plugins, List<Object> available) {
        return build(plugins, plugin -> available);
    }

    /**
     * Builds every action the plugins declared, resolving each against what its own plugin may be given.
     *
     * @param plugins the action plugins
     * @param availableFor what a constructor of the given plugin's actions may be given, most specific
     *        first: the node's services and that plugin's own components
     * @return the registry
     * @throws IllegalStateException if an action cannot be built, naming what was missing
     */
    public static PluginActions build(List<ActionPlugin> plugins, java.util.function.Function<ActionPlugin, List<Object>> availableFor) {
        final Map<String, TransportAction<? extends ActionRequest, ? extends ActionResponse>> built = new LinkedHashMap<>();
        final List<String> failures = new ArrayList<>();
        for (ActionPlugin plugin : plugins) {
            final List<Object> available = new ArrayList<>(availableFor.apply(plugin));
            for (ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse> handler : plugin.getActions()) {
                final String name = handler.getAction().name();
                if (built.containsKey(name)) {
                    // Two plugins claiming one action name is the same problem as two request wrappers:
                    // which one answers would depend on load order, and nobody chose that.
                    throw new IllegalStateException(
                        "two plugins provide the action [" + name + "]; which one answered would depend on load order"
                    );
                }
                try {
                    built.put(name, construct(handler.getTransportAction(), available));
                } catch (IllegalStateException e) {
                    failures.add(e.getMessage());
                }
            }
        }
        // Every action that cannot be built, in one refusal: a plugin with several gaps would otherwise be
        // discovered one restart at a time.
        if (failures.isEmpty() == false) {
            throw new IllegalStateException(failures.size() + " plugin action(s) cannot be built:\n" + String.join("\n", failures));
        }
        return new PluginActions(built);
    }

    @SuppressWarnings("unchecked")
    private static TransportAction<? extends ActionRequest, ? extends ActionResponse> construct(Class<?> type, List<Object> available) {
        final List<Constructor<?>> candidates = new ArrayList<>();
        for (Constructor<?> constructor : type.getConstructors()) {
            if (arguments(constructor, available) != null) {
                candidates.add(constructor);
            }
        }
        if (candidates.isEmpty()) {
            throw new IllegalStateException(
                "cannot build the transport action ["
                    + type.getName()
                    + "]: no public constructor could be satisfied. Its parameters are "
                    + parametersOf(type)
                    + "; this node has nothing for "
                    + missing(type, available)
            );
        }
        candidates.sort((a, b) -> Integer.compare(b.getParameterCount(), a.getParameterCount()));
        if (candidates.size() > 1 && candidates.get(0).getParameterCount() == candidates.get(1).getParameterCount()) {
            throw new IllegalStateException(
                "cannot build the transport action ["
                    + type.getName()
                    + "]: two constructors of "
                    + candidates.get(0).getParameterCount()
                    + " parameters can both be satisfied, and choosing between them would depend on reflection order"
            );
        }
        final Constructor<?> chosen = candidates.get(0);
        try {
            return (TransportAction<? extends ActionRequest, ? extends ActionResponse>) chosen.newInstance(arguments(chosen, available));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("the transport action [" + type.getName() + "] threw while being built", e);
        }
    }

    /** The arguments for a constructor, or null if any parameter cannot be supplied. */
    private static Object[] arguments(Constructor<?> constructor, List<Object> available) {
        return arguments(constructor, available, 0);
    }

    private static Object[] arguments(Constructor<?> constructor, List<Object> available, int depth) {
        final Class<?>[] parameters = constructor.getParameterTypes();
        final Object[] arguments = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            final Object match = resolve(parameters[i], available, depth);
            if (match == null) {
                return null;
            }
            arguments[i] = match;
        }
        return arguments;
    }

    /** How deep a chain of injectable helpers may go before resolution gives up; Guice has no limit, a cycle would. */
    private static final int MAX_DEPTH = 8;

    /**
     * Finds an argument of a type: something the node or the plugin supplied, or else a helper built for it.
     *
     * <p><b>The helper is Guice's just-in-time binding, and only that.</b> A classic node constructs on demand any
     * concrete class whose constructor carries {@code @Inject}, from what it can already supply. Plugins lean on
     * it: cross-cluster replication's actions take services it never returns from {@code createComponents}, which
     * exist only because Guice builds them. Doing the same here, for a class that asked for it with the
     * annotation, is not inventing a container -- a class without the annotation is never constructed, so
     * nothing is built that its author did not mark as buildable. One instance per type per plugin, kept in
     * the list so later actions of the same plugin share it, as they would share Guice's singleton.
     */
    private static Object resolve(Class<?> type, List<Object> available, int depth) {
        for (Object candidate : available) {
            if (type.isInstance(candidate)) {
                return candidate;
            }
        }
        if (depth >= MAX_DEPTH || type.isInterface() || java.lang.reflect.Modifier.isAbstract(type.getModifiers())) {
            return null;
        }
        for (Constructor<?> constructor : type.getConstructors()) {
            if (injectable(constructor) == false) {
                continue;
            }
            final Object[] arguments = arguments(constructor, available, depth + 1);
            if (arguments == null) {
                continue;
            }
            try {
                final Object built = constructor.newInstance(arguments);
                available.add(built);
                return built;
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("the injectable [" + type.getName() + "] threw while being built", e);
            }
        }
        return null;
    }

    private static boolean injectable(Constructor<?> constructor) {
        for (java.lang.annotation.Annotation annotation : constructor.getAnnotations()) {
            final String name = annotation.annotationType().getName();
            if (name.equals("org.opensearch.common.inject.Inject")
                || name.equals("javax.inject.Inject")
                || name.equals("jakarta.inject.Inject")) {
                return true;
            }
        }
        return false;
    }

    /** The parameter types, across every public constructor, that nothing can supply. */
    private static List<String> missing(Class<?> type, List<Object> available) {
        final java.util.Set<String> names = new java.util.TreeSet<>();
        for (Constructor<?> constructor : type.getConstructors()) {
            for (Class<?> parameter : constructor.getParameterTypes()) {
                if (resolve(parameter, available, 0) == null) {
                    names.add(parameter.getSimpleName());
                }
            }
        }
        return List.copyOf(names);
    }

    private static List<String> parametersOf(Class<?> type) {
        final List<String> described = new ArrayList<>();
        for (Constructor<?> constructor : type.getConstructors()) {
            final List<String> names = new ArrayList<>();
            for (Class<?> parameter : constructor.getParameterTypes()) {
                names.add(parameter.getSimpleName());
            }
            described.add(names.toString());
        }
        return described;
    }

    /**
     * Finds the action a name belongs to.
     *
     * @param name the action name
     * @return the action, or empty if no plugin provides it
     */
    public Optional<TransportAction<? extends ActionRequest, ? extends ActionResponse>> find(String name) {
        return Optional.ofNullable(byName.get(name));
    }

    /**
     * Returns the action names the plugins provide.
     *
     * @return the names
     */
    public java.util.Set<String> names() {
        return byName.keySet();
    }

    /**
     * Reports whether any plugin provided an action.
     *
     * @return true when there are none
     */
    public boolean isEmpty() {
        return byName.isEmpty();
    }
}
