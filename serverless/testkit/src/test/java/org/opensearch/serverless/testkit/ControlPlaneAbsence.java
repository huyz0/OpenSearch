/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.SuppressForbidden;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Walks a constructed object graph and reports which forbidden classes are reachable.
 *
 * <p>This is how {@code rfc-serverless-shell.md} §11 criterion 5 is asserted: the claim "there is no
 * control plane in this process" is checked against the object graph rather than against the source,
 * because the interesting failure is a dependency pulled in transitively by something else.
 *
 * <p>A walk that silently stops working visits nothing and reports success, so callers must assert on
 * {@link Result#visited} as well as {@link Result#found}. {@code ControlPlaneAbsenceTests} is the
 * negative control proving the walk can actually fail.
 */
public final class ControlPlaneAbsence {

    /** The classes whose presence means a control plane was constructed. */
    public static final Set<String> CONTROL_PLANE = Set.of(
        "org.opensearch.cluster.coordination.Coordinator",
        "org.opensearch.cluster.routing.allocation.AllocationService",
        "org.opensearch.gateway.GatewayMetaState",
        "org.opensearch.node.Node"
    );

    private ControlPlaneAbsence() {}

    /** The outcome of one walk. */
    public static final class Result {
        /** Forbidden class names actually reached. */
        public final Set<String> found;
        /** How many distinct objects were visited — guards against a vacuous pass. */
        public final int visited;

        Result(Set<String> found, int visited) {
            this.found = found;
            this.visited = visited;
        }
    }

    /**
     * Performs a bounded breadth-first walk from the given roots.
     *
     * @param forbidden class names to look for
     * @param roots objects to start from; nulls are ignored
     * @return what was found and how much was searched
     */
    @SuppressForbidden(reason = "the object-graph walk is the mechanism of RFC §11 criterion 5; reflection is the point")
    public static Result scan(Set<String> forbidden, Object... roots) {
        final IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        final Deque<Object> queue = new ArrayDeque<>();
        final Set<String> found = new HashSet<>();
        for (Object r : roots) {
            if (r != null) {
                queue.add(r);
            }
        }
        int visited = 0;
        final int cap = 500_000;
        while (queue.isEmpty() == false && visited < cap) {
            final Object o = queue.poll();
            if (seen.put(o, Boolean.TRUE) != null) {
                continue;
            }
            visited++;
            final Class<?> cls = o.getClass();
            final String name = cls.getName();
            if (forbidden.contains(name)) {
                found.add(name);
                continue;
            }
            if (name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("sun.")) {
                continue;
            }
            if (cls.isArray()) {
                if (cls.getComponentType().isPrimitive() == false) {
                    for (Object e : (Object[]) o) {
                        if (e != null) {
                            queue.add(e);
                        }
                    }
                }
                continue;
            }
            for (Class<?> c = cls; c != null && c != Object.class; c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    if (Modifier.isStatic(f.getModifiers()) || f.getType().isPrimitive()) {
                        continue;
                    }
                    try {
                        f.setAccessible(true);
                        final Object v = f.get(o);
                        if (v != null) {
                            queue.add(v);
                        }
                    } catch (Throwable ignored) {
                        // inaccessible under the module system; skipping only weakens the check
                    }
                }
            }
        }
        return new Result(found, visited);
    }
}
