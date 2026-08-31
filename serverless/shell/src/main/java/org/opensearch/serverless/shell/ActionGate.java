/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Where a plugin gets to say no.
 *
 * <p><b>The gap this closes.</b> Authorization in OpenSearch is keyed on action names and enforced by
 * {@link ActionFilter}s, which classic {@code ActionModule} wraps around every {@code TransportAction}.
 * &sect;6.3 declined to build an action layer, so until now a plugin could authenticate a caller and had no
 * way whatsoever to restrict what that caller then did. The shell's own authentication plugin says as much
 * in its own documentation: it authenticates and does not authorize. This is the seam that changes.
 *
 * <p><b>There is no action layer here, and this does not build one.</b> What it does is take the two things
 * a filter actually needs — an action name and a request describing what is about to happen — and produce
 * them at the handful of places where the shell performs work. A plugin's existing {@code ActionFilter}
 * runs unmodified against them.
 *
 * <p><b>What a filter sees, and what it does not.</b> The work happens <em>inside</em> the chain, so a
 * filter that refuses before calling {@code chain.proceed} prevents it, and one that wraps the listener
 * runs after it. What a filter does not get is the response: the shell's operations return their own types
 * — a {@code Read}, a {@code SearchOutcome} — and manufacturing an {@code ActionResponse} to carry them
 * would be lossy invention. The listener is completed with {@link Admitted}, which says only that the work
 * was done. A filter that inspects or rewrites responses will not work here; one that decides whether a
 * caller may proceed will, and that is the one authorization is made of.
 *
 * <p><b>Synchronous by construction.</b> The shell's operations block, so the chain is driven to completion
 * on the calling thread. That thread is never a transport thread — every call site already dispatched — but
 * it does mean an {@code ActionFilter} that answers asynchronously holds a worker until it does.
 *
 * <p><b>Indices are reported as unresolved</b>, via {@link ActionRequestMetadata#empty()}. That is the truth
 * rather than a shortcut: a node cannot resolve an index expression here, for the reasons set out in
 * {@link RefusingIndexNameExpressionResolver}, and a filter told "unknown" can decide what to do about it
 * rather than being handed a subset that looks complete.
 */
public final class ActionGate {

    /** The response a filter chain is completed with: the work was admitted and done. */
    public static final class Admitted extends ActionResponse {

        static final Admitted INSTANCE = new Admitted();

        private Admitted() {}

        @Override
        public void writeTo(StreamOutput out) {
            // Never leaves this node.
        }
    }

    private final List<ActionFilter> filters;

    /**
     * Creates the gate.
     *
     * @param filters the plugins' filters, in any order
     */
    public ActionGate(List<ActionFilter> filters) {
        final List<ActionFilter> sorted = new ArrayList<>(filters);
        // Lowest order first, which is what ActionFilter#order documents and what a plugin expects when it
        // places itself relative to another one.
        sorted.sort(Comparator.comparingInt(ActionFilter::order));
        this.filters = List.copyOf(sorted);
    }

    /**
     * Reports whether any plugin installed a filter.
     *
     * @return true when there is nothing to run
     */
    public boolean isEmpty() {
        return filters.isEmpty();
    }

    /**
     * Runs one operation through the filters, or refuses it.
     *
     * @param action the action name, from core's own vocabulary
     * @param request a request describing what is about to happen
     * @param work the operation itself
     * @param <T> what the operation returns
     * @return whatever the operation returned
     * @throws Exception if a filter refuses, or the operation fails
     */
    public <T> T run(String action, ActionRequest request, CheckedSupplier<T, Exception> work) throws Exception {
        if (filters.isEmpty()) {
            // The overwhelmingly common case, and it must cost nothing: no task, no future, no chain.
            return work.get();
        }

        final AtomicReference<T> result = new AtomicReference<>();
        ActionFilterChain<ActionRequest, Admitted> chain = (task, name, req, listener) -> {
            try {
                result.set(work.get());
                listener.onResponse(Admitted.INSTANCE);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        };
        for (int i = filters.size() - 1; i >= 0; i--) {
            final ActionFilter filter = filters.get(i);
            final ActionFilterChain<ActionRequest, Admitted> next = chain;
            chain = (task, name, req, listener) -> filter.apply(task, name, req, ActionRequestMetadata.empty(), listener, next);
        }

        final PlainActionFuture<Admitted> future = PlainActionFuture.newFuture();
        final Task task = new Task(0L, "serverless", action, "", TaskId.EMPTY_TASK_ID, Map.of());
        chain.proceed(task, action, request, future);
        // Throws whatever a filter reported, which for a refusal is the plugin's own exception carrying the
        // plugin's own status. Nothing here reinterprets it: a security plugin's 403 should reach the caller
        // as the security plugin wrote it.
        future.actionGet();
        return result.get();
    }
}
