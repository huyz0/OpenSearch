/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;

import java.util.List;
import java.util.function.UnaryOperator;

/** Action filters that exercise the native request and response contract of REST adapters. */
public final class PolicyFilterPlugin extends Plugin implements ActionPlugin {
    private final String target;
    private final UnaryOperator<ActionRequest> requestRewrite;
    private final UnaryOperator<ActionResponse> responseRewrite;
    private final ActionResponse cached;

    PolicyFilterPlugin(
        String target,
        UnaryOperator<ActionRequest> requestRewrite,
        UnaryOperator<ActionResponse> responseRewrite,
        ActionResponse cached
    ) {
        this.target = target;
        this.requestRewrite = requestRewrite;
        this.responseRewrite = responseRewrite;
        this.cached = cached;
    }

    static PolicyFilterPlugin request(String action, UnaryOperator<ActionRequest> rewrite) {
        return new PolicyFilterPlugin(action, rewrite, null, null);
    }

    static PolicyFilterPlugin response(String action, UnaryOperator<ActionResponse> rewrite) {
        return new PolicyFilterPlugin(action, null, rewrite, null);
    }

    static PolicyFilterPlugin cached(String action, ActionResponse response) {
        return new PolicyFilterPlugin(action, null, null, response);
    }

    static PolicyFilterPlugin denyAfter(String action) {
        return response(action, response -> {
            throw new org.opensearch.OpenSearchSecurityException("policy denied", org.opensearch.core.rest.RestStatus.FORBIDDEN);
        });
    }

    @Override
    public List<org.opensearch.action.support.ActionFilter> getActionFilters() {
        return List.of(new org.opensearch.action.support.ActionFilter() {
            @Override
            public int order() {
                return 0;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                org.opensearch.tasks.Task task,
                String action,
                Request request,
                org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                org.opensearch.core.action.ActionListener<Response> listener,
                org.opensearch.action.support.ActionFilterChain<Request, Response> chain
            ) {
                if (target.equals(action) == false) {
                    chain.proceed(task, action, request, listener);
                } else if (cached != null) {
                    listener.onResponse((Response) cached);
                } else {
                    final Request selected = requestRewrite == null ? request : (Request) requestRewrite.apply(request);
                    chain.proceed(
                        task,
                        action,
                        selected,
                        org.opensearch.core.action.ActionListener.wrap(
                            response -> listener.onResponse(
                                responseRewrite == null ? response : (Response) responseRewrite.apply(response)
                            ),
                            listener::onFailure
                        )
                    );
                }
            }
        });
    }
}
