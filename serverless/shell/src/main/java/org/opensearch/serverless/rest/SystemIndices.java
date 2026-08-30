/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.regex.Regex;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;

import java.util.List;

/**
 * The indices a plugin keeps its own state in, and which the request path will not touch.
 *
 * <p><b>Why this is a rule and not a convention.</b> A plugin that stores anything sensitive in an index —
 * the shell's own authentication stores password records — is safe only while nobody can reach that index
 * through the ordinary API. Without this, any caller who can search could read the account records, and any
 * caller who can write could put a record of their own choosing under an existing account name and become
 * that account. Naming the index with a leading dot is decoration; refusing to route to it is the control.
 *
 * <p><b>Refused entirely, rather than refused to some callers.</b> The shell has no authorization layer
 * (&sect;6.3), so "only administrators may read this" is not a sentence it can enforce. What it can enforce
 * is that <em>no</em> REST request reaches a declared system index, whoever sent it. The owning plugin
 * still reaches it through the {@code Client}, which is the path it was always meant to use, so the rule
 * costs the plugin nothing and closes the hole completely rather than partially.
 *
 * <p><b>Declared by the plugin, through core's API.</b> {@code SystemIndexPlugin#getSystemIndexDescriptors}
 * is how every OpenSearch plugin already says which indices are its own, including the security plugin.
 * Honouring the existing hook means a plugin does not need to know it is running on this shell.
 */
public final class SystemIndices {

    private final List<String> patterns;

    /**
     * Creates the set of patterns.
     *
     * @param patterns index patterns, in the simple {@code *} glob syntax core's descriptors use
     */
    public SystemIndices(List<String> patterns) {
        this.patterns = List.copyOf(patterns);
    }

    /**
     * Reports whether an index belongs to a plugin.
     *
     * @param index the index name, which may be null for a route that names none
     * @return true if it is a system index
     */
    public boolean contains(String index) {
        if (index == null) {
            return false;
        }
        for (String pattern : patterns) {
            if (Regex.simpleMatch(pattern, index)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reports whether any plugin declared a system index at all.
     *
     * @return true when there is nothing to guard
     */
    public boolean isEmpty() {
        return patterns.isEmpty();
    }

    /**
     * Wraps a handler so that a request naming a system index is refused before it runs.
     *
     * <p>Applied at registration to every handler the shell serves, rather than added to each one, because
     * a guard a handler has to remember to call is a guard that one handler will not call. The route
     * parameter is always {@code index} across the shell's surface; a route that has none reads null and
     * passes through.
     *
     * <p>{@code _bulk} is the one case this cannot cover, because a bulk request names its indices in the
     * body rather than the path. {@code BulkHandler} checks each item itself, and says so.
     *
     * @param handler the handler to guard
     * @param systemIndices the declared system indices
     * @return the guarded handler, or the original when nothing is declared
     */
    public static RestHandler guard(RestHandler handler, SystemIndices systemIndices) {
        if (systemIndices.isEmpty()) {
            return handler;
        }
        // RestHandler.Wrapper delegates every other method, including the ones a handler uses to say it
        // streams content or must not trip the circuit breaker. Re-implementing those by hand is how a
        // wrapped handler quietly loses a property it declared.
        return new RestHandler.Wrapper(handler) {
            @Override
            public void handleRequest(
                RestRequest request,
                org.opensearch.rest.RestChannel channel,
                org.opensearch.transport.client.node.NodeClient client
            ) throws Exception {
                final String index = request.param("index");
                if (systemIndices.contains(index)) {
                    channel.sendResponse(refusal(channel, index));
                    return;
                }
                handler.handleRequest(request, channel, client);
            }
        };
    }

    /**
     * The answer a request for a system index gets.
     *
     * @param channel the channel, for its content type
     * @param index the index that was asked for
     * @return the response
     * @throws java.io.IOException if the body cannot be written
     */
    public static org.opensearch.rest.BytesRestResponse refusal(org.opensearch.rest.RestChannel channel, String index)
        throws java.io.IOException {
        // 403 rather than 404: pretending the index does not exist would send an operator looking for a
        // missing index, and it hides nothing an attacker could not learn by installing the same plugin.
        return IndexAdminHandler.error(
            channel,
            RestStatus.FORBIDDEN,
            "system_index",
            "["
                + index
                + "] belongs to a plugin and is not reachable through the request path; "
                + "the plugin that owns it reaches it through the client"
        );
    }
}
