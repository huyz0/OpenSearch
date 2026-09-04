/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.script;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.settings.Settings;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptService;
import org.opensearch.script.StoredScriptSource;

import java.util.Map;

/**
 * Core's script service, resolving a stored script from this deployment's store rather than from cluster
 * state.
 *
 * <p>Everything else is core's: the engines, the contexts, the caches, the compile-rate limits. The one
 * method that reached into cluster state is the one overridden.
 */
public final class ServerlessScriptService extends ScriptService {

    private final StoredScripts stored;

    /**
     * Creates the service.
     *
     * @param settings node settings
     * @param engines the engines, by language
     * @param contexts the contexts
     * @param stored where stored scripts come from
     */
    public ServerlessScriptService(
        Settings settings,
        Map<String, ScriptEngine> engines,
        Map<String, ScriptContext<?>> contexts,
        StoredScripts stored
    ) {
        super(settings, engines, contexts);
        this.stored = stored;
    }

    @Override
    protected StoredScriptSource getScriptFromClusterState(String id) {
        return stored.lookup(id).orElseThrow(() -> new ResourceNotFoundException("unable to find script [" + id + "]"));
    }

    /**
     * Returns the stored-script cache, for the handler that changes it.
     *
     * @return the cache
     */
    public StoredScripts stored() {
        return stored;
    }
}
