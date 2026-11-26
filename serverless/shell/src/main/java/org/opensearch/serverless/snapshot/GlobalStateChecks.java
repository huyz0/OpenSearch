/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.snapshot;

import org.opensearch.common.settings.Settings;
import org.opensearch.script.StoredScriptSource;
import org.opensearch.serverless.node.NodeContext;

import java.util.Map;

/**
 * The checks a restored global-state entry must pass, the ones its own PUT route makes. They live in the REST layer, which
 * this package may not depend on, so the node's wiring supplies them.
 */
public interface GlobalStateChecks {

    /**
     * Checks an index or component template body.
     *
     * @param serving the node
     * @param component whether it is a component template
     * @param body the template's JSON
     * @throws Exception if the body is refused
     */
    void template(NodeContext serving, boolean component, String body) throws Exception;

    /**
     * Checks a stored script.
     *
     * @param serving the node
     * @param source the script
     * @throws Exception if the script is refused
     */
    void storedScript(NodeContext serving, StoredScriptSource source) throws Exception;

    /**
     * Checks persistent settings.
     *
     * @param serving the node
     * @param flat dotted key to scalar value
     * @throws Exception if a value is refused
     */
    void settings(NodeContext serving, Map<String, Object> flat) throws Exception;

    /**
     * Makes settings just written take effect on this node.
     *
     * @param serving the node
     * @param written the register's settings after the write
     */
    void settingsWritten(NodeContext serving, Settings written);
}
