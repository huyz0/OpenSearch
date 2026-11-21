/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.logging;

import org.opensearch.common.logging.OpenSearchLogMessage;

import java.util.Map;

/** An operational message with fields emitted by the standard OpenSearch JSON layout. */
public final class ServerlessLogMessage extends OpenSearchLogMessage {

    /** Builds a message from fixed text and allowlisted operational fields. */
    public ServerlessLogMessage(String message, Map<String, Object> fields) {
        super(Map.copyOf(fields), message);
    }
}
