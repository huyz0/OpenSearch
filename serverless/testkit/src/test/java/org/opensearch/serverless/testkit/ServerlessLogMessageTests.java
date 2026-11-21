/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.common.logging.OpenSearchJsonLayout;
import org.opensearch.serverless.logging.ServerlessLogMessage;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.util.Map;

public class ServerlessLogMessageTests extends OpenSearchTestCase {

    @BeforeClass
    public static void initializeNodeName() {
        LogConfigurator.setNodeName("serverless-logging-test");
    }

    public void testStandardLayoutWritesOperationalFieldsOutsideTheMessage() {
        final var layout = OpenSearchJsonLayout.newBuilder().setType("server").setOpenSearchMessageFields("event,shard,error_type").build();
        final var message = new ServerlessLogMessage("Search copy could not answer", Map.of("event", "search_copy_failed", "shard", 3));
        final var event = Log4jLogEvent.newBuilder().setLoggerName("org.opensearch.serverless.ops.SearchFanout")
            .setLevel(Level.DEBUG)
            .setMessage(message)
            .build();
        final String line = layout.toSerializable(event);
        assertTrue(line, line.contains("\"message\": \"Search copy could not answer\""));
        assertTrue(line, line.contains("\"event\": \"search_copy_failed\""));
        assertTrue(line, line.contains("\"shard\": \"3\""));
        assertFalse(line, line.contains("\"error_type\""));
    }
}
