/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.membership;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class NodeLeaseParsingTests extends OpenSearchTestCase {
    public void testUnknownNestedFieldsCannotChangeTheLeaseOrEndItEarly() throws Exception {
        for (String unknown : new String[] {
            "{\"node_id\":\"wrong\",\"expires_at_millis\":0}",
            "[{\"node_id\":\"wrong\",\"expires_at_millis\":0}]" }) {
            final String json = "{\"node_id\":\"member\",\"ephemeral_id\":\"process\",\"address\":\"127.0.0.1:9300\","
                + "\"future\":"
                + unknown
                + ",\"roles\":[\"ingest\"],\"expires_at_millis\":10000}";
            final NodeLease lease = NodeLease.fromStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
            assertEquals("member", lease.nodeId());
            assertEquals(10_000L, lease.expiresAtMillis());
            assertEquals(Set.of("ingest"), lease.roles());
            assertFalse(lease.isExpiredAt(1_000L));
        }
    }

    public void testNonObjectLeaseIsRefused() {
        expectThrows(IOException.class, () -> NodeLease.fromStream(new ByteArrayInputStream(new byte[] { '[', ']' })));
    }
}
