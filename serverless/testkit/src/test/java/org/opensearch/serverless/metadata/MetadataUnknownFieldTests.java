/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class MetadataUnknownFieldTests extends OpenSearchTestCase {
    public void testUnknownWalStructuresCannotReplaceOperationIdentity() throws Exception {
        for (String nested : new String[] { "{\"id\":\"wrong\",\"seq_no\":999}", "[{\"id\":\"wrong\",\"seq_no\":999}]" }) {
            final String json = "{\"id\":\"document\",\"source\":\"{}\",\"future\":"
                + nested
                + ",\"seq_no\":7,\"primary_term\":3,\"version\":2}";
            final var record = WalRecord.fromStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
            assertEquals("document", record.id());
            assertEquals(7L, record.seqNo());
            assertEquals(3L, record.primaryTerm());
            assertEquals(2L, record.version());
        }
    }

    public void testUnknownHeadStructuresCannotReplaceOwnership() throws Exception {
        for (String nested : new String[] {
            "{\"term\":999,\"owner_node_id\":\"wrong\"}",
            "[{\"term\":999,\"owner_node_id\":\"wrong\"}]" }) {
            final String json = "{\"index\":\"index\",\"shard\":0,\"term\":7,\"owner_node_id\":\"member\",\"future\":"
                + nested
                + ",\"lease_expires_at_millis\":10000}";
            final var head = ShardHead.fromStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
            assertEquals("member", head.ownerNodeId());
            assertEquals(7L, head.term());
            assertTrue(head.isHeldAt(1_000L));
        }
    }
}
