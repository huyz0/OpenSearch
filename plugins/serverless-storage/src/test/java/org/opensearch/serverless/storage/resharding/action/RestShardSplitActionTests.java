/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Proves {@link RestShardSplitAction#prepareRequest} correctly parses a well-formed body and
 * rejects a malformed one, mirroring {@code RestShardCloneActionTests}'s own source/target shape
 * but extended for this action's {@code partition_index}/{@code num_partitions} fields.
 * Deliberately doesn't exercise the actual dispatch through {@code client.executeLocally} (that
 * requires a real {@link NodeClient}/transport, already covered end to end by {@code
 * ServerlessStorageShardSplitActionIT}); this only proves body-parsing correctness in isolation.
 */
public class RestShardSplitActionTests extends OpenSearchTestCase {

    private final RestShardSplitAction action = new RestShardSplitAction();

    private static RestRequest requestWithBody(String json) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray(json), XContentType.JSON)
            .withMethod(RestRequest.Method.POST)
            .withPath("/_plugins/_serverless/storage/_split")
            .build();
    }

    public void testPrepareRequestParsesAWellFormedBody() throws Exception {
        RestRequest request = requestWithBody(
            "{\"source\":{\"index_uuid\":\"source-idx\",\"shard_id\":0},"
                + "\"target\":{\"index_uuid\":\"target-idx\",\"shard_id\":0},"
                + "\"partition_index\":0,\"num_partitions\":2}"
        );
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsAMissingSourceObject() {
        RestRequest request = requestWithBody(
            "{\"target\":{\"index_uuid\":\"target-idx\",\"shard_id\":0}," + "\"partition_index\":0,\"num_partitions\":2}"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage().contains("source"));
    }

    public void testPrepareRequestRejectsAMissingTargetObject() {
        RestRequest request = requestWithBody(
            "{\"source\":{\"index_uuid\":\"source-idx\",\"shard_id\":0}," + "\"partition_index\":0,\"num_partitions\":2}"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage().contains("target"));
    }

    public void testPrepareRequestRejectsAMissingIndexUuidField() {
        RestRequest request = requestWithBody(
            "{\"source\":{\"shard_id\":0},\"target\":{\"index_uuid\":\"target-idx\",\"shard_id\":0},"
                + "\"partition_index\":0,\"num_partitions\":2}"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage().contains("source"));
    }

    public void testPrepareRequestRejectsMissingPartitionIndexOrNumPartitions() {
        RestRequest request = requestWithBody(
            "{\"source\":{\"index_uuid\":\"source-idx\",\"shard_id\":0}," + "\"target\":{\"index_uuid\":\"target-idx\",\"shard_id\":0}}"
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage().contains("partition_index"));
        assertTrue(e.getMessage().contains("num_partitions"));
    }

    public void testPrepareRequestRejectsANonNumericPartitionIndex() {
        RestRequest request = requestWithBody(
            "{\"source\":{\"index_uuid\":\"source-idx\",\"shard_id\":0},"
                + "\"target\":{\"index_uuid\":\"target-idx\",\"shard_id\":0},"
                + "\"partition_index\":\"zero\",\"num_partitions\":2}"
        );
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonNumericShardId() {
        RestRequest request = requestWithBody(
            "{\"source\":{\"index_uuid\":\"source-idx\",\"shard_id\":\"zero\"},"
                + "\"target\":{\"index_uuid\":\"target-idx\",\"shard_id\":0},"
                + "\"partition_index\":0,\"num_partitions\":2}"
        );
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }
}
