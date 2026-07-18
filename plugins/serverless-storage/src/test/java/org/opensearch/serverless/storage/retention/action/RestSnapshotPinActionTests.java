/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Proves {@link RestSnapshotPinAction#prepareRequest} correctly parses a well-formed body and
 * rejects a malformed one, mirroring {@code RestShardCloneActionTests}'s own shape.
 * Deliberately doesn't exercise the actual dispatch through {@code client.executeLocally} (that
 * requires a real {@link NodeClient}/transport, already covered end to end by {@code
 * ServerlessStorageSnapshotPinActionIT}); this only proves body-parsing correctness in isolation.
 */
public class RestSnapshotPinActionTests extends OpenSearchTestCase {

    private final RestSnapshotPinAction action = new RestSnapshotPinAction();

    private static RestRequest requestWithBody(String json) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray(json), XContentType.JSON)
            .withMethod(RestRequest.Method.POST)
            .withPath("/_plugins/_serverless/storage/_snapshot_pin")
            .build();
    }

    public void testPrepareRequestParsesAWellFormedBody() throws Exception {
        RestRequest request = requestWithBody("{\"index_uuid\":\"my-idx\",\"shard_id\":0,\"snapshot_id\":\"snap-1\"}");
        // A non-null consumer proves parsing succeeded without throwing -- dispatch itself needs a
        // real NodeClient, out of scope here (see class javadoc).
        assertNotNull(action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsAMissingIndexUuidField() {
        RestRequest request = requestWithBody("{\"shard_id\":0,\"snapshot_id\":\"snap-1\"}");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage().contains("index_uuid"));
    }

    public void testPrepareRequestRejectsAMissingShardIdField() {
        RestRequest request = requestWithBody("{\"index_uuid\":\"my-idx\",\"snapshot_id\":\"snap-1\"}");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage().contains("shard_id"));
    }

    public void testPrepareRequestRejectsAMissingSnapshotIdField() {
        RestRequest request = requestWithBody("{\"index_uuid\":\"my-idx\",\"shard_id\":0}");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage().contains("snapshot_id"));
    }

    public void testPrepareRequestRejectsANonNumericShardId() {
        RestRequest request = requestWithBody("{\"index_uuid\":\"my-idx\",\"shard_id\":\"zero\",\"snapshot_id\":\"snap-1\"}");
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
    }

    public void testPrepareRequestRejectsANonStringIndexUuid() {
        RestRequest request = requestWithBody("{\"index_uuid\":123,\"shard_id\":0,\"snapshot_id\":\"snap-1\"}");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertTrue(e.getMessage().contains("index_uuid"));
    }
}
