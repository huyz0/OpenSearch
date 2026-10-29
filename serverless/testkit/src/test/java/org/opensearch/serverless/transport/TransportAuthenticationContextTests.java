/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.transport;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

public class TransportAuthenticationContextTests extends OpenSearchTestCase {
    private TransportAuthenticator authenticator(String node) {
        final var secrets = new MetadataPlane.TransportSecrets(1L, "ab".repeat(32), null, 0L);
        return new TransportAuthenticator(() -> secrets, () -> {}, () -> node, () -> 1_000L, () -> 30_000L);
    }

    public void testStreamingAuthenticationAcceptsABufferedFrame() throws Exception {
        final var request = new GetRequest("index", "id-" + "π😀".repeat(1024));
        // Captured from the previous BytesStreamOutput implementation, with the test's fixed key and clock.
        final String buffered =
            "1:sender:1000:cfc0df316d1382c4021bb375492b3d93:2810a7ed70450bf3deca8d07a86ccd9afb13d5b888f293d6512fb7ac5e142c6f";
        authenticator("receiver").verify("action", request, buffered, Map.of("caller", "alice"));
    }

    public void testCapturedFrameCannotBeUsedAtAnotherNode() throws Exception {
        final var request = new GetRequest("index", "id");
        final String mac = authenticator("sender").sign("action", request, "receiver", Map.of());
        expectThrows(OpenSearchSecurityException.class, () -> authenticator("other-node").verify("action", request, mac, Map.of()));
        authenticator("receiver").verify("action", request, mac, Map.of());
    }

    public void testCapturedFrameCannotReplayAfterReceiverRestarts() throws Exception {
        final var before = new org.opensearch.cluster.node.DiscoveryNode(
            "receiver",
            buildNewFakeTransportAddress(),
            org.opensearch.Version.CURRENT
        );
        final var after = new org.opensearch.cluster.node.DiscoveryNode("receiver", before.getAddress(), org.opensearch.Version.CURRENT);
        assertEquals(before.getId(), after.getId());
        assertNotEquals(before.getEphemeralId(), after.getEphemeralId());
        final var request = new GetRequest("index", "id");
        final var sender = authenticator("sender");
        final String captured = sender.sign("action", request, TransportAuthenticator.identity(before), Map.of());
        authenticator(TransportAuthenticator.identity(before)).verify("action", request, captured, Map.of());
        // The replacement has an empty replay cache, so only the receiver incarnation prevents replay.
        final var replacement = authenticator(TransportAuthenticator.identity(after));
        expectThrows(OpenSearchSecurityException.class, () -> replacement.verify("action", request, captured, Map.of()));
        final String fresh = sender.sign("action", request, TransportAuthenticator.identity(after), Map.of());
        replacement.verify("action", request, fresh, Map.of());
    }

    public void testCallerAndPluginOriginCannotBeChanged() throws Exception {
        final var request = new GetRequest("index", "id");
        final Map<String, String> headers = Map.of("caller", "alice", ShardRouter.PLUGIN_ORIGIN_HEADER, "false");
        final String mac = authenticator("sender").sign("action", request, "receiver", headers);
        final var receiver = authenticator("receiver");
        expectThrows(
            OpenSearchSecurityException.class,
            () -> receiver.verify("action", request, mac, Map.of("caller", "bob", ShardRouter.PLUGIN_ORIGIN_HEADER, "false"))
        );
        expectThrows(
            OpenSearchSecurityException.class,
            () -> receiver.verify("action", request, mac, Map.of("caller", "alice", ShardRouter.PLUGIN_ORIGIN_HEADER, "true"))
        );
        receiver.verify("action", request, mac, headers);
    }

    public void testHeaderOrderDoesNotAffectAuthentication() throws Exception {
        final var request = new GetRequest("index", "id");
        final Map<String, String> first = new LinkedHashMap<>();
        first.put("z", "last");
        first.put("a", "first");
        final Map<String, String> reversed = new LinkedHashMap<>();
        reversed.put("a", "first");
        reversed.put("z", "last");
        final String mac = authenticator("sender").sign("action", request, "receiver", first);
        authenticator("receiver").verify("action", request, mac, reversed);
    }
}
