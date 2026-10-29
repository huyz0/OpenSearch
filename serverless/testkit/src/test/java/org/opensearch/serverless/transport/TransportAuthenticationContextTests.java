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

    public void testCapturedFrameCannotBeUsedAtAnotherNode() throws Exception {
        final var request = new GetRequest("index", "id");
        final String mac = authenticator("sender").sign("action", request, "receiver", Map.of());
        expectThrows(OpenSearchSecurityException.class, () -> authenticator("other-node").verify("action", request, mac, Map.of()));
        authenticator("receiver").verify("action", request, mac, Map.of());
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
