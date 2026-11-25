/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

/** The stock gRPC transport, loaded as a plugin, serving on a shell node. */
public class ServerlessGrpcTests extends OpenSearchTestCase {

    /**
     * The real gRPC transport, from the stock module, answers a health call on a shell node: HTTP/2 with
     * gRPC framing, started by the shell with the node.
     */
    public void testTheGrpcTransportServesOnAShellNode() throws Exception {
        final Settings settings = Settings.builder()
            .put("node.name", "grpc-on")
            .put("cluster.name", "serverless-grpc")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .putList("aux.transport.types", "transport-grpc")
            .build();
        try (ServerlessNode node = new ServerlessNode(settings, List.of(new org.opensearch.transport.grpc.GrpcPlugin()))) {
            node.startup().start();
            final org.opensearch.transport.AuxTransport grpc = node.auxTransports.get(0);
            final java.net.InetSocketAddress address = grpc.getBoundAddress().publishAddress().address();
            final ManagedChannel channel = NettyChannelBuilder.forAddress(
                org.opensearch.common.network.NetworkAddress.format(address.getAddress()),
                address.getPort()
            ).usePlaintext().build();
            try {
                final HealthCheckResponse response = HealthGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(10, TimeUnit.SECONDS)
                    .check(HealthCheckRequest.newBuilder().build());
                assertEquals(HealthCheckResponse.ServingStatus.SERVING, response.getStatus());
            } finally {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }
}
