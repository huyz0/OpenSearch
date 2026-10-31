/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Explicit footprint probe; small groups, three replicas in one JVM, not a deployment benchmark. */
@ThreadLeakLingering(linger = 10_000)
@ThreadLeakFilters(filters = RatisPersistenceSpikeTests.GlobalTimerFilter.class)
public class RatisGroupCountBenchmarkTests extends OpenSearchTestCase {

    public void testGroupFootprint() throws Exception {
        final int count = Integer.parseInt(System.getProperty("tests.consensus.groups", "10"));
        assertTrue("group count must be between 1 and 16 for this bounded feasibility probe", count >= 1 && count <= 16);
        final List<RaftPeer> peers = new ArrayList<>();
        final List<ServerSocket> reservations = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                final var socket = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"));
                reservations.add(socket);
                peers.add(RaftPeer.newBuilder().setId("peer-" + i).setAddress("127.0.0.1:" + socket.getLocalPort()).build());
            }
        } finally {
            for (ServerSocket socket : reservations) {
                socket.close();
            }
        }
        final RaftGroup seed = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
        final Path storage = createTempDir();
        final List<RaftServer> servers = new ArrayList<>();
        final List<Map<String, Object>> steps = new ArrayList<>();
        try {
            for (RaftPeer peer : peers) {
                servers.add(RatisPersistenceSpikeTests.start(peer, seed, storage));
            }
            RatisPersistenceSpikeTests.append(seed, 1, 1);
            steps.add(sample(1));
            for (int i = 1; i < count; i++) {
                final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
                final RaftProperties properties = new RaftProperties();
                RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
                try (RaftClient client = RaftClient.newBuilder().setRaftGroup(seed).setProperties(properties).build()) {
                    for (RaftPeer peer : peers) {
                        final var reply = client.getGroupManagementApi(peer.getId()).add(group);
                        assertTrue(reply.toString(), reply.isSuccess());
                    }
                }
                RatisPersistenceSpikeTests.append(group, 1, 1);
                steps.add(sample(i + 1));
            }
        } finally {
            RatisPersistenceSpikeTests.closeServers(servers);
        }
        try (var report = org.opensearch.common.xcontent.json.JsonXContent.contentBuilder().prettyPrint()) {
            report.startObject();
            report.field("scope", "three_local_replicas_in_one_jvm");
            report.field("ratis_version", "3.3.1");
            report.field("unsafe_flush", false);
            report.field("async_flush", false);
            report.field("java_version", System.getProperty("java.version"));
            report.field("max_heap_bytes", Runtime.getRuntime().maxMemory());
            report.field("all_servers_closed", true);
            report.field("samples", steps);
            report.endObject();
            final Path output = Path.of(System.getProperty("tests.consensus.output"));
            Files.createDirectories(output.toAbsolutePath().getParent());
            Files.write(output, BytesReference.toBytes(BytesReference.bytes(report)));
            logger.info("Consensus group footprint: {}", BytesReference.bytes(report).utf8ToString());
        }
    }

    private static Map<String, Object> sample(int groups) {
        final var memory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        return Map.of(
            "groups",
            groups,
            "threads",
            ManagementFactory.getThreadMXBean().getThreadCount(),
            "heap_used_bytes",
            memory.getUsed(),
            "heap_committed_bytes",
            memory.getCommitted()
        );
    }
}
