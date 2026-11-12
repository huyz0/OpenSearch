/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer whose own lease is past its deadline may have lost the shard to a taker, so it renews no search node's grant:
 * a search node that went on serving under one would miss every write the new owner takes.
 */
public class TailGrantDoubtTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String INDEX = "doubt";
    private static final String UUID = "uuid-doubt-0000000000";

    public void testAWriterPastItsLeaseRenewsNoGrant() throws Exception {
        final AtomicLong skew = new AtomicLong();
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(8192, createTempDir(), false),
            BlobPath.cleanPath(),
            () -> System.currentTimeMillis() + skew.get(),
            TTL
        );
        plane.createIndex(new IndexDescriptor(INDEX, UUID, 1, "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}", Settings.EMPTY));
        final Settings settings = Settings.builder()
            .put("node.name", "doubt-writer")
            .put("cluster.name", "serverless-grant-doubt")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(RefreshVisibility.NODE_SETTING, RefreshVisibility.Mode.EVERYWHERE.settingValue())
            .build();
        try (ServerlessNode writer = new ServerlessNode(settings)) {
            writer.startup().start();
            writer.setMetadataPlane(plane);
            writer.leases().renewLease(plane);
            final ShardId shardId = writer.activation().activateWriter(plane, INDEX, 0).orElseThrow();
            final TailPush push = writer.router().tailPush();

            final TailFetchResponse granted = push.answerFetch(new TailFetchRequest(shardId, -1L, "search-a", TailFetchRequest.NEW));
            assertNotEquals("a writer whose lease is live subscribes a search node", TailFetchResponse.NONE, granted.subscription());

            // Past the lease's deadline by this node's own clock: a taker may already have the shard.
            skew.set(2 * TTL);
            final TailFetchResponse renewal = push.answerFetch(new TailFetchRequest(shardId, -1L, "search-a", granted.subscription()));
            assertEquals("no grant renewed while the shard may be another node's", TailFetchResponse.NONE, renewal.subscription());
            final TailFetchResponse fresh = push.answerFetch(new TailFetchRequest(shardId, -1L, "search-b", TailFetchRequest.NEW));
            assertEquals("nor a new one", TailFetchResponse.NONE, fresh.subscription());
            assertEquals("and no claim a commit could be compared with", Long.MAX_VALUE, fresh.claimedThrough());

            // Renewed, and every head read again: the writer may grant once more.
            skew.set(0);
            writer.leases().renewLease(plane);
            writer.leases().verifyHeads(plane);
            final TailFetchResponse again = push.answerFetch(new TailFetchRequest(shardId, -1L, "search-a", TailFetchRequest.NEW));
            assertNotEquals(TailFetchResponse.NONE, again.subscription());
        }
    }
}
