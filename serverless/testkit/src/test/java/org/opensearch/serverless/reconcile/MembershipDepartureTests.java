/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.membership.BlobLeaseMembership;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MembershipDepartureTests extends OpenSearchTestCase {
    public void testARejoinedProcessCanDepartAgain() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final ThreadPool pool = mock(ThreadPool.class);
        final ExecutorService work = mock(ExecutorService.class);
        when(pool.generic()).thenReturn(work);
        final java.util.ArrayDeque<Runnable> queued = new java.util.ArrayDeque<>();
        org.mockito.Mockito.doAnswer(invocation -> {
            queued.add(invocation.getArgument(0));
            return null;
        }).when(work).execute(any(Runnable.class));
        final Settings settings = Settings.builder()
            .put("node.name", "rejoined-departure")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .build();
        try (
            var store = new org.opensearch.serverless.testkit.HookedBlobStore(new FsBlobStore(1024, createTempDir(), false));
            ServerlessNode node = new ServerlessNode(settings)
        ) {
            node.start();
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, 30_000L);
            final var member = new NodeLease("member", "same-process", "127.0.0.1:9300", Set.of("ingest"), 0L);
            plane.membership().renew(member);
            plane.membership().refresh();
            try (
                var scheduler = new ReconcileScheduler(
                    new BackgroundReconciler(node, plane),
                    pool,
                    clock::get,
                    TimeValue.timeValueSeconds(10),
                    TimeValue.timeValueMillis(100),
                    null
                ).start()
            ) {
                for (int departure = 0; departure < 2; departure++) {
                    plane.membership().release("member");
                    plane.membership().refresh();
                    store.onNextRegisterRead("members", "lease-member", () -> {});
                    assertEquals(1, queued.size());
                    queued.remove().run();
                    assertTrue("departure " + departure + " must check whether failover is needed", store.fired());
                    plane.membership().renew(member);
                    plane.membership().refresh();
                }
            }
        }
    }

    public void testRenewalDoesNotProbeDepartureButRestartStillDoes() throws Exception {
        final long ttl = 30_000L;
        final AtomicLong clock = new AtomicLong(1_000L);
        final ThreadPool pool = mock(ThreadPool.class);
        final ExecutorService work = mock(ExecutorService.class);
        when(pool.generic()).thenReturn(work);
        final Settings settings = Settings.builder()
            .put("node.name", "membership-delta")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .build();
        try (FsBlobStore store = new FsBlobStore(1024, createTempDir(), false); ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, ttl);
            final var before = new NodeLease("member", "process", "127.0.0.1:9300", Set.of("ingest"), 0L);
            plane.membership().renew(before);
            plane.membership().refresh();
            try (
                ReconcileScheduler scheduler = new ReconcileScheduler(
                    new BackgroundReconciler(node, plane),
                    pool,
                    clock::get,
                    TimeValue.timeValueSeconds(10),
                    TimeValue.timeValueMillis(100),
                    null
                ).start()
            ) {
                clock.addAndGet(ttl + 1);
                plane.membership().renew(before);
                plane.membership().refresh();
                verify(work, never()).execute(any(Runnable.class));
                final var restarted = new BlobLeaseMembership(
                    store.blobContainer(org.opensearch.serverless.metadata.RegisterMap.members(BlobPath.cleanPath())),
                    clock::get,
                    ttl
                );
                clock.addAndGet(ttl + 1);
                restarted.renew(new NodeLease("member", "new-process", "127.0.0.1:9300", Set.of("ingest"), 0L));
                plane.membership().refresh();
                verify(work).execute(any(Runnable.class));
                restarted.release("member");
                plane.membership().refresh();
                verify(work, times(2)).execute(any(Runnable.class));
            }
        }
    }
}
