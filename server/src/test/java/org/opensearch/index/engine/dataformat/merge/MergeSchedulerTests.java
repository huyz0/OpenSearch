/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the tiering freeze semantics of {@link MergeScheduler}: when frozen, no new merges
 * may be triggered or force-merged, and the frozen state is derived from both an explicit
 * {@link MergeScheduler#freeze()} and the index's {@code INDEX_TIERING_STATE} setting.
 */
public class MergeSchedulerTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId(new Index("test", "_na_"), 0);
        threadPool = new TestThreadPool(getClass().getName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    private IndexSettings indexSettings(IndexModule.TieringState tieringState) {
        return IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexModule.INDEX_TIERING_STATE.getKey(), tieringState.name())
                .build()
        );
    }

    private MergeScheduler newScheduler(MergeHandler mergeHandler, IndexModule.TieringState tieringState) {
        return new MergeScheduler(mergeHandler, (result, merge) -> {}, () -> {}, shardId, indexSettings(tieringState), threadPool);
    }

    public void testFreezeBlocksTriggerMerges() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.freeze();
        assertTrue("scheduler must report frozen after freeze()", scheduler.isFrozen());

        scheduler.triggerMerges();
        // Frozen: the scheduler must not even ask the handler to find/register merges.
        verify(mergeHandler, never()).findAndRegisterMerges();
    }

    public void testTriggerMergesWhenNotFrozenInvokesHandler() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        when(mergeHandler.hasPendingMerges()).thenReturn(false);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        assertFalse("scheduler must not be frozen for a HOT index", scheduler.isFrozen());
        scheduler.triggerMerges();
        verify(mergeHandler, times(1)).findAndRegisterMerges();
    }

    public void testUnfreezeAllowsMergesAgain() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        when(mergeHandler.hasPendingMerges()).thenReturn(false);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.freeze();
        scheduler.triggerMerges();
        verify(mergeHandler, never()).findAndRegisterMerges();

        scheduler.unfreeze();
        assertFalse("scheduler must report unfrozen after unfreeze()", scheduler.isFrozen());
        scheduler.triggerMerges();
        // unfreeze() itself fires triggerMerges() on a real frozen→unfrozen transition (resumes merges
        // without needing a follow-up call), and the explicit triggerMerges() above adds a second
        // invocation. Hence findAndRegisterMerges is invoked twice.
        verify(mergeHandler, times(2)).findAndRegisterMerges();
    }

    public void testIsFrozenReflectsHotToWarmTieringState() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT_TO_WARM);
        assertTrue("HOT_TO_WARM tiering state must report frozen", scheduler.isFrozen());
    }

    public void testIsNotFrozenForHotTieringState() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);
        assertFalse("HOT tiering state must not report frozen", scheduler.isFrozen());
    }

    public void testForceMergeSkipsAfterShutdown() throws IOException {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.shutdown();

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        verify(mergeHandler, never()).findForceMerges(anyInt());
    }

    public void testForceMergeAbortsRemainingMergesOnShutdown() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);

        Segment s1 = new Segment(1L, Map.of());
        Segment s2 = new Segment(2L, Map.of());
        OneMerge merge1 = new OneMerge(List.of(s1));
        OneMerge merge2 = new OneMerge(List.of(s2));

        when(mergeHandler.findForceMerges(1)).thenReturn(List.of(merge1, merge2));
        when(mergeHandler.doMerge(merge1)).thenReturn(new MergeResult(Map.of()));
        List<OneMerge> unregistered = recordUnregistered(mergeHandler);

        final java.util.concurrent.atomic.AtomicReference<MergeScheduler> schedulerRef =
            new java.util.concurrent.atomic.AtomicReference<>();

        MergeScheduler scheduler = new MergeScheduler(
            mergeHandler,
            (result, merge) -> { schedulerRef.get().shutdown(); },
            () -> {},
            shardId,
            indexSettings(IndexModule.TieringState.HOT),
            threadPool
        );
        schedulerRef.set(scheduler);

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        verify(mergeHandler).doMerge(merge1);
        verify(mergeHandler, never()).doMerge(merge2);
        // merge2 was registered up front by findForceMerges but never ran — its segments must be handed
        // back, otherwise they stay in currentlyMergingSegments forever and are silently excluded from
        // every future merge.
        assertEquals("the un-run merge must be unregistered on the shutdown exit", List.of(merge2), unregistered);
    }

    /**
     * A force merge that fails part-way must hand back the registrations of every group it never
     * reached. {@code findForceMerges} registers ALL selected groups up front (they are not queued as
     * pending merges, the force-merge caller runs them itself); only the failing group is cleaned up by
     * {@code onMergeFailure}. Without the fix, groups 2..N stayed in {@code currentlyMergingSegments}
     * for the lifetime of the engine — invisible to every later background and force merge, so a
     * subsequent force merge "succeeds" without ever reaching the requested segment count.
     */
    public void testForceMergeUnregistersRemainingMergesOnFailure() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);

        OneMerge merge1 = new OneMerge(List.of(new Segment(1L, Map.of())));
        OneMerge merge2 = new OneMerge(List.of(new Segment(2L, Map.of())));
        OneMerge merge3 = new OneMerge(List.of(new Segment(3L, Map.of())));

        when(mergeHandler.findForceMerges(1)).thenReturn(List.of(merge1, merge2, merge3));
        when(mergeHandler.doMerge(merge1)).thenThrow(new IOException("merge blew up"));
        List<OneMerge> unregistered = recordUnregistered(mergeHandler);

        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            expectThrows(IOException.class, () -> scheduler.forceMerge(1));
        } finally {
            Thread.currentThread().setName(oldName);
        }

        // The failing merge cleans itself up through onMergeFailure...
        verify(mergeHandler).onMergeFailure(merge1);
        verify(mergeHandler, never()).doMerge(merge2);
        verify(mergeHandler, never()).doMerge(merge3);
        // ...and everything after it is handed back explicitly.
        assertEquals("all un-run merges must be unregistered", List.of(merge2, merge3), unregistered);
    }

    /**
     * A force merge that completes normally must not unregister anything — every selected group ran and
     * owns its own cleanup via {@code onMergeFinished}.
     */
    public void testForceMergeDoesNotUnregisterWhenAllMergesRun() throws Exception {
        MergeHandler mergeHandler = mock(MergeHandler.class);

        OneMerge merge1 = new OneMerge(List.of(new Segment(1L, Map.of())));
        OneMerge merge2 = new OneMerge(List.of(new Segment(2L, Map.of())));
        when(mergeHandler.findForceMerges(1)).thenReturn(List.of(merge1, merge2));
        when(mergeHandler.doMerge(merge1)).thenReturn(new MergeResult(Map.of()));
        when(mergeHandler.doMerge(merge2)).thenReturn(new MergeResult(Map.of()));

        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        verify(mergeHandler).doMerge(merge1);
        verify(mergeHandler).doMerge(merge2);
        verify(mergeHandler, never()).unregisterMerges(any());
    }

    /**
     * The frozen state must be re-checked after the force-merge lock is acquired. The engine's
     * pre-flight {@code isFrozenForTiering()} check happens before the caller parks on the lock, and a
     * caller can sit there for the whole duration of the force merge already running (minutes) — long
     * enough for tiering to start, drain, and take its "last ever" flush of the shard.
     */
    public void testForceMergeAbandonedWhenFrozenAfterAcquiringLock() throws IOException {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        MergeScheduler scheduler = newScheduler(mergeHandler, IndexModule.TieringState.HOT);

        scheduler.freeze();

        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("TEST-" + ThreadPool.Names.FORCE_MERGE + "-0");
        try {
            scheduler.forceMerge(1);
        } finally {
            Thread.currentThread().setName(oldName);
        }

        verify(mergeHandler, never()).findForceMerges(anyInt());
        assertEquals("the abandoned force merge must give back its active-merge slot", 0, scheduler.getActiveMergeCount());
    }

    /**
     * A merge submission the executor rejects must give back the active-merge slot it took. Otherwise
     * {@code activeMerges} stays permanently above zero and no drain listener can ever fire on the
     * shard again, so every subsequent tiering prepare times out.
     */
    public void testRejectedMergeSubmissionReleasesActiveMergeSlot() {
        MergeHandler mergeHandler = mock(MergeHandler.class);
        when(mergeHandler.hasPendingMerges()).thenReturn(true, false);
        when(mergeHandler.getNextMerge()).thenReturn(new OneMerge(List.of(new Segment(1L, Map.of()))));

        ThreadPool rejectingThreadPool = mock(ThreadPool.class);
        ExecutorService rejectingExecutor = mock(ExecutorService.class);
        when(rejectingThreadPool.executor(ThreadPool.Names.MERGE)).thenReturn(rejectingExecutor);
        doThrow(new OpenSearchRejectedExecutionException("merge queue full")).when(rejectingExecutor).execute(any(Runnable.class));

        MergeScheduler scheduler = new MergeScheduler(
            mergeHandler,
            (result, merge) -> {},
            () -> {},
            shardId,
            indexSettings(IndexModule.TieringState.HOT),
            rejectingThreadPool
        );

        scheduler.triggerMerges();

        assertEquals("a rejected submission must not leak an active-merge slot", 0, scheduler.getActiveMergeCount());
        verify(mergeHandler).onMergeFailure(any(OneMerge.class));
    }

    /**
     * Installs a recorder on {@code unregisterMerges} and returns the (initially empty) list it appends
     * to, so a test can assert exactly which un-run merges were handed back.
     */
    private static List<OneMerge> recordUnregistered(MergeHandler mergeHandler) {
        List<OneMerge> unregistered = new ArrayList<>();
        doAnswer(invocation -> {
            unregistered.addAll(invocation.<Collection<OneMerge>>getArgument(0));
            return null;
        }).when(mergeHandler).unregisterMerges(any());
        return unregistered;
    }
}
