/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.store.WalStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The shard write-ahead log under a crash or a lost answer at every store call it makes.
 *
 * <p>Each test records the calls a writer or a successor makes, then faults each one in turn with every
 * {@link CrashPointBlobStore.Fault} and checks, through a later owner, that every acknowledged record is
 * replayed and no record that was never sent appears. Subclasses bind a store: the filesystem in the default
 * test run, the GCS and Azure emulators in {@code emulatorTest}.
 */
public abstract class WalCrashPointTestCase extends OpenSearchTestCase {

    /**
     * Returns the store every run shares. Each run writes under its own path.
     *
     * @return the store
     * @throws Exception if it cannot be made
     */
    protected abstract BlobStore newStore() throws Exception;

    private static WalRecord record(String id, long seqNo, long term) {
        return new WalRecord(id, "{\"msg\":\"" + id + "\"}", seqNo, term, 1L);
    }

    private static Set<String> ids(List<WalRecord> records) {
        final Set<String> ids = new HashSet<>();
        for (WalRecord record : records) {
            ids.add(record.id());
        }
        return ids;
    }

    private static BlobPath shard(BlobPath root) {
        return root.add("segments").add("alpha#uuid#0");
    }

    /** Recovery as the next owner runs it: fence every older term, then read what is replayable. */
    private static Set<String> recoverAt(BlobStore store, BlobPath root, long term) throws Exception {
        final WalStore next = new WalStore(store, shard(root));
        next.fenceOlderTerms(term);
        return ids(next.replayableAfterFencing());
    }

    /**
     * Appends one record and acknowledges it only if the append returned. A failed append is not acknowledged and
     * the writer moves on to the next record, as a node does for the next request; only a crash stops it. That is
     * what exercises the writer after an append whose outcome it could not learn.
     */
    private static void appendAndAcknowledge(WalStore writer, long term, String id, long seqNo, CrashPointExplorer.Ledger ledger)
        throws CrashPointBlobStore.CrashedException {
        ledger.attempted(id);
        try {
            writer.append(term, List.of(record(id, seqNo, term)));
        } catch (java.io.IOException e) {
            if (e instanceof CrashPointBlobStore.CrashedException crashed) {
                throw crashed;
            }
            return;
        }
        ledger.acknowledged(id);
    }

    /** A writer that opens its term and appends one record per call, acknowledging each when the append returns. */
    public void testEveryAcknowledgedAppendSurvivesAFaultAtAnyCallOfTheWriter() throws Exception {
        final BlobStore store = newStore();
        final var result = CrashPointExplorer.exploreAll(store, BlobPath.cleanPath().add("writer"), (faulty, root, ledger) -> {
            final WalStore writer = new WalStore(faulty, shard(root));
            writer.establish(1L, null);
            for (int i = 0; i < 4; i++) {
                appendAndAcknowledge(writer, 1L, "w" + i, i, ledger);
            }
        }, (honest, root, ledger, where) -> ledger.assertRecovered(recoverAt(honest, root, 2L), where));
        logger.info("writer: {} calls, {} faulted runs", result.recorded().size(), result.explored().size());
        assertFalse(result.explored().isEmpty());
    }

    /**
     * A successor that fences, opens its own term and appends, faulted at each of its calls. A third owner must
     * still find what the first acknowledged and what the successor acknowledged before it stopped.
     */
    public void testATakeoverInterruptedAtAnyCallLosesNothingAcknowledged() throws Exception {
        final BlobStore store = newStore();
        final var result = CrashPointExplorer.exploreAll(store, BlobPath.cleanPath().add("takeover"), (faulty, root, ledger) -> {
            // The first owner runs on the honest store: only the successor's calls are faulted.
            final WalStore first = new WalStore(store, shard(root));
            first.establish(1L, null);
            for (int i = 0; i < 2; i++) {
                ledger.attempted("first-" + i);
                first.append(1L, List.of(record("first-" + i, i, 1L)));
                ledger.acknowledged("first-" + i);
            }
            final WalStore successor = new WalStore(faulty, shard(root));
            successor.fenceOlderTerms(2L);
            successor.replayableAfterFencing();
            successor.establish(2L, null);
            for (int i = 0; i < 2; i++) {
                appendAndAcknowledge(successor, 2L, "second-" + i, 2 + i, ledger);
            }
        }, (honest, root, ledger, where) -> ledger.assertRecovered(recoverAt(honest, root, 3L), where));
        logger.info("takeover: {} calls, {} faulted runs", result.recorded().size(), result.explored().size());
        assertFalse(result.explored().isEmpty());
    }
}
