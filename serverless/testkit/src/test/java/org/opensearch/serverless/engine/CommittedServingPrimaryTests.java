/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;

import java.io.IOException;

/** Native primary-serving boundary, separate from quorum authority and launcher integration. */
public class CommittedServingPrimaryTests extends EngineTestCase {
    public void testOrdinaryClientIndexDeleteAndGapFillingCannotChangeServingState() throws Exception {
        try (var store = createStore(); var serving = serving(store)) {
            expectThrows(IllegalStateException.class, () -> serving.index(indexForDoc(createParsedDoc("speculative", null))));
            expectThrows(
                IllegalStateException.class,
                () -> serving.delete(
                    new Engine.Delete(
                        "speculative",
                        newUid("speculative"),
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        primaryTerm.get(),
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(),
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0
                    )
                )
            );
            expectThrows(
                IllegalStateException.class,
                () -> serving.noOp(
                    new Engine.NoOp(
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        primaryTerm.get(),
                        Engine.Operation.Origin.PRIMARY,
                        System.nanoTime(),
                        "not resolved"
                    )
                )
            );
            assertEquals(-1, serving.getProcessedLocalCheckpoint());
            assertEquals(0, serving.fillSeqNoGaps(primaryTerm.get()));
            assertVisibleCount(serving, 0);
        }
    }

    public void testPreparedInsertUpdateDeleteAndReplayPreserveIdentityWithoutRepeatingCas() throws Exception {
        try (var store = createStore(); var serving = serving(store)) {
            final var first = engine.index(indexForDoc(createParsedDoc("id", "route")));
            engine.refresh("private speculative insert");
            assertVisibleCount(serving, 0);
            final var inserted = serving.index(resolvedIndex("id", "route", first.getSeqNo(), first.getVersion()));
            assertIdentity(first, inserted);
            assertVisibleCount(serving, 1);
            // Duplicate delivery must not create a second live document or allocate a new identity.
            final var duplicate = serving.index(resolvedIndex("id", "route", first.getSeqNo(), first.getVersion()));
            assertEquals(Engine.Result.Type.SUCCESS, duplicate.getResultType());
            assertVisibleCount(serving, 1);

            final var second = engine.index(indexForDoc(createParsedDoc("id", "route")));
            engine.refresh("private speculative update");
            try (var visible = serving.get(new Engine.Get(true, true, "id", newUid("id")), serving::acquireSearcher)) {
                assertEquals(first.getVersion(), visible.version());
            }
            assertIdentity(second, serving.index(resolvedIndex("id", "route", second.getSeqNo(), second.getVersion())));
            assertVisibleCount(serving, 1);
            try (var visible = serving.get(new Engine.Get(true, true, "id", newUid("id")), serving::acquireSearcher)) {
                assertEquals(second.getVersion(), visible.version());
                assertEquals(second.getSeqNo(), visible.docIdAndVersion().seqNo);
                assertEquals(second.getTerm(), visible.docIdAndVersion().primaryTerm);
            }
            final var removed = engine.delete(
                new Engine.Delete(
                    "id",
                    newUid("id"),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    primaryTerm.get(),
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    System.nanoTime(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0,
                    "route"
                )
            );
            assertVisibleCount(serving, 1);
            final var applied = serving.delete(
                new Engine.Delete(
                    "id",
                    newUid("id"),
                    removed.getSeqNo(),
                    removed.getTerm(),
                    removed.getVersion(),
                    VersionType.EXTERNAL,
                    Engine.Operation.Origin.PRIMARY,
                    System.nanoTime(),
                    999,
                    primaryTerm.get(),
                    "route"
                )
            );
            assertIdentity(removed, applied);
            assertVisibleCount(serving, 0);
        }
    }

    public void testOnlySuppliedResolvedNoOpsAdvanceMissingSequencePositions() throws Exception {
        try (var store = createStore(); var serving = serving(store)) {
            serving.index(resolvedIndex("id", null, 2, 1));
            assertEquals(-1, serving.getProcessedLocalCheckpoint());
            assertEquals(0, serving.fillSeqNoGaps(primaryTerm.get()));
            assertEquals(-1, serving.getProcessedLocalCheckpoint());
            serving.noOp(new Engine.NoOp(0, primaryTerm.get(), Engine.Operation.Origin.PRIMARY, System.nanoTime(), "resolved gap zero"));
            assertEquals(0, serving.getProcessedLocalCheckpoint());
            serving.noOp(new Engine.NoOp(1, primaryTerm.get(), Engine.Operation.Origin.PRIMARY, System.nanoTime(), "resolved gap one"));
            assertEquals(2, serving.getProcessedLocalCheckpoint());
            assertVisibleCount(serving, 1);
        }
    }

    public void testOutOfOrderUpdatesCannotResurrectDeletedOrReplaceNewerDocuments() throws Exception {
        try (var store = createStore(); var serving = serving(store)) {
            final var deletion = new Engine.Delete(
                "id",
                newUid("id"),
                2,
                primaryTerm.get(),
                3,
                VersionType.EXTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            );
            serving.delete(deletion);
            serving.index(resolvedIndex("id", null, 0, 1));
            serving.index(resolvedIndex("id", null, 1, 2));
            assertVisibleCount(serving, 0);
            serving.index(resolvedIndex("id", null, 3, 4));
            serving.delete(deletion);
            serving.index(resolvedIndex("id", null, 1, 2));
            assertVisibleCount(serving, 1);
            try (var visible = serving.get(new Engine.Get(true, true, "id", newUid("id")), serving::acquireSearcher)) {
                assertEquals(4, visible.version());
                assertEquals(3, visible.docIdAndVersion().seqNo);
            }
            assertEquals(3, serving.getProcessedLocalCheckpoint());
        }
    }

    private Engine.Index resolvedIndex(String id, String routing, long seqNo, long version) {
        final var doc = createParsedDoc(id, routing);
        return new Engine.Index(
            newUid(doc),
            doc,
            seqNo,
            primaryTerm.get(),
            version,
            VersionType.EXTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            true,
            999,
            primaryTerm.get()
        );
    }

    private static void assertIdentity(Engine.Result expected, Engine.Result actual) {
        assertEquals(Engine.Result.Type.SUCCESS, actual.getResultType());
        assertEquals(expected.getSeqNo(), actual.getSeqNo());
        assertEquals(expected.getTerm(), actual.getTerm());
        assertEquals(expected.getVersion(), actual.getVersion());
    }

    private CommittedServingEngine serving(Store store) throws IOException {
        final var config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
        store.createEmpty(config.getIndexSettings().getIndexVersionCreated().luceneVersion);
        final String uuid = Translog.createEmptyTranslog(
            config.getTranslogConfig().getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            primaryTerm.get()
        );
        store.associateIndexWithNewTranslog(uuid);
        final var serving = new CommittedServingEngine(config);
        try {
            serving.translogManager()
                .recoverFromTranslog(
                    createTranslogHandler(config.getIndexSettings(), serving),
                    serving.getProcessedLocalCheckpoint(),
                    Long.MAX_VALUE
                );
            return serving;
        } catch (IOException | RuntimeException | Error failure) {
            serving.close();
            throw failure;
        }
    }
}
