/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.log.JournalConfiguration;
import org.opensearch.serverless.log.JournalDiskIdentity;
import org.opensearch.serverless.log.OperationEnvelope;
import org.opensearch.serverless.log.RatisJournal;
import org.opensearch.serverless.log.ShardLogId;
import org.opensearch.serverless.metadata.JournalGroupCatalogRuntimeTests;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Real local committed journal to native shard replay; neither launcher wiring nor an AZ test. */
@SuppressFileSystems("*")
@ThreadLeakLingering(linger = 70_000)
@ThreadLeakFilters(filters = JournalGroupCatalogRuntimeTests.GlobalTimerFilter.class)
public class CommittedMutationReplayTests extends IndexShardTestCase {
    private final Map<Store, CommittedServingEngine> engines = new IdentityHashMap<>();

    public void testPreparedRefreshCannotExposeWritesUntilCommittedJournalApplication() throws Exception {
        final IndexShard preparation = newStartedShard(true);
        IndexShard serving = newServing();
        try (var fixture = fixture()) {
            final var stream = stream(fixture, serving);
            final var replay = replayer(fixture, serving, stream);
            final IndexShard other = newServing();
            try {
                final IndexShard target = serving;
                expectThrows(IllegalArgumentException.class, () -> replayer(fixture, target, stream, engines.get(other.store())));
                final var wrongIncarnation = new ShardLogId(
                    fixture.deployment,
                    "wrong-index-uuid",
                    stream.shardNumber(),
                    stream.activationId()
                );
                expectThrows(IllegalArgumentException.class, () -> replayer(fixture, target, wrongIncarnation));
            } finally {
                closeShards(other);
            }
            final long term = serving.getOperationPrimaryTerm();
            assertEquals(term, preparation.getOperationPrimaryTerm());
            final var first = indexDoc(preparation, "_doc", "id", "{}");
            assertTrue(first.isCreated());
            preparation.refresh("private speculative refresh");
            assertTrue(replay.poll().isEmpty());
            assertDocCount(serving, 0);
            final var original = new ResolvedMutation(
                UUID.randomUUID(),
                new Translog.Index("id", first.getSeqNo(), term, first.getVersion(), "{}".getBytes(StandardCharsets.UTF_8), null, -1),
                ResolvedMutation.Outcome.CREATED
            );
            final var checkpoint = fixture.leader.append(fixture.group.id(), envelope(serving, stream, original, 1))
                .toCompletableFuture()
                .get(20, TimeUnit.SECONDS);
            serving.refresh("committed but not applied");
            assertDocCount(serving, 0);
            final var applied = replay.poll();
            assertEquals(1, applied.size());
            assertEquals(original.operationId(), applied.getFirst().mutation().operationId());
            assertEquals(ResolvedMutation.Outcome.CREATED, applied.getFirst().mutation().outcome());
            assertEquals(checkpoint, replay.lastAppliedCheckpoint().orElseThrow());
            serving.refresh("applied committed insert");
            assertDocCount(serving, 1);

            final var deleted = preparation.applyDeleteOperationOnPrimary(
                Versions.MATCH_ANY,
                "id",
                null,
                VersionType.INTERNAL,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            );
            assertTrue(deleted.isFound());
            final var deletion = new ResolvedMutation(
                UUID.randomUUID(),
                new Translog.Delete("id", deleted.getSeqNo(), term, deleted.getVersion()),
                ResolvedMutation.Outcome.DELETED
            );
            fixture.leader.append(fixture.group.id(), envelope(serving, stream, deletion, 1))
                .toCompletableFuture()
                .get(20, TimeUnit.SECONDS);
            assertDocCount(serving, 1);
            assertEquals(ResolvedMutation.Outcome.DELETED, replay.poll().getFirst().mutation().outcome());
            serving.refresh("applied committed delete");
            assertDocCount(serving, 0);

            // The retained journal remains authoritative when a serving copy is recreated locally.
            serving = reinitShard(serving);
            recoverShardFromStore(serving);
            final var restarted = replayer(fixture, serving, stream);
            final var recovered = restarted.poll();
            assertEquals(2, recovered.size());
            assertEquals(ResolvedMutation.Outcome.CREATED, recovered.getFirst().mutation().outcome());
            assertEquals(ResolvedMutation.Outcome.DELETED, recovered.getLast().mutation().outcome());
            serving.refresh("journal replay after local restart");
            assertDocCount(serving, 0);
            assertEquals(1, serving.getProcessedLocalCheckpoint());
        } finally {
            closeShards(serving, preparation);
        }
    }

    public void testWrongOwnershipSchemaKindOrSequenceStopsBeforeServingMutation() throws Exception {
        try (var fixture = fixture()) {
            for (int invalid = 0; invalid < 8; invalid++) {
                final IndexShard serving = newServing();
                try {
                    final var stream = stream(fixture, serving);
                    final var replay = replayer(fixture, serving, stream);
                    final long term = serving.getOperationPrimaryTerm();
                    final var mutation = new ResolvedMutation(
                        UUID.randomUUID(),
                        new Translog.Index(
                            "id",
                            invalid == 3 ? 1 : 0,
                            invalid == 5 ? term + 1 : term,
                            1,
                            new byte[] { '{', '}' },
                            null,
                            -1
                        ),
                        ResolvedMutation.Outcome.CREATED
                    );
                    final var valid = envelope(serving, stream, mutation, invalid == 0 ? 2 : 1);
                    final var operation = new OperationEnvelope(
                        stream,
                        invalid == 2 ? OperationEnvelope.Kind.NO_OP
                            : invalid == 6 ? OperationEnvelope.Kind.SCHEMA
                            : OperationEnvelope.Kind.DOCUMENTS,
                        valid.producerEpoch(),
                        invalid == 4 ? term + 1 : term,
                        invalid == 1 ? valid.schemaVersion() + 1 : valid.schemaVersion(),
                        invalid == 7 ? new byte[] { 1 } : mutation.encode(16_384)
                    );
                    fixture.leader.append(fixture.group.id(), operation).toCompletableFuture().get(20, TimeUnit.SECONDS);
                    expectThrows(IOException.class, replay::poll);
                    assertEquals(-1, replay.throughIndex());
                    assertTrue(replay.lastAppliedCheckpoint().isEmpty());
                    expectThrows(IOException.class, replay::poll);
                    serving.refresh("rejected committed payload");
                    assertDocCount(serving, 0);
                    assertEquals(-1, serving.getProcessedLocalCheckpoint());
                } finally {
                    closeShards(serving);
                }
            }
        }
    }

    public void testBoundedPhysicalCursorSkipsOtherStreamsAndAppliesExplicitNoOp() throws Exception {
        final IndexShard serving = newServing();
        try (var fixture = fixture()) {
            final var stream = stream(fixture, serving);
            final var replay = new CommittedMutationReplayer(
                fixture.leader,
                fixture.group.id(),
                serving,
                engines.get(serving.store()),
                stream,
                1,
                serving.getOperationPrimaryTerm(),
                serving.indexSettings().getIndexMetadata().getMappingVersion(),
                1,
                1024 * 1024,
                16_384
            );
            final var noise = new OperationEnvelope(
                stream(fixture, serving),
                OperationEnvelope.Kind.DOCUMENTS,
                1,
                serving.getOperationPrimaryTerm(),
                0,
                new byte[] { 1 }
            );
            for (int i = 0; i < 3; i++) {
                fixture.leader.append(fixture.group.id(), noise).toCompletableFuture().get(20, TimeUnit.SECONDS);
            }
            final var noOp = new ResolvedMutation(
                UUID.randomUUID(),
                new Translog.NoOp(0, serving.getOperationPrimaryTerm(), "resolved sequence gap"),
                ResolvedMutation.Outcome.NO_OP
            );
            fixture.leader.append(fixture.group.id(), envelope(serving, stream, noOp, 1)).toCompletableFuture().get(20, TimeUnit.SECONDS);
            final var insert = new ResolvedMutation(
                UUID.randomUUID(),
                new Translog.Index("id", 1, serving.getOperationPrimaryTerm(), 1, new byte[] { '{', '}' }, null, -1),
                ResolvedMutation.Outcome.CREATED
            );
            final var last = fixture.leader.append(fixture.group.id(), envelope(serving, stream, insert, 1))
                .toCompletableFuture()
                .get(20, TimeUnit.SECONDS);
            final var results = new ArrayList<CommittedMutationReplayer.AppliedMutation>();
            for (int poll = 0; poll < 64 && replay.throughIndex() < last.index(); poll++) {
                final long before = replay.throughIndex();
                final var batch = replay.poll();
                assertTrue(batch.size() <= 1);
                assertTrue(replay.throughIndex() - before <= 1);
                results.addAll(batch);
            }
            assertEquals(last.index(), replay.throughIndex());
            assertEquals(last, replay.lastAppliedCheckpoint().orElseThrow());
            assertEquals(2, results.size());
            assertEquals(ResolvedMutation.Outcome.NO_OP, results.getFirst().mutation().outcome());
            assertEquals(ResolvedMutation.Outcome.CREATED, results.getLast().mutation().outcome());
            assertEquals(1, serving.getProcessedLocalCheckpoint());
            serving.refresh("selected committed stream");
            assertDocCount(serving, 1);
        } finally {
            closeShards(serving);
        }
    }

    public void testFailureRetainsOnlyTheSuccessfullyAppliedPrefix() throws Exception {
        final IndexShard serving = newServing();
        try (var fixture = fixture()) {
            final var stream = stream(fixture, serving);
            final var replay = replayer(fixture, serving, stream);
            final var first = new ResolvedMutation(
                UUID.randomUUID(),
                new Translog.Index("id", 0, serving.getOperationPrimaryTerm(), 1, new byte[] { '{', '}' }, null, -1),
                ResolvedMutation.Outcome.CREATED
            );
            final var checkpoint = fixture.leader.append(fixture.group.id(), envelope(serving, stream, first, 1))
                .toCompletableFuture()
                .get(20, TimeUnit.SECONDS);
            fixture.leader.append(fixture.group.id(), envelope(serving, stream, first, 2)).toCompletableFuture().get(20, TimeUnit.SECONDS);
            expectThrows(IOException.class, replay::poll);
            assertEquals(checkpoint.index(), replay.throughIndex());
            assertEquals(checkpoint, replay.lastAppliedCheckpoint().orElseThrow());
            expectThrows(IOException.class, replay::poll);
            serving.refresh("committed prefix before invalid binding");
            assertDocCount(serving, 1);
            assertEquals(0, serving.getProcessedLocalCheckpoint());
        } finally {
            closeShards(serving);
        }
    }

    private IndexShard newServing() throws IOException {
        return newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(config -> {
            final var engine = new CommittedServingEngine(config);
            engines.put(config.getStore(), engine);
            return engine;
        }));
    }

    private static ShardLogId stream(Fixture fixture, IndexShard shard) {
        return new ShardLogId(fixture.deployment, shard.shardId().getIndex().getUUID(), shard.shardId().id(), UUID.randomUUID());
    }

    private CommittedMutationReplayer replayer(Fixture fixture, IndexShard serving, ShardLogId stream) {
        return replayer(fixture, serving, stream, engines.get(serving.store()));
    }

    private static CommittedMutationReplayer replayer(
        Fixture fixture,
        IndexShard serving,
        ShardLogId stream,
        CommittedServingEngine servingEngine
    ) {
        return new CommittedMutationReplayer(
            fixture.leader,
            fixture.group.id(),
            serving,
            servingEngine,
            stream,
            1,
            serving.getOperationPrimaryTerm(),
            serving.indexSettings().getIndexMetadata().getMappingVersion(),
            4096,
            1024 * 1024,
            16_384
        );
    }

    private static OperationEnvelope envelope(IndexShard shard, ShardLogId stream, ResolvedMutation mutation, long epoch)
        throws IOException {
        return new OperationEnvelope(
            stream,
            mutation.outcome() == ResolvedMutation.Outcome.NO_OP ? OperationEnvelope.Kind.NO_OP : OperationEnvelope.Kind.DOCUMENTS,
            epoch,
            shard.getOperationPrimaryTerm(),
            shard.indexSettings().getIndexMetadata().getMappingVersion(),
            mutation.encode(16_384)
        );
    }

    private Fixture fixture() throws Exception {
        final var directories = new ArrayList<Path>();
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        final var sockets = new ArrayList<ServerSocket>();
        try {
            for (int i = 0; i < 3; i++) {
                final Path path = createTempDir();
                directories.add(path);
                final var socket = new ServerSocket(0, 0, InetAddress.getByName("127.0.0.1"));
                sockets.add(socket);
                peers.add(new JournalConfiguration.Peer(JournalDiskIdentity.discover(path), "127.0.0.1", socket.getLocalPort(), "az-" + i));
            }
        } finally {
            IOUtils.close(sockets);
        }
        final var keys = KeyStore.getInstance("PKCS12");
        try (var input = getClass().getResourceAsStream("/org/opensearch/serverless/testkit/tls-test-keystore.p12")) {
            keys.load(input, "testonly".toCharArray());
        }
        final var keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyFactory.init(keys, "testonly".toCharArray());
        final var trust = KeyStore.getInstance("PKCS12");
        try (var input = getClass().getResourceAsStream("/org/opensearch/serverless/testkit/tls-test-truststore.p12")) {
            trust.load(input, "testonly".toCharArray());
        }
        final var trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trust);
        final var fixture = new Fixture(UUID.randomUUID(), new JournalConfiguration.Group(UUID.randomUUID(), peers));
        final var limits = new JournalConfiguration.Limits(2, 16_384, 32, 1024 * 1024, 1024 * 1024, 2 * 1024 * 1024, 0, 10_000, 1);
        try {
            for (int i = 0; i < 3; i++) {
                fixture.journals.add(
                    RatisJournal.open(
                        new JournalConfiguration(
                            fixture.deployment,
                            peers.get(i).id(),
                            directories.get(i),
                            List.of(fixture.group),
                            true,
                            limits
                        ),
                        (X509KeyManager) keyFactory.getKeyManagers()[0],
                        (X509TrustManager) trustFactory.getTrustManagers()[0],
                        () -> Long.MAX_VALUE
                    )
                );
            }
            assertBusy(() -> {
                for (var journal : fixture.journals) {
                    if (journal.isLeaderReady(fixture.group.id())) {
                        fixture.leader = journal;
                        return;
                    }
                }
                fail("no ready journal leader");
            }, 30, TimeUnit.SECONDS);
            fixture.leader.activateGroup(fixture.group.id()).toCompletableFuture().get(20, TimeUnit.SECONDS);
            return fixture;
        } catch (Exception | Error failure) {
            IOUtils.closeWhileHandlingException(fixture);
            throw failure;
        }
    }

    private static final class Fixture implements Closeable {
        private final UUID deployment;
        private final JournalConfiguration.Group group;
        private final List<RatisJournal> journals = new ArrayList<>();
        private RatisJournal leader;

        private Fixture(UUID deployment, JournalConfiguration.Group group) {
            this.deployment = deployment;
            this.group = group;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(journals);
        }
    }
}
