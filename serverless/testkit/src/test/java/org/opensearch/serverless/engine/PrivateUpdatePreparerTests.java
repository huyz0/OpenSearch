/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.opensearch.Version;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@SuppressFileSystems("*")
public class PrivateUpdatePreparerTests extends IndexShardTestCase {
    private final Map<Store, CommittedServingEngine> engines = new IdentityHashMap<>();

    public void testMergeAndDetectedNoopReadPrivateStateWithoutConsumingASequence() throws Exception {
        final var serving = serving(false);
        try (var resources = resources(serving); var scripts = scripts(Map.of())) {
            assertEquals(
                Engine.Result.Type.SUCCESS,
                apply(resources, serving, new IndexRequest().id("id").routing("route").source("{\"count\":1}", MediaTypeRegistry.JSON))
                    .getResultType()
            );
            final var adapter = new PrivateUpdatePreparer(resources, serving, scripts);
            final var plan = adapter.prepare(
                new UpdateRequest(serving.shardId().getIndexName(), "id").doc(
                    new IndexRequest().source("{\"count\":2}", MediaTypeRegistry.JSON)
                ),
                () -> 123
            );
            assertEquals(DocWriteResponse.Result.UPDATED, plan.getResponseResult());
            final IndexRequest translated = plan.action();
            assertEquals("route", translated.routing());
            assertEquals(0, translated.ifSeqNo());
            assertEquals(serving.getOperationPrimaryTerm(), translated.ifPrimaryTerm());
            assertEquals(Map.of("count", 2), translated.sourceAsMap());
            assertEquals(0, resources.engine().getProcessedLocalCheckpoint());
            assertDocCount(serving, 0);
            final var updated = apply(resources, serving, translated);
            assertEquals(1, updated.getSeqNo());
            assertEquals(2, updated.getVersion());
            final var noop = adapter.prepare(
                new UpdateRequest(serving.shardId().getIndexName(), "id").doc(
                    new IndexRequest().source("{\"count\":2}", MediaTypeRegistry.JSON)
                ),
                () -> 456
            );
            assertEquals(DocWriteResponse.Result.NOOP, noop.getResponseResult());
            final UpdateResponse response = noop.action();
            assertEquals(1, response.getSeqNo());
            assertEquals(2, response.getVersion());
            final var durable = ResolvedResultOnly.noOp(response, 4096);
            assertArrayEquals(durable.json(), ResolvedResultOnly.decode(durable.encode(4096), 4096).json());
            assertEquals(1, resources.engine().getProcessedLocalCheckpoint());
            assertDocCount(serving, 0);
        } finally {
            closeShards(serving);
        }
    }

    public void testScriptsUsePrivateRoutingSourceAndClockAndTranslateDeleteAndNoop() throws Exception {
        final var calls = new AtomicInteger();
        final var serving = serving(false);
        try (var resources = resources(serving); var scripts = scripts(Map.of("increment", vars -> {
            calls.incrementAndGet();
            final var ctx = context(vars);
            assertEquals("route", ctx.get("_routing"));
            assertEquals(123L, ctx.get("_now"));
            source(ctx).put("count", ((Number) source(ctx).get("count")).intValue() + 1);
            return null;
        }, "noop", vars -> {
            calls.incrementAndGet();
            context(vars).put("op", "none");
            return null;
        }, "delete", vars -> {
            calls.incrementAndGet();
            context(vars).put("op", "delete");
            return null;
        }))) {
            apply(resources, serving, new IndexRequest().id("id").routing("route").source("{\"count\":1}", MediaTypeRegistry.JSON));
            final var adapter = new PrivateUpdatePreparer(resources, serving, scripts);
            final var increment = adapter.prepare(update(serving, "id", "increment"), () -> 123);
            assertEquals(1, calls.get());
            assertEquals(Map.of("count", 2), ((IndexRequest) increment.action()).sourceAsMap());
            assertEquals(0, resources.engine().getProcessedLocalCheckpoint());
            final var noop = adapter.prepare(update(serving, "id", "noop"), () -> 124);
            assertEquals(DocWriteResponse.Result.NOOP, noop.getResponseResult());
            assertEquals(2, calls.get());
            assertEquals(Map.of("count", 1), noop.updatedSourceAsMap());
            assertEquals(0, resources.engine().getProcessedLocalCheckpoint());
            final var delete = adapter.prepare(update(serving, "id", "delete"), () -> 125);
            assertEquals(DocWriteResponse.Result.DELETED, delete.getResponseResult());
            final DeleteRequest translated = delete.action();
            assertEquals("route", translated.routing());
            assertEquals(0, translated.ifSeqNo());
            assertEquals(serving.getOperationPrimaryTerm(), translated.ifPrimaryTerm());
            final var removed = resources.delete(
                resources.engine()
                    .prepareDelete(
                        "id",
                        translated.routing(),
                        -2,
                        serving.getOperationPrimaryTerm(),
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        translated.ifSeqNo(),
                        translated.ifPrimaryTerm()
                    )
            );
            assertTrue(removed.isFound());
            assertEquals(1, removed.getSeqNo());
            assertEquals(3, calls.get());
            assertDocCount(serving, 0);
        } finally {
            closeShards(serving);
        }
    }

    public void testUpsertsAndStaleCasUseCoreSemanticsBeforeScriptExecution() throws Exception {
        final var calls = new AtomicInteger();
        final var serving = serving(false);
        try (var resources = resources(serving); var scripts = scripts(Map.of("increment", vars -> {
            calls.incrementAndGet();
            final var data = source(context(vars));
            data.put("count", ((Number) data.get("count")).intValue() + 1);
            return null;
        }, "noop", vars -> {
            calls.incrementAndGet();
            context(vars).put("op", "none");
            return null;
        }))) {
            final var adapter = new PrivateUpdatePreparer(resources, serving, scripts);
            final var upsert = adapter.prepare(
                new UpdateRequest(serving.shardId().getIndexName(), "id").doc(
                    new IndexRequest().source("{\"count\":1}", MediaTypeRegistry.JSON)
                ).docAsUpsert(true),
                () -> 123
            );
            assertEquals(DocWriteResponse.Result.CREATED, upsert.getResponseResult());
            final IndexRequest create = upsert.action();
            assertEquals(DocWriteRequest.OpType.CREATE, create.opType());
            assertEquals(-1, resources.engine().getProcessedLocalCheckpoint());
            assertEquals(0, apply(resources, serving, create).getSeqNo());
            expectThrows(
                DocumentMissingException.class,
                () -> adapter.prepare(
                    new UpdateRequest(serving.shardId().getIndexName(), "absent").doc(
                        new IndexRequest().source("{\"count\":1}", MediaTypeRegistry.JSON)
                    ),
                    () -> 123
                )
            );
            final var stale = update(serving, "id", "increment").setIfSeqNo(999).setIfPrimaryTerm(serving.getOperationPrimaryTerm());
            expectThrows(VersionConflictEngineException.class, () -> adapter.prepare(stale, () -> 123));
            assertEquals(0, calls.get());
            assertEquals(0, resources.engine().getProcessedLocalCheckpoint());
            final var scripted = adapter.prepare(
                update(serving, "upsert", "increment").scriptedUpsert(true)
                    .upsert(new IndexRequest().source("{\"count\":10}", MediaTypeRegistry.JSON)),
                () -> 123
            );
            assertEquals(DocWriteResponse.Result.CREATED, scripted.getResponseResult());
            assertEquals(Map.of("count", 11), ((IndexRequest) scripted.action()).sourceAsMap());
            assertEquals(1, calls.get());
            final var none = adapter.prepare(
                update(serving, "missing-noop", "noop").scriptedUpsert(true)
                    .upsert(new IndexRequest().source("{\"count\":10}", MediaTypeRegistry.JSON)),
                () -> 123
            );
            assertEquals(DocWriteResponse.Result.NOOP, none.getResponseResult());
            assertEquals(2, calls.get());
            assertEquals(0, resources.engine().getProcessedLocalCheckpoint());
            assertDocCount(serving, 0);
        } finally {
            closeShards(serving);
        }
    }

    public void testSourceFiltersAndSchemaBindingAreCheckedBeforeAnotherScript() throws Exception {
        final var calls = new AtomicInteger();
        final var serving = serving(true);
        final var other = serving(false);
        try (var resources = resources(serving); var scripts = scripts(Map.of("check", vars -> {
            calls.incrementAndGet();
            assertFalse(source(context(vars)).containsKey("secret"));
            return null;
        }))) {
            apply(
                resources,
                serving,
                new IndexRequest().id("id").routing("route").source("{\"count\":1,\"secret\":\"hidden\"}", MediaTypeRegistry.JSON)
            );
            final var adapter = new PrivateUpdatePreparer(resources, serving, scripts);
            assertEquals(DocWriteResponse.Result.UPDATED, adapter.prepare(update(serving, "id", "check"), () -> 123).getResponseResult());
            assertEquals(1, calls.get());
            expectThrows(IllegalArgumentException.class, () -> new PrivateUpdatePreparer(resources, other, scripts));
            expectThrows(
                IllegalArgumentException.class,
                () -> adapter.prepare(new UpdateRequest("foreign-index", "id").script(script("check")), () -> 123)
            );
            final long version = serving.indexSettings().getIndexMetadata().getMappingVersion();
            updateMappings(
                serving,
                IndexMetadata.builder(serving.indexSettings().getIndexMetadata())
                    .putMapping("{\"properties\":{\"fresh\":{\"type\":\"boolean\"}}}")
                    .mappingVersion(version)
                    .build()
            );
            expectThrows(IOException.class, () -> adapter.prepare(update(serving, "id", "check"), () -> 123));
            assertEquals(1, calls.get());
            assertEquals(0, resources.engine().getProcessedLocalCheckpoint());
            assertDocCount(serving, 0);
        } finally {
            closeShards(serving, other);
        }
    }

    private static UpdateRequest update(IndexShard serving, String id, String script) {
        return new UpdateRequest(serving.shardId().getIndexName(), id).script(script(script));
    }

    private static Script script(String name) {
        return new Script(ScriptType.INLINE, MockScriptEngine.NAME, name, Map.of());
    }

    private static ScriptService scripts(Map<String, Function<Map<String, Object>, Object>> scripts) {
        final var engine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Map.of());
        return new ScriptService(Settings.EMPTY, Map.of(engine.getType(), engine), ScriptModule.CORE_CONTEXTS);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> context(Map<String, Object> vars) {
        return (Map<String, Object>) vars.get("ctx");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> source(Map<String, Object> ctx) {
        return (Map<String, Object>) ctx.get("_source");
    }

    private IndexShard serving(boolean filtered) throws IOException {
        final var metadata = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUID.randomUUID().toString())
            )
            .primaryTerm(0, primaryTerm)
            .putMapping(
                "{"
                    + (filtered ? "\"_source\":{\"excludes\":[\"secret\"]}," : "")
                    + "\"properties\":{\"count\":{\"type\":\"integer\"},\"secret\":{\"type\":\"keyword\"}}}"
            )
            .build();
        final var routing = TestShardRouting.newShardRouting(
            new ShardId(metadata.getIndex(), 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        final var factory = new EngineBackedIndexerFactory(configuration -> {
            final var engine = new CommittedServingEngine(configuration);
            engines.put(configuration.getStore(), engine);
            return engine;
        });
        return newStartedShard(primary -> newShard(routing, metadata, null, factory, () -> {}, RetentionLeaseSyncer.EMPTY, null), true);
    }

    private PrivatePreparationResources resources(IndexShard serving) throws IOException {
        final var group = UUID.randomUUID();
        return PrivatePreparationResources.openEmpty(
            new PreparationEngineAdmission(Set.of(group), 1, 64L << 20, 64L << 20),
            group,
            engines.get(serving.store()),
            createTempDir(),
            64L << 20,
            4L << 20
        );
    }

    private static Engine.IndexResult apply(PrivatePreparationResources resources, IndexShard serving, IndexRequest request)
        throws IOException {
        final var engine = resources.engine();
        return resources.index(
            engine.prepareIndex(
                engine.config().getDocumentMapperForTypeSupplier().get(),
                new SourceToParse(
                    serving.shardId().getIndexName(),
                    request.id(),
                    request.source(),
                    request.getContentType(),
                    request.routing()
                ),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                serving.getOperationPrimaryTerm(),
                request.opType() == DocWriteRequest.OpType.CREATE ? Versions.MATCH_DELETED : Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                -1,
                false,
                request.ifSeqNo(),
                request.ifPrimaryTerm()
            )
        );
    }
}
