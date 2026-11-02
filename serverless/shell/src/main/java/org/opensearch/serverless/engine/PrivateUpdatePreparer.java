/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.index.Term;
import org.opensearch.action.update.UpdateHelper;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.get.ShardGetService;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.script.ScriptService;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Native update translation over private document state using core's stored-source/routing loader
 * and UpdateHelper. The caller must perform retry lookup first and hold admitted ownership/schema,
 * mutation and lifecycle exclusion. Translation allocates no document sequence and applies no index
 * or delete. Its mutable core result belongs to that single admitted request; the host must account
 * for source/script/result memory and encode a bounded immutable outcome before proposal. Unexpected
 * native failures require host fencing/rebuild. This is not a script sandbox or request sequencer.
 */
final class PrivateUpdatePreparer {
    private final PrivatePreparationResources resources;
    private final IndexShard schema;
    private final PrivatePreparationEngine engine;
    private final DocumentMapper mapper;
    private final long mappingVersion;
    private final ShardGetService privateGet;
    private final NativeHelper helper;
    private boolean bindingFailed;

    PrivateUpdatePreparer(PrivatePreparationResources resources, IndexShard schema, ScriptService scripts) throws IOException {
        this.resources = Objects.requireNonNull(resources);
        this.schema = Objects.requireNonNull(schema);
        engine = resources.engine();
        if (engine.config().getShardId().equals(schema.shardId()) == false
            || engine.config().getStore() == schema.store()
            || engine.config().getMapperService() != schema.mapperService()) {
            throw new IllegalArgumentException("private update requires the same admitted shard and mapper context");
        }
        mapper = Objects.requireNonNull(schema.mapperService().documentMapper());
        mappingVersion = schema.indexSettings().getIndexMetadata().getMappingVersion();
        // Own metrics and no public get: this service only loads the supplied private engine result.
        privateGet = new ShardGetService(engine.config().getIndexSettings(), schema, schema.mapperService());
        helper = new NativeHelper(Objects.requireNonNull(scripts));
        requireBinding();
    }

    UpdateHelper.Result prepare(UpdateRequest request, LongSupplier nowInMillis) throws IOException {
        Objects.requireNonNull(request);
        Objects.requireNonNull(nowInMillis);
        requireBinding();
        if (schema.shardId().getIndexName().equals(request.index()) == false) {
            throw new IllegalArgumentException("private update request differs from the admitted concrete index");
        }
        final var invalid = request.validate();
        if (invalid != null) {
            throw invalid;
        }
        final long before = engine.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo();
        final var get = new Engine.Get(true, true, request.id(), new Term(IdFieldMapper.NAME, Uid.encodeId(request.id()))).setIfSeqNo(
            request.ifSeqNo()
        ).setIfPrimaryTerm(request.ifPrimaryTerm());
        final GetResult document;
        try (var result = engine.get(get, engine::acquireSearcher)) {
            document = privateGet.get(result, request.id(), new String[] { RoutingFieldMapper.NAME }, FetchSourceContext.FETCH_SOURCE);
        }
        requireBinding();
        final var result = helper.translate(schema.shardId(), request, document, nowInMillis);
        requireBinding();
        if (engine.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo() != before) {
            bindingFailed = true;
            throw new IOException("private history changed during update translation");
        }
        return result;
    }

    private void requireBinding() throws IOException {
        if (bindingFailed
            || resources.engine() != engine
            || schema.mapperService().documentMapper() != mapper
            || schema.indexSettings().getIndexMetadata().getMappingVersion() != mappingVersion) {
            bindingFailed = true;
            throw new IOException("private update schema/context requires rebuild");
        }
    }

    private static final class NativeHelper extends UpdateHelper {
        private NativeHelper(ScriptService scripts) {
            super(scripts);
        }

        private Result translate(ShardId shard, UpdateRequest request, GetResult document, LongSupplier nowInMillis) {
            return super.prepare(shard, request, document, nowInMillis);
        }
    }
}
