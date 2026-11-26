/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.repository;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * A record's top-level {@code global_state} member is kept opaque and unvalidated by every parse and rewrite, so a node
 * that cannot restore it never deletes it by re-encoding the record.
 */
public class SnapshotGlobalStateCarryTests extends OpenSearchTestCase {

    /** A record with every family of global state; the same text is the fixture the Rust format is checked against. */
    static final String WITH_STATE = "{\"repo\":\"repo\",\"name\":\"view\",\"uuid\":\"uuid\",\"shallow\":false,"
        + "\"start_time_in_millis\":1000,\"end_time_in_millis\":2000,"
        + "\"global_state\":{\"persistent_settings\":{\"cluster.a\":\"1\",\"cluster.b\":true},"
        + "\"index_templates\":{\"t\":{\"index_patterns\":[\"x-*\"],\"template\":{\"settings\":{\"number_of_shards\":1}}}},"
        + "\"component_templates\":{\"c\":{\"template\":{}}},"
        + "\"ingest_pipelines\":{\"p\":{\"processors\":[{\"set\":{\"field\":\"f\",\"value\":1}}]}},"
        + "\"search_pipelines\":{\"s\":{\"request_processors\":[]}},"
        + "\"stored_scripts\":{\"sc\":{\"script\":{\"lang\":\"painless\",\"source\":\"1\"}}}},"
        + "\"indices\":{\"index\":{\"uuid\":\"uuid-index\",\"number_of_shards\":1,"
        + "\"shards\":[{\"shard\":0,\"term\":1,\"files\":{\"file\":\"t=1\"}}]}}}";

    /** The record with its state replaced by the given JSON, or without one. */
    private static String withState(String state) {
        final int from = WITH_STATE.indexOf("\"global_state\"");
        final int to = WITH_STATE.indexOf("\"indices\"");
        return WITH_STATE.substring(0, from) + (state == null ? "" : "\"global_state\":" + state + ",") + WITH_STATE.substring(to);
    }

    private static SnapshotRecord parse(String json) throws IOException {
        return SnapshotRecord.fromStream(new BytesArray(json).streamInput());
    }

    public void testARoundTripKeepsTheStateByteForByte() throws Exception {
        assertEquals(WITH_STATE, parse(WITH_STATE).toBytes().utf8ToString());
    }

    public void testTheStorageGenerationRewriteKeepsIt() throws Exception {
        final String generation = org.opensearch.common.UUIDs.randomBase64UUID();
        final String rewritten = parse(WITH_STATE).withStorageGeneration(generation).toBytes().utf8ToString();
        assertEquals(WITH_STATE.replace("\"shallow\":false", "\"storage_generation\":\"" + generation + "\",\"shallow\":false"), rewritten);
    }

    public void testARecordWithoutItWritesNoMember() throws Exception {
        final String plain = withState(null);
        assertFalse(parse(plain).toBytes().utf8ToString().contains("global_state"));
    }

    public void testAMalformedStateDoesNotStopASummaryParseAndIsCarriedThrough() throws Exception {
        for (String corrupt : new String[] { "\"not an object\"", "[1,2]", "7", "{\"index_templates\":\"x\"}", "{\"unknown\":{}}" }) {
            final String json = withState(corrupt);
            assertEquals(json, parse(json).toBytes().utf8ToString());
        }
    }

    public void testTheConstructorCarriesIt() throws Exception {
        final var state = Map.of("persistent_settings", Map.of("k", "v"));
        final var record = new SnapshotRecord("repo", "view", "uuid", false, 1L, 2L, Map.of(), state);
        assertTrue(record.toBytes().utf8ToString().contains("\"global_state\":{\"persistent_settings\":{\"k\":\"v\"}}"));
        assertEquals(state, parse(record.toBytes().utf8ToString()).globalState());
    }

    /** Adoption goes fromStream then re-encodes into the name's register: the state must still be there on both reads. */
    public void testAdoptingALegacyRecordKeepsIt() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, 30_000L);
            plane.putRepository(new RepositoryDescriptor("repo", "fs", Map.of(), 1_000L));
            plane.createIndex(new IndexDescriptor("index", "uuid-index", 1, "{}", null));
            final var legacy = parse(WITH_STATE);
            final var bytes = legacy.toBytes();
            try (var input = bytes.streamInput()) {
                store.blobContainer(SnapshotPaths.snapshots(BlobPath.cleanPath()))
                    .writeBlobAtomic(legacy.key(), input, bytes.length(), true);
            }
            assertEquals(WITH_STATE, plane.snapshot("repo", "view").orElseThrow().toBytes().utf8ToString());
            assertEquals(WITH_STATE, plane.snapshot("repo", "view").orElseThrow().toBytes().utf8ToString());
        }
    }
}
