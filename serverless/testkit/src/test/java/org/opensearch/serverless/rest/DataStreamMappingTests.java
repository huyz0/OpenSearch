/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.metadata.TemplateResolver;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class DataStreamMappingTests extends OpenSearchTestCase {
    public void testTimestampFieldNamesAreJsonEscaped() throws Exception {
        final String field = "event\"time\\stamp";
        final var request = DataStreamHandler.withTimestamp(new IndexAdminHandler.CreateRequest(1, null, Settings.EMPTY), field);
        assertEquals(
            Map.of(
                "properties",
                Map.of(field, Map.of("type", "date")),
                "_data_stream_timestamp",
                Map.of("enabled", true, "timestamp_field", Map.of("name", field))
            ),
            mapping(request)
        );
    }

    public void testTimestampMappingKeepsAnExplicitShardCount() throws Exception {
        final var original = IndexAdminHandler.CreateRequest.parse(Map.of("settings", Map.of("number_of_shards", 1)), "", 1);
        assertTrue(original.explicitShards);
        final var request = DataStreamHandler.withTimestamp(original, "@timestamp");
        assertTrue("adding a timestamp mapping must preserve explicit shard selection", request.explicitShards);
        final var inherited = new TemplateResolver.Inherited(Map.of("number_of_shards", 5), Map.of(), "template");
        assertEquals(1, IndexAdminHandler.CreateRequest.shardsWith(request.shards, request.explicitShards, inherited));
    }

    public void testExistingTimestampTypeAndFormatArePreserved() throws Exception {
        final var definition = Map.of("type", "date_nanos", "format", "strict_date_optional_time_nanos");
        final var mapping = Map.of(
            "properties",
            Map.of("@timestamp", definition),
            "_data_stream_timestamp",
            Map.of("enabled", true, "timestamp_field", Map.of("name", "@timestamp"))
        );
        final var original = new IndexAdminHandler.CreateRequest(
            1,
            "{\"properties\":{\"@timestamp\":{\"type\":\"date_nanos\",\"format\":\"strict_date_optional_time_nanos\"}}}",
            Settings.EMPTY
        );
        assertEquals(mapping, mapping(DataStreamHandler.withTimestamp(original, "@timestamp")));
    }

    private static Map<String, Object> mapping(IndexAdminHandler.CreateRequest request) {
        return XContentHelper.convertToMap(new BytesArray(request.mapping), false, XContentType.JSON).v2();
    }

    public void testAProvidedMappingCannotDisableTheTimestampGuard() throws Exception {
        final var original = new IndexAdminHandler.CreateRequest(
            1,
            "{\"_data_stream_timestamp\":{\"enabled\":false,\"timestamp_field\":{\"name\":\"other\"}}}",
            Settings.EMPTY
        );
        final var selected = DataStreamHandler.withTimestamp(original, "@timestamp");
        assertEquals(
            Map.of("enabled", true, "timestamp_field", Map.of("name", "@timestamp")),
            mapping(selected).get("_data_stream_timestamp")
        );
    }
}
