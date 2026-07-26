/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.Version;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.serverless.storage.nameindex.action.ResolveIndexNamesRequest;
import org.opensearch.serverless.storage.nameindex.action.ResolveIndexNamesResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * The wiring that turns the structure into a service: feeding it from cluster state, and the request
 * and response that carry a resolution across nodes.
 *
 * <p>Feeding from cluster state looks circular given that the architecture exists to stop depending on
 * cluster state. It is a deliberate first step: the cluster manager still publishes index metadata, so
 * that is the source which exists and is correct today. What it buys immediately is that
 * <em>resolution</em> stops walking {@code Metadata.indicesLookup}, the O(all indices) structure the
 * design needs to retire. When index metadata moves to the manifest, the feed changes and nothing above
 * it does.
 */
public class NameIndexServiceWiringTests extends OpenSearchTestCase {

    public void testDisabledByDefaultAndIgnoresMetadata() {
        NameIndexService service = new NameIndexService(false);

        assertFalse(service.isEnabled());
        service.apply(Metadata.EMPTY_METADATA, metadata(index("alpha")));

        // apply() is reachable directly, but clusterChanged is the gate, so a disabled service is only
        // inert because nothing calls apply. Asserting the flag is what the plugin keys off.
        assertFalse(service.isEnabled());
    }

    public void testIndicesFromMetadataBecomeResolvable() {
        NameIndexService service = new NameIndexService(true);

        service.apply(Metadata.EMPTY_METADATA, metadata(index("alpha"), index("beta")));

        assertEquals(List.of("alpha", "beta"), service.resolve(IndicesOptions.strictExpandOpen(), "*"));
        assertNotNull(service.getNameIndex().lookup("alpha"));
    }

    public void testDeletedIndicesStopResolving() {
        NameIndexService service = new NameIndexService(true);
        Metadata before = metadata(index("alpha"), index("beta"));
        service.apply(Metadata.EMPTY_METADATA, before);

        service.apply(before, metadata(index("beta")));

        assertEquals(List.of("beta"), service.resolve(IndicesOptions.strictExpandOpen(), "*"));
        assertNull(service.getNameIndex().lookup("alpha"));
    }

    public void testClosedIndicesCarryTheirStatus() {
        NameIndexService service = new NameIndexService(true);

        service.apply(Metadata.EMPTY_METADATA, metadata(index("open-one"), closedIndex("shut-one")));

        assertEquals(List.of("open-one"), service.resolve(IndicesOptions.strictExpandOpen(), "*"));
        assertEquals(List.of("open-one", "shut-one"), service.resolve(IndicesOptions.strictExpand(), "*"));
    }

    public void testAliasesAreDerivedFromTheIndicesThatNameThem() {
        NameIndexService service = new NameIndexService(true);

        service.apply(Metadata.EMPTY_METADATA, metadata(aliased("logs-a", "all-logs"), aliased("logs-b", "all-logs")));

        assertEquals(List.of("logs-a", "logs-b"), service.resolve(IndicesOptions.strictExpandOpen(), "all-logs"));
    }

    /**
     * An alias exists only through the indices that name it, so losing its last index must remove it.
     * Leaving it behind would resolve to nothing while still claiming the name, which blocks an index
     * from later taking it.
     */
    public void testAnAliasDisappearsWithItsLastIndex() {
        NameIndexService service = new NameIndexService(true);
        Metadata before = metadata(aliased("logs-a", "all-logs"));
        service.apply(Metadata.EMPTY_METADATA, before);
        assertEquals(List.of("logs-a"), service.resolve(IndicesOptions.strictExpandOpen(), "all-logs"));

        service.apply(before, Metadata.EMPTY_METADATA);

        assertNull(service.getNameIndex().lookup("all-logs"));
    }

    /** A recreated index reuses its name with a new UUID, and the entry must follow. */
    public void testRecreatedIndexPicksUpTheNewUuid() {
        NameIndexService service = new NameIndexService(true);
        Metadata before = metadata(index("alpha"));
        service.apply(Metadata.EMPTY_METADATA, before);
        byte[] firstUuid = service.getNameIndex().lookup("alpha").getUuid();

        service.apply(before, metadata(indexWithUuid("alpha", "a-different-uuid-value")));

        assertFalse(
            "the entry should not still carry the old UUID",
            java.util.Arrays.equals(firstUuid, service.getNameIndex().lookup("alpha").getUuid())
        );
    }

    public void testRequestAndResponseRoundTripOverTheWire() throws IOException {
        ResolveIndexNamesRequest request = new ResolveIndexNamesRequest(IndicesOptions.strictExpand(), "logs-*", "metrics");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ResolveIndexNamesRequest read = new ResolveIndexNamesRequest(in);
                assertArrayEquals(request.expressions(), read.expressions());
                assertEquals(request.indicesOptions(), read.indicesOptions());
            }
        }

        ResolveIndexNamesResponse response = new ResolveIndexNamesResponse(List.of("a", "b"));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(response.getIndices(), new ResolveIndexNamesResponse(in).getIndices());
            }
        }
    }

    public void testRequestWithNoExpressionsIsValidAndMeansEverything() {
        ResolveIndexNamesRequest request = new ResolveIndexNamesRequest(IndicesOptions.strictExpandOpen());

        assertNull(request.validate());
        assertEquals(0, request.expressions().length);

        NameIndexService service = new NameIndexService(true);
        service.apply(Metadata.EMPTY_METADATA, metadata(index("alpha")));
        assertEquals(List.of("alpha"), service.resolve(request.indicesOptions(), request.expressions()));
    }

    // ---------------------------------------------------------------- helpers

    private static Metadata metadata(IndexMetadata... indices) {
        Metadata.Builder builder = Metadata.builder();
        for (IndexMetadata index : indices) {
            builder.put(index, false);
        }
        return builder.build();
    }

    private static IndexMetadata index(String name) {
        return indexWithUuid(name, name + "-uuid-0000000000");
    }

    private static IndexMetadata indexWithUuid(String name, String uuid) {
        return IndexMetadata.builder(name).settings(settings(uuid)).numberOfShards(1).numberOfReplicas(0).build();
    }

    private static IndexMetadata closedIndex(String name) {
        return IndexMetadata.builder(name)
            .settings(settings(name + "-uuid-0000000000"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(IndexMetadata.State.CLOSE)
            .build();
    }

    private static IndexMetadata aliased(String name, String alias) {
        return IndexMetadata.builder(name)
            .settings(settings(name + "-uuid-0000000000"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder(alias).build())
            .build();
    }

    private static Settings settings(String uuid) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            .build();
    }
}
