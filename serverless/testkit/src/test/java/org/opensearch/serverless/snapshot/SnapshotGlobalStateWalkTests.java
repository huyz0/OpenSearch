/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.snapshot;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.template.TemplateStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/** A document deleted while a set is read restarts that set once; a second change fails the take. */
public class SnapshotGlobalStateWalkTests extends OpenSearchTestCase {

    private static SnapshotGlobalState.Walk changingTimes(int changes, AtomicInteger calls) {
        return visitor -> {
            if (calls.getAndIncrement() < changes) {
                visitor.accept("first", "{\"a\":1}");
                throw new TemplateStore.StateChangedException("gone");
            }
            visitor.accept("first", "{\"a\":1}");
            visitor.accept("second", "{\"b\":2}");
        };
    }

    public void testASetThatChangedOnceIsReadAgainFromItsMarker() throws Exception {
        final Map<String, Object> state = new LinkedHashMap<>();
        final AtomicInteger calls = new AtomicInteger();
        SnapshotGlobalState.add(state, "index_templates", changingTimes(1, calls), 0);
        assertEquals(2, calls.get());
        assertEquals("the partial first pass is not kept", 2, ((Map<?, ?>) state.get("index_templates")).size());
    }

    public void testASetThatChangedTwiceFailsTheTake() throws Exception {
        final Map<String, Object> state = new LinkedHashMap<>();
        final AtomicInteger calls = new AtomicInteger();
        expectThrows(
            SnapshotGlobalState.ChangedException.class,
            () -> SnapshotGlobalState.add(state, "index_templates", changingTimes(2, calls), 0)
        );
        assertEquals(2, calls.get());
        assertTrue(state.isEmpty());
    }

    public void testTheStoreReportsADocumentDeletedBetweenItsMarkerAndItsBody() throws Exception {
        final Path root = createTempDir();
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, root, false), BlobPath.cleanPath(), () -> 1_000L, 300_000L);
        plane.indexTemplates().put("t", "{\"index_patterns\":[\"x-*\"]}");
        // The marker still names the body, which is gone: what a concurrent delete leaves a walk that already read the marker.
        try (Stream<Path> files = Files.walk(root)) {
            for (Path file : (Iterable<Path>) files.filter(f -> f.getFileName().toString().startsWith("blob-"))::iterator) {
                Files.delete(file);
            }
        }
        final TemplateStore.StateChangedException e = expectThrows(
            TemplateStore.StateChangedException.class,
            () -> plane.indexTemplates().forEachDocument((name, source) -> fail("no body to read"))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[t]"));
    }
}
