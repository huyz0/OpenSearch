/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.testkit.HookedBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

public class TemplateContentRaceTests extends OpenSearchTestCase {
    public void testReplacingContentCannotDeleteAConcurrentPutsBytes() throws Exception {
        assertConcurrentPutSurvives(false);
    }

    public void testDeletingContentCannotDeleteAConcurrentPutsBytes() throws Exception {
        assertConcurrentPutSurvives(true);
    }

    public void testARejectedNewTemplateDoesNotWriteOrphanedContent() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(BlobPath.cleanPath());
            try (var body = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                body.startObject().field("version", 1L).startObject("entries");
                for (int i = 0; i < TemplateStore.MAX_TEMPLATES; i++) {
                    body.field("template-" + i, "existing-content");
                }
                body.endObject().endObject();
                container.createRegisterIfAbsent("_version", org.opensearch.core.common.bytes.BytesReference.bytes(body));
            }
            final TemplateStore templates = new TemplateStore(container);
            expectThrows(TemplateStore.TooManyTemplatesException.class, () -> templates.put("overflow", "{}"));
            assertTrue("a refused write must not leave a new unindexed content object", container.listBlobsByPrefix("blob-").isEmpty());
        }
    }

    private void assertConcurrentPutSurvives(boolean delete) throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final BlobPath path = BlobPath.cleanPath().add("templates");
            final var container = store.blobContainer(path);
            final TemplateStore first = new TemplateStore(container);
            final TemplateStore second = new TemplateStore(container);
            final String original = "{\"script\":\"original\"}";
            final String replacement = "{\"script\":\"replacement\"}";
            first.put("first", original);
            final String oldBlob = container.listBlobsByPrefix("blob-").keySet().iterator().next();
            store.onNextBlobDelete("templates", oldBlob, () -> {
                try {
                    second.put("second", original);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            if (delete) {
                assertTrue(first.delete("first"));
            } else {
                first.put("first", replacement);
            }
            assertTrue("the second put must complete immediately before the first operation deletes old bytes", store.fired());
            final TemplateStore cold = new TemplateStore(container);
            assertEquals(original, cold.get("second").orElseThrow());
            assertEquals(delete ? Map.of("second", original) : Map.of("first", replacement, "second", original), cold.all());
        }
    }

    public void testPrefixReadsRetryASelectedRevisionRemovedAfterTheMarkerRead() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var container = store.blobContainer(BlobPath.cleanPath().add("prefix-race"));
            final var writer = new TemplateStore(container);
            writer.put("chosen-a", "original");
            writer.put("unrelated", "unchanged");
            store.onNextRegisterRead("prefix-race", "_version", () -> {
                try {
                    writer.put("chosen-a", "replacement");
                    writer.put("chosen-b", "new");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            assertEquals(Map.of("chosen-a", "replacement", "chosen-b", "new"), new TemplateStore(container).withPrefix("chosen-"));
            assertTrue(store.fired());
        }
    }
}
