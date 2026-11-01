/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.engine.DefaultPrimaryOperationPolicy;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PrivatePreparationEngineConfigTests extends EngineTestCase {
    private static final long PRIVATE_BUFFER_BYTES = 4 * 1024 * 1024;

    public void testPrivateRefreshFailureMergePolicyAndBufferDoNotAffectServingEngine() throws Exception {
        final var publicRefreshes = new AtomicInteger();
        final var publicWarmers = new AtomicInteger();
        final var publicFailures = new AtomicInteger();
        final var privateFailures = new AtomicInteger();
        final var refresh = new ReferenceManager.RefreshListener() {
            @Override
            public void beforeRefresh() {
                publicRefreshes.incrementAndGet();
            }

            @Override
            public void afterRefresh(boolean didRefresh) {}
        };
        try (var publicStore = filesystemStore(); var privateStore = filesystemStore()) {
            final var publicConfig = publicConfig(publicStore).toBuilder()
                .primaryOperationPolicy(CommittedPrimaryOperationPolicy.INSTANCE)
                .externalRefreshListener(List.of(refresh))
                .internalRefreshListener(List.of(refresh))
                .warmer(searcher -> publicWarmers.incrementAndGet())
                .indexReaderWarmer(reader -> publicWarmers.incrementAndGet())
                .eventListener(failures(publicFailures))
                .build();
            try (var serving = createEngine(publicConfig)) {
                final var privateConfig = privateConfig(publicConfig, privateStore, createTempDir(), failures(privateFailures));
                assertSame(DefaultPrimaryOperationPolicy.INSTANCE, privateConfig.getPrimaryOperationPolicy());
                assertNotSame(publicConfig.getMergePolicy(), privateConfig.getMergePolicy());
                assertNotSame(publicConfig.getIndexSettings(), privateConfig.getIndexSettings());
                assertSame(publicConfig.getAnalyzer(), privateConfig.getAnalyzer());
                assertSame(publicConfig.getMapperService(), privateConfig.getMapperService());
                assertTrue(privateConfig.getExternalRefreshListener().isEmpty());
                assertTrue(privateConfig.getInternalRefreshListener().isEmpty());
                assertNull(privateConfig.getIndexReaderWarmer());
                assertNull(privateConfig.getQueryCache());
                assertTrue(privateConfig.getTranslogFactory() instanceof InternalTranslogFactory);
                assertTrue(privateConfig.getIndexingBufferSize().getBytes() <= PRIVATE_BUFFER_BYTES);
                final int refreshesBefore = publicRefreshes.get();
                final int warmersBefore = publicWarmers.get();
                try (var preparation = createEngine(privateConfig)) {
                    final var result = preparation.index(indexForDoc(createParsedDoc("private-id", null)));
                    assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
                    assertEquals(0, result.getSeqNo());
                    preparation.refresh("private external refresh");
                    preparation.writeIndexingBuffer();
                    assertVisibleCount(preparation, 1);
                    assertEquals(refreshesBefore, publicRefreshes.get());
                    assertEquals(warmersBefore, publicWarmers.get());
                    assertVisibleCount(serving, 0);
                    preparation.failEngine("private failure", new IOException("injected private engine failure"));
                    assertEquals(1, privateFailures.get());
                    assertEquals(0, publicFailures.get());
                    assertVisibleCount(serving, 0);
                }
            }
        }
    }

    public void testOverlappingStoreTranslogAndServingPathsAreRefusedBeforeOpening() throws Exception {
        try (var publicStore = filesystemStore(); var privateStore = filesystemStore()) {
            final var publicConfig = publicConfig(publicStore);
            final var listener = failures(new AtomicInteger());
            expectThrows(IllegalArgumentException.class, () -> privateConfig(publicConfig, publicStore, createTempDir(), listener));
            expectThrows(
                IOException.class,
                () -> privateConfig(publicConfig, privateStore, publicConfig.getTranslogConfig().getTranslogPath(), listener)
            );
            final Path privateIndex = org.opensearch.storage.utils.DirectoryUtils.unwrapFSDirectory(privateStore.directory())
                .getDirectory();
            expectThrows(IOException.class, () -> privateConfig(publicConfig, privateStore, privateIndex, listener));
            final Path publicIndex = org.opensearch.storage.utils.DirectoryUtils.unwrapFSDirectory(publicStore.directory()).getDirectory();
            expectThrows(IOException.class, () -> privateConfig(publicConfig, privateStore, publicIndex, listener));
            final Path regularFile = Files.createFile(createTempDir().resolve("not-a-directory"));
            expectThrows(IOException.class, () -> privateConfig(publicConfig, privateStore, regularFile, listener));
            expectThrows(
                IllegalArgumentException.class,
                () -> privateConfig(publicConfig, privateStore, createTempDir(), publicConfig.getEventListener())
            );
        }
    }

    public void testInvalidBudgetTermAndCustomCommitterAreRefused() throws Exception {
        try (var publicStore = filesystemStore(); var privateStore = filesystemStore()) {
            final var publicConfig = publicConfig(publicStore);
            final var listener = failures(new AtomicInteger());
            final Path privateLog = createTempDir();
            for (long budget : new long[] { 0, -1, (long) Integer.MAX_VALUE + 1 }) {
                expectThrows(
                    IllegalArgumentException.class,
                    () -> PrivatePreparationEngineConfig.create(
                        publicConfig,
                        privateStore,
                        privateLog,
                        primaryTerm.get(),
                        budget,
                        () -> SequenceNumbers.NO_OPS_PERFORMED,
                        listener
                    )
                );
            }
            expectThrows(
                IllegalArgumentException.class,
                () -> PrivatePreparationEngineConfig.create(
                    publicConfig,
                    privateStore,
                    privateLog,
                    0,
                    PRIVATE_BUFFER_BYTES,
                    () -> SequenceNumbers.NO_OPS_PERFORMED,
                    listener
                )
            );
            final var custom = publicConfig.toBuilder().committerFactory(config -> null).build();
            expectThrows(IllegalArgumentException.class, () -> privateConfig(custom, privateStore, privateLog, listener));
            final var customMetadata = publicConfig.toBuilder().documentMetadataResolver((reader, id) -> null).build();
            expectThrows(IllegalArgumentException.class, () -> privateConfig(customMetadata, privateStore, privateLog, listener));
        }
    }

    @SuppressForbidden(reason = "Exercise core's legacy indexing-buffer override; restore the previous value in finally")
    public void testCoreSystemPropertyCannotOverridePrivateBufferBudget() throws Exception {
        try (var publicStore = filesystemStore(); var privateStore = filesystemStore()) {
            final var publicConfig = publicConfig(publicStore);
            final String key = "opensearch.index.memory.max_index_buffer_size";
            final String previous = System.setProperty(key, "128mb");
            try {
                expectThrows(
                    IllegalArgumentException.class,
                    () -> privateConfig(publicConfig, privateStore, createTempDir(), failures(new AtomicInteger()))
                );
            } finally {
                if (previous == null) {
                    System.clearProperty(key);
                } else {
                    System.setProperty(key, previous);
                }
            }
        }
    }

    private Store filesystemStore() throws IOException {
        return createStore(newFSDirectory(createTempDir()));
    }

    private EngineConfig publicConfig(Store publicStore) {
        return config(defaultSettings, publicStore, createTempDir(), newMergePolicy(), null);
    }

    private EngineConfig privateConfig(EngineConfig publicConfig, Store privateStore, Path privateLog, Engine.EventListener failures)
        throws IOException {
        return PrivatePreparationEngineConfig.create(
            publicConfig,
            privateStore,
            privateLog,
            primaryTerm.get(),
            PRIVATE_BUFFER_BYTES,
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            failures
        );
    }

    private static Engine.EventListener failures(AtomicInteger failures) {
        return new Engine.EventListener() {
            @Override
            public void onFailedEngine(String reason, Exception exception) {
                failures.incrementAndGet();
            }
        };
    }
}
