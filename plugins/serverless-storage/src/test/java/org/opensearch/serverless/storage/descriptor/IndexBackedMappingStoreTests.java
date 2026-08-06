/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetResponse;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * T43. Whether this store can tell "there is no mapping" from "I could not find out".
 *
 * <p>It could not. Every exception was caught and answered null, and null is what an index with no fields
 * looks like, so a cluster block, an unavailable shard and a timeout were all reported to callers as an
 * empty mapping.
 *
 * <p><b>What it cost is not the obvious thing.</b> The obvious reading is silent field loss:
 * {@code MappingGenerationStore.updateMapping} merges onto what it read, so reading "no fields" for an index
 * with plenty would swap them away. Through this store it cannot, because the swap writes the generation as
 * an external version and external versioning refuses a version that is not greater than the stored one. So
 * a merge from empty proposed generation 1, was rejected, and went round the loop until the caller was told
 * its update "did not converge, which means sustained contention" -- a wrong diagnosis of an unreachable
 * store, after thirty-two round trips. The pair of tests below pins both halves: the store reports the
 * failure, and the write path raises that failure rather than a contention message.
 *
 * <p>One absence is still inferred and only one: a missing mapping index. Nothing has ever been written to
 * it, so no index can have a stored mapping, and there is nothing to guess.
 */
public class IndexBackedMappingStoreTests extends OpenSearchTestCase {

    /** A get that fails for a reason that is not "the index is not there" must not be reported as absent. */
    public void testAReadThatFailsPropagatesRatherThanLookingEmpty() {
        try (NoOpClient client = failingGet(() -> new ClusterBlockException(Set.of(Metadata.CLUSTER_READ_ONLY_BLOCK)))) {
            IndexBackedMappingStore store = new IndexBackedMappingStore(client);

            expectThrows(ClusterBlockException.class, () -> store.read("idx"));
        }
    }

    /** The same for the shape a slow or unavailable shard arrives in. */
    public void testAReadThatTimesOutPropagates() {
        try (NoOpClient client = failingGet(() -> new RuntimeException("no shard available"))) {
            IndexBackedMappingStore store = new IndexBackedMappingStore(client);

            RuntimeException thrown = expectThrows(RuntimeException.class, () -> store.read("idx"));
            assertEquals("no shard available", thrown.getMessage());
        }
    }

    /**
     * A missing mapping index still reads as no mapping, which is the one absence that can be inferred.
     *
     * <p>Without this the fix would break every cluster before its first mapped gated creation: the index is
     * created lazily by the first write, so until then every read fails this way.
     */
    public void testAMissingMappingIndexIsStillNoMapping() {
        try (NoOpClient client = failingGet(() -> new IndexNotFoundException(new Index(IndexBackedMappingStore.MAPPING_INDEX, "_na_")))) {
            IndexBackedMappingStore store = new IndexBackedMappingStore(client);

            assertNull(store.read("idx"));
        }
    }

    /** A get that succeeds and says the document is not there is an absence, and reads as one. */
    public void testAnIndexWithNoStoredMappingReadsAsNull() {
        try (
            NoOpClient client = getReturning(
                new GetResult(
                    IndexBackedMappingStore.MAPPING_INDEX,
                    "idx",
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                    -1,
                    false,
                    null,
                    Map.of(),
                    Map.of()
                )
            )
        ) {
            IndexBackedMappingStore store = new IndexBackedMappingStore(client);

            assertNull(store.read("idx"));
        }
    }

    /**
     * The consequence, at the caller that matters: an unreadable store fails the update as itself.
     *
     * <p>Driven through a real {@link IndexBackedMappingStore} over a failing client rather than through a
     * hand-written throwing double. A double that throws from its own body would pass against the code this
     * task replaced, since the blanket catch being removed lives in the store and a double does not have
     * one -- the test would assert nothing about the change. Registering the real store on the real seam is
     * what makes it fail when the catch comes back.
     */
    public void testAnUnreadableStoreFailsAnUpdateAsItselfRatherThanAsContention() {
        try (NoOpClient client = failingGet(() -> new ClusterBlockException(Set.of(Metadata.CLUSTER_READ_ONLY_BLOCK)))) {
            MappingGenerationStore.register(new IndexBackedMappingStore(client));
            try {
                ClusterBlockException thrown = expectThrows(
                    ClusterBlockException.class,
                    () -> MappingGenerationStore.updateMapping("idx", Map.of("age", "long"))
                );

                assertFalse(
                    "the caller must be told the store could not be read, not that its update was contended: " + thrown.getMessage(),
                    thrown.getMessage().contains("did not converge")
                );
            } finally {
                MappingGenerationStore.register(null);
            }
        }
    }

    /**
     * The configured shard count reaches the index that gets created, rather than a literal.
     *
     * <p>Asserted on the captured request because there is nowhere else to see it: the index's shard count
     * is fixed when it is created, so a setting that failed to arrive would be undetectable afterwards
     * without deleting the index.
     */
    public void testTheConfiguredShardCountReachesTheCreatedIndex() {
        AtomicReference<CreateIndexRequest> captured = new AtomicReference<>();
        try (NoOpClient client = capturingCreate(captured)) {
            new IndexBackedMappingStore(client, 17).compareAndSwap("idx", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of()));

            assertNotNull("the mapping index must be created on first write", captured.get());
            assertEquals(Integer.valueOf(17), IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(captured.get().settings()));
        }
    }

    /** And the default is the number the code used before it was configurable. */
    public void testTheDefaultShardCountIsUnchanged() {
        AtomicReference<CreateIndexRequest> captured = new AtomicReference<>();
        try (NoOpClient client = capturingCreate(captured)) {
            new IndexBackedMappingStore(client).compareAndSwap("idx", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of()));

            assertEquals(
                Integer.valueOf(IndexBackedMappingStore.DEFAULT_SHARDS),
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(captured.get().settings())
            );
            assertEquals(
                "the number this index was born with, so T45 changed nothing at the default",
                5,
                IndexBackedMappingStore.DEFAULT_SHARDS
            );
        }
    }

    private NoOpClient capturingCreate(AtomicReference<CreateIndexRequest> captured) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof CreateIndexRequest create) {
                    captured.set(create);
                }
                listener.onResponse(null);
            }
        };
    }

    /**
     * Fails the get and nothing else.
     *
     * <p>Failing every action instead would make
     * {@link #testAnUnreadableStoreFailsAnUpdateAsItselfRatherThanAsContention} pass for the wrong reason:
     * the exception would come from the swap, so the test would still be green with the blanket catch on
     * the read restored. Only the read may fail here, so the write succeeding is what a swallowed read
     * failure would look like.
     */
    private NoOpClient failingGet(Supplier<RuntimeException> failure) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.name().equals(GetAction.NAME)) {
                    listener.onFailure(failure.get());
                } else {
                    listener.onResponse(null);
                }
            }
        };
    }

    private NoOpClient getReturning(GetResult result) {
        return new NoOpClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) new GetResponse(result));
            }
        };
    }

}
