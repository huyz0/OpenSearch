/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * W16. Making a deletion's tombstone durable before the deletion is acknowledged.
 *
 * <p>W4 had to make tombstone writes asynchronous, because the publish hook runs on the cluster state
 * thread where a blocking write deadlocks. W10 added retries, which closes transient failures and not a
 * crash between the commit and the write landing. W11 established that inferring the tombstone instead is
 * unsafe: an unavailable store and a store with no record give the same answer, so acting on absence would
 * discard live shard data.
 *
 * <p>Ordering is what remains. The window after the cluster state is committed and before the client is
 * told the delete succeeded is the only place a write can be both off the cluster state thread and ahead of
 * the acknowledgement.
 *
 * <p>The property that matters most here is the failure one. A delete whose tombstone could not be written
 * has not achieved what a delete promises, and must not report success. That is deliberately the opposite
 * of the scale-down bug this project found and left alone, where a failed operation reported success
 * because its failure path returned the unchanged state.
 */
public class DurableTombstonesTests extends OpenSearchTestCase {

    @After
    public void clearRegistration() {
        DurableTombstones.register(null);
    }

    /** With nothing registered, deletion is unchanged: the listener runs straight through. */
    public void testNothingRegisteredCompletesImmediately() {
        assertFalse(DurableTombstones.isRegistered());
        AtomicBoolean completed = new AtomicBoolean();

        DurableTombstones.whenDurable(List.of(indexMetadata("gone")), ActionListener.wrap(ignored -> completed.set(true), e -> {}));

        assertTrue("an unregistered writer must not defer the acknowledgement", completed.get());
    }

    /** An empty deletion does not consult the writer, so a no-op delete costs nothing. */
    public void testAnEmptyDeletionDoesNotConsultTheWriter() {
        AtomicBoolean consulted = new AtomicBoolean();
        DurableTombstones.register((deleted, whenStored) -> {
            consulted.set(true);
            whenStored.onResponse(null);
        });

        AtomicBoolean completed = new AtomicBoolean();
        DurableTombstones.whenDurable(List.of(), ActionListener.wrap(ignored -> completed.set(true), e -> {}));

        assertTrue(completed.get());
        assertFalse("nothing deleted means nothing to make durable", consulted.get());
    }

    /**
     * The ordering the whole task exists for: the acknowledgement must not run until the write completes.
     */
    public void testTheAcknowledgementWaitsForTheWrite() {
        AtomicReference<ActionListener<Void>> heldOpen = new AtomicReference<>();
        DurableTombstones.register((deleted, whenStored) -> heldOpen.set(whenStored));

        AtomicBoolean acknowledged = new AtomicBoolean();
        DurableTombstones.whenDurable(List.of(indexMetadata("gone")), ActionListener.wrap(ignored -> acknowledged.set(true), e -> {}));

        assertFalse(
            "the delete must not be acknowledged while the tombstone is still in flight, or a crash "
                + "between the two loses the only record that the index was deleted",
            acknowledged.get()
        );

        heldOpen.get().onResponse(null);

        assertTrue("and must be acknowledged once the tombstone is durable", acknowledged.get());
    }

    /**
     * A failed tombstone write must fail the delete. Reporting success would leave the caller believing a
     * durable no exists when it does not, which is the precise condition that lets a partitioned node
     * resurrect the index later.
     */
    public void testAFailedWriteFailsTheDelete() {
        DurableTombstones.register((deleted, whenStored) -> whenStored.onFailure(new IllegalStateException("descriptor store down")));

        AtomicBoolean acknowledged = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        DurableTombstones.whenDurable(List.of(indexMetadata("gone")), ActionListener.wrap(ignored -> acknowledged.set(true), failure::set));

        assertFalse("a delete whose tombstone could not be written must not report success", acknowledged.get());
        assertNotNull("it must report the failure instead", failure.get());
    }

    /** A writer that throws is a failure too, rather than a silent success. */
    public void testAThrowingWriterFailsTheDelete() {
        DurableTombstones.register((deleted, whenStored) -> { throw new IllegalStateException("writer exploded"); });

        AtomicReference<Exception> failure = new AtomicReference<>();
        DurableTombstones.whenDurable(List.of(indexMetadata("gone")), ActionListener.wrap(ignored -> {}, failure::set));

        assertNotNull("a throwing writer must fail the delete rather than acknowledge it", failure.get());
    }

    /** The deleted indices reach the writer, since it needs their uuids to tombstone them. */
    public void testTheWriterReceivesWhatWasDeleted() {
        AtomicReference<List<IndexMetadata>> seen = new AtomicReference<>();
        DurableTombstones.register((deleted, whenStored) -> {
            seen.set(deleted);
            whenStored.onResponse(null);
        });

        DurableTombstones.whenDurable(List.of(indexMetadata("first"), indexMetadata("second")), ActionListener.wrap(i -> {}, e -> {}));

        assertEquals(2, seen.get().size());
        assertEquals("first", seen.get().get(0).getIndex().getName());
    }

    private static IndexMetadata indexMetadata(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
