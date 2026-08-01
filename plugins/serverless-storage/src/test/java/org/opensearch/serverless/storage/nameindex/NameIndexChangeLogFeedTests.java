/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.serverless.storage.descriptor.DescriptorChange;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * The feed a gated index actually appears in.
 *
 * <p>`NameIndexService` listened only to cluster state, which a gated index has no entry in by
 * construction, so it was blind to precisely the indices it exists to resolve. These tests are about the
 * property that makes the replacement feed usable: the change log has no total order, so applying it has
 * to be idempotent and order-insensitive rather than sequential.
 */
public class NameIndexChangeLogFeedTests extends OpenSearchTestCase {

    private NameIndexService service() {
        return new NameIndexService(true);
    }

    private static DescriptorChange created(String name, String uuid) {
        return new DescriptorChange(name, uuid, DescriptorChange.Kind.CREATED, 0L);
    }

    private static DescriptorChange deleted(String name, String uuid) {
        return new DescriptorChange(name, uuid, DescriptorChange.Kind.DELETED, 0L);
    }

    public void testACreatedNameBecomesResolvable() {
        NameIndexService service = service();

        assertEquals(1, service.apply(List.of(created("tenant-a", "uuid-1"))));
        assertNotNull(service.getNameIndex().lookup("tenant-a"));
    }

    public void testADeletedNameStopsResolving() {
        NameIndexService service = service();
        service.apply(List.of(created("tenant-a", "uuid-1")));

        assertEquals(1, service.apply(List.of(deleted("tenant-a", "uuid-1"))));
        assertNull(service.getNameIndex().lookup("tenant-a"));
    }

    /** Replay has to be harmless, because a consumer that crashes mid-batch re-reads the whole bucket. */
    public void testApplyingTheSameBatchTwiceChangesNothingTheSecondTime() {
        NameIndexService service = service();
        List<DescriptorChange> batch = List.of(created("tenant-a", "uuid-1"), created("tenant-b", "uuid-2"));

        assertEquals(2, service.apply(batch));
        assertEquals("a replay must be a no-op, not a duplicate", 0, service.apply(batch));
        assertNotNull(service.getNameIndex().lookup("tenant-a"));
        assertNotNull(service.getNameIndex().lookup("tenant-b"));
    }

    /**
     * The case the uuid on a change exists for. Delete-then-recreate produces two entries for one name
     * with no defined order between them, and a stale delete applied by name alone would remove the live
     * index. That is a silent data-visibility loss, which is the failure this area is most careful about.
     */
    public void testAStaleDeleteCannotRemoveARecreatedIndex() {
        NameIndexService service = service();
        service.apply(List.of(created("tenant-a", "uuid-old")));
        service.apply(List.of(deleted("tenant-a", "uuid-old")));
        service.apply(List.of(created("tenant-a", "uuid-new")));

        assertEquals("the stale delete must not apply", 0, service.apply(List.of(deleted("tenant-a", "uuid-old"))));
        assertNotNull("the recreated index must survive it", service.getNameIndex().lookup("tenant-a"));
    }

    /** And the same batch in the opposite order reaches the same state, since the log has no order. */
    public void testOrderWithinABatchDoesNotChangeTheOutcome() {
        NameIndexService forwards = service();
        NameIndexService backwards = service();
        List<DescriptorChange> batch = List.of(created("tenant-a", "uuid-1"), created("tenant-b", "uuid-2"), deleted("tenant-b", "uuid-2"));

        forwards.apply(batch);
        backwards.apply(batch.reversed());

        assertNotNull(forwards.getNameIndex().lookup("tenant-a"));
        assertNotNull(backwards.getNameIndex().lookup("tenant-a"));
        assertNull(forwards.getNameIndex().lookup("tenant-b"));
        assertNull(
            "a delete arriving before its create must still leave the name gone once both are seen",
            backwards.getNameIndex().lookup("tenant-b")
        );
    }

    /** A disabled service is inert, matching how every other mechanism on this branch ships. */
    public void testADisabledServiceAppliesNothing() {
        assertEquals(0, new NameIndexService(false).apply(List.of(created("tenant-a", "uuid-1"))));
    }
}
