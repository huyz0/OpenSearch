/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.storage.util.IndexMetadataUuidIndex;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The shard-existence precondition shared by all three retention transports.
 *
 * <p>Why it exists rather than a character check: a request's {@code indexUuid} is concatenated
 * into a blob path ({@code BlobPath.cleanPath().add(indexUuid)}) whose segments the underlying
 * store resolves without normalising, and these actions used to pass it straight through with no
 * cross-check against anything. That made the <em>path</em>, rather than the shard, the caller's to
 * choose: a {@code ..} segment walks out of the repository base on a filesystem repository, and --
 * the case no character test can ever catch -- a perfectly well-formed string that happens to name
 * a different index reaches that index's shards with no scoping at all. Only resolving the uuid
 * through cluster metadata answers "does this name a real shard", which is why that is the control
 * and the charset check in the request is only defence in depth.
 */
public class TransportSnapshotPinActionTests extends OpenSearchTestCase {

    private static final String REAL_UUID = "aReallyRealIndexUuid1";
    private static final String OTHER_UUID = "anotherRealIndexUuid2";

    /** One index with 2 shards under {@link #REAL_UUID}, one with 1 shard under {@link #OTHER_UUID}. */
    private static Metadata twoIndices() {
        return Metadata.builder().put(index("real-idx", REAL_UUID, 2), false).put(index("other-idx", OTHER_UUID, 1), false).build();
    }

    private static IndexMetadata index(String name, String uuid, int numberOfShards) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            )
            .numberOfShards(numberOfShards)
            .numberOfReplicas(0)
            .build();
    }

    public void testARealIndexAndShardIsAccepted() {
        TransportSnapshotPinAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), REAL_UUID, 1);
    }

    /** The traversal-shaped uuid: refused because it names no index, not because of its characters. */
    public void testATraversalShapedUuidIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportSnapshotPinAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), "../../../etc", 0)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }

    /** A well-formed string that simply is not any index's uuid -- the case a charset check misses. */
    public void testAWellFormedButUnknownUuidIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportSnapshotPinAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), "notAnIndexUuidAtAll", 0)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }

    /**
     * The unrelated-but-real case: {@link #OTHER_UUID} is a genuine index, but shard 1 belongs to
     * the <em>other</em> index -- reaching it through this uuid must not be allowed just because
     * the uuid itself resolves.
     */
    public void testARealUuidWithAShardIdBelongingToADifferentIndexIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportSnapshotPinAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), OTHER_UUID, 1)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("out of bounds"));
        assertTrue("the error must name the index it actually resolved to", e.getMessage().contains("other-idx"));
    }

    /** An index name is not an index uuid, however real the name is. */
    public void testAnIndexNameIsNotAcceptedInPlaceOfItsUuid() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportSnapshotPinAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), "real-idx", 0)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }
}
