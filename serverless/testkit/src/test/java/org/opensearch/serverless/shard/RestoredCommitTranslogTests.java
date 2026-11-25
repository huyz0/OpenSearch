/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogHeader;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** A commit another engine wrote has no translog UUID, and the shard a restore builds from it must still open. */
public class RestoredCommitTranslogTests extends OpenSearchTestCase {

    private static Store storeHolding(Map<String, String> userData) throws Exception {
        final ShardId shardId = new ShardId("restored", "_na_", 0);
        final IndexSettings settings = IndexSettingsModule.newIndexSettings("restored", Settings.EMPTY);
        final Directory directory = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            writer.setLiveCommitData(userData.entrySet());
            writer.commit();
        }
        return new Store(shardId, settings, directory, new DummyShardLock(shardId));
    }

    /** What a Rust writer's commit carries before it learns the keys: sequence numbers and nothing of a translog. */
    private static Map<String, String> rustCommitData() {
        final Map<String, String> data = new HashMap<>();
        data.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, "-1");
        data.put(SequenceNumbers.MAX_SEQ_NO, "-1");
        return data;
    }

    public void testACommitWithoutATranslogUuidGetsOneThatTheNewTranslogCarries() throws Exception {
        try (Store store = storeHolding(rustCommitData())) {
            store.bootstrapNewHistory();
            final SegmentInfos before = store.readLastCommittedSegmentsInfo();
            assertNull(before.getUserData().get(Translog.TRANSLOG_UUID_KEY));

            final String uuid = ShardOpening.ensureTranslogUuid(store, before);

            assertNotNull(uuid);
            assertEquals(uuid, store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY));
            final Path translogDir = createTempDir();
            Translog.createEmptyTranslog(translogDir, new ShardId("restored", "_na_", 0), -1L, 1L, uuid, FileChannel::open);
            final Path checkpoint = translogDir.resolve(Translog.CHECKPOINT_FILE_NAME);
            assertTrue(Files.exists(checkpoint));
            try (FileChannel channel = FileChannel.open(translogDir.resolve(Translog.getFilename(1)))) {
                assertEquals(uuid, TranslogHeader.read(translogDir.resolve(Translog.getFilename(1)), channel).getTranslogUUID());
            }
        }
    }

    public void testACommitThatNamesATranslogKeepsIt() throws Exception {
        final Map<String, String> data = rustCommitData();
        data.put(Translog.TRANSLOG_UUID_KEY, "kept-translog-uuid");
        try (Store store = storeHolding(data)) {
            final SegmentInfos commit = store.readLastCommittedSegmentsInfo();
            assertEquals("kept-translog-uuid", ShardOpening.ensureTranslogUuid(store, commit));
            assertEquals("kept-translog-uuid", store.readLastCommittedSegmentsInfo().getUserData().get(Translog.TRANSLOG_UUID_KEY));
        }
    }
}
