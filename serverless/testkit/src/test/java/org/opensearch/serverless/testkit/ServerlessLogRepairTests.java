/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Repairs one log record that shares its sequence number with another, so its shard opens with both writes.
 *
 * <p>Not a test: an operator's tool, run against a real store only when its properties are given, and skipped
 * otherwise. A shard whose log holds one sequence number for two acknowledged operations refuses to open rather than
 * drop one. Both were acknowledged, so the repair keeps both: the named record is rewritten without its sequence
 * number, which replay applies as a fresh operation with a new one -- the way records from before the log carried
 * sequence numbers are applied. The original is kept beside it, under a name nothing reading the log counts.
 *
 * <pre>
 * ./gradlew :serverless:testkit:test --tests "*ServerlessLogRepairTests" -Dtests.repair.endpoint=http://127.0.0.1:9200 \
 *     -Dtests.repair.bucket=B -Dtests.repair.shard="logs-0000120#uuid-logs-0000120#0" -Dtests.repair.term=41 \
 *     -Dtests.repair.ordinal=301 -Dtests.repair.seq_no=14752
 * </pre>
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ServerlessLogRepairTests extends OpenSearchTestCase {

    public void testResequenceOneRecord() throws Exception {
        final String endpoint = System.getProperty("tests.repair.endpoint");
        assumeTrue("no repair requested", endpoint != null);
        final String bucket = System.getProperty("tests.repair.bucket");
        final String shard = System.getProperty("tests.repair.shard");
        final long term = Long.parseLong(System.getProperty("tests.repair.term"));
        final String name = String.format(java.util.Locale.ROOT, "%020d", Long.parseLong(System.getProperty("tests.repair.ordinal")));
        final long expectedSeqNo = Long.parseLong(System.getProperty("tests.repair.seq_no"));
        final String access = System.getProperty("tests.repair.access_key", "rustfsadmin");
        final String secret = System.getProperty("tests.repair.secret_key", "rustfsadmin");

        try (BlobStore store = org.opensearch.repositories.s3.MinioBlobStores.create(endpoint, access, secret, bucket, createTempDir())) {
            final BlobContainer log = store.blobContainer(BlobPath.cleanPath().add("segments").add(shard).add("wal").add("t=" + term));
            final byte[] original;
            try (InputStream in = log.readBlob(name)) {
                original = in.readAllBytes();
            }
            final WalRecord record = WalRecord.fromStream(new ByteArrayInputStream(original));
            assertFalse("a deletion is not re-sequenced", record.isDeletion());
            assertEquals("the record named is the one expected", expectedSeqNo, record.seqNo());
            assertTrue("it carries a sequence number to drop", record.hasSequenceIdentity());

            // The original first, so a repair that stops half way has lost nothing.
            log.writeBlob("repaired-" + name, new ByteArrayInputStream(original), original.length, true);
            final byte[] fresh = BytesReference.toBytes(new WalRecord(record.id(), record.source()).toBytes());
            log.writeBlob(name, new ByteArrayInputStream(fresh), fresh.length, false);
            logger.info("re-sequenced record {} of {} term {}: {} is now applied as a fresh operation", name, shard, term, record.id());
        }
    }
}
