/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.opensearch.serverless.store.IndexUsage.Kind.BLOB_READ;
import static org.opensearch.serverless.store.IndexUsage.Kind.BLOB_WRITE;
import static org.opensearch.serverless.store.IndexUsage.Kind.BYTES_READ;
import static org.opensearch.serverless.store.IndexUsage.Kind.BYTES_WRITTEN;
import static org.opensearch.serverless.store.IndexUsage.Kind.LISTING;
import static org.opensearch.serverless.store.IndexUsage.Kind.REGISTER_READ;
import static org.opensearch.serverless.store.IndexUsage.Kind.REGISTER_WRITE;
import static org.opensearch.serverless.store.IndexUsage.ONE_REQUEST;

@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class StoreUsageTests extends OpenSearchTestCase {

    private MeteredBlobStore store() throws IOException {
        return new MeteredBlobStore(new FsBlobStore(8192, createTempDir(), false));
    }

    private static void put(BlobContainer container, String name, int size) throws IOException {
        container.writeBlob(name, new ByteArrayInputStream(new byte[size]), size, false);
    }

    private static void drain(InputStream in) throws IOException {
        try (in) {
            in.readAllBytes();
        }
    }

    public void testRegisterMapKeysAreChargedToTheirIndex() throws IOException {
        final MeteredBlobStore store = store();
        final BlobPath base = BlobPath.cleanPath();
        final BlobContainer shard = store.blobContainer(RegisterMap.shardData(base, "logs-a", "uuid1", 3).add("t=1"));
        put(shard, "seg", 100);
        drain(shard.readBlob("seg"));
        final ConditionalBlobContainer heads = ConditionalBlobContainer.of(store.blobContainer(RegisterMap.shards(base)));
        heads.createRegisterIfAbsent(RegisterMap.shardHeadBlob("logs-b", 7), new BytesArray("h"));
        heads.readRegister(RegisterMap.shardHeadBlob("logs-b", 7));
        final ConditionalBlobContainer descriptors = ConditionalBlobContainer.of(store.blobContainer(RegisterMap.indices(base)));
        descriptors.createRegisterIfAbsent(RegisterMap.descriptorBlob("logs-c"), new BytesArray("d"));
        put(store.blobContainer(RegisterMap.assignments(base, "node-1")), RegisterMap.assignmentBlob("logs-d", 0), 5);
        ConditionalBlobContainer.of(store.blobContainer(RegisterMap.members(base))).createRegisterIfAbsent("node-1", new BytesArray("l"));

        final StoreUsage usage = store.usage();
        assertEquals(ONE_REQUEST, usage.of("logs-a").get(BLOB_WRITE));
        assertEquals(ONE_REQUEST, usage.of("logs-a").get(BLOB_READ));
        assertEquals(100L, usage.of("logs-a").get(BYTES_WRITTEN));
        assertEquals(100L, usage.of("logs-a").get(BYTES_READ));
        assertEquals(ONE_REQUEST, usage.of("logs-b").get(REGISTER_WRITE));
        assertEquals(ONE_REQUEST, usage.of("logs-b").get(REGISTER_READ));
        assertEquals(ONE_REQUEST, usage.of("logs-c").get(REGISTER_WRITE));
        assertEquals(ONE_REQUEST, usage.of("logs-d").get(BLOB_WRITE));
        assertEquals("a lease is the node's", ONE_REQUEST, usage.of(StoreUsage.NODE).get(REGISTER_WRITE));
        assertAddsUp(store);
    }

    public void testSplitSharesAnUploadByWeightAndAddsUpExactly() throws IOException {
        final MeteredBlobStore store = store();
        final BlobContainer log = store.blobContainer(BlobPath.cleanPath().add("nodelog").add("node-1"));
        final Map<String, Long> weights = new LinkedHashMap<>();
        weights.put("small", 1L);
        weights.put("big", 2L);
        weights.put("none", 0L);
        StoreUsage.split(weights, () -> {
            put(log, "00000000000000000001", 1000);
            return null;
        });
        final StoreUsage usage = store.usage();
        assertEquals(333L, usage.of("small").get(BLOB_WRITE));
        assertEquals(667L, usage.of("big").get(BLOB_WRITE));
        assertEquals(333L, usage.of("small").get(BYTES_WRITTEN));
        assertEquals(667L, usage.of("big").get(BYTES_WRITTEN));
        assertEquals(0L, usage.of(StoreUsage.NODE).get(BLOB_WRITE));
        assertFalse("weight zero is left out", usage.lines().containsKey("none"));
        put(log, "00000000000000000002", 10);
        assertEquals("outside the scope it is the node's again", ONE_REQUEST, usage.of(StoreUsage.NODE).get(BLOB_WRITE));
        assertAddsUp(store);
    }

    public void testScopeCarriedToAnotherThread() throws Exception {
        final MeteredBlobStore store = store();
        final BlobContainer log = store.blobContainer(BlobPath.cleanPath().add("nodelog"));
        final StoreUsage.Scope[] carried = new StoreUsage.Scope[1];
        StoreUsage.split(Map.of("x", 1L), () -> {
            carried[0] = StoreUsage.currentScope();
            return null;
        });
        final Thread other = new Thread(() -> {
            try {
                StoreUsage.within(carried[0], () -> {
                    put(log, "b", 1);
                    return null;
                });
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        other.start();
        other.join();
        assertEquals(ONE_REQUEST, store.usage().of("x").get(BLOB_WRITE));
    }

    public void testKeyBeatsScopeAndChargedContainerBeatsBoth() throws IOException {
        final MeteredBlobStore store = store();
        final BlobPath shardBase = RegisterMap.shardData(BlobPath.cleanPath(), "owner", "u", 0);
        final BlobContainer pack = store.blobContainer(BlobPath.cleanPath().add(NodePackStore.ROOT).add("node-1").add("e=1"));
        put(pack, "p", 50);
        final BlobContainer charged = MeteredBlobStore.chargedTo(pack, shardBase);
        StoreUsage.split(Map.of("someone-else", 1L), () -> {
            drain(charged.readBlob("p", 10, 20));
            put(store.blobContainer(shardBase.add("t=1")), "s", 1);
            return null;
        });
        final StoreUsage usage = store.usage();
        assertEquals(ONE_REQUEST, usage.of("owner").get(BLOB_READ));
        assertEquals(20L, usage.of("owner").get(BYTES_READ));
        assertEquals("the key names its index", ONE_REQUEST, usage.of("owner").get(BLOB_WRITE));
        assertFalse(usage.lines().containsKey("someone-else"));
        assertAddsUp(store);
    }

    public void testIndicesPastTheLimitShareOneLine() throws IOException {
        final MeteredBlobStore store = new MeteredBlobStore(new FsBlobStore(8192, createTempDir(), false), new StoreUsage(2));
        for (String index : new String[] { "a", "b", "c", "d" }) {
            put(store.blobContainer(RegisterMap.assignments(BlobPath.cleanPath(), "n")), RegisterMap.assignmentBlob(index, 0), 1);
        }
        final StoreUsage usage = store.usage();
        assertEquals(2 * ONE_REQUEST, usage.of(StoreUsage.OTHER).get(BLOB_WRITE));
        assertEquals(ONE_REQUEST, usage.of("a").get(BLOB_WRITE));
        assertAddsUp(store);
    }

    public void testMeteringOffKeepsNodeCounters() throws IOException {
        final MeteredBlobStore store = new MeteredBlobStore(new FsBlobStore(8192, createTempDir(), false), null);
        put(store.blobContainer(BlobPath.cleanPath().add("x")), "b", 1);
        store.blobContainer(BlobPath.cleanPath().add("x")).listBlobs();
        assertNull(store.usage());
        assertEquals(1L, store.blobWrites());
        assertEquals(1L, store.listings());
    }

    public void testSplitOfHugeAmountsStillAddsUp() {
        final StoreUsage usage = new StoreUsage(10);
        final StoreUsage.Scope scope = StoreUsage.Scope.of(Map.of("a", Long.MAX_VALUE / 3, "b", Long.MAX_VALUE / 3));
        scope.charge(usage, BYTES_WRITTEN, Long.MAX_VALUE / 2);
        assertEquals(Long.MAX_VALUE / 2, usage.total(BYTES_WRITTEN));
        assertTrue(usage.of("a").get(BYTES_WRITTEN) > 0L);
    }

    public void testCostUnits() {
        final IndexUsage usage = new IndexUsage();
        usage.add(BLOB_WRITE, ONE_REQUEST);
        usage.add(LISTING, ONE_REQUEST);
        usage.add(BLOB_READ, 100 * ONE_REQUEST);
        usage.add(REGISTER_WRITE, ONE_REQUEST);
        // 1 PUT + 1 LIST + 100 GET at 0.08 + 1 CAS at 1.08
        assertEquals(2_000L + 8_000L + 1_080L, usage.costMilliUnits());
    }

    /** Every request and byte the store counted is on exactly one line. */
    static void assertAddsUp(MeteredBlobStore store) {
        final StoreUsage usage = store.usage();
        assertEquals(store.registerReads() * ONE_REQUEST, usage.total(REGISTER_READ));
        assertEquals(store.registerWrites() * ONE_REQUEST, usage.total(REGISTER_WRITE));
        assertEquals(store.blobReads() * ONE_REQUEST, usage.total(BLOB_READ));
        assertEquals(store.blobWrites() * ONE_REQUEST, usage.total(BLOB_WRITE));
        assertEquals(store.listings() * ONE_REQUEST, usage.total(LISTING));
        assertEquals(store.deletes() * ONE_REQUEST, usage.total(IndexUsage.Kind.DELETE));
        assertEquals(store.bytesRead(), usage.total(BYTES_READ));
        assertEquals(store.bytesWritten(), usage.total(BYTES_WRITTEN));
    }
}
