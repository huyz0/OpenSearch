/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.repositories.gcs.GcsEmulatorBlobStores;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** The node-log store under faults and races on fake-gcs-server, through the production GCS blob store. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class GcsEmulatorNodeLogStoreFaultTests extends NodeLogStoreFaultTestCase {

    private final List<String> created = new ArrayList<>();
    private final List<BlobStore> stores = new ArrayList<>();

    @Override
    protected BlobStore newStore() throws Exception {
        final String endpoint = GcsEmulatorBlobContainerConformanceTests.gcsEndpoint();
        assumeTrue(
            "no gcs emulator at " + endpoint + "; start one with: " + GcsEmulatorBlobContainerConformanceTests.gcsHowToStart(),
            EmulatorConformanceTestCase.reachable(endpoint + "/storage/v1/b")
        );
        final String bucket = "nodelog-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        created.add(bucket);
        final BlobStore store = GcsEmulatorBlobStores.create(endpoint, bucket);
        stores.add(store);
        return store;
    }

    @org.junit.After
    public void removeBucketsThisTestMade() throws Exception {
        try {
            org.opensearch.common.util.io.IOUtils.close(stores);
        } finally {
            stores.clear();
            for (String bucket : created) {
                GcsEmulatorBlobStores.deleteBucket(GcsEmulatorBlobContainerConformanceTests.gcsEndpoint(), bucket);
            }
            created.clear();
        }
    }
}
