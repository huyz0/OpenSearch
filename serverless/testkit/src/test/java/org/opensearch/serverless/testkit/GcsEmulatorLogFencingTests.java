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

/** The WAL fencing histories on fake-gcs-server, through the production GCS blob store. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class GcsEmulatorLogFencingTests extends LogFencingTestCase {

    private final List<String> created = new ArrayList<>();

    @Override
    protected BlobStore newStore() throws Exception {
        final String endpoint = GcsEmulatorBlobContainerConformanceTests.gcsEndpoint();
        assumeTrue(
            "no gcs emulator at " + endpoint + "; start one with: " + GcsEmulatorBlobContainerConformanceTests.gcsHowToStart(),
            EmulatorConformanceTestCase.reachable(endpoint + "/storage/v1/b")
        );
        final String bucket = "fence-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        created.add(bucket);
        return GcsEmulatorBlobStores.create(endpoint, bucket);
    }

    @org.junit.After
    public void removeBucketsThisTestMade() {
        for (String bucket : created) {
            GcsEmulatorBlobStores.deleteBucket(GcsEmulatorBlobContainerConformanceTests.gcsEndpoint(), bucket);
        }
        created.clear();
    }
}
