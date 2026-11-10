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

/**
 * The conformance suite against fake-gcs-server, through the production {@code GoogleCloudStorageBlobStore}.
 *
 * <p>What it covers: the register's {@code doesNotExist} and {@code generationMatch} preconditions on create
 * and delete, GCS object generations as register generations, and the {@code startOffset} paged listing.
 *
 * <p>What it does not: fake-gcs-server is one process with an in-memory backend, so it is linearizable by
 * construction. Agreement here shows the client maps GCS semantics correctly; it says nothing about how the real
 * service behaves under contention.
 */
// The Google HTTP client keeps connection threads alive past the test; they belong to the client.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class GcsEmulatorBlobContainerConformanceTests extends EmulatorConformanceTestCase {

    /** System property naming the fake-gcs-server endpoint. */
    public static final String ENDPOINT = "tests.serverless.gcs.endpoint";

    private final List<String> created = new ArrayList<>();

    static String gcsEndpoint() {
        return System.getProperty(ENDPOINT, "http://127.0.0.1:4443");
    }

    static String gcsHowToStart() {
        return "docker run -d --name serverless-fake-gcs -p 4443:4443 fsouza/fake-gcs-server:1.56.1 "
            + "-scheme http -port 4443 -public-host 127.0.0.1:4443";
    }

    @Override
    protected String endpoint() {
        return gcsEndpoint();
    }

    @Override
    protected String probeUrl() {
        return gcsEndpoint() + "/storage/v1/b";
    }

    @Override
    protected String howToStart() {
        return gcsHowToStart();
    }

    @Override
    protected String provider() {
        return "gcs";
    }

    @Override
    protected BlobStore newStore() throws Exception {
        final String bucket = "conformance-" + randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        created.add(bucket);
        return GcsEmulatorBlobStores.create(gcsEndpoint(), bucket);
    }

    /**
     * fake-gcs-server ignores {@code ifGenerationMatch} on a delete (real GCS honours it), so the stale-delete
     * assertion only runs where the probe finds the condition honoured.
     */
    @Override
    public void testAConditionalDeleteRefusesAStaleGeneration() throws Exception {
        assumeTrue("this emulator ignores the generation condition on a delete", honoursConditionalDelete());
        super.testAConditionalDeleteRefusesAStaleGeneration();
    }

    /** Removes the buckets this test made, so one run's leftovers cannot decide another's listing assertions. */
    @org.junit.After
    public void removeBucketsThisTestMade() {
        for (String bucket : created) {
            GcsEmulatorBlobStores.deleteBucket(gcsEndpoint(), bucket);
        }
        created.clear();
    }
}
