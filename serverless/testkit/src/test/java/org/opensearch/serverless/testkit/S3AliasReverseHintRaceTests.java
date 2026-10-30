/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.repositories.s3.MinioBlobStores;

import java.util.Locale;

/** The alias publication races over real S3 conditional requests. */
@ThreadLeakFilters(filters = S3AliasReverseHintRaceTests.SdkTimeoutThreadFilter.class)
public class S3AliasReverseHintRaceTests extends AliasReverseHintRaceTests {
    /** The SDK's shared daemon timeout scheduler has no client shutdown hook. */
    public static class SdkTimeoutThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("response-input-stream-timeout-scheduler");
        }
    }

    private String bucket;

    private static String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, "http://127.0.0.1:9000");
    }

    private static String accessKey() {
        return System.getProperty("tests.serverless.s3.access_key", "minioadmin");
    }

    private static String secretKey() {
        return System.getProperty("tests.serverless.s3.secret_key", "minioadmin");
    }

    @Override
    protected BlobStore newStore() throws Exception {
        assumeTrue(
            "set tests.serverless.s3.endpoint to run S3 alias races",
            System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT) != null
        );
        bucket = "alias-" + randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        return MinioBlobStores.create(endpoint(), accessKey(), secretKey(), bucket, createTempDir());
    }

    @org.junit.After
    public void removeBucketThisTestMade() throws Exception {
        if (bucket != null) {
            MinioBlobStores.deleteBucket(endpoint(), accessKey(), secretKey(), bucket, createTempDir());
            bucket = null;
        }
    }
}
