/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.ThreadFilter;

import org.opensearch.repositories.s3.MinioBlobStores;

/**
 * Ignores the threads an S3 client keeps after a test: its executors, the SDK's event loop and its response timeout scheduler.
 * For tests that run on the filesystem by default and on a live endpoint through {@link MeterStores}, so they keep the thread
 * leak check for everything else rather than turning it off.
 */
public final class S3ClientThreads implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        final String name = t.getName();
        return name.startsWith(MinioBlobStores.S3_CLIENT_THREAD_PREFIX)
            || name.startsWith("AwsEventLoop")
            || name.equals("response-input-stream-timeout-scheduler")
            || name.startsWith("sdk-async-response")
            || name.startsWith("idle-connection-reaper");
    }
}
