/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The AWS SDK's Apache HTTP client starts one shared reaper thread and keeps it after a client is closed,
 * so a test that builds a real client leaks it by the suite's reckoning. Same shape as
 * {@link EventLoopThreadFilter}, for the synchronous client instead of the asynchronous one.
 */
public class IdleConnectionReaperThreadFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("idle-connection-reaper");
    }
}
