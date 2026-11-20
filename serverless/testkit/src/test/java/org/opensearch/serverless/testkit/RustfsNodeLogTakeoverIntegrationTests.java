/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.junit.Before;

/** Runs node replay with the S3 SDK's process-wide pools. */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RustfsNodeLogTakeoverIntegrationTests extends NodeLogTakeoverIntegrationTests {

    @Before
    public void requireS3() {
        assumeTrue("requires a live S3 store", MeterStores.onS3());
    }
}
