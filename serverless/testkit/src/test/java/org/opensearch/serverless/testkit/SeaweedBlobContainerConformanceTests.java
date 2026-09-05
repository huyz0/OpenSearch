/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * The same suite against a second, unrelated S3 implementation.
 *
 * <p><b>Why a second one matters.</b> MinIO is one implementation of the S3 API, and a suite that has only
 * ever run against one implementation cannot tell "this property holds" from "this implementation happens
 * to do what our client asks". The conditional-write mapping in particular — {@code If-Match} and
 * {@code If-None-Match} preconditions, and what an S3 client does with a 412 — is code of ours that a
 * second dialect can find bugs in.
 *
 * <p><b>What it still is not.</b> Neither MinIO nor SeaweedFS is AWS S3, GCS or R2. Two compatible
 * implementations agreeing is evidence that the assumptions are not artefacts of one of them; it is not
 * evidence about a provider neither of them is. R11 stays open until it runs against an account.
 *
 * <p>LocalStack was the obvious second target and is not usable: its free S3 image was discontinued in
 * March 2026 and the remaining images require a licence.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class SeaweedBlobContainerConformanceTests extends MinioBlobContainerConformanceTests {

    /** Where to find a SeaweedFS S3 endpoint. */
    public static final String SEAWEED_ENDPOINT = "tests.serverless.seaweed.endpoint";

    @Override
    protected String endpoint() {
        return System.getProperty(SEAWEED_ENDPOINT, "http://127.0.0.1:8333");
    }

    @Override
    protected String howToStart() {
        return "docker run -d --name serverless-seaweed -p 8333:8333 chrislusf/seaweedfs:latest " + "server -s3 -s3.port=8333 -dir=/data";
    }

    // SeaweedFS accepts any credentials when no identity file is configured, which is what the default
    // container does. Naming them anyway, so a configured deployment fails on authorization rather than
    // silently succeeding as anonymous.
    @Override
    protected String accessKey() {
        return "seaweed";
    }

    @Override
    protected String secretKey() {
        return "seaweedsecret";
    }
}
