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
 * The same suite against RustFS, single node, single drive.
 *
 * <p>A third S3 implementation, and the first here that claims to honour {@code If-Match} on
 * {@code DeleteObject}: MinIO RELEASE.2025-04-22 ignores it, so a node on MinIO keeps tombstones and the
 * conditional-delete path has had no S3-API store to run on. Validated against {@code rustfs/rustfs:1.0.0},
 * pinned by digest {@code sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff}.
 *
 * <p>Evidence for RustFS, not for AWS: R11 stays open for S3, GCS and Azure until it runs against them.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RustfsBlobContainerConformanceTests extends MinioBlobContainerConformanceTests {

    /** Where to find this RustFS endpoint. */
    public static final String ENDPOINT_PROPERTY = "tests.serverless.rustfs.endpoint";

    @Override
    protected String endpoint() {
        return System.getProperty(ENDPOINT_PROPERTY, "http://127.0.0.1:9200");
    }

    @Override
    protected String howToStart() {
        return "docker run -d --name serverless-rustfs-single -p 9200:9000 -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin "
            + "rustfs/rustfs@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff /data";
    }

    @Override
    protected String accessKey() {
        return "rustfsadmin";
    }

    @Override
    protected String secretKey() {
        return "rustfsadmin";
    }
}
