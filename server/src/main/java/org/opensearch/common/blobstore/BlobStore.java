/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.blobstore;

import org.opensearch.cluster.metadata.RepositoryMetadata;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;

/**
 * An interface for storing blobs.
 *
 * @opensearch.internal
 */
public interface BlobStore extends Closeable {

    /**
     * Get a blob container instance for storing blobs at the given {@link BlobPath}.
     */
    BlobContainer blobContainer(BlobPath path);

    /**
     * Checks backend-specific key constraints before publishing metadata that refers to the key.
     * Implementations must not create the container or write data. Decorators must delegate this check.
     *
     * @param path the container path
     * @param blobName a blob that will be addressed in the container
     * @throws IllegalArgumentException if the backend cannot represent the key
     */
    default void validateKey(BlobPath path, String blobName) {}

    /**
     * Returns statistics on the count of operations that have been performed on this blob store
     */
    /**
     * Returns statistics on the count of operations that have been performed on this blob store
     */
    default Map<String, Long> stats() {
        return Collections.emptyMap();
    }

    /**
     * Returns details statistics of operations that have been performed on this blob store
     */
    default Map<Metric, Map<String, Long>> extendedStats() {
        return Collections.emptyMap();
    }

    /**
     * Reload the blob store inplace
     */
    default void reload(RepositoryMetadata repositoryMetadata) {}

    /**
     * Returns a boolean indicating if blobStore has object metadata support enabled
     */
    default boolean isBlobMetadataEnabled() {
        return false;
    }

    /**
     * Metrics for BlobStore interactions
     */
    enum Metric {
        GENERIC_STATS("generic_stats"),
        REQUEST_SUCCESS("request_success_total"),
        REQUEST_FAILURE("request_failures_total"),
        REQUEST_LATENCY("request_time_in_millis"),
        RETRY_COUNT("request_retry_count_total");

        private String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        public String metricName() {
            return this.metricName;
        }
    }

}
