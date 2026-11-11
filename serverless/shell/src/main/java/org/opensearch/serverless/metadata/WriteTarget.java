/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.serverless.cluster.IndexDescriptor;

/**
 * Where a write addressed to a name goes.
 *
 * @param index the index the write lands in
 * @param dataStream the data stream the name resolved through, or null when the name was an index
 */
public record WriteTarget(IndexDescriptor index, org.opensearch.serverless.cluster.AliasRecord dataStream) {
}
