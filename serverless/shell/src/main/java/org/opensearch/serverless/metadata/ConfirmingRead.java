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
 * A descriptor read from the store, and when that read began by {@link System#nanoTime}.
 *
 * @param descriptor what was read
 * @param readStartedNanos when the read began
 */
public record ConfirmingRead(IndexDescriptor descriptor, long readStartedNanos) {
}
