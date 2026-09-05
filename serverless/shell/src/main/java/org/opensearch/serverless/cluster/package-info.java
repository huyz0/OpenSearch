/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Truth records, and the projection of them into a node-local cluster state.
 *
 * <p>Nothing in this package is replicated, agreed, or published. Each node computes its own view from
 * descriptors and the shard assignments it actually holds; two nodes are expected to hold different
 * views and never to reconcile them with each other. See {@code rfc-serverless-shell.md} §5.
 */
package org.opensearch.serverless.cluster;
