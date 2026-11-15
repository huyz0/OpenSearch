/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * A node's core and its own services: writes and their fences, writer activation and hibernation, reader serving,
 * leases, storage and the descriptors it holds. {@link org.opensearch.serverless.node.NodeContext} is what every
 * package above sees of a node; nothing here reaches up to the transport, reconcile or REST layers.
 */
package org.opensearch.serverless.node;
