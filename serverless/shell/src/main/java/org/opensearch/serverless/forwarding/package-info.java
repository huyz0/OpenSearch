/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * The requests a node forwards to the node that serves a shard, and their answers: index, bulk, get, search,
 * explain and stats, as they travel over the transport.
 *
 * <p>Only the wire messages live here. The router that picks the serving node and sends them, and the handlers that
 * answer them, are in the transport package, beside the node-to-node protocols for tails, commits and cache warming.
 */
package org.opensearch.serverless.forwarding;
