/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Node-to-node forwarding for the data path.
 *
 * <p>The only thing that crosses a node boundary in this design. Metadata never does — every node reads
 * the object store for itself — so these actions carry documents and query results, and nothing else.
 *
 * <p>Peers are found through their leases: a node's lease already records the transport address it
 * bound, so membership doubles as the address book and no separate discovery is needed.
 */
package org.opensearch.serverless.transport;
