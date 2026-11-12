/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Schemas shared across indices.
 *
 * <p>A multi-tenant fleet holds many indices created from a handful of templates, so most open indices on a node
 * carry the same mapping and the same analysis settings. Parsed, each one costs about as much heap as the rest of
 * an open index put together. Sharing it keeps one parsed copy per distinct schema on a node instead of one per
 * index, without changing what any index's mapping is: an index that updates its mapping gets a mapping of its own,
 * exactly as before.
 */
package org.opensearch.serverless.schema;
