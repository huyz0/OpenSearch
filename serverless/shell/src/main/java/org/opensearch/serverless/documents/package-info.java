/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Document writes below the REST layer: a bulk body parsed into items, routed through its pipelines, grouped by shard
 * and dispatched, and a single document's write planned and applied or forwarded. The REST handlers in
 * {@code rest} read the request and render the outcome; nothing here sees a channel.
 */
package org.opensearch.serverless.documents;
