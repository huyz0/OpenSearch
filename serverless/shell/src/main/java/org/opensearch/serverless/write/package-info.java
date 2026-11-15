/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * The values a write passes between layers: what a write or bulk asks for and what it was told, the document a read
 * returns, the hold that parks a shard's writes while it moves, and how work forwarded to a shard's owner failed.
 */
package org.opensearch.serverless.write;
