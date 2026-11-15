/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * What a node does to shards wherever they live: get, write, update and by-query operations, and the search fan-out
 * they share. Above the transport they forward through, below the REST handlers and clients that call them.
 */
package org.opensearch.serverless.ops;
