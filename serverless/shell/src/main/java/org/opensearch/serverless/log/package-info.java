/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Durable operation boundaries shared by the existing object-store path and the low-latency log.
 * Implementing this interface alone does not establish leadership, quorum replication, or fresh reads.
 */
package org.opensearch.serverless.log;
