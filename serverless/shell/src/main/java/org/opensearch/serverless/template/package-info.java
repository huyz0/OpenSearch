/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Operator-authored configuration kept as named documents: index and component templates, ingest and search
 * pipelines, and stored scripts.
 *
 * <p>Each store is a set of immutable revision blobs behind one register that names them, with a marker that moves on
 * every change, so a node's cached copy costs one small register read to check. What a new index inherits from the
 * templates matching its name is resolved here too. The metadata plane hands out one store per container.
 */
package org.opensearch.serverless.template;
