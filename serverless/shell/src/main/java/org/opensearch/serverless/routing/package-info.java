/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Which unit of an index a document belongs to: the shard mode, the 64-bit routing hash, and the range
 * map from hash ranges to units.
 *
 * <p>A user's {@code number_of_shards} is a routing concept here. Declared shard {@code i} of {@code N}
 * is the {@code i}-th of {@code N} equal ranges of the unsigned 64-bit hash space, and a <em>unit</em> is
 * one Lucene index covering a contiguous range of that space. In {@code fixed} mode an index starts with
 * one unit per declared shard and keeps them; in {@code adaptive} mode it starts as one unit covering
 * everything and is cut as it grows. Routing is the same code in both: hash the routing value, then find
 * the unit whose range holds the hash. A unit is always either a run of whole declared shards or a piece
 * of one, so a declared shard always maps to the units that cover its range.
 *
 * <p>An index without a shard mode keeps the routing it always had, core's {@code OperationRouting}.
 */
package org.opensearch.serverless.routing;
