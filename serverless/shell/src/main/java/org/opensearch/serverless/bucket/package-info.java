/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Buckets: many small indices treated as one unit by the object store.
 *
 * <p>A small index pays fixed costs that have nothing to do with how much it holds: a head taken and released, a
 * manifest register swapped, a descriptor and head re-read while it is held, a handful of reads to open it cold. At
 * millions of mostly idle indices those fixed costs are most of the bill. A bucket shares them: one owner, one head,
 * one manifest register mapping each member to its own commit, one stream of packs holding only its members' files,
 * and one cache and placement unit.
 *
 * <p>What a bucket does not share is anything that defines an index's behaviour. Each member keeps its own Lucene
 * index, mapping, analysis, scoring statistics, deletes and encryption key; every entry inside the bucket is tagged
 * with the member index's uuid. An index that outgrows its bucket is promoted to its own unit.
 */
package org.opensearch.serverless.bucket;
