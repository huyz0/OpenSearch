/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Resharding-by-copy (rfc-serverless-opensearch.md &sect;16 Phase 5): split a shard into {@code
 * numPartitions} zero-copy target shards without rewriting any bundle bytes, and the reader-side
 * doc-routing partition filter ({@link org.opensearch.serverless.storage.resharding.PartitionFilteringDirectoryReader})
 * that makes each target correctly serve only its own slice of the pre-split document space in the
 * meantime -- "logical-first, physical-later," per that section's own status note.
 *
 * <p>Explicitly out of scope for this increment: physically rewriting a split target's bundles
 * down to just its own partition (which would let the doc-routing filter be dropped once done),
 * and shrink (the inverse operation, merging several shards' document spaces back into one) --
 * both real, sizeable follow-ups on top of the split primitive this package implements.
 */
package org.opensearch.serverless.storage.resharding;
