/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Splitting a unit in two without reindexing: the halves are built from the old unit's last published
 * commit by copying, per segment, only the documents whose routing hash falls in each half.
 *
 * <p>A build reads the old unit only through the object store, so any node can run it and the owner's
 * write and search path pays nothing. Nothing routes to the halves until a descriptor swap installs the
 * split layout; until then they are published files that a dropped plan leaves to garbage collection.
 */
package org.opensearch.serverless.split;
