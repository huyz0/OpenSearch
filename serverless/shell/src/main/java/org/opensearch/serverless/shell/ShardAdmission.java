/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

/**
 * What a reader or a frozen view asks before taking a slot on this node: the cap, and whether room
 * can be made under it.
 *
 * <p>The cap and the eviction policy live in the reconciler; readers and views used to compare
 * against the constant default and refuse, so a node configured for two hundred shards opened a
 * thousand readers, and one at the constant refused the next search until the idle sweep came
 * round. {@code BackgroundReconciler} installs itself here when it is built.
 */
public interface ShardAdmission {
    /**
     * Returns how many shards this node will hold.
     *
     * @return the cap
     */
    int maxShardsHeld();

    /**
     * Tries to free a slot by letting go of a shard nobody has used lately.
     *
     * @return true if there is now room under the cap
     */
    boolean makeRoom();
}
