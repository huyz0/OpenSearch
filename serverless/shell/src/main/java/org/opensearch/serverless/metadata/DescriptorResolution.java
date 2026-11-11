/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.serverless.cluster.IndexDescriptor;

/** What a name turned out to be, and the register generation it was read at -- one read for both. */
public final class DescriptorResolution {

    private final IndexDescriptor index;
    private final org.opensearch.serverless.cluster.AliasRecord alias;
    private final long generation;

    DescriptorResolution(IndexDescriptor index, org.opensearch.serverless.cluster.AliasRecord alias) {
        this(index, alias, BlobRegister.ABSENT_GENERATION);
    }

    DescriptorResolution(IndexDescriptor index, org.opensearch.serverless.cluster.AliasRecord alias, long generation) {
        this.index = index;
        this.alias = alias;
        this.generation = generation;
    }

    /**
     * Returns the generation the register held when this was read, for a compare-and-swap on what
     * was read -- never a generation read separately, which guards nothing (see
     * {@code MetadataPlane.AliasAtGeneration}).
     *
     * @return the generation, or {@link BlobRegister#ABSENT_GENERATION} when the name is absent
     */
    public long generation() {
        return generation;
    }

    /**
     * Returns the index, if the name is one.
     *
     * @return the descriptor, or null
     */
    public IndexDescriptor index() {
        return index;
    }

    /**
     * Returns the alias, if the name is one.
     *
     * @return the alias, or null
     */
    public org.opensearch.serverless.cluster.AliasRecord alias() {
        return alias;
    }

    /**
     * Reports whether the name stands for nothing at all.
     *
     * @return true if absent
     */
    public boolean absent() {
        return index == null && alias == null;
    }
}
