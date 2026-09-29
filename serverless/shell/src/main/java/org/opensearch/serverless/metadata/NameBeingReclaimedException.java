/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import java.io.IOException;

/**
 * Thrown when creating a name whose old tombstone a sweep is removing at that moment.
 *
 * <p>Not "already exists": the name is free, and will be creatable within seconds. The create is refused
 * rather than allowed to swap over the tombstone because the sweep's delete is unconditional -- a create
 * that landed between the sweep claiming the tombstone and deleting it would be deleted with it. See
 * {@code DescriptorStore#sweepTombstones}.
 */
public class NameBeingReclaimedException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param name the name being reclaimed
     */
    public NameBeingReclaimedException(String name) {
        super("the name [" + name + "] belonged to a deleted index whose record is being removed right now; retry shortly");
    }
}
