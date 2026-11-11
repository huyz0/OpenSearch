/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.serverless.cluster.IndexDescriptor;

import java.util.LinkedHashMap;
import java.util.Map;

/** A bounded page of descriptors, and where to resume. */
public final class DescriptorPage {

    private final Map<String, IndexDescriptor> descriptors;
    private final String nextAfter;

    DescriptorPage(Map<String, IndexDescriptor> descriptors, String nextAfter) {
        this.descriptors = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(descriptors));
        this.nextAfter = nextAfter;
    }

    /**
     * Returns the descriptors in this page.
     *
     * @return descriptors by index name, in name order
     */
    public Map<String, IndexDescriptor> descriptors() {
        return descriptors;
    }

    /**
     * Returns the cursor to resume from, or null when the walk is complete.
     *
     * @return the next cursor, or null
     */
    public String nextAfter() {
        return nextAfter;
    }

    /**
     * Reports whether another page follows.
     *
     * @return true when there is more
     */
    public boolean hasMore() {
        return nextAfter != null;
    }
}
