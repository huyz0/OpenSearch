/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

/**
 * An explanation, and which copy produced it.
 *
 * @param exists whether the document was found at all
 * @param explanation Lucene's own account of the score, or null when the document does not exist
 * @param servedBy the node that produced it, which may not be the node asked
 * @param realtime whether it came from a writer, and so sees writes not yet published
 */
public record Explained(boolean exists, org.apache.lucene.search.Explanation explanation, String servedBy, boolean realtime) {

    /**
     * Reports whether the document matched.
     *
     * @return true when it exists and scored a match
     */
    public boolean matched() {
        return exists && explanation != null && explanation.isMatch();
    }
}
