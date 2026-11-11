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
 * More names share a prefix than a caller is willing to be handed.
 *
 * <p>A refusal rather than a truncation, and that is the whole point of the class existing: an answer
 * cut off at a limit looks exactly like a complete one, and a search that quietly covered the first
 * five hundred indices of a thousand is the confident partial answer this design refuses everywhere
 * else.
 */
public final class TooManyMatchesException extends IOException {

    private final String prefix;
    private final int cap;

    TooManyMatchesException(String prefix, int cap) {
        super(
            "more than "
                + cap
                + " indices begin with ["
                + prefix
                + "]. Narrowing the pattern or naming the indices is the answer; returning the first "
                + cap
                + " would be a partial result that looks complete."
        );
        this.prefix = prefix;
        this.cap = cap;
    }

    /**
     * Returns the prefix that matched too much.
     *
     * @return the prefix
     */
    public String prefix() {
        return prefix;
    }

    /**
     * Returns the cap that was exceeded.
     *
     * @return the cap
     */
    public int cap() {
        return cap;
    }
}
