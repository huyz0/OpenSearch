/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import java.io.IOException;

/** More templates than a deployment may hold. */
public final class TooManyTemplatesException extends IOException {
    /**
     * Creates the exception.
     *
     * @param cap the limit that was reached
     */
    public TooManyTemplatesException(int cap) {
        super(
            "this deployment already holds "
                + cap
                + " templates, which is the most it will hold: index creation reads every template to find "
                + "the ones that match, so an unbounded number of them would make index creation "
                + "unboundedly slow. Delete one before adding another"
        );
    }
}
