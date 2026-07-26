/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/** Expressions plus the {@link IndicesOptions} that decide how they expand. */
public class ResolveIndexNamesRequest extends ActionRequest {

    private final String[] expressions;
    private final IndicesOptions indicesOptions;

    public ResolveIndexNamesRequest(IndicesOptions indicesOptions, String... expressions) {
        this.indicesOptions = Objects.requireNonNull(indicesOptions, "indicesOptions");
        this.expressions = expressions == null ? new String[0] : expressions;
    }

    public ResolveIndexNamesRequest(StreamInput in) throws IOException {
        super(in);
        this.expressions = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(expressions);
        indicesOptions.writeIndicesOptions(out);
    }

    public String[] expressions() {
        return expressions;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public ActionRequestValidationException validate() {
        // No expression means everything, which the resolver handles, so there is nothing to reject.
        return null;
    }
}
