/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/** Concrete index names, in the order the resolver produced them. */
public class ResolveIndexNamesResponse extends ActionResponse implements ToXContentObject {

    private final List<String> indices;

    public ResolveIndexNamesResponse(List<String> indices) {
        this.indices = List.copyOf(indices);
    }

    public ResolveIndexNamesResponse(StreamInput in) throws IOException {
        this.indices = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(indices);
    }

    public List<String> getIndices() {
        return indices;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("indices", indices);
        builder.endObject();
        return builder;
    }
}
