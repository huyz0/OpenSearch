/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The actual content of one manifest shard
 * blob -- the {@link UploadedIndexMetadata} entries {@link ManifestShardFunction} partitioned into it.
 *
 * <p>{@link UploadedManifestShard} is only the top-level manifest's *reference* to a shard (its id, blob
 * name, and entry count); this is what that blob name actually points at. Kept as its own small type,
 * the same way the top-level manifest's own {@code indices} array is a list of {@code
 * UploadedIndexMetadata}, rather than reusing {@link ClusterMetadataManifest} itself for something that
 * is not a manifest.
 */
public class ManifestShardContent implements Writeable, ToXContentFragment {

    private static final String INDICES_FIELD = "indices";

    private final List<UploadedIndexMetadata> indices;

    public ManifestShardContent(List<UploadedIndexMetadata> indices) {
        this.indices = Collections.unmodifiableList(indices);
    }

    public ManifestShardContent(StreamInput in) throws IOException {
        this.indices = Collections.unmodifiableList(in.readList(UploadedIndexMetadata::new));
    }

    public List<UploadedIndexMetadata> getIndices() {
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(indices);
    }

    /**
     * A fragment, not a self-wrapping object: matches {@link ClusterMetadataManifest}'s own {@code
     * ToXContentFragment} choice, for the same reason -- {@link org.opensearch.repositories.blobstore.BaseBlobStoreFormat#serialize}
     * already wraps whatever it serializes in one top-level {@code startObject()}/{@code endObject()}
     * pair; a fragment writes only its fields inside that, where a self-wrapping {@code ToXContentObject}
     * would open a second, field-name-less object and fail to serialize at all.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(INDICES_FIELD);
        for (UploadedIndexMetadata index : indices) {
            builder.startObject();
            index.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    /**
     * Entries are parsed at {@link ClusterMetadataManifest#CODEC_V5} fidelity (descriptor-capable)
     * regardless of the outer manifest's own codec -- a shard blob's entry format is independent of the
     * top-level manifest wrapper that references it.
     */
    public static ManifestShardContent fromXContent(XContentParser parser) throws IOException {
        List<UploadedIndexMetadata> indices = new ArrayList<>();
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        while (token != XContentParser.Token.END_OBJECT && token != null) {
            if (token == XContentParser.Token.FIELD_NAME && INDICES_FIELD.equals(parser.currentName())) {
                parser.nextToken(); // START_ARRAY
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    indices.add(UploadedIndexMetadata.fromXContent(parser, ClusterMetadataManifest.CODEC_V5));
                }
            }
            token = parser.nextToken();
        }
        return new ManifestShardContent(indices);
    }
}
