/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * One entry in the descriptor change log: a name, the uuid it referred to, and what happened to it.
 *
 * <p>Deliberately not the descriptor itself. Two consumers read this and neither needs the body. Cache
 * invalidation needs only the name, and the name index needs name, uuid and liveness. Carrying the whole
 * descriptor would make the log as large as the population it describes and would give a reader a second,
 * older copy of something the descriptor store already answers authoritatively.
 *
 * @param name the index name the change applies to
 * @param uuid the uuid the name referred to at the time, which is what distinguishes a delete from the
 *             delete-then-recreate that follows it
 * @param kind what happened
 * @param atMillis wall clock at append, used for bucketing and for ordering within a bucket on a best
 *                 effort basis, never as a cursor
 */
public record DescriptorChange(String name, String uuid, Kind kind, long atMillis) implements Writeable {

    /** What happened to a name. */
    public enum Kind {
        CREATED,
        UPDATED,
        DELETED
    }

    public DescriptorChange {
        Objects.requireNonNull(name, "a change needs a name");
        Objects.requireNonNull(uuid, "a change needs a uuid, so a recreate is distinguishable from a delete");
        Objects.requireNonNull(kind, "a change needs a kind");
    }

    public DescriptorChange(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readEnum(Kind.class), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(uuid);
        out.writeEnum(kind);
        out.writeVLong(atMillis);
    }

    /** Whether the name is live after this change, which is all the name index needs to decide. */
    public boolean live() {
        return kind != Kind.DELETED;
    }
}
