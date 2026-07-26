/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One resolved entry from the name index: a name, the UUID of what it names, and a status byte.
 *
 * <p>Lookups hand back this rather than an ordinal into {@link CompactNameIndex}. An ordinal is only
 * meaningful against the exact structure instance it came from, and {@link NameIndex} swaps that
 * instance out from under readers when it rebuilds, so an ordinal that escaped would silently start
 * meaning a different index.
 *
 * <p>The UUID stays as raw bytes because that is how it is stored: 16 bytes rather than the 36
 * characters of its canonical text form. At 100M entries that difference is most of the reason the
 * compact representation costs 45 B/name against a {@code HashMap}'s 145.5 B.
 */
public final class IndexNameEntry implements Writeable {

    /** An ordinary index. */
    public static final byte STATUS_OPEN = 0;

    /** A closed index. Still resolvable by name, which is why it is a status rather than an absence. */
    public static final byte STATUS_CLOSED = 1;

    /**
     * An alias, which names a set of indices rather than one.
     *
     * <p>Aliases share the name space with indices rather than living in a structure of their own,
     * because OpenSearch forbids an alias and an index having the same name. Keeping them together makes
     * that constraint checkable, and lets one prefix scan resolve both, which is what a wildcard over a
     * mixed name space needs.
     */
    public static final byte STATUS_ALIAS = 2;

    private final String name;
    private final byte[] uuid;
    private final byte status;

    /**
     * Target index names, for an alias. Empty for an ordinary index.
     *
     * <p>Carried on the entry rather than only in {@link CompactNameIndex}'s side arrays because an
     * alias created into {@link NameIndexOverlay} has no ordinals yet, and because a rebuild reads
     * entries back out and would otherwise silently drop every alias's targets.
     */
    private final List<String> targets;

    public IndexNameEntry(String name, byte[] uuid, byte status) {
        this(name, uuid, status, Collections.emptyList());
    }

    public IndexNameEntry(String name, byte[] uuid, byte status, List<String> targets) {
        this.name = Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uuid, "uuid");
        if (uuid.length != CompactNameIndex.UUID_LENGTH) {
            throw new IllegalArgumentException("uuid must be " + CompactNameIndex.UUID_LENGTH + " bytes, was " + uuid.length);
        }
        // Defensive copy on the way in and on the way out. The array is small and entries are handed to
        // callers that outlive the structure they came from, so sharing it would let a caller mutate
        // what another caller sees.
        this.uuid = uuid.clone();
        this.status = status;
        this.targets = List.copyOf(Objects.requireNonNull(targets, "targets"));
    }

    public IndexNameEntry(StreamInput in) throws IOException {
        this(in.readString(), in.readByteArray(), in.readByte(), in.readStringList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeByteArray(uuid);
        out.writeByte(status);
        // Targets are written for every entry, not only aliases. An empty list costs one vInt, and a
        // format that varies by status is a format two readers can disagree about.
        out.writeStringCollection(targets);
    }

    public String getName() {
        return name;
    }

    public byte[] getUuid() {
        return uuid.clone();
    }

    public byte getStatus() {
        return status;
    }

    public boolean isClosed() {
        return status == STATUS_CLOSED;
    }

    public boolean isAlias() {
        return status == STATUS_ALIAS;
    }

    /** Target index names for an alias, empty for an ordinary index. Immutable. */
    public List<String> getTargets() {
        return targets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof IndexNameEntry == false) {
            return false;
        }
        IndexNameEntry that = (IndexNameEntry) o;
        return status == that.status && name.equals(that.name) && Arrays.equals(uuid, that.uuid) && targets.equals(that.targets);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * (31 * name.hashCode() + Arrays.hashCode(uuid)) + status) + targets.hashCode();
    }

    @Override
    public String toString() {
        return "IndexNameEntry[" + name + ", status=" + status + "]";
    }
}
