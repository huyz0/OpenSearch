/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import java.util.Arrays;
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
public final class IndexNameEntry {

    /** An ordinary index. */
    public static final byte STATUS_OPEN = 0;

    /** A closed index. Still resolvable by name, which is why it is a status rather than an absence. */
    public static final byte STATUS_CLOSED = 1;

    private final String name;
    private final byte[] uuid;
    private final byte status;

    public IndexNameEntry(String name, byte[] uuid, byte status) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof IndexNameEntry == false) {
            return false;
        }
        IndexNameEntry that = (IndexNameEntry) o;
        return status == that.status && name.equals(that.name) && Arrays.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * name.hashCode() + Arrays.hashCode(uuid)) + status;
    }

    @Override
    public String toString() {
        return "IndexNameEntry[" + name + ", status=" + status + "]";
    }
}
