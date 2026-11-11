/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.IOException;
import java.util.Optional;

/**
 * Opens, extends and releases points in time, checking that a capture still names the incarnation it froze. Split
 * out of {@link MetadataPlane}, which keeps the public entry points.
 */
final class PointsInTime {

    private final PointInTimeStore pointInTimeStore;
    private final DescriptorStore descriptors;

    PointsInTime(PointInTimeStore pointInTimeStore, DescriptorStore descriptors) {
        this.pointInTimeStore = pointInTimeStore;
        this.descriptors = descriptors;
    }

    /**
     * Freezes the current commit of every shard of an index.
     *
     * <p>Written before it is used and read by the collector, which is the ordering that matters: a view
     * that existed only in the memory of the node that took it would be a promise the sweep never heard.
     *
     * @param pit the view to record
     * @throws IOException if the write fails
     */
    public void createPointInTime(PointInTime pit) throws IOException {
        pointInTimeStore.create(pit);
    }

    /**
     * Records that a freeze has begun, before any manifest is read.
     *
     * <p>The first half of the two-phase capture {@link PointInTime#isCapturing} describes: this record
     * names the index and holds no shards, and the collector deletes nothing for that index while it
     * stands. {@link #finishPointInTime} replaces it with the real view.
     *
     * @param pit a record naming the index, with no shards
     * @throws IOException if the write fails
     * @throws IllegalArgumentException if the record is not a capture marker
     */
    public void beginPointInTime(PointInTime pit) throws IOException {
        if (pit.isCapturing() == false) {
            throw new IllegalArgumentException("a capture marker must name an index and hold no shards: " + pit.id());
        }
        pointInTimeStore.create(pit);
        try {
            verifyCaptureIncarnation(pit.index(), pit.indexUuid());
        } catch (IOException | RuntimeException e) {
            try {
                releasePointInTime(pit.id());
            } catch (Exception cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    /**
     * Checks an index after its provisional pin was published, before reading any captured files.
     *
     * @param index the captured index
     * @param uuid its expected incarnation, or null for a legacy capture
     * @throws IOException if the index was deleted or recreated
     */
    public void verifyCaptureIncarnation(String index, String uuid) throws IOException {
        final Optional<IndexDescriptor> current = descriptors.get(index);
        if (current.isEmpty() || (uuid != null && uuid.equals(current.get().uuid()) == false)) {
            throw new IOException("index [" + index + "] was deleted or recreated while starting capture");
        }
    }

    /**
     * Replaces a capture marker with the finished view, under the same id.
     *
     * <p>Overwrites rather than creates, which is the whole point: the id is already pinning the index,
     * and a second id would leave the marker behind to expire on its own while the real view stood beside
     * it.
     *
     * @param pit the finished view
     * @throws IOException if the write fails
     */
    public void finishPointInTime(PointInTime pit) throws IOException {
        pointInTimeStore.finish(pit);
    }

    /**
     * Moves a frozen view's deadline later.
     *
     * <p>The same record with a later expiry, written over the old one. Nothing about which commits the
     * view holds changes -- a keep-alive extends a view, it does not refresh it -- so the collector's view
     * of what is referenced is unchanged and only the reaper's deadline moves.
     *
     * @param pit the view, carrying its new deadline
     * @throws IOException if the write fails
     */
    public void extendPointInTime(PointInTime pit) throws IOException {
        if (pointInTimeStore.extend(pit, null) == false) {
            throw new IOException("point in time is no longer active");
        }
    }

    /**
     * Atomically extends the view if its deadline still matches the caller's observation.
     *
     * @param pit the view with its new deadline
     * @param expectedExpiresAtMillis the deadline observed by the caller
     * @return whether this extension won
     * @throws IOException if the backing store fails
     */
    public boolean extendPointInTime(PointInTime pit, long expectedExpiresAtMillis) throws IOException {
        return pointInTimeStore.extend(pit, expectedExpiresAtMillis);
    }

    /**
     * Reads a frozen view.
     *
     * @param id the identifier
     * @return the view, or empty if it never existed or has been released
     * @throws IOException if the read fails
     */
    public java.util.Optional<PointInTime> pointInTime(String id) throws IOException {
        return pointInTimeStore.read(id);
    }

    /**
     * Releases a frozen view, so the collector stops holding its files.
     *
     * @param id the identifier
     * @return true if there was one
     * @throws IOException if the delete fails
     */
    public boolean releasePointInTime(String id) throws IOException {
        return pointInTimeStore.release(id);
    }

    /**
     * Reads every frozen view that has not expired.
     *
     * <p>Expiry is claimed atomically before excluding a view, so a keep-alive acknowledged on
     * another node cannot lose its files to a collector using a stale deadline.
     *
     * @param nowMillis the current time
     * @return the live views
     * @throws IOException if listing or reading fails
     */
    public java.util.List<PointInTime> livePointsInTime(long nowMillis) throws IOException {
        return pointInTimeStore.live(nowMillis);
    }

    /**
     * Removes frozen views that have expired.
     *
     * @param nowMillis the current time
     * @return how many were removed
     * @throws IOException if listing or deleting fails
     */
    public int reapPointsInTime(long nowMillis) throws IOException {
        return pointInTimeStore.reap(nowMillis);
    }
}
