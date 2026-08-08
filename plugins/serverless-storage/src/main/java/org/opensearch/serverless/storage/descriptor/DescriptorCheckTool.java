/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.common.blobstore.BlobContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Diagnostic utility for inspecting Object Storage descriptor blobs and tombstones.
 */
public final class DescriptorCheckTool {

    public record Report(int totalLiveDescriptors, int totalTombstones, List<String> corruptBlobs) {}

    private DescriptorCheckTool() {}

    /**
     * Inspects live descriptors and tombstones in the container, checking for corrupted or unparseable entries.
     */
    public static Report checkIntegrity(BlobContainer descriptorContainer, BlobContainer tombstoneContainer) {
        int liveCount = 0;
        int tombstoneCount = 0;
        List<String> corrupt = new ArrayList<>();

        if (descriptorContainer != null) {
            try {
                var blobs = descriptorContainer.listBlobs();
                liveCount = blobs.size();
                for (String name : blobs.keySet()) {
                    var reg = descriptorContainer.readRegister(name);
                    if (reg.isPresent()) {
                        try {
                            BlobDescriptorBackend.decode(reg.get().value());
                        } catch (Exception e) {
                            corrupt.add(name + ": " + e.getMessage());
                        }
                    }
                }
            } catch (IOException e) {
                corrupt.add("descriptors-container: " + e.getMessage());
            }
        }

        if (tombstoneContainer != null) {
            try {
                var blobs = tombstoneContainer.listBlobs();
                tombstoneCount = blobs.size();
                for (String name : blobs.keySet()) {
                    var reg = tombstoneContainer.readRegister(name);
                    if (reg.isPresent()) {
                        try {
                            BlobDescriptorBackend.decode(reg.get().value());
                        } catch (Exception e) {
                            corrupt.add("tombstone-" + name + ": " + e.getMessage());
                        }
                    }
                }
            } catch (IOException e) {
                corrupt.add("tombstones-container: " + e.getMessage());
            }
        }

        return new Report(liveCount, tombstoneCount, corrupt);
    }
}
