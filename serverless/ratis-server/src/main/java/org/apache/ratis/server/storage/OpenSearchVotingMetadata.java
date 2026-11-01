/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.ratis.server.storage;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.InternalApi;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

/** Bounded checksummed voting-state verification before the host starts any native service. */
@InternalApi
public final class OpenSearchVotingMetadata {
    private OpenSearchVotingMetadata() {}

    /**
     * Reads required metadata without the native missing-file default used during initial formatting.
     * @param file existing term/vote file on the exclusively owned volume
     * @return verified persisted term and vote
     * @throws IOException if the file is missing, oversized, corrupt or from an unsupported format
     */
    @SuppressForbidden(reason = "Pinned SDK metadata reader accepts java.io.File")
    public static RaftStorageMetadata verify(Path file) throws IOException {
        if (Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) == false) {
            throw new IOException("voting metadata must be a regular file");
        }
        return RaftStorageMetadataFileImpl.loadRequired(file.toFile());
    }
}
