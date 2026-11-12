/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.OptionalLong;

/** Copies a published commit's files out of the blob store: whole, or just the commit point to read its user data. */
final class CommitDownloads {

    private CommitDownloads() {}

    /** A sequence number recorded in a published commit point's user data, under a key. */
    static OptionalLong userData(SegmentPublisher publisher, CommitManifest manifest, String key) throws IOException {
        // Only the commit point and its segments' info files: a few small blobs, nothing of the segments themselves.
        try (Directory scratch = new ByteBuffersDirectory()) {
            String commitFile = null;
            for (Map.Entry<String, String> file : manifest.files().entrySet()) {
                final String name = file.getKey();
                final boolean isCommit = name.startsWith(IndexFileNames.SEGMENTS + "_");
                if (isCommit == false && name.endsWith(".si") == false) {
                    continue;
                }
                if (isCommit) {
                    commitFile = name;
                }
                try (InputStream in = publisher.openFile(manifest, name); IndexOutput out = scratch.createOutput(name, IOContext.DEFAULT)) {
                    copy(in, out);
                }
            }
            if (commitFile == null) {
                return OptionalLong.empty();
            }
            final String value = SegmentInfos.readCommit(scratch, commitFile).getUserData().get(key);
            return value == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(value));
        }
    }

    /** Writes every file of a published commit into a directory. */
    static void restore(SegmentPublisher publisher, CommitManifest manifest, Directory target, ShardId shardId) throws IOException {
        for (Map.Entry<String, String> file : manifest.files().entrySet()) {
            // Local disk is a cache, and a node re-opening onto a newer commit finds files from an earlier
            // one still there. The segment counter is carried in the commit, so a successor that restored
            // this node's last published commit mints the same next segment name this node did for a
            // segment it never published: a same-named file is not necessarily the same bytes. Overwrite
            // rather than trust the name.
            try {
                target.deleteFile(file.getKey());
            } catch (IOException ignored) {
                // absent, which is the normal case on a cold node
            }
            try (
                InputStream in = publisher.openFile(manifest, file.getKey());
                IndexOutput out = target.createOutput(file.getKey(), IOContext.DEFAULT)
            ) {
                copy(in, out);
            } catch (AlreadyClosedException e) {
                throw new IOException("directory closed while restoring " + shardId, e);
            }
        }
    }

    private static void copy(InputStream in, IndexOutput out) throws IOException {
        final byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) > 0) {
            out.writeBytes(buffer, read);
        }
    }
}
