/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.FileUtils;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

/** The vote metadata API must fail if it cannot persist the rename into its parent directory. */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class AtomicFileOutputStreamDirectorySyncTests extends OpenSearchTestCase {

    public void testUsesOneMergedPatchedArtifactAndPreservesUpstreamHelpers() throws Exception {
        final var replacement = AtomicFileOutputStream.class.getProtectionDomain().getCodeSource().getLocation();
        final var upstream = FileUtils.class.getProtectionDomain().getCodeSource().getLocation();
        assertEquals("helper and replacement must come from the same merged artifact", replacement, upstream);
        assertTrue(replacement.toString(), replacement.toString().contains("opensearch-ratis-common"));
        final var copies = AtomicFileOutputStream.class.getClassLoader().getResources("org/apache/ratis/util/AtomicFileOutputStream.class");
        assertTrue(copies.hasMoreElements());
        copies.nextElement();
        assertFalse("an unpatched duplicate must not leak onto the runtime classpath", copies.hasMoreElements());
    }

    public void testSuccessfulClosePublishesCompleteContents() throws Exception {
        final Path destination = createTempDir().resolve("raft-meta");
        Files.writeString(destination, "old");
        try (var out = new AtomicFileOutputStream(legacyFile(destination))) {
            out.write("new metadata".getBytes(StandardCharsets.UTF_8));
            assertEquals("old", Files.readString(destination));
        }
        assertEquals("new metadata", Files.readString(destination));
    }

    public void testDirectorySyncFailureIsNotReportedAsSuccessfulPublication() throws Exception {
        final Path directory = createTempDir();
        assumeTrue("requires POSIX directory permissions", Files.getFileAttributeView(directory, PosixFileAttributeView.class) != null);
        final var original = Files.getPosixFilePermissions(directory);
        final Path destination = directory.resolve("raft-meta");
        try (var out = new AtomicFileOutputStream(legacyFile(destination))) {
            out.write("term-and-vote".getBytes(StandardCharsets.UTF_8));
            try {
                // Rename needs write+execute; opening the directory for synchronization also needs read.
                Files.setPosixFilePermissions(directory, Set.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
                assumeFalse("root or a privileged filesystem bypasses this permission fault", Files.isReadable(directory));
                expectThrows(IOException.class, out::close);
                assertTrue(out.isClosed());
            } finally {
                Files.setPosixFilePermissions(directory, original);
            }
        }
        // The failed outcome is ambiguous: rename happened, but directory durability was not established.
        assertEquals("term-and-vote", Files.readString(destination));
    }

    @SuppressForbidden(reason = "The upstream AtomicFileOutputStream constructor requires java.io.File")
    private static File legacyFile(Path path) {
        return path.toFile();
    }
}
