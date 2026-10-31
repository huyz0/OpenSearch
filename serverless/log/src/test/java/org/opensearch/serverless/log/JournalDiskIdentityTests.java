/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

/** Real file locking and directory synchronization rather than a simulated filesystem. */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class JournalDiskIdentityTests extends OpenSearchTestCase {

    public void testIdentitySurvivesReopenAndExclusiveLeasePreventsConcurrentUse() throws Exception {
        final var directory = createTempDir();
        final var id = JournalDiskIdentity.discover(directory);
        try (var lease = JournalDiskIdentity.acquire(directory)) {
            assertEquals(id, lease.id);
            assertEquals(directory.toRealPath(), lease.directory);
            assertTrue(
                expectThrows(IOException.class, () -> JournalDiskIdentity.acquire(directory)).getMessage().contains("already in use")
            );
        }
        assertEquals(id, JournalDiskIdentity.discover(directory));
        final var reopened = JournalDiskIdentity.acquire(directory);
        reopened.close();
        reopened.close();
        assertEquals(id, JournalDiskIdentity.discover(directory));
    }

    public void testReplacementDirectoryGetsNewIdentityAndLostMarkerDoesNotResetExistingData() throws Exception {
        final var old = createTempDir();
        final var id = JournalDiskIdentity.discover(old);
        assertNotEquals(id, JournalDiskIdentity.discover(createTempDir()));
        Files.writeString(old.resolve("group-log"), "existing history");
        Files.delete(old.resolve("voter-identity"));
        assertTrue(expectThrows(IOException.class, () -> JournalDiskIdentity.discover(old)).getMessage().contains("nonempty"));
        assertFalse(Files.exists(old.resolve("voter-identity")));
        assertEquals("existing history", Files.readString(old.resolve("group-log")));
    }

    public void testCorruptTruncatedAndOversizedMarkersFailWithoutRetainingLock() throws Exception {
        final var directory = createTempDir();
        final var id = JournalDiskIdentity.discover(directory);
        final var marker = directory.resolve("voter-identity");
        final byte[] original = Files.readAllBytes(marker);
        for (int i = 0; i < original.length; i++) {
            final byte[] corrupt = original.clone();
            corrupt[i] ^= 1;
            Files.write(marker, corrupt);
            expectThrows(IOException.class, () -> JournalDiskIdentity.discover(directory));
            Files.write(marker, original);
            assertEquals(id, JournalDiskIdentity.discover(directory));
        }
        for (byte[] malformed : new byte[][] { new byte[0], new byte[original.length - 1], new byte[1024 * 1024] }) {
            Files.write(marker, malformed);
            expectThrows(IOException.class, () -> JournalDiskIdentity.discover(directory));
        }
        Files.write(marker, original);
        assertEquals(id, JournalDiskIdentity.discover(directory));
    }

    public void testUnrelatedNonemptyDirectoryIsNotModified() throws Exception {
        final var directory = createTempDir();
        Files.writeString(directory.resolve("user-data"), "untouched");
        expectThrows(IOException.class, () -> JournalDiskIdentity.discover(directory));
        try (var files = Files.list(directory)) {
            assertEquals(1L, files.count());
        }
        assertEquals("untouched", Files.readString(directory.resolve("user-data")));
    }

    public void testMissingDirectoryIsNotRecreatedOnAnotherFilesystem() throws Exception {
        final var directory = createTempDir().resolve("unmounted-or-misspelled");
        assertTrue(expectThrows(IOException.class, () -> JournalDiskIdentity.discover(directory)).getMessage().contains("provisioned"));
        assertFalse(Files.exists(directory));
    }

    public void testExistingIdentityStillRequiresSuccessfulDirectorySynchronization() throws Exception {
        final var directory = createTempDir();
        final var id = JournalDiskIdentity.discover(directory);
        assumeTrue("requires POSIX permissions", Files.getFileAttributeView(directory, PosixFileAttributeView.class) != null);
        final var permissions = Files.getPosixFilePermissions(directory);
        try {
            Files.setPosixFilePermissions(directory, Set.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
            assumeFalse("requires an effective directory read fault", Files.isReadable(directory));
            expectThrows(IOException.class, () -> JournalDiskIdentity.discover(directory));
        } finally {
            Files.setPosixFilePermissions(directory, permissions);
        }
        assertEquals(id, JournalDiskIdentity.discover(directory));
    }
}
