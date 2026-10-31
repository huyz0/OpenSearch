/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Modifications Copyright OpenSearch Contributors: persist the parent directory after publication.
package org.apache.ratis.util;

import org.opensearch.common.SuppressForbidden;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * A {@link FilterOutputStream} that writes to a file atomically.
 * The output file will not show up until it has been entirely written and sync'ed to disk.
 * It uses a temporary file when it is being written.
 * The default temporary file has a .tmp suffix.
 * <p>
 * When the output stream is closed, it is
 * (1) flushed
 * (2) sync'ed, and
 * (3) renamed/moved from the temporary file to the output file.
 * If the output file already exists, it will be overwritten.
 * <p>
 * NOTE that on Windows platforms, the output file, if it exists, is deleted
 * before the temporary file is moved.
 */
@SuppressForbidden(reason = "Maintained upstream Ratis API uses java.io.File; the durability fix uses NIO directory synchronization")
public final class AtomicFileOutputStream extends FilterOutputStream {
    static final Logger LOG = LoggerFactory.getLogger(AtomicFileOutputStream.class);

    /** Suffix of the unpublished temporary file. */
    public static final String TMP_EXTENSION = ".tmp";

    /**
     * Returns the unpublished temporary file name.
     * @param outFile destination file
     * @return temporary file beside the destination
     */
    public static File getTemporaryFile(File outFile) {
        return new File(outFile.getParentFile(), outFile.getName() + TMP_EXTENSION);
    }

    private final File outFile;
    private final File tmpFile;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    /**
     * Creates a stream publishing to the supplied destination.
     * @param outFile destination file
     * @throws IOException on failure to open its temporary file
     */
    public AtomicFileOutputStream(File outFile) throws IOException {
        this(outFile, getTemporaryFile(outFile));
    }

    /**
     * Creates a stream with an explicitly chosen temporary file.
     * @param outFile destination file
     * @param tmpFile temporary file, on the same filesystem
     * @throws IOException on failure to open the temporary file
     */
    public AtomicFileOutputStream(File outFile, File tmpFile) throws IOException {
        super(FileUtils.newOutputStreamForceAtClose(tmpFile, CREATE, TRUNCATE_EXISTING, WRITE));
        this.outFile = outFile.getAbsoluteFile();
        this.tmpFile = tmpFile.getAbsoluteFile();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    /**
     * Returns whether close has been attempted.
     * @return whether the stream was closed
     */
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void close() throws IOException {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        try {
            super.close();
            FileUtils.move(tmpFile, outFile, StandardCopyOption.REPLACE_EXISTING);
            // The file sync precedes rename. Persist that rename before a vote/term update can succeed.
            // Unsupported directory synchronization fails closed rather than weakening durability.
            try (FileChannel parent = FileChannel.open(outFile.getParentFile().toPath(), StandardOpenOption.READ)) {
                parent.force(true);
            }
        } catch (Exception e) {
            try {
                FileUtils.deleteIfExists(tmpFile);
            } catch (IOException ioe) {
                e.addSuppressed(ioe);
            }
            throw e;
        }
    }

    /**
     * Close the atomic file, but do not "commit" the temporary file
     * on top of the destination. This should be used if there is a failure
     * in writing.
     */
    public void abort() {
        if (isClosed.get()) {
            return;
        }
        try {
            super.close();
        } catch (IOException ioe) {
            LOG.warn("Unable to abort file " + tmpFile, ioe);
        } finally {
            try {
                FileUtils.deleteIfExists(tmpFile);
            } catch (IOException ioe) {
                LOG.warn("Unable to delete tmp file during abort " + tmpFile, ioe);
            }
        }
    }
}
