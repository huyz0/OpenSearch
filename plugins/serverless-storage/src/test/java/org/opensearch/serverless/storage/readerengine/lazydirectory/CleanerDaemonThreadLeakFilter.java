/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.lazydirectory;

import com.carrotsearch.randomizedtesting.ThreadFilter;

import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;

/**
 * Local copy of core's own {@code org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter}
 * (a server test-only class this plugin's test source set cannot depend on): the {@link
 * java.lang.ref.Cleaner} instance {@link AbstractBlockIndexInput} uses (via {@link
 * LazyBundleIndexInput}) creates a daemon thread that is never stopped and for which core hands
 * out no handle to stop it -- excluded from thread-leak detection for the same reason core's own
 * tests exclude it.
 */
public final class CleanerDaemonThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith(AbstractBlockIndexInput.CLEANER_THREAD_NAME_PREFIX);
    }
}
