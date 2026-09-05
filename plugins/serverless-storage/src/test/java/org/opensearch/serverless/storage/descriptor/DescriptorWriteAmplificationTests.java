/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.mapper.UnknownFieldRefresh;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;

/**
 * How many nodes write a descriptor when one index's metadata changes, and for which indices.
 *
 * <h2>The arithmetic this pins</h2>
 *
 * {@code Metadata.Builder.put} calls {@code ClaimedIndexLifecycleRegistry.recordChange} on the cluster
 * manager's state update thread <em>and</em> on every follower's applier thread as that state's diff is
 * applied -- core's own javadoc there says so explicitly, and calls the diff case "the case this must not
 * skip". {@link DescriptorBackedIndexLifecycle#recordChange} then issued a store write with no filter of
 * any kind. On a 200-node cluster, adding an alias to one index was 200 read-plus-compare-and-swap pairs
 * against a single object-store key: one wins, 199 conflict, most exhaust {@link BlobDescriptorBackend}'s
 * three attempts and report "sustained contention" for contention the cluster inflicted on itself -- up to
 * 1,200 operations on one key, every one of them writing identical bytes derived from the state all 200
 * nodes had just received. At the ten thousand nodes this design targets it is up to 60,000.
 *
 * <p>The sibling comment in that method diagnoses exactly this for the change-log append beside it --
 * "Appending here was therefore N nodes each writing the same entry" -- and removes the append. The
 * descriptor write next to it was left alone. These tests are what stops it being left alone again.
 */
public class DescriptorWriteAmplificationTests extends OpenSearchTestCase {

    @After
    public void alwaysUninstall() {
        DescriptorGate.uninstall();
    }

    /** A backend that records every write it is asked for and performs none. */
    private static final class RecordingBackend implements DescriptorBackend, DescriptorPrefixBackend {

        private final List<String> written = new ArrayList<>();

        @Override
        public void put(IndexDescriptor descriptor) {
            written.add(descriptor.name());
        }

        @Override
        public void putAsync(IndexDescriptor descriptor) {
            written.add(descriptor.name());
        }

        @Override
        public void putTombstoneAsync(IndexDescriptor tombstone) {
            written.add("tombstone:" + tombstone.name());
        }

        @Override
        public void putTombstoneAsync(IndexDescriptor tombstone, ActionListener<Void> whenDurable) {
            written.add("tombstone:" + tombstone.name());
            whenDurable.onResponse(null);
        }

        @Override
        public IndexDescriptor get(String name) {
            return null;
        }

        @Override
        public boolean create(IndexDescriptor descriptor) {
            written.add("create:" + descriptor.name());
            return true;
        }

        @Override
        public CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor) {
            written.add("create:" + descriptor.name());
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void invalidate(String name) {}

        @Override
        public boolean available() {
            return true;
        }

        @Override
        public void warmAsync(Collection<String> names, ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        public AbsentIndexDescriptorSuppliers.PrefixExpansion expandPrefix(String prefix, int limit) {
            return AbsentIndexDescriptorSuppliers.PrefixExpansion.of(List.of());
        }
    }

    private RecordingBackend install() {
        RecordingBackend backend = new RecordingBackend();
        DescriptorGate.install(
            backend,
            backend,
            mock(MappingGenerationStore.Store.class),
            mock(org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator.Aggregator.class),
            mock(UnknownFieldRefresh.Refresher.class),
            true
        );
        return backend;
    }

    private static IndexMetadata index(String name, boolean serverless) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid");
        if (serverless) {
            settings.put("index.serverless_storage.enabled", true);
        }
        return IndexMetadata.builder(name).settings(settings.build()).numberOfShards(1).numberOfReplicas(0).build();
    }

    /**
     * A node that is not the elected cluster manager writes nothing when a diff is applied to it.
     *
     * <p>This is the whole of the amplification fix. The follower is not being denied anything: the
     * descriptor is a projection of a cluster state it has just received in full, so it already holds
     * everything the write would have recorded.
     */
    public void testAFollowerApplyingADiffIssuesNoDescriptorWrite() {
        RecordingBackend backend = install();
        DescriptorGate.observeElectedClusterManager(false);

        new DescriptorBackedIndexLifecycle().recordChange(index("serverless_orders", true));

        assertEquals(
            "a follower must not write the descriptor the manager is already writing: " + backend.written,
            0,
            backend.written.size()
        );
    }

    /** The manager does write it, or nothing would. */
    public void testTheElectedManagerStillWritesIt() {
        RecordingBackend backend = install();
        DescriptorGate.observeElectedClusterManager(true);

        new DescriptorBackedIndexLifecycle().recordChange(index("serverless_orders", true));

        assertEquals(List.of("serverless_orders"), backend.written);
    }

    /**
     * A node that has not yet observed any cluster state writes, rather than staying silent.
     *
     * <p>The flag defaults to "nobody has said yet", and that has to mean "go ahead": a node that has not
     * applied a state yet includes the only node of a single-node cluster during bootstrap, and silencing
     * the descriptor write there loses a record that has nowhere else to live. The failure directions are
     * not symmetric -- an extra identical write costs operations, a missing one costs the index.
     */
    public void testANodeThatHasNotSeenAClusterStateYetStillWrites() {
        RecordingBackend backend = install();

        new DescriptorBackedIndexLifecycle().recordChange(index("serverless_orders", true));

        assertEquals(List.of("serverless_orders"), backend.written);
    }

    /**
     * An index this plugin does not own is not recorded at all, even on the manager.
     *
     * <p>Nothing ever read those descriptors: resolution through {@code AbsentIndexDescriptorSuppliers} is
     * consulted only for names absent from cluster state, and an index reaching {@code recordChange} is by
     * definition in it. What writing them cost was a descriptor blob per ordinary index, rewritten on every
     * settings, alias or mapping change, on a cluster that may have no serverless index at all.
     */
    public void testAnIndexThisPluginDoesNotOwnIsNotRecorded() {
        RecordingBackend backend = install();
        DescriptorGate.observeElectedClusterManager(true);

        new DescriptorBackedIndexLifecycle().recordChange(index("ordinary-index", false));

        assertEquals("an ordinary index must cost zero descriptor I/O: " + backend.written, 0, backend.written.size());
    }

    /** And the two guards compose the way the arithmetic assumes: one write per change, cluster-wide. */
    public void testOneChangeCostsOneWriteAcrossAWholeCluster() {
        RecordingBackend backend = install();
        IndexMetadata changed = index("serverless_orders", true);

        // The manager, then 199 followers applying the same diff. Modelled by flipping the flag, because
        // the flag is exactly what distinguishes them.
        DescriptorGate.observeElectedClusterManager(true);
        new DescriptorBackedIndexLifecycle().recordChange(changed);
        DescriptorGate.observeElectedClusterManager(false);
        for (int follower = 0; follower < 199; follower++) {
            new DescriptorBackedIndexLifecycle().recordChange(changed);
        }

        assertEquals("200 nodes, one metadata change, one descriptor write: " + backend.written, 1, backend.written.size());
    }
}
