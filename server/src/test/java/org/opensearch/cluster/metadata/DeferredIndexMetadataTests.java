/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.Diff;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * {@link Metadata} stores each index behind an {@link IndexMetadataHolder}, so an index can be present
 * -- routable, resolvable, countable -- without its settings, mappings and allocation ids being on the
 * heap. Nothing in core installs a deferred entry, so these tests install one directly and then check
 * the two things that make the split worth having:
 *
 * <ol>
 *   <li><b>It stays deferred.</b> Building metadata, rebuilding the indices lookup, resolving names,
 *       diffing and applying a diff must all leave an untouched index untouched. A loader that fires
 *       during any of those means the deferral bought nothing.</li>
 *   <li><b>It is indistinguishable.</b> Everything {@link Metadata} reports must be the same whether an
 *       index was added materialized or deferred.</li>
 * </ol>
 */
public class DeferredIndexMetadataTests extends OpenSearchTestCase {

    /** Counts loads so a test can assert none happened. */
    private static final class CountingLoader {
        private final IndexMetadata metadata;
        private final AtomicInteger loads = new AtomicInteger();

        CountingLoader(IndexMetadata metadata) {
            this.metadata = metadata;
        }

        LazyIndexMetadata stub() {
            return LazyIndexMetadata.of(metadata, () -> {
                loads.incrementAndGet();
                return metadata;
            });
        }

        int loads() {
            return loads.get();
        }
    }

    public void testBuildingMetadataDoesNotMaterializeADeferredIndex() {
        CountingLoader loader = new CountingLoader(index("cold", 3, 1, false, false));
        Metadata metadata = Metadata.builder().putStub(loader.stub()).build();

        assertEquals("building metadata must not load the index", 0, loader.loads());
        assertFalse("the entry must still be a stub", metadata.indexHolder("cold").isResolved());
        assertEquals(1, metadata.indexHolders().size());
        assertTrue(metadata.hasIndex("cold"));
        assertEquals(1, metadata.indices().size());
        assertArrayEquals(new String[] { "cold" }, metadata.getConcreteAllIndices());
        assertArrayEquals(new String[] { "cold" }, metadata.getConcreteAllOpenIndices());
        assertEquals("descriptor-only reads must still not have loaded it", 0, loader.loads());

        // The shard totals are computed at construction and must have come from the descriptor.
        assertEquals(3 * 2, metadata.getTotalNumberOfShards());
        assertEquals(0, loader.loads());

        // The lookup is built at the same time and holds one abstraction per index.
        assertTrue(metadata.getIndicesLookup().containsKey("cold"));
        IndexAbstraction abstraction = metadata.getIndicesLookup().get("cold");
        assertEquals(IndexAbstraction.Type.CONCRETE_INDEX, abstraction.getType());
        assertEquals("cold", abstraction.getName());
        assertFalse(abstraction.isHidden());
        assertFalse(abstraction.isSystem());
        assertEquals("the indices lookup must be answerable from the descriptor", 0, loader.loads());

        // ...and asking the abstraction for the metadata itself is what finally loads it.
        assertEquals(metadata.index("cold"), abstraction.getWriteIndex());
        assertEquals(1, loader.loads());
        assertTrue(metadata.indexHolder("cold").isResolved());
    }

    public void testHiddenAndClosedDeferredIndicesLandInTheRightArrays() {
        CountingLoader hiddenOpen = new CountingLoader(index("hidden-open", 1, 0, true, false));
        CountingLoader visibleClosed = new CountingLoader(closed(index("visible-closed", 1, 0, false, false)));
        CountingLoader hiddenClosed = new CountingLoader(closed(index("hidden-closed", 1, 0, true, false)));

        Metadata metadata = Metadata.builder()
            .putStub(hiddenOpen.stub())
            .putStub(visibleClosed.stub())
            .putStub(hiddenClosed.stub())
            .build();

        assertEquals(List.of("hidden-open"), sorted(metadata.getConcreteAllOpenIndices()));
        assertEquals(List.of(), sorted(metadata.getConcreteVisibleOpenIndices()));
        assertEquals(List.of("hidden-closed", "visible-closed"), sorted(metadata.getConcreteAllClosedIndices()));
        assertEquals(List.of("visible-closed"), sorted(metadata.getConcreteVisibleClosedIndices()));
        assertEquals(List.of("visible-closed"), sorted(metadata.getConcreteVisibleIndices()));

        assertEquals("the hidden and closed splits are descriptor-level facts", 0, hiddenOpen.loads());
        assertEquals(0, visibleClosed.loads());
        assertEquals(0, hiddenClosed.loads());
    }

    private static List<String> sorted(String[] names) {
        return Arrays.stream(names).sorted().collect(Collectors.toList());
    }

    /** Aliases are part of the descriptor, so alias resolution must not load anything either. */
    public void testAliasResolutionDoesNotMaterializeADeferredIndex() {
        IndexMetadata aliased = IndexMetadata.builder("aliased")
            .settings(settings())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("the-alias").build())
            .build();
        CountingLoader loader = new CountingLoader(aliased);

        Metadata metadata = Metadata.builder().putStub(loader.stub()).build();

        assertTrue(metadata.hasAlias("the-alias"));
        IndexAbstraction alias = metadata.getIndicesLookup().get("the-alias");
        assertEquals(IndexAbstraction.Type.ALIAS, alias.getType());
        assertFalse(alias.isHidden());
        assertFalse(alias.isSystem());
        assertEquals(List.of("aliased"), sorted(metadata.findAllAliases(new String[] { "aliased" }).keySet().toArray(new String[0])));
        assertEquals("alias resolution must be answerable from the descriptor", 0, loader.loads());

        // The alias's write index is the concrete index, and asking for it loads.
        assertEquals("aliased", alias.getWriteIndex().getIndex().getName());
        assertEquals(1, loader.loads());
    }

    /** A deferred index that this diff does not touch must survive the diff untouched. */
    public void testApplyingADiffDoesNotMaterializeUnchangedDeferredIndices() {
        CountingLoader untouched = new CountingLoader(index("untouched", 2, 1, false, false));
        Metadata before = Metadata.builder().putStub(untouched.stub()).put(index("changed", 1, 0, false, false), false).build();
        assertEquals(0, untouched.loads());

        Metadata after = Metadata.builder(before).put(IndexMetadata.builder(before.index("changed")).numberOfReplicas(2)).build();
        assertEquals("rebuilding metadata around a changed neighbour must not load it", 0, untouched.loads());

        Diff<Metadata> diff = after.diff(before);
        assertEquals("computing a diff must not load an index neither side changed", 0, untouched.loads());

        Metadata applied = diff.apply(before);
        assertEquals("applying a diff must not load an index the diff does not mention", 0, untouched.loads());

        assertTrue(applied.hasIndex("untouched"));
        assertEquals(2, applied.index("changed").getNumberOfReplicas());
        assertEquals("reading a neighbour still must not load it", 0, untouched.loads());

        assertEquals(1, applied.index("untouched").getNumberOfReplicas());
        assertEquals("reading the index itself is what loads it", 1, untouched.loads());
    }

    /** A diff that does change a deferred index must apply to it correctly, loading it once. */
    public void testApplyingADiffToADeferredIndexProducesTheRightResult() {
        CountingLoader loader = new CountingLoader(index("target", 2, 1, false, false));
        Metadata before = Metadata.builder().putStub(loader.stub()).build();

        // Reading it to build the changed version is the one and only load; everything after is
        // working with the already-materialized result.
        Metadata after = Metadata.builder(before).put(IndexMetadata.builder(before.index("target")).numberOfReplicas(3)).build();
        assertEquals(1, loader.loads());

        Metadata applied = after.diff(before).apply(before);
        assertEquals(3, applied.index("target").getNumberOfReplicas());
        assertEquals("target", applied.index("target").getIndex().getName());
        assertEquals("diffing and applying must not load it a second time", 1, loader.loads());
    }

    /** Concurrent readers must see one load and one instance. */
    public void testConcurrentReadersLoadOnce() throws Exception {
        CountingLoader loader = new CountingLoader(index("raced", 1, 0, false, false));
        LazyIndexMetadata stub = loader.stub();

        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        List<IndexMetadata> seen = Collections.synchronizedList(new ArrayList<>());
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                seen.add(stub.get());
            });
            t.start();
            workers.add(t);
        }
        start.countDown();
        for (Thread t : workers) {
            t.join();
        }

        assertEquals(threads, seen.size());
        assertEquals("the loader must run once however many threads race", 1, loader.loads());
        for (IndexMetadata metadata : seen) {
            assertSame("every reader must see the same instance", seen.get(0), metadata);
        }
    }

    /** Deleting a deferred index through a diff must remove it without ever loading it. */
    public void testDeletingADeferredIndexThroughADiffNeverLoadsIt() {
        CountingLoader doomed = new CountingLoader(index("doomed", 1, 0, false, false));
        Metadata before = Metadata.builder().putStub(doomed.stub()).put(index("kept", 1, 0, false, false), false).build();
        Metadata after = Metadata.builder(before).remove("doomed").build();

        Metadata applied = after.diff(before).apply(before);

        assertFalse(applied.hasIndex("doomed"));
        assertTrue(applied.hasIndex("kept"));
        assertEquals("a deleted index should never need loading", 0, doomed.loads());
    }

    /** Serializing writes the resolved metadata, so the wire form is unchanged by the deferral. */
    public void testSerializationRoundTripsThroughADeferredIndex() throws Exception {
        IndexMetadata original = IndexMetadata.builder("wire")
            .settings(settings())
            .numberOfShards(4)
            .numberOfReplicas(2)
            .putAlias(AliasMetadata.builder("wire-alias").build())
            .build();
        Metadata deferred = Metadata.builder().putStub(LazyIndexMetadata.of(original, () -> original)).build();
        Metadata materialized = Metadata.builder().put(original, false).build();

        NamedWriteableRegistry registry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        Metadata fromDeferred = copyWriteable(deferred, registry, Metadata::readFrom);
        Metadata fromMaterialized = copyWriteable(materialized, registry, Metadata::readFrom);

        assertEquals(fromMaterialized.index("wire"), fromDeferred.index("wire"));
        assertEquals(fromMaterialized.getTotalNumberOfShards(), fromDeferred.getTotalNumberOfShards());
        assertEquals(fromMaterialized.getIndicesLookup().keySet(), fromDeferred.getIndicesLookup().keySet());
    }

    /** The whole point of the split is that nothing downstream can tell the difference. */
    public void testDeferredAndMaterializedMetadataAreIndistinguishable() {
        IndexMetadata plain = index("plain", 3, 1, false, false);
        IndexMetadata hidden = index("hidden", 1, 0, true, false);
        IndexMetadata system = index("system", 2, 0, false, true);
        IndexMetadata aliased = IndexMetadata.builder("aliased")
            .settings(settings())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putAlias(AliasMetadata.builder("shared").build())
            .build();

        Metadata materialized = Metadata.builder().put(plain, false).put(hidden, false).put(system, false).put(aliased, false).build();
        Metadata deferred = Metadata.builder()
            .putStub(LazyIndexMetadata.of(plain, () -> plain))
            .putStub(LazyIndexMetadata.of(hidden, () -> hidden))
            .putStub(LazyIndexMetadata.of(system, () -> system))
            .putStub(LazyIndexMetadata.of(aliased, () -> aliased))
            .build();

        assertArrayEquals(materialized.getConcreteAllIndices(), deferred.getConcreteAllIndices());
        assertArrayEquals(materialized.getConcreteVisibleIndices(), deferred.getConcreteVisibleIndices());
        assertArrayEquals(materialized.getConcreteAllOpenIndices(), deferred.getConcreteAllOpenIndices());
        assertArrayEquals(materialized.getConcreteVisibleOpenIndices(), deferred.getConcreteVisibleOpenIndices());
        assertArrayEquals(materialized.getConcreteAllClosedIndices(), deferred.getConcreteAllClosedIndices());
        assertArrayEquals(materialized.getConcreteVisibleClosedIndices(), deferred.getConcreteVisibleClosedIndices());
        assertEquals(materialized.getTotalNumberOfShards(), deferred.getTotalNumberOfShards());
        assertEquals(materialized.getTotalOpenIndexShards(), deferred.getTotalOpenIndexShards());
        assertEquals(materialized.getIndicesLookup().keySet(), deferred.getIndicesLookup().keySet());
        assertEquals(materialized.indices().keySet(), deferred.indices().keySet());
        assertTrue(materialized.equalsAliases(deferred));
        assertTrue(deferred.equalsAliases(materialized));

        for (String name : materialized.indices().keySet()) {
            assertEquals(name, materialized.index(name), deferred.index(name));
            assertEquals(name, materialized.hasIndex(name), deferred.hasIndex(name));
            assertEquals(name, materialized.routingRequired(name), deferred.routingRequired(name));
            IndexAbstraction expected = materialized.getIndicesLookup().get(name);
            IndexAbstraction actual = deferred.getIndicesLookup().get(name);
            assertEquals(name, expected.getType(), actual.getType());
            assertEquals(name, expected.isHidden(), actual.isHidden());
            assertEquals(name, expected.isSystem(), actual.isSystem());
            assertEquals(name, expected.getIndices(), actual.getIndices());
            assertEquals(name, expected.getWriteIndex(), actual.getWriteIndex());
        }
    }

    /** A loader that returns a different index is a bug worth failing loudly on, not silently taking. */
    public void testLoaderReturningTheWrongIndexIsRejected() {
        IndexMetadata declared = index("declared", 1, 0, false, false);
        IndexMetadata wrong = index("wrong", 1, 0, false, false);
        LazyIndexMetadata stub = LazyIndexMetadata.of(declared, () -> wrong);

        IllegalStateException e = expectThrows(IllegalStateException.class, stub::get);
        assertTrue(e.getMessage(), e.getMessage().contains("returned metadata for"));
    }

    public void testLoaderRunsAtMostOnce() {
        CountingLoader loader = new CountingLoader(index("once", 1, 0, false, false));
        LazyIndexMetadata stub = loader.stub();

        assertFalse(stub.isResolved());
        IndexMetadata first = stub.get();
        assertTrue(stub.isResolved());
        assertSame(first, stub.get());
        assertSame(first, stub.get());
        assertEquals(1, loader.loads());
    }

    /** A concrete index must not be given an alias's name, deferred or not. */
    public void testDuplicateAliasAndIndexNameIsStillDetectedWithDeferredIndices() {
        IndexMetadata clashing = IndexMetadata.builder("clash")
            .settings(settings())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("other").build())
            .build();
        IndexMetadata other = index("other", 1, 0, false, false);

        Metadata.Builder builder = Metadata.builder()
            .putStub(LazyIndexMetadata.of(clashing, () -> clashing))
            .putStub(LazyIndexMetadata.of(other, () -> other));

        IllegalStateException e = expectThrows(IllegalStateException.class, builder::build);
        assertTrue(e.getMessage(), e.getMessage().contains("need to be unique"));
    }

    private static IndexMetadata index(String name, int shards, int replicas, boolean hidden, boolean system) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(settings()).put(IndexMetadata.SETTING_INDEX_HIDDEN, hidden))
            .numberOfShards(shards)
            .numberOfReplicas(replicas)
            .system(system)
            .build();
    }

    private static IndexMetadata closed(IndexMetadata open) {
        return IndexMetadata.builder(open).state(IndexMetadata.State.CLOSE).build();
    }

    private static Settings settings() {
        return Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
    }
}
