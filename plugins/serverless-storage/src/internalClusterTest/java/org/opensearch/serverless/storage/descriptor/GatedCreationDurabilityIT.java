/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;

/**
 * T17. Whether an acknowledged gated creation means the index exists.
 *
 * <p>{@code MetadataCreateIndexService.clusterStateCreateIndex} says of the gated branch: "The descriptor
 * write is the creation, and it is the thing that must succeed", and "Failure semantics invert from H2b's
 * here. During dual write a lost descriptor cost a comparison; now it costs the index."
 *
 * <p>It then calls {@code IndexDescriptorPublisher.publish}, which returns whether a publisher was
 * <em>invoked</em>, not whether the write <em>landed</em>. The publisher registered by {@code DescriptorGate}
 * routes a live descriptor to {@code BlobDescriptorBackend.putAsync}, which submits and returns, logging
 * failures at warn.
 *
 * <p>So on the reading of the code, an acknowledged creation means the write was submitted. This test exists
 * because that reading has been wrong repeatedly in this area: the temporary {@code IndexService} looked
 * like the cost of creation and was not, and the cluster state queue looked like its ceiling and was not.
 * The question is settled by making the descriptor write fail and seeing what the client is told.
 *
 * <p>The failure is arranged by making the descriptor container refuse writes, which is a real condition
 * rather than an injected fault: an object store that is unreachable, throttling or returning errors is what
 * a node sees often enough to design for. It used to be arranged by closing the descriptor system index,
 * which is where the wording of the assertions below comes from; that index was removed on 2026-08-05 and a
 * bucket cannot be closed, so the injection moved to the container.
 */
public class GatedCreationDurabilityIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    private FailableDescriptorContainer container;

    private BlobDescriptorBackend install() throws Exception {
        container = newFailableDescriptorContainer();
        return installOverFailableContainer(container).points();
    }

    /** The control: with a healthy descriptor store, an acknowledged creation is a real index. */
    public void testAnAcknowledgedCreationNormallyExists() throws Exception {
        BlobDescriptorBackend store = install();

        assertTrue(client().admin().indices().create(gated("serverless_healthy-idx")).actionGet().isAcknowledged());

        assertBusy(
            () -> assertNotNull("an acknowledged gated creation must be resolvable", readWhenAvailable(store, "serverless_healthy-idx"))
        );
    }

    /**
     * The question. With the container refusing writes, the descriptor write cannot land. Does the client
     * still get an acknowledgement for an index that does not exist anywhere?
     *
     * <p><b>It does. This test fails, and it is committed failing on purpose.</b> Measured:
     * {@code acknowledged=true} with the store unwritable, and the descriptor absent afterwards. So
     * a gated creation reports success for an index that exists in no cluster state entry and no
     * descriptor, which is the one outcome the design says must not happen.
     *
     * <p><b>Why it is not fixed here.</b> W4 established the constraint that produced this: the publish
     * hook runs on the cluster state thread while a state is being built, and a blocking descriptor write
     * there deadlocked the node rather than failing. {@code putAsync} was the answer to that deadlock, and
     * it silently traded the durability the gated branch depends on.
     *
     * <p>Fixing it properly means moving the descriptor write off the cluster state task and onto the
     * request path, which is what H3 describes in the first place: {@code create()} with
     * {@code op_type=create} is the uniqueness gate and the creation, so it belongs where it can block and
     * report failure. That is a change to how index creation is sequenced, not a patch, and it needs its
     * own design rather than being appended to a performance pass.
     *
     * <p>Marked {@code AwaitsFix} rather than deleted or weakened, so the defect stays reproducible and CI
     * stays honest about it. Weakening it to assert the broken behaviour would have made this area's
     * signature failure into a specification.
     */
    public void testAcknowledgementWhenTheDescriptorWriteCannotLand() throws Exception {
        BlobDescriptorBackend store = install();

        // One creation that must succeed first, so the container is failing rather than merely untouched
        // when the creation under test runs.
        assertTrue(client().admin().indices().create(gated("serverless_seed-idx")).actionGet().isAcknowledged());
        assertBusy(() -> assertNotNull(readWhenAvailable(store, "serverless_seed-idx")));
        container.failing = true;

        boolean acknowledged;
        try {
            acknowledged = client().admin().indices().create(gated("serverless_doomed-idx")).actionGet().isAcknowledged();
        } catch (Exception e) {
            // The safe outcome: the client is told the creation failed.
            logger.info("creation failed rather than acknowledging, which is the safe answer", e);
            return;
        }

        logger.warn(
            "T17: gated creation of [doomed-idx] returned acknowledged={} while the descriptor store was "
                + "closed, so the descriptor write could not have landed",
            acknowledged
        );

        // Reopen so the store can be asked what actually exists.
        container.failing = false;
        store.invalidate("serverless_doomed-idx");

        assertFalse(
            "an acknowledged creation whose descriptor never landed is an index the client believes exists "
                + "and which exists nowhere. The descriptor write is the creation for a gated index, so "
                + "acknowledging before it lands acknowledges something that did not happen",
            acknowledged && store.get("serverless_doomed-idx") == null
        );
    }

    /**
     * Reads inside a polling loop, treating "cannot tell yet" as "not yet". T12 made the store distinguish
     * an absent descriptor from an unreadable one, and a store that is briefly unreachable
     * is genuinely unreadable, so a loop waiting for a write to land should keep waiting.
     */
    private static org.opensearch.cluster.metadata.IndexDescriptor readWhenAvailable(BlobDescriptorBackend store, String name)
        throws Exception {
        try {
            return store.get(name);
        } catch (org.opensearch.cluster.metadata.DescriptorUnavailableException e) {
            throw new AssertionError("descriptor not readable yet for [" + name + "]", e);
        }
    }

    private static CreateIndexRequest gated(String name) throws Exception {
        return new CreateIndexRequest(name).settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.serverless_storage.enabled", true)
                .build()
        );
    }
}
