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
 * Asking a gated index for its settings must return its settings.
 *
 * <h2>How this was found, and why it took so long</h2>
 *
 * {@code GatedPopulationSoakIT} resolves a slice of the population it builds, as a check that a name written
 * at scale can still be read back. It reported <b>0 resolved out of 50,000 attempts, with 0 failures</b>, and
 * that line sat in the output of run after run without anyone reading it as a defect, because a resolve phase
 * that reports no errors reads like a resolve phase that worked.
 *
 * <p>{@code TransportGetSettingsAction} took its metadata straight from the cluster state map rather than
 * through the descriptor seam. A gated index has no entry there -- that is the entire point of gating -- so
 * every lookup answered null and fell through a {@code continue}, and the response came back with no entry
 * for that index and no error attached to it.
 *
 * <p>That is the failure shape this area produces over and over: not a crash, but an empty answer that is
 * shaped exactly like a correct one. A caller cannot tell "this index has no settings" from "I dropped your
 * index on the floor", and neither could the soak.
 *
 * <h2>Why the assertion is on a setting's value</h2>
 *
 * Asserting the response is non-empty would pass against a version that returned the index with no settings
 * in it, which is the same silent-empty failure one level down. So the test names a setting it put there
 * and requires that exact value back.
 */
public class GatedSettingsLookupIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

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

    public void testAGatedIndexReportsItsSettings() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_gated-settings").settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.serverless_storage.enabled", true)
                        .build()
                )
            )
            .actionGet();

        var response = client().admin().indices().prepareGetSettings("serverless_gated-settings").get();

        assertFalse(
            "a gated index asked for its settings by name must appear in the response; an empty map here is "
                + "the silent drop that made the population soak report 0 of 50,000 resolved with 0 failures",
            response.getIndexToSettings().isEmpty()
        );
        Settings settings = response.getIndexToSettings().get("serverless_gated-settings");
        assertNotNull("the index asked for must be the one that came back", settings);
        assertEquals(
            "and its settings must be its own, not an empty placeholder that merely proves the name existed",
            "3",
            settings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)
        );
    }

    /**
     * The same index through {@code GET /index}, which failed differently and worse.
     *
     * <p>{@code TransportGetIndexAction} read {@code state.metadata().index(name).getSettings()} with no
     * null check at all, so a gated index did not come back empty -- it threw a {@link NullPointerException}
     * on a name the resolver had just accepted as valid. Same root cause as the settings action, opposite
     * symptom, which is why an audit was worth more than fixing the one site the soak happened to exercise.
     *
     * <p>Mappings and aliases are deliberately not asserted here. Those features are answered from
     * {@code Metadata.findMappings} and {@code findAllAliases}, which walk the cluster state map, and a
     * descriptor carries neither: mappings live in {@code MappingGenerationStore} and
     * {@code IndexDescriptor#toIndexMetadata} does not put them back. Asking for them returns empty, and
     * that is a real limitation rather than a fixed one.
     */
    public void testAGatedIndexCanBeFetchedByGetIndex() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("serverless_gated-getindex").settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.serverless_storage.enabled", true)
                        .build()
                )
            )
            .actionGet();

        var response = client().admin().indices().prepareGetIndex().setIndices("serverless_gated-getindex").get();

        Settings settings = response.getSettings().get("serverless_gated-getindex");
        assertNotNull("GET /index on a gated index must return its settings rather than throwing", settings);
        assertEquals("2", settings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS));
    }

    /**
     * An ordinary index keeps answering exactly as it did, which is the R1 half.
     *
     * <p>The seam falls back to the metadata map when nothing is registered, so this should be untouched --
     * but "should be" is what a control is for. A change that made gated indices resolvable by breaking
     * ordinary ones would be a worse trade than the bug.
     */
    public void testAnOrdinaryIndexStillReportsItsSettings() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("ordinary-settings").settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
            )
            .actionGet();

        Settings settings = client().admin()
            .indices()
            .prepareGetSettings("ordinary-settings")
            .get()
            .getIndexToSettings()
            .get("ordinary-settings");

        assertNotNull("an ordinary index must still resolve through the metadata map", settings);
        assertEquals("2", settings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS));
    }
}
