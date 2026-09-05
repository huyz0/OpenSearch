/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Reserving the {@code serverless_} name prefix, or rather: refusing the one thing the prefix can do
 * on a cluster that configured nothing.
 *
 * <p>{@code ServerlessStorageIndexSettingProvider} opts an index into object-store storage purely on
 * its name, gated by nothing at all. On a cluster that installed this plugin and configured neither
 * {@code serverless_storage.base_path} nor {@code serverless_storage.repository}, {@code PUT
 * /serverless_foo} was therefore accepted, and its shards then failed to open because there was no
 * container to resolve. That is a classic cluster broken by a name the operator was free to choose,
 * and the error arrived on a shard rather than on the request.
 *
 * <p>These tests pin the narrow rule that closes it: an index asking for object-store storage -- by
 * name or by setting, it does not matter which -- on a node that has no object store is refused at
 * creation.
 */
public class UnbackedServerlessIndexValidatorTests extends OpenSearchTestCase {

    private static IndexSettings indexNamed(String name, Boolean explicitOptIn) {
        Settings.Builder indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (explicitOptIn != null) {
            indexSettings.put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), explicitOptIn);
        }
        return new IndexSettings(IndexMetadata.builder(name).settings(indexSettings).build(), Settings.EMPTY);
    }

    private static ServerlessStoragePlugin.UnbackedServerlessIndexValidator validatorWith(Settings nodeSettings) {
        return new ServerlessStoragePlugin.UnbackedServerlessIndexValidator(nodeSettings);
    }

    /**
     * The exact scenario from the report: the plugin is installed, nothing is configured, and
     * {@code PUT /serverless_foo} was accepted, producing an index whose shards then failed to open
     * with an {@code IllegalStateException} nobody had asked for.
     */
    public void testRefusesAPrefixedIndexWhenNoObjectStoreIsConfigured() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validatorWith(Settings.EMPTY).validate(null, indexNamed("serverless_foo", null))
        );
        assertTrue("must name the base_path setting: " + e.getMessage(), e.getMessage().contains("serverless_storage.base_path"));
        assertTrue("must name the repository setting: " + e.getMessage(), e.getMessage().contains("serverless_storage.repository"));
    }

    /**
     * Nothing is refused once a store exists, which is the whole population of clusters that
     * deliberately use this plugin.
     */
    public void testAcceptsAPrefixedIndexWhenABasePathIsConfigured() {
        Settings nodeSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), "/var/lib/serverless")
            .build();
        validatorWith(nodeSettings).validate(null, indexNamed("serverless_foo", null));
    }

    /** A repository is the other way to configure one, and must count equally. */
    public void testAcceptsAPrefixedIndexWhenARepositoryIsConfigured() {
        Settings nodeSettings = Settings.builder()
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey(), "my-s3-repo")
            .build();
        validatorWith(nodeSettings).validate(null, indexNamed("serverless_foo", null));
    }

    /**
     * The explicit opt-in fails in exactly the same way as the name-derived one and had exactly the
     * same excuse, so it is caught by the same rule rather than by a second one keyed on the prefix.
     */
    public void testRefusesAnExplicitlyOptedInIndexWhenNoObjectStoreIsConfigured() {
        expectThrows(IllegalArgumentException.class, () -> validatorWith(Settings.EMPTY).validate(null, indexNamed("ordinary-name", true)));
    }

    /**
     * The property Goal 6 rests on: a cluster that installed the plugin and opted no index in is
     * untouched. An ordinary index on an unconfigured cluster must still be creatable -- this
     * validator has to be invisible to it.
     */
    public void testLeavesOrdinaryIndicesAloneOnAnUnconfiguredCluster() {
        validatorWith(Settings.EMPTY).validate(null, indexNamed("ordinary-name", null));
        validatorWith(Settings.EMPTY).validate(null, indexNamed("ordinary-name", false));
    }

    /**
     * The prefix is checked directly, not only through the setting the provider derives from it.
     * In the normal creation flow the two always agree, but the ordering that makes them agree is
     * core's, not this plugin's, and a check that silently depends on somebody else's ordering is
     * a check that stops working without anyone noticing.
     */
    public void testTheNameAloneIsEnoughEvenWithoutTheDerivedSetting() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validatorWith(Settings.EMPTY).validate(null, indexNamed("serverless_tenant-4711", null))
        );
        assertTrue(
            "the message must say the name is what selected it: " + e.getMessage(),
            e.getMessage().contains("selected by its name alone")
        );
    }

    /**
     * The validator reads only settings, so it must not force core to build a throwaway
     * {@code IndexService} to produce a {@code MapperService} it never looks at -- that build happens
     * under a lock on {@code IndicesService} and dominated a creation-heavy profile.
     */
    public void testDoesNotRequireMappings() {
        assertFalse(validatorWith(Settings.EMPTY).requiresMappings());
    }
}
