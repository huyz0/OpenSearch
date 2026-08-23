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
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy;
import org.opensearch.test.OpenSearchTestCase;

/**
 * An index is one kind or the other for its whole life, so the storage opt-in and the shard-recovery strategy
 * have to agree, and creation is the only moment they can be made to -- both settings are final afterwards.
 *
 * <p>The case that makes this validator necessary rather than merely defensive is
 * {@link #testRejectsAnExplicitLocalLuceneStrategyOnAServerlessIndex}: the setting provider injects the right
 * strategy, but {@code MetadataCreateIndexService} applies provider settings before the request's own, so an
 * explicit value in the create request wins. Without this check that request is accepted and the
 * contradiction surfaces later as a shard that will not open.
 */
public class ServerlessStorageRecoveryStrategyValidatorTests extends OpenSearchTestCase {

    private final ServerlessStorageRecoveryStrategyValidator validator = new ServerlessStorageRecoveryStrategyValidator();

    private IndexSettings newIndexSettings(boolean serverlessEnabled, String recoveryStrategy) {
        Settings.Builder indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (serverlessEnabled) {
            indexSettings.put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true);
        }
        if (recoveryStrategy != null) {
            indexSettings.put(IndexModule.INDEX_RECOVERY_STRATEGY_SETTING.getKey(), recoveryStrategy);
        }
        return new IndexSettings(IndexMetadata.builder("my-index").settings(indexSettings).build(), Settings.EMPTY);
    }

    public void testAcceptsAServerlessIndexCarryingTheObjectStoreStrategy() {
        validator.validate(null, newIndexSettings(true, ObjectStoreShardRecoveryStrategy.NAME));
    }

    /** The hole this exists to close: an explicit request setting overrides the one the provider injected. */
    public void testRejectsAnExplicitLocalLuceneStrategyOnAServerlessIndex() {
        IndexSettings indexSettings = newIndexSettings(true, "local-lucene");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> validator.validate(null, indexSettings));

        assertTrue(
            "the message must name both settings so an operator knows which to remove; got: " + e.getMessage(),
            e.getMessage().contains(IndexModule.INDEX_RECOVERY_STRATEGY_SETTING.getKey())
                && e.getMessage().contains(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey())
        );
    }

    /**
     * An index that says nothing reads the {@code local-lucene} default, which for a serverless index is the
     * same contradiction as stating it -- and is exactly the shape a pre-existing index has.
     */
    public void testRejectsAServerlessIndexThatStatesNoStrategyAtAll() {
        expectThrows(IllegalArgumentException.class, () -> validator.validate(null, newIndexSettings(true, null)));
    }

    /** An ordinary index is none of this validator's business, whatever strategy it names. */
    public void testIgnoresIndicesThatDidNotOptIntoServerlessStorage() {
        validator.validate(null, newIndexSettings(false, null));
        validator.validate(null, newIndexSettings(false, "local-lucene"));
    }

    /** Decided from settings alone, so core must not be made to build mappings on its account. */
    public void testDoesNotRequireMappings() {
        assertFalse(validator.requiresMappings());
    }
}
