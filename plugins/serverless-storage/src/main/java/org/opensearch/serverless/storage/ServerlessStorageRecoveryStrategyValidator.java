/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.index.IndexCreationValidator;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy;

/**
 * Refuses to create a serverless index whose shard-recovery strategy is not this plugin's.
 *
 * <p>An index is one kind or the other for its whole life. A serverless index's durable copy lives in the
 * object store and is addressed by its own manifest, so core's {@code local-lucene} strategy cannot recover
 * it -- there is nothing authoritative on local disk to recover from. The two settings therefore have to
 * agree, and the only moment they can be made to agree is creation, because both are final afterwards.
 *
 * <p><b>Why this check is needed even though the setting is injected.</b>
 * {@code ServerlessStorageIndexSettingProvider} puts the strategy in for every serverless index, but
 * {@code MetadataCreateIndexService} applies provider settings <em>before</em> the create request's own
 * settings, so an explicit value in the request wins. Without this, {@code PUT /idx} carrying both
 * {@code index.serverless_storage.enabled: true} and {@code index.recovery.strategy: local-lucene} is
 * accepted, and the contradiction surfaces much later as a shard that will not open.
 *
 * <p>Failing here makes the create request itself carry the error, which is the only place an operator can
 * still act on it: after creation both settings are final, so there is no repair short of deleting the
 * index.
 */
public final class ServerlessStorageRecoveryStrategyValidator implements IndexCreationValidator {

    /** Creates a validator with no configuration state; every decision comes from the {@link IndexSettings} passed in. */
    public ServerlessStorageRecoveryStrategyValidator() {}

    /** Decided entirely from settings, so the mappings this would otherwise force core to build are not needed. */
    @Override
    public boolean requiresMappings() {
        return false;
    }

    @Override
    public void validate(MapperService mapperService, IndexSettings indexSettings) {
        if (ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexSettings.getSettings()) == false) {
            return;
        }
        String strategy = IndexModule.INDEX_RECOVERY_STRATEGY_SETTING.get(indexSettings.getSettings());
        if (ObjectStoreShardRecoveryStrategy.NAME.equals(strategy) == false) {
            throw new IllegalArgumentException(
                "index ["
                    + indexSettings.getIndex().getName()
                    + "] sets ["
                    + ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey()
                    + "] but its ["
                    + IndexModule.INDEX_RECOVERY_STRATEGY_SETTING.getKey()
                    + "] is ["
                    + strategy
                    + "]; a serverless index's durable copy lives in the object store and can only be recovered by ["
                    + ObjectStoreShardRecoveryStrategy.NAME
                    + "]. Both settings are final once the index exists, so remove the explicit ["
                    + IndexModule.INDEX_RECOVERY_STRATEGY_SETTING.getKey()
                    + "] from this request and let it be set for you."
            );
        }
    }
}
