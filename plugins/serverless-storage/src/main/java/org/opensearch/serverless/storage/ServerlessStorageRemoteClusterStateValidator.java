/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.index.IndexCreationValidator;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;

/**
 * Enforces rfc-serverless-opensearch.md &sect;10's "cluster state: remote cluster state ...
 * becomes mandatory in serverless mode": rejects {@code index.serverless_storage.enabled=true} at
 * creation time unless the node has {@link RemoteClusterStateService#REMOTE_CLUSTER_STATE_ENABLED_SETTING}.
 *
 * <p>A real, node-level prerequisite check like this belongs on {@link IndexCreationValidator},
 * not {@link org.opensearch.index.shard.IndexSettingProvider} (which is what this check used to be
 * built on, in {@code ServerlessStorageIndexSettingProvider}): that interface's contract is a pure
 * function of per-request/per-template index settings, with no access to real node settings at
 * all, which forced a mutable-field, late-setter workaround (constructed before {@code
 * ServerlessStoragePlugin#createComponents} runs, wired afterward) purely to smuggle the node's
 * resolved remote-cluster-state flag in. {@link IndexCreationValidator#validate} instead receives
 * a real {@link IndexSettings} per call, and {@link IndexSettings#getNodeSettings()} already
 * exposes real node settings with no injection, no ordering hazard, and no mutable state needed at
 * all -- this class is a plain, stateless function of its arguments, exactly the shape {@code
 * ServerlessStorageIndexSettingProvider} always claimed for itself but, for this one check,
 * couldn't actually deliver.
 */
public final class ServerlessStorageRemoteClusterStateValidator implements IndexCreationValidator {

    /** Creates a validator with no configuration state; every decision is derived from the {@link IndexSettings} passed to it. */
    public ServerlessStorageRemoteClusterStateValidator() {}

    /**
     * False, and the {@link #validate} body below is the whole argument: it reads two settings and never
     * names {@code mapperService}.
     *
     * <p>This is what lets a gated creation skip building a throwaway {@link org.opensearch.index.IndexService}
     * purely to hand one over. Being the only validator this plugin registers, saying so here is what makes
     * that bypass reachable at all -- {@code MetadataCreateIndexService} checks every registered validator,
     * so one answering true would keep every gated creation on the slow path.
     */
    @Override
    public boolean requiresMappings() {
        return false;
    }

    @Override
    public void validate(MapperService mapperService, IndexSettings indexSettings) {
        if (ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(indexSettings.getSettings()) == false) {
            return;
        }
        if (RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(indexSettings.getNodeSettings()) == false) {
            throw new IllegalArgumentException(
                "index ["
                    + indexSettings.getIndex().getName()
                    + "] cannot set "
                    + ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey()
                    + "=true because this node does not have "
                    + RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey()
                    + "=true: serverless storage requires remote cluster state to be enabled "
                    + "cluster-wide (rfc-serverless-opensearch.md section 10)"
            );
        }
    }
}
