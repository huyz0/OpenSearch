/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of the background reconciler.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class ReconcileSettings implements SettingsProvider {

    /**
     * Whether a publish trusts a head it saw naming this node, of any age, while this node's own lease has held since,
     * rather than reading the head again once the sighting is a renewal old. True by default.
     */
    public static final Setting<Boolean> TRUST_OWN_HEAD = Setting.boolSetting(
        "serverless.publish.trust_own_head",
        true,
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public ReconcileSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(TRUST_OWN_HEAD);
    }
}
