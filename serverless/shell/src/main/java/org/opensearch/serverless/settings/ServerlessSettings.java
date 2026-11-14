/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeSet;

/**
 * Every setting the shell defines, collected from the {@link SettingsProvider}s on the class path.
 */
public final class ServerlessSettings {

    /** The prefix of a node or cluster setting the shell owns. */
    public static final String NODE_PREFIX = "serverless.";
    /** The prefix of an index setting the shell owns. */
    public static final String INDEX_PREFIX = "index.serverless.";

    private static volatile List<Setting<?>> all;

    private ServerlessSettings() {}

    /**
     * Every setting every provider defines.
     *
     * @return the settings, in provider order
     * @throws IllegalStateException if two settings share a key
     */
    public static List<Setting<?>> all() {
        List<Setting<?>> found = all;
        if (found == null) {
            found = collect(ServiceLoader.load(SettingsProvider.class, SettingsProvider.class.getClassLoader()));
            all = found;
        }
        return found;
    }

    /**
     * Collects the settings of the given providers.
     *
     * @param providers the providers
     * @return their settings
     * @throws IllegalStateException if two settings share a key
     */
    static List<Setting<?>> collect(Iterable<SettingsProvider> providers) {
        final List<Setting<?>> found = new ArrayList<>();
        final Map<String, String> owners = new HashMap<>();
        for (SettingsProvider provider : providers) {
            for (Setting<?> setting : provider.settings()) {
                final String previous = owners.put(setting.getKey(), provider.getClass().getName());
                if (previous != null) {
                    throw new IllegalStateException(
                        "setting [" + setting.getKey() + "] is defined by both " + previous + " and " + provider.getClass().getName()
                    );
                }
                found.add(setting);
            }
        }
        return Collections.unmodifiableList(found);
    }

    /**
     * The index-scoped settings, which a shard's index settings are validated against.
     *
     * @return the settings with {@link Setting.Property#IndexScope}
     */
    public static List<Setting<?>> indexScoped() {
        final List<Setting<?>> found = new ArrayList<>();
        for (Setting<?> setting : all()) {
            if (setting.hasIndexScope()) {
                found.add(setting);
            }
        }
        return found;
    }

    /**
     * The keys under {@value #NODE_PREFIX} in a node's settings that no provider defines -- usually a typo, which
     * would otherwise be ignored without a word.
     *
     * @param nodeSettings the node's settings
     * @return the unknown keys, sorted
     */
    public static List<String> unknownNodeKeys(Settings nodeSettings) {
        final TreeSet<String> unknown = new TreeSet<>();
        for (String key : nodeSettings.keySet()) {
            if (key.startsWith(NODE_PREFIX) && defines(key) == false) {
                unknown.add(key);
            }
        }
        return new ArrayList<>(unknown);
    }

    private static boolean defines(String key) {
        for (Setting<?> setting : all()) {
            if (setting.match(key)) {
                return true;
            }
        }
        return false;
    }
}
