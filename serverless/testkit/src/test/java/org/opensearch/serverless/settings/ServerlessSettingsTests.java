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
import org.opensearch.test.OpenSearchTestCase;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;

public class ServerlessSettingsTests extends OpenSearchTestCase {

    public void testEveryListedProviderLoadsAndContributes() throws Exception {
        final Set<String> listed = new TreeSet<>();
        try (
            InputStream in = SettingsProvider.class.getClassLoader()
                .getResourceAsStream("META-INF/services/" + SettingsProvider.class.getName())
        ) {
            assertNotNull("the services file is on the class path", in);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                reader.lines().map(String::trim).filter(line -> line.isEmpty() == false).forEach(listed::add);
            }
        }
        final Set<String> loaded = new TreeSet<>();
        for (SettingsProvider provider : ServiceLoader.load(SettingsProvider.class, SettingsProvider.class.getClassLoader())) {
            loaded.add(provider.getClass().getName());
            assertFalse(provider.getClass().getName() + " contributes settings", provider.settings().isEmpty());
        }
        assertEquals(listed, loaded);
    }

    public void testEveryKeyIsDefinedOnceUnderTheShellsPrefixes() {
        final Set<String> keys = new HashSet<>();
        for (Setting<?> setting : ServerlessSettings.all()) {
            final String key = setting.getKey();
            assertTrue(key, key.startsWith(ServerlessSettings.NODE_PREFIX) || key.startsWith(ServerlessSettings.INDEX_PREFIX));
            assertTrue("defined once: " + key, keys.add(key));
            // Every default parses: a default that does not is an exception on the first read of an unset setting.
            setting.get(Settings.EMPTY);
        }
        assertTrue("found the providers' settings, got " + keys.size(), keys.size() > 100);
    }

    public void testTwoProvidersDefiningOneKeyAreRefused() {
        final SettingsProvider first = () -> List.of(Setting.boolSetting("serverless.test.twice", true, Setting.Property.NodeScope));
        final SettingsProvider second = () -> List.of(Setting.intSetting("serverless.test.twice", 1, Setting.Property.NodeScope));
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> ServerlessSettings.collect(List.of(first, second)));
        assertTrue(e.getMessage(), e.getMessage().contains("serverless.test.twice"));
    }

    public void testIndexScopedSettingsAreExactlyTheShellsIndexSettings() {
        final Set<String> keys = new TreeSet<>();
        for (Setting<?> setting : ServerlessSettings.indexScoped()) {
            keys.add(setting.getKey());
        }
        assertEquals(
            new TreeSet<>(
                Set.of(
                    "index.serverless.storage_uuid",
                    "index.serverless.bucket",
                    "index.serverless.bucket_group",
                    "index.serverless.segment_placement",
                    "index.serverless.tail_overlay",
                    "index.serverless.cost.guardrails",
                    "index.serverless.cost.tier",
                    "index.serverless.cost.budget"
                )
            ),
            keys
        );
    }

    public void testUnknownNodeKeysAreReportedAndKnownOnesAreNot() {
        final Settings settings = Settings.builder()
            .put("serverless.store.type", "fs")
            .put("serverless.activation.max_shard", 10)
            .put("node.name", "n1")
            .build();
        assertEquals(List.of("serverless.activation.max_shard"), ServerlessSettings.unknownNodeKeys(settings));
    }

    public void testTheMostSpecificLayerThatParsesWins() {
        assertEquals("index", LayeredSetting.resolve("index", "cluster", value -> value, "node"));
        assertEquals("cluster", LayeredSetting.resolve(null, "cluster", value -> value, "node"));
        assertEquals("cluster", LayeredSetting.resolve("", "cluster", value -> value, "node"));
        assertEquals("node", LayeredSetting.resolve(null, null, value -> value, "node"));
        final java.util.function.Function<String, Boolean> strict = value -> {
            if ("true".equals(value) == false && "false".equals(value) == false) {
                throw new IllegalArgumentException(value);
            }
            return Boolean.parseBoolean(value);
        };
        assertEquals(Boolean.FALSE, LayeredSetting.resolve("bogus", "false", strict, true));
        assertEquals(Boolean.TRUE, LayeredSetting.resolve("bogus", "bogus", strict, true));
    }
}
