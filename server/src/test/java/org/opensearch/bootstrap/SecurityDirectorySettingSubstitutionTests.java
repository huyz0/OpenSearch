/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.FilePermission;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.Permission;
import java.security.Policy;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * End-to-end test for the generic directory-setting policy substitution mechanism
 * (plugin-security.properties, {@code directory.settings}). Verifies that:
 *  - For a declared setting key with a value, PolicyFile resolves
 *    {@code ${{opensearch.<key>}}/-} to the configured path + "/-".
 *  - When the setting is unset, the resolver leaves the literal {@code ${{...}}/-} in place,
 *    granting no real filesystem path.
 *  - A setting that is NOT declared by the plugin is not substituted, even when set.
 *
 * This is the test that would catch a regression where a plugin policy is written with
 * single-brace ${...} instead of double-brace ${{...}} (which is hardcoded to a small allowlist
 * in PolicyFile and would silently produce a useless permission).
 */
@SuppressWarnings("removal")
@SuppressForbidden(reason = "https://github.com/opensearch-project/OpenSearch/issues/19640")
public class SecurityDirectorySettingSubstitutionTests extends OpenSearchTestCase {

    private static final String SETTING = "my_plugin.scratch_directory";
    private static final String PROPERTY = "opensearch." + SETTING;

    public void testFilePermissionResolvesWhenDirectorySettingConfigured() throws Exception {
        Path policy = writePluginStylePolicy();
        Settings settings = Settings.builder().put(SETTING, "/tmp/test-scratch").build();
        Policy parsed = Security.readPolicy(policy.toUri().toURL(), Collections.emptyMap(), settings, List.of(SETTING));
        FilePermission resolved = firstFilePermission(parsed);
        assertEquals("/tmp/test-scratch/-", resolved.getName());
        // The property must not leak past readPolicy.
        assertNull(System.getProperty(PROPERTY));
    }

    public void testFilePermissionLeavesLiteralWhenDirectorySettingEmpty() throws Exception {
        Path policy = writePluginStylePolicy();
        Policy parsed = Security.readPolicy(policy.toUri().toURL(), Collections.emptyMap(), Settings.EMPTY, List.of(SETTING));
        FilePermission resolved = firstFilePermission(parsed);
        assertEquals("${{" + PROPERTY + "}}/-", resolved.getName());
        assertNull(System.getProperty(PROPERTY));
    }

    public void testUndeclaredSettingIsNotSubstituted() throws Exception {
        Path policy = writePluginStylePolicy();
        Settings settings = Settings.builder().put(SETTING, "/tmp/test-scratch").build();
        Policy parsed = Security.readPolicy(policy.toUri().toURL(), Collections.emptyMap(), settings, Collections.emptyList());
        FilePermission resolved = firstFilePermission(parsed);
        assertEquals("${{" + PROPERTY + "}}/-", resolved.getName());
        assertNull(System.getProperty(PROPERTY));
    }

    private Path writePluginStylePolicy() throws Exception {
        Path policy = createTempDir().resolve("plugin.policy");
        Files.writeString(
            policy,
            "grant {\n" + "  permission java.io.FilePermission \"${{" + PROPERTY + "}}/-\", \"read,write,delete\";\n" + "};\n",
            StandardCharsets.UTF_8
        );
        return policy;
    }

    private static FilePermission firstFilePermission(Policy policy) {
        CodeSource cs = new CodeSource(null, (Certificate[]) null);
        Enumeration<Permission> permissions = policy.getPermissions(cs).elements();
        while (permissions.hasMoreElements()) {
            Permission p = permissions.nextElement();
            if (p instanceof FilePermission) {
                return (FilePermission) p;
            }
        }
        throw new AssertionError("No FilePermission found in parsed policy");
    }
}
