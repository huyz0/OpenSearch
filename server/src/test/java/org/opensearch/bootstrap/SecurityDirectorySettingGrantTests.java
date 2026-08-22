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
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;

import java.io.FilePermission;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Permission;
import java.security.Permissions;
import java.util.Enumeration;

/**
 * Verifies the core-side FilePermission ledger entry for operator-configured directories
 * that installed plugins declare via {@code plugin-security.properties} ({@code directory.settings}).
 * This grant is required because {@link org.opensearch.node.Node#assertCanWritePluginHealthPaths}
 * and {@link org.opensearch.monitor.fs.FsHealthService} probe such directories from
 * core code (not plugin code), so the plugin policy alone is insufficient.
 *
 * <p>The behavior under test:
 * <ul>
 *   <li>Setting unset/empty: no grant added; no exception.</li>
 *   <li>Setting set but not declared by any installed plugin: no grant added.</li>
 *   <li>Declared setting points at an existing directory: grant added covering both the
 *       directory itself and its recursive contents.</li>
 *   <li>Declared setting points at a nonexistent path: boot fails loudly with a clear message
 *       (no auto-create — the directory must be on a pre-mounted volume).</li>
 *   <li>Declared setting points at a regular file: same boot-failure behavior.</li>
 * </ul>
 */
@SuppressWarnings("removal")
@SuppressForbidden(reason = "https://github.com/opensearch-project/OpenSearch/issues/19640")
public class SecurityDirectorySettingGrantTests extends OpenSearchTestCase {

    private static final String SETTING = "my_plugin.scratch_directory";

    public void testDirectorySettingUnsetAddsNoGrant() throws Exception {
        Permissions policy = runAddFilePermissions(Settings.EMPTY, true);
        assertFalse(
            "no directory-setting FilePermission should be granted when the setting is unset",
            hasFilePermissionMatching(policy, "directory-setting-marker-not-present")
        );
    }

    public void testDirectorySettingEmptyStringAddsNoGrant() throws Exception {
        Settings settings = Settings.builder().put(SETTING, "").build();
        Permissions policy = runAddFilePermissions(settings, true);
        assertFalse(
            "no directory-setting FilePermission should be granted when the setting is empty",
            hasFilePermissionMatching(policy, "directory-setting-marker-not-present")
        );
    }

    public void testUndeclaredDirectorySettingAddsNoGrant() throws Exception {
        Path dir = createTempDir();
        Settings settings = Settings.builder().put(SETTING, dir.toString()).build();
        Permissions policy = runAddFilePermissions(settings, false);
        assertFalse("a setting no installed plugin declares must not produce a grant", hasFilePermissionExact(policy, dir.toString()));
    }

    public void testDeclaredDirectorySettingPointsToExistingDirectoryAddsBothGrants() throws Exception {
        Path dir = createTempDir();
        Settings settings = Settings.builder().put(SETTING, dir.toString()).build();
        Permissions policy = runAddFilePermissions(settings, true);
        assertTrue("expected FilePermission on the directory itself (" + dir + ")", hasFilePermissionExact(policy, dir.toString()));
        assertTrue(
            "expected recursive FilePermission on files under the directory (" + dir + "/-)",
            hasFilePermissionExact(policy, dir.toString() + dir.getFileSystem().getSeparator() + "-")
        );
    }

    public void testDeclaredDirectorySettingMissingDirectoryFailsBoot() {
        Path missing = createTempDir().resolve("does_not_exist_subdir");
        Settings settings = Settings.builder().put(SETTING, missing.toString()).build();
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> runAddFilePermissions(settings, true));
        assertTrue(
            "expected error message to reference the missing path; got: " + ex.getMessage(),
            ex.getMessage().contains(missing.toString())
        );
        assertTrue(
            "expected error message to mention mounting the volume; got: " + ex.getMessage(),
            ex.getMessage().contains("volume is mounted")
        );
        // Confirm no auto-create happened on the wrong filesystem.
        assertFalse("Security.addFilePermissions must NOT auto-create the directory", Files.exists(missing));
    }

    public void testDeclaredDirectorySettingPointsToRegularFileFailsBoot() throws Exception {
        Path file = createTempDir().resolve("regular_file");
        Files.write(file, new byte[] { 0x00 });
        Settings settings = Settings.builder().put(SETTING, file.toString()).build();
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> runAddFilePermissions(settings, true));
        assertTrue("expected error message to reference the bad path; got: " + ex.getMessage(), ex.getMessage().contains(file.toString()));
    }

    /**
     * Build a minimal {@link Environment} whose plugins dir contains one fake installed plugin,
     * optionally declaring {@link #SETTING} in plugin-security.properties, then run
     * {@link Security#addFilePermissions} into a fresh {@link Permissions}.
     */
    private Permissions runAddFilePermissions(Settings extraSettings, boolean declareSetting) throws Exception {
        Path home = createTempDir();
        Path pluginDir = home.resolve("plugins").resolve("fake-plugin");
        Files.createDirectories(pluginDir);
        if (declareSetting) {
            Files.writeString(
                pluginDir.resolve("plugin-security.properties"),
                "directory.settings=" + SETTING + "\n",
                StandardCharsets.UTF_8
            );
        }
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home.toString()).put(extraSettings).build();
        Environment environment = new Environment(nodeSettings, null);
        Permissions policy = new Permissions();
        Security.addFilePermissions(policy, environment);
        return policy;
    }

    private static boolean hasFilePermissionExact(Permissions policy, String name) {
        Enumeration<Permission> e = policy.elements();
        while (e.hasMoreElements()) {
            Permission p = e.nextElement();
            if (p instanceof FilePermission && p.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    /** Returns true iff any FilePermission's name *contains* the given marker — used for negative assertions. */
    private static boolean hasFilePermissionMatching(Permissions policy, String marker) {
        Enumeration<Permission> e = policy.elements();
        while (e.hasMoreElements()) {
            Permission p = e.nextElement();
            if (p instanceof FilePermission && p.getName().contains(marker)) {
                return true;
            }
        }
        return false;
    }
}
