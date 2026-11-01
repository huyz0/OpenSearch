/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.log.JournalConfiguration;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Configuration parsing does not acquire a disk identity, publish catalog state or start a service. */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class JournalAssignmentLoaderTests extends OpenSearchTestCase {
    public void testExplicitPlacementAndLimitsArePreservedWithoutTouchingTheVolume() throws Exception {
        final Path config = createTempDir();
        final var manifest = manifest(config);
        manifest.put("limits", Map.of("maximum_groups", 16, "maximum_retained_entries", 4096));
        final var parsed = JournalAssignmentLoader.parse(encode(manifest), config);
        assertEquals(UUID.fromString((String) manifest.get("local_voter_id")), parsed.localVoterId());
        assertEquals(config.resolve("ssd"), parsed.directory());
        assertTrue(parsed.bootstrap());
        assertEquals(16, parsed.limits().maximumGroups());
        assertEquals(4096, parsed.limits().maximumRetainedEntries());
        assertEquals(JournalConfiguration.Limits.defaults().maximumPendingBytes(), parsed.limits().maximumPendingBytes());
        try (var files = Files.list(parsed.directory())) {
            assertEquals(0, files.count());
        }
        manifest.put("bootstrap", false);
        assertFalse(JournalAssignmentLoader.parse(encode(manifest), config).bootstrap());
        final var joining = Map.of(
            "id",
            UUID.randomUUID().toString(),
            "host",
            "new-voter.example",
            "port",
            12349,
            "availability_zone",
            "az-0"
        );
        manifest.put("local_voter_id", joining.get("id"));
        manifest.put("joining_peer", joining);
        final var learner = JournalAssignmentLoader.parse(encode(manifest), config);
        assertEquals(learner.joiningPeer(), learner.localPeer());
        assertEquals(joining.get("id"), learner.localVoterId().toString());
    }

    public void testMissingUnknownDuplicateAndTrailingContentAreRefused() throws Exception {
        final Path config = createTempDir();
        final var original = manifest(config);
        for (String field : List.of("format_version", "deployment_id", "local_voter_id", "directory", "bootstrap", "groups")) {
            final var missing = new LinkedHashMap<>(original);
            missing.remove(field);
            expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(missing), config));
        }
        final var unknown = new LinkedHashMap<>(original);
        unknown.put("bootstarp", true);
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(unknown), config));
        final String json = new String(encode(original), StandardCharsets.UTF_8);
        expectThrows(
            Exception.class,
            () -> JournalAssignmentLoader.parse(("{\"format_version\":1," + json.substring(1)).getBytes(StandardCharsets.UTF_8), config)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> JournalAssignmentLoader.parse((json + " {}").getBytes(StandardCharsets.UTF_8), config)
        );
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(new byte[65537], config));
    }

    public void testVersionTypesGroupBoundsAndProvisioningAreStrict() throws Exception {
        final Path config = createTempDir();
        final var manifest = manifest(config);
        for (Object version : List.of(2, "1", 1.0)) {
            manifest.put("format_version", version);
            expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        }
        manifest.put("format_version", 1);
        manifest.put("bootstrap", "true");
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        manifest.put("bootstrap", true);
        final Object groups = manifest.get("groups");
        manifest.put("groups", List.of());
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        manifest.put("groups", java.util.Collections.nCopies(17, ((List<?>) groups).getFirst()));
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        manifest.put("groups", groups);
        manifest.put("directory", "missing-ssd");
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        assertFalse(Files.exists(config.resolve("missing-ssd")));
    }

    public void testNestedPlacementAndLimitFieldsCannotSilentlyChangeMeaning() throws Exception {
        final Path config = createTempDir();
        final var manifest = manifest(config);
        manifest.put("limits", Map.of("maximum_pending_requests", 0));
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        manifest.put("limits", Map.of("maximum_pending_requests", 1.5));
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        manifest.put("limits", Map.of("maximum_pending_requests", 1L + Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        manifest.put("limits", Map.of("max_pending", 5));
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        manifest.remove("limits");
        manifest.put(
            "joining_peer",
            Map.of("id", manifest.get("local_voter_id"), "host", "localhost", "port", 1234, "availability_zone", "az-0", "ignored", true)
        );
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
        manifest.put("joining_peer", null);
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.parse(encode(manifest), config));
    }

    public void testDedicatedTlsCredentialsAndActualDiskProbeLoadFromSecureSettings() throws Exception {
        final Path config = createTempDir();
        final Settings settings = credentials(config);
        final var assignment = JournalAssignmentLoader.resolve(settings, config, null);
        assertNotNull(assignment);
        assertTrue(assignment.diskSpace().usableBytes() > 0);
        assertTrue(
            assignment.diskSpace().usableBytes() <= org.opensearch.env.Environment.getFileStore(config.resolve("ssd")).getTotalSpace()
        );
        assertTrue(assignment.trustManager().getAcceptedIssuers().length > 0);
        assertEquals(1, assignment.configuration().groups().size());
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.resolve(settings, config, assignment));
        assertNull(JournalAssignmentLoader.resolve(Settings.EMPTY, config, null));
        assertSame(assignment, JournalAssignmentLoader.resolve(Settings.EMPTY, config, assignment));
        expectThrows(
            IllegalArgumentException.class,
            () -> JournalAssignmentLoader.resolve(
                Settings.builder().put(JournalAssignmentLoader.KEYSTORE_PATH, "member.p12").build(),
                config,
                null
            )
        );
    }

    public void testMissingWrongAndPlaintextPasswordsDoNotFallBackToJvmTrust() throws Exception {
        final Path config = createTempDir();
        final Settings settings = credentials(config);
        expectThrows(
            IllegalArgumentException.class,
            () -> JournalAssignmentLoader.resolve(
                Settings.builder().put(settings).put(JournalAssignmentLoader.KEYSTORE_PASSWORD, "testonly").build(),
                config,
                null
            )
        );
        final var wrong = new MockSecureSettings();
        wrong.setString(JournalAssignmentLoader.KEYSTORE_PASSWORD, "incorrect-test-password");
        wrong.setString(JournalAssignmentLoader.TRUSTSTORE_PASSWORD, "testonly");
        expectThrows(
            Exception.class,
            () -> JournalAssignmentLoader.resolve(Settings.builder().put(settings, false).setSecureSettings(wrong).build(), config, null)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> JournalAssignmentLoader.resolve(
                Settings.builder().put(settings, false).put(JournalAssignmentLoader.KEYSTORE_PASSWORD, "testonly").build(),
                config,
                null
            )
        );
        expectThrows(Exception.class, () -> JournalAssignmentLoader.resolve(Settings.builder().put(settings, false).build(), config, null));
    }

    public void testEmptyTrustPrivateKeysInTrustAndOversizedFilesAreRefused() throws Exception {
        final Path config = createTempDir();
        final Settings settings = credentials(config);
        final var empty = KeyStore.getInstance("PKCS12");
        empty.load(null, "testonly".toCharArray());
        try (var output = Files.newOutputStream(config.resolve("trust.p12"))) {
            empty.store(output, "testonly".toCharArray());
        }
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.resolve(settings, config, null));
        Files.copy(config.resolve("member.p12"), config.resolve("trust.p12"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.resolve(settings, config, null));
        Files.write(config.resolve("member.p12"), new byte[1024 * 1024 + 1]);
        expectThrows(IllegalArgumentException.class, () -> JournalAssignmentLoader.resolve(settings, config, null));
    }

    public void testMalformedAssignmentFailsBeforeTheNodeStarts() throws Exception {
        final Path files = createTempDir();
        Files.writeString(files.resolve("assignment.json"), "{\"unexpected\":true}");
        final Path home = createTempDir();
        final var settings = Settings.builder()
            .put("node.name", "invalid-journal-assignment")
            .put("path.home", home)
            .put("network.host", "invalid.invalid")
            .put(ServerlessBootstrap.STORE_PATH, createTempDir())
            .put(JournalAssignmentLoader.ASSIGNMENT_FILE, files.resolve("assignment.json"))
            .build();
        final var failure = expectThrows(IllegalArgumentException.class, () -> ServerlessBootstrap.start(settings));
        assertTrue(failure.getMessage(), failure.getMessage().contains("unknown journal assignment field"));
        assertFalse(Files.exists(home.resolve("data")));
    }

    public void testDiskIdentityCommandIsStableAndNeverOverwritesItsOutput() throws Exception {
        final Path directory = createTempDir();
        final Path output = createTempDir().resolve("voter-id");
        final var args = new String[] { "--journal-identity", directory.toString(), output.toString() };
        ServerlessBootstrap.main(args);
        final UUID id = UUID.fromString(Files.readString(output).trim());
        assertEquals(id, org.opensearch.serverless.log.JournalDiskIdentity.discover(directory));
        expectThrows(java.nio.file.FileAlreadyExistsException.class, () -> ServerlessBootstrap.main(args));
        assertEquals(id, UUID.fromString(Files.readString(output).trim()));
        final Path second = output.resolveSibling("same-disk-id");
        ServerlessBootstrap.main(new String[] { "--journal-identity", directory.toString(), second.toString() });
        assertEquals(id, UUID.fromString(Files.readString(second).trim()));
        final Path unrelated = createTempDir();
        Files.writeString(unrelated.resolve("unrelated-data"), "keep");
        final Path refused = output.resolveSibling("refused-id");
        expectThrows(
            java.io.IOException.class,
            () -> ServerlessBootstrap.main(new String[] { "--journal-identity", unrelated.toString(), refused.toString() })
        );
        assertFalse(Files.exists(refused));
        assertEquals("keep", Files.readString(unrelated.resolve("unrelated-data")));
        expectThrows(IllegalArgumentException.class, () -> ServerlessBootstrap.main(new String[] { "unknown-command" }));
    }

    private Settings credentials(Path config) throws Exception {
        Files.write(config.resolve("assignment.json"), encode(manifest(config)));
        for (var entry : Map.of("member.p12", "keystore", "trust.p12", "truststore").entrySet()) {
            try (var input = getClass().getResourceAsStream("/org/opensearch/serverless/testkit/tls-test-" + entry.getValue() + ".p12")) {
                Files.copy(input, config.resolve(entry.getKey()));
            }
        }
        final var secure = new MockSecureSettings();
        secure.setString(JournalAssignmentLoader.KEYSTORE_PASSWORD, "testonly");
        secure.setString(JournalAssignmentLoader.TRUSTSTORE_PASSWORD, "testonly");
        return Settings.builder()
            .put(JournalAssignmentLoader.ASSIGNMENT_FILE, "assignment.json")
            .put(JournalAssignmentLoader.KEYSTORE_PATH, "member.p12")
            .put(JournalAssignmentLoader.TRUSTSTORE_PATH, "trust.p12")
            .setSecureSettings(secure)
            .build();
    }

    private Map<String, Object> manifest(Path config) throws Exception {
        Files.createDirectory(config.resolve("ssd"));
        final var peers = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 3; i++) {
            peers.add(Map.of("id", UUID.randomUUID().toString(), "host", "127.0.0.1", "port", 12340 + i, "availability_zone", "az-" + i));
        }
        final var manifest = new LinkedHashMap<String, Object>();
        manifest.put("format_version", 1);
        manifest.put("deployment_id", UUID.randomUUID().toString());
        manifest.put("local_voter_id", peers.getFirst().get("id"));
        manifest.put("directory", "ssd");
        manifest.put("bootstrap", true);
        manifest.put("groups", List.of(Map.of("id", UUID.randomUUID().toString(), "peers", peers)));
        return manifest;
    }

    private static byte[] encode(Map<String, Object> manifest) throws Exception {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.map(manifest);
            return BytesReference.toBytes(BytesReference.bytes(builder));
        }
    }
}
