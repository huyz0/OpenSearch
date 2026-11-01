/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.serverless.log.JournalConfiguration;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Loads explicit journal placement and dedicated TLS material before the node starts. */
final class JournalAssignmentLoader {
    static final String ASSIGNMENT_FILE = "serverless.journal.assignment_file";
    static final String KEYSTORE_PATH = "serverless.journal.keystore.path";
    static final String TRUSTSTORE_PATH = "serverless.journal.truststore.path";
    static final String KEYSTORE_PASSWORD = "serverless.journal.keystore.secure_password";
    static final String TRUSTSTORE_PASSWORD = "serverless.journal.truststore.secure_password";
    private static final int MAX_ASSIGNMENT_BYTES = 64 * 1024;
    private static final int MAX_TLS_BYTES = 1024 * 1024;
    private static final Set<String> FIELDS = Set.of(
        "format_version",
        "deployment_id",
        "local_voter_id",
        "directory",
        "bootstrap",
        "groups",
        "joining_peer",
        "limits"
    );
    private static final Set<String> LIMIT_FIELDS = Set.of(
        "maximum_groups",
        "maximum_payload_bytes",
        "maximum_pending_requests",
        "maximum_pending_bytes",
        "queue_bytes",
        "segment_bytes",
        "cached_segment_bytes",
        "maximum_retained_entries",
        "minimum_free_bytes"
    );

    private JournalAssignmentLoader() {}

    static ServerlessBootstrap.JournalAssignment resolve(Settings settings, Path configPath, ServerlessBootstrap.JournalAssignment explicit)
        throws Exception {
        if (settings.hasValue(ASSIGNMENT_FILE) == false) {
            if (settings.hasValue(KEYSTORE_PATH) || settings.hasValue(TRUSTSTORE_PATH)) {
                throw new IllegalArgumentException("journal TLS paths require " + ASSIGNMENT_FILE);
            }
            return explicit;
        }
        if (settings.hasValue(KEYSTORE_PASSWORD) || settings.hasValue(TRUSTSTORE_PASSWORD)) {
            throw new IllegalArgumentException("journal passwords must be stored only in secure settings");
        }
        if (explicit != null) {
            throw new IllegalArgumentException("use one explicit journal assignment source");
        }
        final var configuration = parse(readBounded(path(settings, configPath, ASSIGNMENT_FILE), MAX_ASSIGNMENT_BYTES), configPath);
        final KeyStore keys = loadStore(settings, configPath, KEYSTORE_PATH, KEYSTORE_PASSWORD);
        int privateKeys = 0;
        for (var aliases = keys.aliases(); aliases.hasMoreElements();) {
            final String alias = aliases.nextElement();
            if (keys.isKeyEntry(alias)) {
                privateKeys++;
                final var chain = keys.getCertificateChain(alias);
                if (chain == null || chain.length == 0 || (chain[0] instanceof X509Certificate) == false) {
                    throw new IllegalArgumentException("journal member key requires an X.509 certificate chain");
                }
            }
        }
        if (privateKeys != 1) {
            throw new IllegalArgumentException("journal member keystore requires exactly one private key");
        }
        final var keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        try (var password = SecureSetting.secureString(KEYSTORE_PASSWORD, null).get(settings)) {
            keyFactory.init(keys, password.getChars());
        }
        final KeyStore trust = loadStore(settings, configPath, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
        if (trust.size() == 0) {
            throw new IllegalArgumentException("journal truststore requires explicit deployment trust anchors");
        }
        for (var aliases = trust.aliases(); aliases.hasMoreElements();) {
            final String alias = aliases.nextElement();
            if (trust.isCertificateEntry(alias) == false || (trust.getCertificate(alias) instanceof X509Certificate) == false) {
                throw new IllegalArgumentException("journal truststore may contain only X.509 trust anchors");
            }
        }
        final var trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trust);
        final var keyManager = Arrays.stream(keyFactory.getKeyManagers())
            .filter(X509KeyManager.class::isInstance)
            .map(X509KeyManager.class::cast)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("missing journal X.509 key manager"));
        final var trustManager = Arrays.stream(trustFactory.getTrustManagers())
            .filter(X509TrustManager.class::isInstance)
            .map(X509TrustManager.class::cast)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("missing journal X.509 trust manager"));
        return new ServerlessBootstrap.JournalAssignment(
            configuration,
            keyManager,
            trustManager,
            () -> org.opensearch.env.Environment.getFileStore(configuration.directory()).getUsableSpace()
        );
    }

    static JournalConfiguration parse(byte[] bytes, Path configPath) throws IOException {
        if (bytes.length > MAX_ASSIGNMENT_BYTES) {
            throw new IllegalArgumentException("journal assignment exceeds its byte limit");
        }
        try (
            var parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes
            )
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("journal assignment must be an object");
            }
            final var root = object(parser.mapOrdered(), FIELDS);
            if (parser.nextToken() != null || integer(root, "format_version") != 1) {
                throw new IllegalArgumentException("unsupported or trailing journal assignment content");
            }
            final var values = root.get("groups");
            if ((values instanceof List<?>) == false || ((List<?>) values).isEmpty() || ((List<?>) values).size() > 16) {
                throw new IllegalArgumentException("journal assignment requires one to sixteen physical groups");
            }
            final var groups = new ArrayList<JournalConfiguration.Group>();
            for (Object value : (List<?>) values) {
                final var group = object(value, Set.of("id", "peers"));
                if ((group.get("peers") instanceof List<?>) == false || ((List<?>) group.get("peers")).size() != 3) {
                    throw new IllegalArgumentException("journal group requires three peers");
                }
                final var peers = new ArrayList<JournalConfiguration.Peer>();
                for (Object peer : (List<?>) group.get("peers")) {
                    peers.add(peer(peer));
                }
                groups.add(new JournalConfiguration.Group(uuid(group, "id"), peers));
            }
            if ((root.get("bootstrap") instanceof Boolean) == false) {
                throw new IllegalArgumentException("journal assignment requires explicit boolean bootstrap authorization");
            }
            final Path directory = configPath.resolve(string(root, "directory")).normalize().toAbsolutePath();
            if (Files.isDirectory(directory) == false) {
                throw new IllegalArgumentException("journal directory must already be provisioned");
            }
            return new JournalConfiguration(
                uuid(root, "deployment_id"),
                uuid(root, "local_voter_id"),
                directory,
                groups,
                (Boolean) root.get("bootstrap"),
                limits(root),
                root.containsKey("joining_peer") ? peer(root.get("joining_peer")) : null
            );
        }
    }

    private static JournalConfiguration.Peer peer(Object value) {
        final var peer = object(value, Set.of("id", "host", "port", "availability_zone"));
        return new JournalConfiguration.Peer(
            uuid(peer, "id"),
            string(peer, "host"),
            integerValue(peer, "port"),
            string(peer, "availability_zone")
        );
    }

    private static JournalConfiguration.Limits limits(Map<String, Object> root) {
        final var defaults = JournalConfiguration.Limits.defaults();
        if (root.containsKey("limits") == false) {
            return defaults;
        }
        final var values = object(root.get("limits"), LIMIT_FIELDS);
        return new JournalConfiguration.Limits(
            intLimit(values, "maximum_groups", defaults.maximumGroups()),
            intLimit(values, "maximum_payload_bytes", defaults.maximumPayloadBytes()),
            intLimit(values, "maximum_pending_requests", defaults.maximumPendingRequests()),
            limit(values, "maximum_pending_bytes", defaults.maximumPendingBytes()),
            intLimit(values, "queue_bytes", defaults.queueBytes()),
            intLimit(values, "segment_bytes", defaults.segmentBytes()),
            limit(values, "cached_segment_bytes", defaults.cachedSegmentBytes()),
            limit(values, "maximum_retained_entries", defaults.maximumRetainedEntries()),
            limit(values, "minimum_free_bytes", defaults.minimumFreeBytes())
        );
    }

    private static int intLimit(Map<String, Object> values, String field, int fallback) {
        return values.containsKey(field) ? integerValue(values, field) : fallback;
    }

    private static int integerValue(Map<String, Object> values, String field) {
        final long value = integer(values, field);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("journal assignment integer is out of range: " + field);
        }
        return (int) value;
    }

    private static long limit(Map<String, Object> values, String field, long fallback) {
        return values.containsKey(field) ? integer(values, field) : fallback;
    }

    private static Map<String, Object> object(Object value, Set<String> allowed) {
        if ((value instanceof Map<?, ?>) == false) {
            throw new IllegalArgumentException("journal assignment field must be an object");
        }
        final var result = new LinkedHashMap<String, Object>();
        for (var entry : ((Map<?, ?>) value).entrySet()) {
            if ((entry.getKey() instanceof String) == false || allowed.contains(entry.getKey()) == false) {
                throw new IllegalArgumentException("unknown journal assignment field");
            }
            result.put((String) entry.getKey(), entry.getValue());
        }
        return result;
    }

    private static long integer(Map<String, Object> values, String field) {
        final Object value = values.get(field);
        if ((value instanceof Integer) == false && (value instanceof Long) == false) {
            throw new IllegalArgumentException("journal assignment requires integer " + field);
        }
        return ((Number) value).longValue();
    }

    private static String string(Map<String, Object> values, String field) {
        if ((values.get(field) instanceof String) == false || ((String) values.get(field)).isBlank()) {
            throw new IllegalArgumentException("journal assignment requires string " + field);
        }
        return (String) values.get(field);
    }

    private static UUID uuid(Map<String, Object> values, String field) {
        final String value = string(values, field);
        final UUID id = UUID.fromString(value);
        if (id.toString().equals(value) == false) {
            throw new IllegalArgumentException("journal assignment requires canonical UUID " + field);
        }
        return id;
    }

    private static Path path(Settings settings, Path configPath, String key) {
        final String value = settings.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("journal assignment requires " + key);
        }
        return configPath.resolve(value).normalize().toAbsolutePath();
    }

    private static KeyStore loadStore(Settings settings, Path configPath, String key, String passwordKey) throws Exception {
        final var store = KeyStore.getInstance("PKCS12");
        final byte[] encoded = readBounded(path(settings, configPath, key), MAX_TLS_BYTES);
        try (var input = new ByteArrayInputStream(encoded); var password = SecureSetting.secureString(passwordKey, null).get(settings)) {
            store.load(input, password.getChars());
        } finally {
            Arrays.fill(encoded, (byte) 0);
        }
        return store;
    }

    private static byte[] readBounded(Path file, int limit) throws IOException {
        if (Files.isRegularFile(file) == false) {
            throw new IllegalArgumentException("journal configuration must be a regular file");
        }
        try (var input = Files.newInputStream(file)) {
            final byte[] bytes = input.readNBytes(limit + 1);
            if (bytes.length > limit) {
                throw new IllegalArgumentException("journal configuration exceeds its byte limit");
            }
            return bytes;
        }
    }
}
