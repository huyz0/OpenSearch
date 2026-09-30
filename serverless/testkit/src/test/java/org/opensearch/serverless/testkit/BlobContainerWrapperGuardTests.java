/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.metadata.ConditionalDeleteProbe;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.ObjectStores;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Nothing between the node and the store may quietly stand an interface default in for the backend's own method.
 *
 * <p>{@code ObjectStores.Metered} -- the wrapper every production store is built behind -- overrode neither the
 * conditional register delete nor the bounded listings, so those calls reached {@code BlobContainer}'s defaults:
 * the delete threw {@code UnsupportedOperationException}, the probe read that as a store that does not honour the
 * condition, and every node stayed on tombstones; the capped listing listed the whole prefix. Every test passed,
 * because the tests built their stores without the wrapper. That is a class of bug, so this guards the class:
 * every {@code BlobContainer} the shell declares must override every default the interface has, and the chain the
 * production factory builds is exercised end to end, including the probe.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class BlobContainerWrapperGuardTests extends OpenSearchTestCase {

    /** Every default method of {@code BlobContainer} a class inherits rather than overrides. */
    static List<String> inheritedDefaults(Class<?> implementation) throws NoSuchMethodException {
        final List<String> inherited = new ArrayList<>();
        for (Method method : BlobContainer.class.getMethods()) {
            if (method.isDefault() == false) {
                continue;
            }
            final Method resolved = implementation.getMethod(method.getName(), method.getParameterTypes());
            if (resolved.getDeclaringClass().isInterface()) {
                inherited.add(
                    method.getName() + Arrays.stream(method.getParameterTypes())
                        .map(Class::getSimpleName)
                        .collect(Collectors.joining(",", "(", ")"))
                );
            }
        }
        inherited.sort(String::compareTo);
        return inherited;
    }

    /** Every concrete {@code BlobContainer} the shell declares, found by walking its compiled classes. */
    static List<Class<?>> shellBlobContainers() throws Exception {
        final Path root = Path.of(ObjectStores.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        final List<String> names = new ArrayList<>();
        if (Files.isDirectory(root)) {
            try (Stream<Path> walk = Files.walk(root)) {
                walk.filter(p -> p.toString().endsWith(".class"))
                    .map(p -> root.relativize(p).toString().replace('\\', '/'))
                    .forEach(names::add);
            }
        } else {
            try (
                java.nio.file.FileSystem jar = java.nio.file.FileSystems.newFileSystem(root);
                Stream<Path> walk = Files.walk(jar.getPath("/"))
            ) {
                walk.map(Path::toString)
                    .filter(n -> n.endsWith(".class"))
                    .map(n -> n.startsWith("/") ? n.substring(1) : n)
                    .forEach(names::add);
            }
        }
        final List<Class<?>> found = new ArrayList<>();
        for (String name : names) {
            if (name.startsWith("org/opensearch/serverless/") == false) {
                continue;
            }
            final Class<?> candidate = Class.forName(
                name.substring(0, name.length() - ".class".length()).replace('/', '.'),
                false,
                ObjectStores.class.getClassLoader()
            );
            if (BlobContainer.class.isAssignableFrom(candidate)
                && candidate.isInterface() == false
                && Modifier.isAbstract(candidate.getModifiers()) == false) {
                found.add(candidate);
            }
        }
        return found;
    }

    public void testNoContainerTheShellDeclaresInheritsADefault() throws Exception {
        final List<Class<?>> containers = shellBlobContainers();
        assertTrue(
            "the production wrapper must be among them, or the scan is not finding anything: " + containers,
            containers.stream().anyMatch(c -> c.getName().startsWith(ObjectStores.Metered.class.getName()))
        );
        final Map<String, List<String>> dropped = new TreeMap<>();
        for (Class<?> container : containers) {
            final List<String> inherited = inheritedDefaults(container);
            if (inherited.isEmpty() == false) {
                dropped.put(container.getName(), inherited);
            }
        }
        assertEquals(
            "a container in the shell inherits a BlobContainer default: whatever the backend does for that method, this does"
                + " the interface's default instead",
            Map.of(),
            dropped
        );
    }

    /**
     * The canary: the wrapper as it was before the fix -- registers and plain listings delegated, the conditional
     * delete and the bounded listings not -- is caught, and the methods named are the ones that were dropped.
     */
    public void testTheGuardCatchesTheWrapperThatDroppedTheConditionalDelete() throws Exception {
        final List<String> inherited = inheritedDefaults(LikeMeteredBeforeTheFix.class);
        assertTrue(inherited.toString(), inherited.contains("deleteRegisterIfUnchanged(String,long)"));
        assertTrue(inherited.toString(), inherited.contains("listBlobsByPrefixInSortedOrder(String,int,BlobNameSortOrder)"));
        assertTrue(inherited.toString(), inherited.contains("listBlobsByPrefix(String,String,int)"));
        assertTrue(inherited.toString(), inherited.contains("children(String,int)"));
        assertFalse("and the registers it did delegate are not flagged", inherited.contains("readRegister(String)"));
    }

    /** Through the production factory on a filesystem: every method reaches the store, and the probe passes. */
    public void testTheProductionChainOnAFilesystem() throws Exception {
        final Path dir = createTempDir();
        try (
            ObjectStores.Handle handle = ObjectStores.create(
                Settings.builder().put(ObjectStores.TYPE, "fs").put("serverless.store.path", dir.toString()).build(),
                null,
                createTempDir()
            )
        ) {
            exercise(handle.blobStore(), true);
        }
    }

    /** Through the production factory on an S3 API: RustFS honours the conditional delete, MinIO does not. */
    public void testTheProductionChainOnAnS3Api() throws Exception {
        final String endpoint = System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, "http://127.0.0.1:1");
        assumeTrue("no S3-compatible endpoint at " + endpoint, reachable(endpoint));
        final String access = System.getProperty("tests.serverless.s3.access_key", "minioadmin");
        final String secret = System.getProperty("tests.serverless.s3.secret_key", "minioadmin");
        final String bucket = "guard-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        org.opensearch.repositories.s3.MinioBlobStores.create(endpoint, access, secret, bucket, createTempDir());
        final MockSecureSettings secure = new MockSecureSettings();
        secure.setString("s3.client.default.access_key", access);
        secure.setString("s3.client.default.secret_key", secret);
        final Settings settings = Settings.builder()
            .put("node.name", "guard")
            .put("cluster.name", "serverless-guard")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put(ObjectStores.TYPE, "s3")
            .put(ObjectStores.BUCKET, bucket)
            .put(ObjectStores.ENDPOINT, endpoint)
            .put(ObjectStores.PATH_STYLE, true)
            .put(ObjectStores.REGION, "us-east-1")
            .setSecureSettings(secure)
            .build();
        final boolean honours = Boolean.parseBoolean(System.getProperty("tests.serverless.guard.conditional_delete", "true"));
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            try (ObjectStores.Handle handle = ObjectStores.create(settings, node.clusterService(), createTempDir())) {
                exercise(handle.blobStore(), honours);
            }
        } finally {
            org.opensearch.repositories.s3.MinioBlobStores.deleteBucket(endpoint, access, secret, bucket, createTempDir());
        }
    }

    /** Each non-default path, through whatever chain {@code store} is, observed at the store. */
    private void exercise(BlobStore store, boolean honoursConditionalDelete) throws Exception {
        assertTrue("the production factory builds behind the wrapper", store instanceof ObjectStores.Metered);
        final ObjectStores.Metered metered = (ObjectStores.Metered) store;
        final BlobContainer container = store.blobContainer(BlobPath.cleanPath().add("guard"));

        // Registers: create, read, swap.
        final BlobRegisterCasResult created = container.createRegisterIfAbsent("reg", bytes("v1"));
        assertTrue(created.applied());
        assertFalse("create-if-absent refuses a second creation", container.createRegisterIfAbsent("reg", bytes("v2")).applied());
        final Optional<BlobRegister> read = container.readRegister("reg");
        assertEquals("v1", read.orElseThrow().value().utf8ToString());
        assertTrue(container.compareAndSwapRegister("reg", read.get().generation(), bytes("v2")).applied());
        assertFalse(
            "a swap at a stale generation loses",
            container.compareAndSwapRegister("reg", read.get().generation(), bytes("v3")).applied()
        );

        // The conditional delete: refused at a stale generation, applied at the current one -- or, on a store that
        // does not honour it, the probe says so rather than the wrapper refusing on the store's behalf.
        final long current = container.readRegister("reg").orElseThrow().generation();
        if (honoursConditionalDelete) {
            assertFalse("a stale conditional delete must be refused", container.deleteRegisterIfUnchanged("reg", read.get().generation()));
            assertTrue("a current one applies", container.deleteRegisterIfUnchanged("reg", current));
            assertTrue("and the register is gone", container.readRegister("reg").isEmpty());
        }
        assertEquals(
            "the node's probe, through the production chain",
            honoursConditionalDelete,
            ConditionalDeleteProbe.honoured(store, BlobPath.cleanPath().add("probe"))
        );

        // Put-if-absent.
        container.writeBlob("once", new ByteArrayInputStream(new byte[] { 1 }), 1, true);
        expectThrows(
            java.nio.file.FileAlreadyExistsException.class,
            () -> container.writeBlob("once", new ByteArrayInputStream(new byte[] { 2 }), 1, true)
        );

        // Bounded listings, counted as one listing each: the backend's own, not a full listing and a sort.
        for (String name : List.of("p-a", "p-b", "p-c", "p-d")) {
            container.writeBlob(name, new ByteArrayInputStream(new byte[] { 1 }), 1, false);
        }
        long before = metered.impliedS3Requests();
        final List<BlobMetadata> page = container.listBlobsByPrefix("p-", "p-a", 2);
        assertEquals(List.of("p-b", "p-c"), page.stream().map(BlobMetadata::name).toList());
        assertEquals("one listing", before + 1, metered.impliedS3Requests());
        before = metered.impliedS3Requests();
        final List<BlobMetadata> sorted = container.listBlobsByPrefixInSortedOrder("p-", 2, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
        assertEquals(List.of("p-a", "p-b"), sorted.stream().map(BlobMetadata::name).toList());
        assertEquals("one listing", before + 1, metered.impliedS3Requests());

        final BlobContainer root = store.blobContainer(BlobPath.cleanPath());
        for (String child : List.of("c-1", "c", "d")) {
            store.blobContainer(BlobPath.cleanPath().add(child)).writeBlob("x", new ByteArrayInputStream(new byte[] { 1 }), 1, false);
        }
        assertEquals(
            "children after a cursor, in key order",
            List.of("d"),
            root.children("c", 5).keySet().stream().filter(n -> n.equals("d") || n.startsWith("c")).toList()
        );
    }

    private static BytesArray bytes(String value) {
        return new BytesArray(value.getBytes(StandardCharsets.UTF_8));
    }

    private static boolean reachable(String endpoint) {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/minio/health/live"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() < 500;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * {@code ObjectStores.Metered.Counting} as it stood at {@code 7bfec04d070}: the abstract methods, the three
     * register methods and the plain listings delegated; nothing else.
     */
    private static final class LikeMeteredBeforeTheFix implements BlobContainer {
        private final BlobContainer inner;

        LikeMeteredBeforeTheFix(BlobContainer inner) {
            this.inner = inner;
        }

        @Override
        public BlobPath path() {
            return inner.path();
        }

        @Override
        public boolean blobExists(String blobName) throws IOException {
            return inner.blobExists(blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            return inner.readBlob(blobName);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return inner.readBlob(blobName, position, length);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            inner.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            inner.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public DeleteResult delete() throws IOException {
            return inner.delete();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
            inner.deleteBlobsIgnoringIfNotExists(blobNames);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() throws IOException {
            return inner.listBlobs();
        }

        @Override
        public Map<String, BlobContainer> children() throws IOException {
            return inner.children();
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
            return inner.listBlobsByPrefix(blobNamePrefix);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return inner.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return inner.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }

        @Override
        public BlobRegisterCasResult createRegisterIfAbsent(String blobName, BytesReference value) throws IOException {
            return inner.createRegisterIfAbsent(blobName, value);
        }
    }
}
