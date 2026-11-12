/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.repositories.s3.MinioBlobStores;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Where a meter keeps its data, and how slow the store is made to be, chosen from system properties so the same meter runs on
 * the in-process filesystem store or on a live S3-compatible endpoint (RustFS, MinIO), with or without injected latency.
 *
 * <ul>
 *   <li>{@code -Dtests.serverless.meter.store=fs|s3}: the filesystem store (default) or a fresh bucket on
 *   {@code -Dtests.serverless.s3.endpoint}, with {@code -Dtests.serverless.s3.access_key} and {@code secret_key}.</li>
 *   <li>{@code -Dtests.serverless.meter.latency=flat|s3|none}: added delay while the meter measures. {@code flat} (default) is
 *   20 ms per request with one in fifty at 200 ms, what the meters have always used; {@code s3} is
 *   {@link LatencyInjectingBlobStore.Profile#S3}; {@code none} adds nothing, which on a live endpoint leaves its own latency.</li>
 * </ul>
 */
final class MeterStores {

    static final String STORE = "tests.serverless.meter.store";
    static final String LATENCY = "tests.serverless.meter.latency";

    private MeterStores() {}

    /** @return true when the meters run on a live S3 endpoint */
    static boolean onS3() {
        return "s3".equals(System.getProperty(STORE, "fs"));
    }

    /** @return the endpoint the meters use when on S3 */
    static String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, "http://127.0.0.1:9000");
    }

    /** @return the latency setting's name */
    static String latency() {
        return System.getProperty(LATENCY, "flat");
    }

    /** @return a short description of the store and the latency, for a meter's report line */
    static String describe() {
        final String where = onS3() ? "s3 at " + endpoint() : "filesystem";
        final String slow = switch (latency()) {
            case "flat" -> "flat 20 ms, 1 in 50 at 200 ms";
            case "none" -> "no added latency";
            default -> LatencyInjectingBlobStore.Profile.named(latency()).toString();
        };
        return where + ", " + slow;
    }

    /**
     * The store a meter writes to, before any latency or counting wrapper.
     *
     * @param dir a directory for the filesystem store
     * @param config a scratch directory standing in for the S3 client's config path
     * @return the store
     */
    static BlobStore create(Path dir, Path config) throws IOException {
        if (onS3() == false) {
            return new FsBlobStore(8192, dir, false);
        }
        final String endpoint = endpoint();
        if (reachable(endpoint) == false) {
            throw new IllegalStateException("no S3-compatible endpoint at " + endpoint + " for -D" + STORE + "=s3");
        }
        final String bucket = "meter-" + Long.toHexString(System.nanoTime()).toLowerCase(Locale.ROOT);
        return MinioBlobStores.create(endpoint, MinioBlobStores.accessKey(), MinioBlobStores.secretKey(), bucket, config);
    }

    /** Applies the chosen latency to a store. */
    static void slowDown(LatencyInjectingBlobStore slow) {
        switch (latency()) {
            case "flat" -> {
                slow.setProfile(null);
                slow.setDelayMillis(20);
                slow.setTail(50, 200);
            }
            case "none" -> speedUp(slow);
            default -> slow.setProfile(LatencyInjectingBlobStore.Profile.named(latency()));
        }
    }

    /** Removes any added latency. */
    static void speedUp(LatencyInjectingBlobStore slow) {
        slow.setProfile(null);
        slow.setDelayMillis(0);
        slow.setTail(0, 0);
    }

    /**
     * Visits every blob under a container, depth first.
     *
     * @param container where to start
     * @param prefix the path of {@code container} relative to the walk's root, ending in a slash unless empty
     * @param visitor called with each blob's path relative to the root and its metadata
     */
    static void walk(BlobContainer container, String prefix, BiConsumer<String, BlobMetadata> visitor) throws IOException {
        for (Map.Entry<String, BlobMetadata> blob : container.listBlobs().entrySet()) {
            visitor.accept(prefix + blob.getKey(), blob.getValue());
        }
        for (Map.Entry<String, BlobContainer> child : container.children().entrySet()) {
            walk(child.getValue(), prefix + child.getKey() + "/", visitor);
        }
    }

    /** The root container of a store. */
    static BlobContainer root(BlobStore store) {
        return store.blobContainer(BlobPath.cleanPath());
    }

    private static boolean reachable(String endpoint) {
        try {
            final HttpURLConnection connection = (HttpURLConnection) URI.create(endpoint).toURL().openConnection(java.net.Proxy.NO_PROXY);
            connection.setConnectTimeout(2_000);
            connection.setReadTimeout(2_000);
            connection.getResponseCode();
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
