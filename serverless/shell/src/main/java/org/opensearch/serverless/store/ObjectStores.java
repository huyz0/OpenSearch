/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.s3.S3RepositoryPlugin;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Path;

/**
 * Where a node's data actually lives.
 *
 * <p>Everything above this treats the object store as one interface with compare-and-swap registers,
 * ranged reads and listing on it. This is the one place that decides which implementation provides them,
 * and it exists because until now there was no such place: {@code ServerlessBootstrap} constructed an
 * {@code FsBlobStore} inline, which meant a node could only run on a local directory even though the
 * conformance suite had established what an object store must provide.
 *
 * <p><b>S3 is reached through {@link S3RepositoryPlugin#getRepositories}, the supported plugin API.</b>
 * The conformance tests build an {@code S3BlobStore} directly, from inside the plugin's own package, because a
 * conformance suite has to exercise exactly the container production uses and nothing else. Production
 * code copying that would be depending on package-private constructors — so it does not. The cost is a
 * repository's worth of scaffolding; the benefit is that this keeps working when those internals change.
 */
public final class ObjectStores {

    /** Which implementation backs the store: {@code fs} or {@code s3}. */
    public static final String TYPE = "serverless.store.type";

    /** For {@code s3}: the bucket. */
    public static final String BUCKET = "serverless.store.bucket";

    /** For {@code s3}: an endpoint override, for S3-compatible services. Optional. */
    public static final String ENDPOINT = "serverless.store.endpoint";

    /** For {@code s3}: the region. */
    public static final String REGION = "serverless.store.region";

    /**
     * For {@code s3}: address the bucket by path rather than by hostname. Required by most S3-compatible
     * services, which do not have a wildcard DNS entry per bucket.
     */
    public static final String PATH_STYLE = "serverless.store.path_style_access";

    private ObjectStores() {}

    /**
     * Builds the blob store a node should use.
     *
     * @param settings the node's settings
     * @param clusterService the node's cluster service, which the repository needs
     * @param configPath a directory standing in for the node's config path
     * @return the store, which the caller must close
     * @throws Exception if the store cannot be opened
     */
    public static ObjectStoreHandle create(Settings settings, ClusterService clusterService, Path configPath) throws Exception {
        final String type = settings.get(TYPE, "fs");
        if ("fs".equals(type)) {
            final String path = settings.get("serverless.store.path");
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException("serverless.store.path is required for a filesystem store");
            }
            return new ObjectStoreHandle(new MeteredBlobStore(new FsBlobStore(8192, Path.of(path), false)), null);
        }
        if ("s3".equals(type) == false) {
            throw new IllegalArgumentException("unknown " + TYPE + ": " + type + "; expected fs or s3");
        }

        final String bucket = settings.get(BUCKET);
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException(BUCKET + " is required for an s3 store");
        }

        // Credentials come from the node's keystore under s3.client.default.*, which is where the s3
        // plugin already looks. Deliberately not a serverless.* setting: a second way to spell an access
        // key is a second thing to leak.
        final Settings.Builder repository = Settings.builder().put("bucket", bucket).put("region", settings.get(REGION, "us-east-1"));
        if (settings.get(ENDPOINT) != null) {
            repository.put("endpoint", settings.get(ENDPOINT));
        }
        repository.put("path_style_access", settings.getAsBoolean(PATH_STYLE, false));

        final S3RepositoryPlugin plugin = new S3RepositoryPlugin(settings, configPath);

        ThreadPool threadPool = null;
        Repository created = null;
        boolean success = false;
        try {
            // The plugin's asynchronous machinery is built in createComponents, out of executors it
            // contributes to the node's thread pool before that pool exists. The shell builds its own pool
            // without asking any plugin, so those executors are simply absent -- and the symptom is not a
            // missing-executor error but a null AsyncExecutorContainer, hit on the first delete.
            //
            // So the object-store client gets a pool of its own, built from the plugin's own executor
            // builders. Wasteful by a few threads, and worth it: object-store IO does not then share a pool
            // with search or indexing, and starving one cannot starve the other.
            threadPool = new ThreadPool(
                Settings.builder().put(settings).put("node.name", settings.get("node.name", "serverless") + "-object-store").build(),
                plugin.getExecutorBuilders(settings).toArray(new ExecutorBuilder<?>[0])
            );
            final Environment environment = new Environment(settings, configPath);
            plugin.createComponents(
                null,
                clusterService,
                threadPool,
                null,
                null,
                NamedXContentRegistry.EMPTY,
                environment,
                null,
                null,
                null,
                null
            );

            final Repository.Factory factory = plugin.getRepositories(
                environment,
                NamedXContentRegistry.EMPTY,
                clusterService,
                new RecoverySettings(settings, clusterService.getClusterSettings())
            ).get("s3");

            created = factory.create(new RepositoryMetadata("serverless", "s3", repository.build()));
            if (created instanceof BlobStoreRepository == false) {
                throw new IllegalStateException("the s3 repository factory returned a " + created.getClass() + ", which has no blob store");
            }
            final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) created;
            // Started here rather than left to the caller: blobStore() is lazy and throws on a repository
            // that is not started, and the failure reads as a store problem rather than a lifecycle one.
            blobStoreRepository.start();
            final ObjectStoreHandle handle = new ObjectStoreHandle(
                new MeteredBlobStore(blobStoreRepository.blobStore()),
                threadPool,
                created,
                plugin
            );
            success = true;
            return handle;
        } finally {
            if (success == false) {
                try {
                    org.opensearch.common.util.io.IOUtils.closeWhileHandlingException(created, plugin);
                } finally {
                    if (threadPool != null) {
                        ThreadPool.terminate(threadPool, 10, java.util.concurrent.TimeUnit.SECONDS);
                    }
                }
            }
        }
    }

    /**
     * The store every node runs on: the real one, with every request counted on the way through.
     *
     * <p>The object store is priced per request and request counts are service levels from day
     * one, yet the only counter lived in the test kit and production returned the raw store.
     * A deployment could measure its cost in a test and not in the field. This is that counter, in the
     * one place every store is built, so {@code /_serverless/stats} can report what the node has spent.
     *
     * <p>Reads and writes are counted by kind because they price differently on every real provider.
     * {@code impliedS3Requests} applies what each kind actually costs -- a compare-and-swap is one call
     * here and two requests on S3, a GET for the ETag then a conditional PUT -- and is an estimate derived
     * from reading the S3 container, not from watching the wire. A listing by prefix and a
     * {@code children()} call are both listings, which price above a GET.
     *
     * <p>Bytes are counted at the stream: a ranged read of a segment counts what was actually pulled,
     * which is the number that says whether reads are lazy.
     */
    /** Where requests made on this thread are also counted, if anywhere; see {@link #attributedTo}. */
    static final ThreadLocal<java.util.concurrent.atomic.AtomicLong> ATTRIBUTION = new ThreadLocal<>();

    /**
     * Runs work and counts every metered store request it makes on this thread into a sink as well, so one activity's
     * share of a node's requests can be told apart from the rest.
     *
     * @param sink where the requests are counted
     * @param work the work
     * @param <T> what it returns
     * @return what it returned
     * @throws Exception whatever it threw
     */
    /**
     * Returns where this thread's requests are being attributed, so work handed to another thread can carry it.
     *
     * @return the sink, or null if none
     */
    public static java.util.concurrent.atomic.AtomicLong currentAttribution() {
        return ATTRIBUTION.get();
    }

    public static <T> T attributedTo(java.util.concurrent.atomic.AtomicLong sink, java.util.concurrent.Callable<T> work) throws Exception {
        if (sink == null) {
            return work.call();
        }
        final java.util.concurrent.atomic.AtomicLong previous = ATTRIBUTION.get();
        ATTRIBUTION.set(sink);
        try {
            return work.call();
        } finally {
            if (previous == null) {
                ATTRIBUTION.remove();
            } else {
                ATTRIBUTION.set(previous);
            }
        }
    }

}
