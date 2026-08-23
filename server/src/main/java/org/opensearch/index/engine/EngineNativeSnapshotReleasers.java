/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.ShardRecoveryStrategy;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level registry mapping the opaque {@code engineId} tag carried on an {@link
 * org.opensearch.index.snapshots.blobstore.EngineNativeShardSnapshot} back to the {@link
 * ShardRecoveryStrategy.EngineNativeSnapshots} whose {@link
 * ShardRecoveryStrategy.EngineNativeSnapshots#release} should be called when a snapshot referencing
 * that tag is deleted.
 *
 * <p>Deliberately a plain, node-wide static registry rather than a constructor-injected
 * dependency of {@code BlobStoreRepository}: a snapshot delete has to resolve a releaser purely
 * from the opaque {@code engineId} string a shard-level blob carries, with no guarantee the
 * originating index (or even its {@link org.opensearch.plugins.EnginePlugin}) still exists in the
 * cluster -- so there is no per-repository-instance state this registry could sensibly hang off
 * of. Every concrete {@code BlobStoreRepository} subclass (the {@code fs}/{@code s3}/{@code
 * azure}/{@code gcs}/{@code hdfs} repository plugins) already needs zero code changes to gain
 * engine-native snapshot support -- threading this through their constructors instead would have
 * undone that.
 *
 * <p>An {@link org.opensearch.plugins.EnginePlugin} that overrides {@link
 * Engine#attemptEngineNativeSnapshot} to tag its pointer bytes with a given {@code engineId} is
 * responsible for calling {@link #register} with that same tag during its own plugin
 * initialization (e.g. from {@code Plugin#createComponents}), so a later delete can find it. A
 * missing registration is never fatal to a delete -- see {@link
 * ShardRecoveryStrategy.EngineNativeSnapshots#release}'s own javadoc on why this is deliberately
 * best-effort.
 *
 * <p>What this registry holds is deliberately <em>not</em> an {@link EngineFactory}: a release runs
 * with no live shard and no engine to build, so keying it to a factory type forced the one
 * implementation in the tree to be an {@link EngineFactory} whose {@code newReadWriteEngine} threw.
 * {@link ShardRecoveryStrategy.EngineNativeSnapshots} is exactly the pair of operations a releaser
 * can actually perform, so no such stub is needed.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class EngineNativeSnapshotReleasers {

    private static final Map<String, ShardRecoveryStrategy.EngineNativeSnapshots> REGISTRY = new ConcurrentHashMap<>();

    private EngineNativeSnapshotReleasers() {}

    /**
     * Registers {@code engineNativeSnapshots} as the releaser for any engine-native snapshot tagged
     * with {@code engineId}. Re-registering the same {@code engineId} replaces the previous entry
     * (e.g. across a node restart within the same JVM in tests, or a plugin reload).
     */
    public static void register(String engineId, ShardRecoveryStrategy.EngineNativeSnapshots engineNativeSnapshots) {
        REGISTRY.put(engineId, engineNativeSnapshots);
    }

    /** Removes any releaser registered under {@code engineId}, if present. */
    public static void unregister(String engineId) {
        REGISTRY.remove(engineId);
    }

    /** Looks up the releaser registered for {@code engineId}, if any. */
    public static Optional<ShardRecoveryStrategy.EngineNativeSnapshots> find(String engineId) {
        return Optional.ofNullable(REGISTRY.get(engineId));
    }

    /**
     * Whether this node has any releaser registered at all. {@code BlobStoreRepository}'s
     * delete-time release scan checks this before touching the shard container: an empty registry
     * means {@link #find} would return empty for every {@code engineId} regardless, exactly the
     * same outcome the existing best-effort contract already accepts -- so a repository that's
     * never loaded a plugin producing engine-native snapshots (almost every real deployment) can
     * skip the per-removed-snapshot blob read entirely rather than confirming an empty result the
     * expensive way, once per snapshot, on every delete.
     */
    public static boolean isEmpty() {
        return REGISTRY.isEmpty();
    }
}
