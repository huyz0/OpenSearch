/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;

import java.util.function.Supplier;

/**
 * Mappings for gated indices stored directly inside {@link IndexDescriptor} objects in Object Storage.
 *
 * <p>Eliminates system index (.opensearch-index-mappings) round-trips entirely. Dynamic field updates
 * perform compare-and-swap register writes directly on the index's {@link IndexDescriptor} in Object Storage.
 */
public final class DescriptorBackedMappingStore implements MappingGenerationStore.Store {

    private static final Logger logger = LogManager.getLogger(DescriptorBackedMappingStore.class);

    private static final Cache<String, String> UUID_TO_NAME = CacheBuilder.<String, String>builder().setMaximumWeight(50_000).build();

    private final Supplier<DescriptorBackend> backendSupplier;
    private final DescriptorCache descriptorCache;

    public DescriptorBackedMappingStore(Supplier<DescriptorBackend> backendSupplier, DescriptorCache descriptorCache) {
        this.backendSupplier = backendSupplier;
        this.descriptorCache = descriptorCache;
    }

    public static void registerUuidToName(String uuid, String name) {
        if (uuid != null && name != null) {
            UUID_TO_NAME.put(uuid, name);
        }
    }

    public static void registerDescriptor(IndexDescriptor descriptor) {
        if (descriptor != null) {
            registerUuidToName(descriptor.uuid(), descriptor.name());
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>The uuid is checked against the descriptor the name resolved to, and that check was missing.</b>
     * {@link #UUID_TO_NAME} answers name-for-uuid from a per-JVM map that nothing outside this JVM can
     * correct. Delete "logs" (uuid A) and recreate it (uuid B) from another node and this node's entry
     * {@code A -> "logs"} survives: the delete's {@code delete(A)} ran on the deleting node only, and the
     * change tailer that would eventually invalidate here runs on a five second interval and can step over
     * a bucket it cannot read. Until then, reading uuid A returned uuid <em>B</em>'s mapping generation,
     * so a shard of the old incarnation believed itself up to date against a mapping it had never seen.
     *
     * <p>A mismatch is answered as absence rather than as a failure, which is the same answer this gives
     * after a delete, and the entry is dropped so the next read does not repeat the resolution. Absence is
     * the honest answer here: for the uuid being asked about there is no mapping, because there is no
     * index.
     */
    @Override
    public MappingGenerationStore.MappingGeneration read(String indexUuid) {
        String name = UUID_TO_NAME.get(indexUuid);
        if (name == null) {
            return null;
        }
        IndexDescriptor descriptor = getDescriptor(name);
        if (descriptor == null || descriptor.exists() == false) {
            return null;
        }
        if (descriptor.uuid().equals(indexUuid) == false) {
            forgetStaleIncarnation(indexUuid, name, descriptor.uuid());
            return null;
        }
        return new MappingGenerationStore.MappingGeneration(descriptor.mappingGeneration(), descriptor.initialMapping());
    }

    /**
     * Drops a uuid-to-name entry that now points at a different index, and the cached descriptor with it.
     *
     * <p>Both, and in this order. Leaving the entry means the next read resolves the same wrong name;
     * leaving the cached descriptor means the next read of the <em>right</em> name may still come from a
     * copy admitted before whatever produced the mismatch.
     */
    private void forgetStaleIncarnation(String indexUuid, String name, String actualUuid) {
        logger.warn(
            "uuid [{}] resolves to index name [{}], whose descriptor is uuid [{}]: that index has been deleted and recreated, "
                + "so this node's uuid-to-name entry is stale and is being dropped",
            indexUuid,
            name,
            actualUuid
        );
        UUID_TO_NAME.remove(indexUuid);
        invalidate(backendSupplier.get(), name);
    }

    /**
     * {@inheritDoc}
     *
     * <h4>Why this reads the store again rather than trusting {@link #read}</h4>
     *
     * A swap is only a swap if the check and the write are one operation against the same value. This used
     * to check the generation on a descriptor that came through a cache with a sixty second freshness
     * window, and then write with {@code put}, which is unconditional and -- worse -- used to retry a lost
     * write at the winner's generation. Two shards inferring different dynamic fields from the same batch
     * both observed generation N, both were told their swap took, and one of the two fields was erased from
     * the mapping while the documents that introduced it were being indexed. A wrong answer that reads as a
     * correct one, on the path where nothing checks.
     *
     * <p>So the generation is checked against a read that goes past the cache, and the write is conditional
     * on the version that read observed. A loser returns false, which is what
     * {@code MappingGenerationStore.updateMapping}'s retry loop is for: it re-reads, merges its field onto
     * whatever the winner wrote, and swaps again.
     *
     * <p><b>Every false return invalidates the cache first.</b> That loop's re-read comes back through
     * {@link #read}, which is cached, so without this it would read the same stale generation sixteen times
     * and report sustained contention for a value it was never going to see change.
     */
    @Override
    public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
        String name = UUID_TO_NAME.get(indexUuid);
        if (name == null) {
            logger.warn("cannot update mapping for uuid [{}]: no index name mapped for uuid", indexUuid);
            return false;
        }
        DescriptorBackend backend = backendSupplier.get();
        if (backend == null) {
            logger.error("cannot update mapping for [{}]: no descriptor backend installed", name);
            return false;
        }

        try {
            DescriptorBackend.VersionedDescriptor current = backend.getForUpdate(name);
            IndexDescriptor descriptor = current == null ? null : current.descriptor();
            if (descriptor == null || descriptor.exists() == false) {
                logger.warn("cannot update mapping for index [{}]: descriptor absent or deleted", name);
                invalidate(backend, name);
                return false;
            }
            // Before the generation is even looked at, because a generation comparison across two
            // incarnations of a name is meaningless and happens to succeed.
            //
            // The interleaving this closes: node C deletes "logs" (uuid A) and recreates it as uuid B at
            // mapping generation 1. Node B still holds a writer shard of uuid A open and its per-JVM
            // uuid-to-name map still says A -> "logs". A document with an unmapped field arrives there, so
            // MappingGenerationStore.updateMapping("A", {"city":"keyword"}) reads *B's* descriptor, sees
            // generation 1, and swaps at generation 1 -- against B's descriptor, at B's store version, and
            // the swap applies. The new index's mapping then carries a field inferred from a document
            // written into the old one; with a type collision it raises MappingConflictException against a
            // document that has nothing to do with it, and without one it corrupts in silence.
            //
            // Raised rather than returned false. False means "someone got there first, re-read and merge",
            // and updateMapping would do exactly that fifteen more times before reporting sustained
            // contention -- a misleading answer during an incident for what is really a write against an
            // index that no longer exists. IndexNotFoundException says the true thing and stops the loop.
            if (descriptor.uuid().equals(indexUuid) == false) {
                forgetStaleIncarnation(indexUuid, name, descriptor.uuid());
                throw new org.opensearch.index.IndexNotFoundException(
                    "the mapping store refuses a write against uuid ["
                        + indexUuid
                        + "], whose name now resolves to a different incarnation (uuid ["
                        + descriptor.uuid()
                        + "]); the index this write belongs to has been deleted",
                    name
                );
            }
            registerDescriptor(descriptor);
            if (descriptor.mappingGeneration() != expectedGeneration) {
                logger.debug(
                    "mapping swap for [{}] conflict: expected gen [{}], current gen [{}]",
                    name,
                    expectedGeneration,
                    descriptor.mappingGeneration()
                );
                invalidate(backend, name);
                return false;
            }

            IndexDescriptor updatedDescriptor = descriptor.withMapping(updated.generation(), updated.fields());
            if (backend.compareAndSwap(updatedDescriptor, current.storeVersion()) == false) {
                logger.debug("mapping swap for [{}] lost the race at generation [{}]; the caller must re-merge", name, expectedGeneration);
                invalidate(backend, name);
                return false;
            }
            invalidate(backend, name);
            registerDescriptor(updatedDescriptor);
            // The fifth descriptor write path, and the last one that told nobody. A dynamic field added
            // here left every other node's cached descriptor claiming the previous mapping generation for
            // the whole freshness window, which is a minute of shards inferring against a mapping they have
            // been told is current and is not.
            DescriptorGate.recordWrite(updatedDescriptor);
            return true;
        } catch (org.opensearch.index.IndexNotFoundException e) {
            // Not swallowed into "false". The catch below turns a backend failure into "re-read and merge",
            // which is right for a transient failure and exactly wrong for this one: the index this write
            // belongs to is gone, no amount of re-reading will bring it back, and reporting it as a lost
            // race sends the caller round the retry loop fifteen more times to report contention. Raised
            // above rather than here, so this only re-raises what this method itself decided.
            invalidate(backend, name);
            throw e;
        } catch (Exception e) {
            logger.warn("mapping swap for [{}] failed at the backend: {}", name, e);
            invalidate(backend, name);
            return false;
        }
    }

    /**
     * Drops the cached descriptor everywhere this class can reach one.
     *
     * <p>Both, because they are not the same cache. The backend keeps its own in front of the object store,
     * and this class may have been handed a second one at construction. A caller about to re-read must not
     * be served the value that just lost by either.
     */
    private void invalidate(DescriptorBackend backend, String name) {
        try {
            if (backend != null) {
                backend.invalidate(name);
            }
            if (descriptorCache != null) {
                descriptorCache.invalidate(name);
            }
        } catch (RuntimeException e) {
            logger.debug("could not invalidate the cached descriptor for [{}]: {}", name, e);
        }
    }

    @Override
    public void delete(String indexUuid) {
        if (indexUuid != null) {
            UUID_TO_NAME.remove(indexUuid);
        }
    }

    private IndexDescriptor getDescriptor(String name) {
        DescriptorBackend backend = backendSupplier.get();
        IndexDescriptor descriptor = null;
        if (descriptorCache != null) {
            descriptor = descriptorCache.get(name, k -> backend != null ? backend.get(k) : null);
        } else if (backend != null) {
            descriptor = backend.get(name);
        }
        if (descriptor != null) {
            registerDescriptor(descriptor);
        }
        return descriptor;
    }
}
