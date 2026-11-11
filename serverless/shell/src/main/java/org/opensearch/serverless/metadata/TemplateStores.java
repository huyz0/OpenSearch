/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;

/**
 * The template, script and pipeline stores, each sharing one cache per store for the life of the plane. Split out of
 * {@link MetadataPlane}, which keeps the public entry points.
 */
final class TemplateStores {

    private final BlobStore blobStore;
    private final BlobPath base;

    TemplateStores(BlobStore blobStore, BlobPath base) {
        this.blobStore = blobStore;
        this.base = base;
    }

    final java.util.Map<String, TemplateCache> storeCaches = new java.util.concurrent.ConcurrentHashMap<>();

    TemplateCache cacheFor(org.opensearch.common.blobstore.BlobPath path) {
        return storeCaches.computeIfAbsent(path.buildAsString(), key -> new TemplateCache());
    }

    /**
     * Returns the store holding index templates.
     *
     * @return the index-template store
     */
    public TemplateStore indexTemplates() {
        return new TemplateStore(blobStore.blobContainer(RegisterMap.indexTemplates(base)), cacheFor(RegisterMap.indexTemplates(base)));
    }

    /**
     * Returns the store holding component templates.
     *
     * @return the component-template store
     */
    /**
     * Returns the store holding ingest pipelines.
     *
     * <p>A {@link TemplateStore} because the shape is identical: named JSON documents, one register each,
     * bounded on creation, and a prefix so a foreign object in the container is not read as one. Reusing it
     * rather than copying it means its handling of foreign files such as the ones ExtrasFS plants in tests applies here too.
     *
     * @return the pipeline store
     */
    /**
     * Returns the stored-script store.
     *
     * @return the store
     */
    public StoredScriptStore scripts() {
        return new StoredScriptStore(blobStore.blobContainer(RegisterMap.scripts(base)), cacheFor(RegisterMap.scripts(base)));
    }

    /**
     * Returns the search-pipeline store.
     *
     * @return the store
     */
    public TemplateStore searchPipelines() {
        return new TemplateStore(blobStore.blobContainer(RegisterMap.searchPipelines(base)), cacheFor(RegisterMap.searchPipelines(base)));
    }

    /**
     * Returns the store holding ingest pipelines.
     *
     * @return the pipeline store
     */
    public TemplateStore pipelines() {
        return new TemplateStore(blobStore.blobContainer(RegisterMap.pipelines(base)), cacheFor(RegisterMap.pipelines(base)));
    }

    /**
     * Returns the store holding component templates.
     *
     * @return the component-template store
     */
    public TemplateStore componentTemplates() {
        return new TemplateStore(
            blobStore.blobContainer(RegisterMap.componentTemplates(base)),
            cacheFor(RegisterMap.componentTemplates(base))
        );
    }
}
