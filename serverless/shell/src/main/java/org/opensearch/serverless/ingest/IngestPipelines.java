/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.ingest;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Pipeline;
import org.opensearch.ingest.Processor;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Compiling and running an ingest pipeline over a document, without a cluster.
 *
 * <p><b>Why this can exist now and could not in M56.</b> The refusal then said the problem was not storing a
 * pipeline — templates store the same way — but running one: every processor a real pipeline uses lives in the
 * {@code ingest-common} module, and this shell loads no modules. M57 answered that by choosing
 * {@code lang-painless} by name, the way the transport had always been chosen, and establishing that "no
 * modules are loaded" was about discovery from disk rather than about the code in {@code modules/}. So this is
 * the same decision applied a third time, not a new one.
 *
 * <p><b>What replaces {@code IngestService}.</b> Core's ingest service holds pipelines in cluster state and
 * hands them out by id. There is no cluster state here, so pipelines live in a register like every other piece
 * of this deployment's configuration and are compiled from their stored JSON when a write names one.
 * {@code Pipeline#create} is core's own compiler and the processor factories are core's own; nothing here
 * interprets a processor.
 *
 * <p><b>A pipeline is compiled when it is stored, not only when it is used.</b> A pipeline that names a
 * processor that does not exist, or configures one wrongly, is refused at {@code PUT} — where the operator is
 * standing — rather than at the first write that references it, which could be days later and in somebody
 * else's request.
 */
public final class IngestPipelines {

    private final Map<String, Processor.Factory> factories;
    private final org.opensearch.script.ScriptService scriptService;

    /**
     * Creates the compiler over a set of processor factories.
     *
     * @param factories the processors available, by type
     * @param scriptService the engine a scripted processor compiles through
     */
    public IngestPipelines(Map<String, Processor.Factory> factories, org.opensearch.script.ScriptService scriptService) {
        this.factories = Map.copyOf(factories);
        this.scriptService = scriptService;
    }

    /**
     * The processor types this deployment can run.
     *
     * @return the type names, for a refusal that says what is available
     */
    public java.util.Set<String> available() {
        return new java.util.TreeSet<>(factories.keySet());
    }

    /**
     * Compiles a pipeline from its stored JSON.
     *
     * @param id the pipeline id
     * @param source the stored definition
     * @return the compiled pipeline
     * @throws Exception if the definition names an unknown processor or configures one wrongly
     */
    public Pipeline compile(String id, String source) throws Exception {
        final Map<String, Object> config = new HashMap<>(
            XContentHelper.convertToMap(new BytesArray(source.getBytes(StandardCharsets.UTF_8)), false, XContentType.JSON).v2()
        );
        return Pipeline.create(id, config, factories, scriptService);
    }

    /**
     * The pipelines one write must run, in order.
     *
     * <p>Core's precedence, kept deliberately identical because a client that has configured
     * {@code index.default_pipeline} has configured it against core's rules:
     *
     * <ul>
     *   <li>A pipeline named on the request wins over the index's {@code index.default_pipeline}. It does
     *       <em>not</em> displace {@code index.final_pipeline}, which is the whole point of a final
     *       pipeline: it is the one a caller cannot opt out of.</li>
     *   <li>{@code _none} means no pipeline. On the request it also suppresses the index's default, which
     *       is how a caller opts out of one; as a setting's value it simply means none is configured.</li>
     *   <li>The final pipeline runs last, after whichever of the two ran first.</li>
     * </ul>
     *
     * <p>Returned as a list rather than resolved one at a time, so the caller runs them in order and
     * cannot accidentally run a final pipeline before a default one — which is the mistake this shape
     * exists to make impossible.
     *
     * @param requestPipeline the pipeline named on the request or the action line, or null
     * @param indexSettings the settings of the index actually being written to
     * @return the pipeline ids to run, in order, never containing {@code _none}
     */
    public static java.util.List<String> pipelinesFor(String requestPipeline, org.opensearch.common.settings.Settings indexSettings) {
        final java.util.List<String> pipelines = new java.util.ArrayList<>(2);
        final String first;
        if (requestPipeline != null) {
            first = requestPipeline;
        } else if (indexSettings != null && org.opensearch.index.IndexSettings.DEFAULT_PIPELINE.exists(indexSettings)) {
            first = org.opensearch.index.IndexSettings.DEFAULT_PIPELINE.get(indexSettings);
        } else {
            first = NONE;
        }
        if (NONE.equals(first) == false) {
            pipelines.add(first);
        }
        if (indexSettings != null && org.opensearch.index.IndexSettings.FINAL_PIPELINE.exists(indexSettings)) {
            final String last = org.opensearch.index.IndexSettings.FINAL_PIPELINE.get(indexSettings);
            if (NONE.equals(last) == false) {
                pipelines.add(last);
            }
        }
        return pipelines;
    }

    /** Core's name for "no pipeline", which both settings and the request parameter accept. */
    private static final String NONE = org.opensearch.ingest.IngestService.NOOP_PIPELINE_NAME;

    /**
     * What running a pipeline did to a document.
     *
     * @param source the document as the pipeline left it, unchanged if no processor touched it
     * @param dropped whether a processor dropped the document, in which case it is not written
     */
    public record Result(String source, boolean dropped) {
    }

    /**
     * Runs a pipeline over a document's source.
     *
     * <p>The document is core's {@code IngestDocument}, carrying the same metadata a classic node gives a
     * processor — {@code _index}, {@code _id} — so a processor that reads them behaves the same here.
     *
     * <p>A pipeline may <em>drop</em> a document, which is the point of the drop processor: the write is not
     * an error and is not performed. That is reported rather than silently turned into an empty document.
     *
     * @param pipeline the compiled pipeline
     * @param index the index being written to
     * @param id the document id
     * @param source the document, as JSON
     * @return the transformed document, or a dropped result
     * @throws Exception if a processor failed
     */
    public Result run(Pipeline pipeline, String index, String id, String source) throws Exception {
        final Map<String, Object> document = new LinkedHashMap<>(
            XContentHelper.convertToMap(new BytesArray(source.getBytes(StandardCharsets.UTF_8)), false, XContentType.JSON).v2()
        );
        final IngestDocument ingestDocument = new IngestDocument(index, id, null, null, null, document);

        // Core's execute is asynchronous by signature and synchronous for every processor that does not do
        // IO. Waiting here is what makes a pipeline part of the write rather than something that happens
        // afterwards -- a document must not be acknowledged before the pipeline that was supposed to shape
        // it has run.
        final CompletableFuture<IngestDocument> done = new CompletableFuture<>();
        pipeline.execute(ingestDocument, (result, failure) -> {
            if (failure != null) {
                done.completeExceptionally(failure);
            } else {
                done.complete(result);
            }
        });

        final IngestDocument after;
        try {
            after = done.get();
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            throw cause instanceof Exception ? (Exception) cause : new IllegalStateException(cause);
        }

        if (after == null) {
            // A processor dropped it. Not a failure, and not a write.
            return new Result(null, true);
        }
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            // Metadata back out: it went in so processors could read it, and it is not part of the document.
            // Writing it into the source would put _index and _id inside every document a pipeline touched.
            final Map<String, Object> body = new LinkedHashMap<>(after.getSourceAndMetadata());
            for (IngestDocument.Metadata metadata : IngestDocument.Metadata.values()) {
                body.remove(metadata.getFieldName());
            }
            builder.map(body);
            return new Result(builder.toString(), false);
        }
    }
}
