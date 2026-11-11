/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;

/**
 * Works out which indices a search names: exact names, prefix patterns and aliases, with the digest rollups
 * pruning the matches a search cannot hit. Split out of {@link SearchHandler}, which keeps the public entry points.
 */
final class SearchIndexResolver {

    private SearchIndexResolver() {}

    /**
     * Turns the names in a request into the indices to fan out over.
     *
     * <p>Extracted from the request path so {@code _msearch} resolves a line the way {@code _search}
     * resolves a request -- aliases, prefix patterns, {@code ignore_unavailable} and the refusals all
     * included. Before this, a multi-search silently dropped a named index that did not exist, which is the
     * confident narrower answer everything else on this surface refuses.
     *
     * <p>A comma-separated list is resolved by looking each name up, which costs one register read per
     * name and no listing. A prefix pattern costs one bounded listing on top of that -- a single
     * ListObjectsV2 with a maximum key count, whose cost is set by the cap rather than by the
     * population, so a deployment with a hundred million indices pays what one with ten pays.
     *
     * <p>That is why the rule against enumerating on a request path does not refuse this. The rule was never "listings are
     * forbidden"; it was that a request must not cost the size of the deployment and must not answer
     * from a subset while looking complete. A capped listing costs a fixed amount, and a pattern that
     * matches more than the cap is refused rather than truncated.
     *
     * <p>A pattern that is not a prefix stays refused, and the reason is the same one: {@code *-2026}
     * cannot be answered by a listing at all, only by reading every name in the deployment and matching
     * each.
     *
     * @param metadata the metadata plane
     * @param serving the node, for its pattern cap
     * @param index the names as the caller wrote them, comma-separated
     * @param ignoreUnavailable whether a name that is not there is an expectation rather than a mistake
     * @return the indices and their shard counts, or the refusal
     * @throws IOException if a register cannot be read
     */
    public static SearchHandler.Resolution resolveIndices(
        MetadataPlane metadata,
        ServerlessNode serving,
        String index,
        boolean ignoreUnavailable
    ) throws IOException {
        return resolveIndices(metadata, serving, index, ignoreUnavailable, true);
    }

    /**
     * Resolves names for a caller that may or may not be on the request path.
     *
     * <p>A plugin's index is unreachable through REST and is exactly what the plugin's own client exists
     * to reach, so the guard is the one thing that differs between the two callers of this method; the
     * resolution of lists, aliases and patterns is deliberately the same code for both.
     *
     * @param metadata the metadata plane
     * @param serving the node, for its pattern cap
     * @param index the names as the caller wrote them, comma-separated
     * @param ignoreUnavailable whether a name that is not there is an expectation rather than a mistake
     * @param throughRequestPath whether a plugin's index is out of reach for this caller
     * @return the indices and their shard counts, or the refusal
     * @throws IOException if a register cannot be read
     */
    public static SearchHandler.Resolution resolveIndices(
        MetadataPlane metadata,
        ServerlessNode serving,
        String index,
        boolean ignoreUnavailable,
        boolean throughRequestPath
    ) throws IOException {
        return resolveIndices(metadata, serving, index, ignoreUnavailable, throughRequestPath, null, 0L);
    }

    /**
     * Resolves names, dropping the pattern matches the digest rollups prove the search cannot match.
     *
     * <p>A wide pattern used to cost a descriptor read per matching index before a shard was asked anything,
     * and then a head and a manifest read per shard to prune it. With the rollups, a pattern whose prefix spans
     * a whole group reads that group's {@value org.opensearch.serverless.metadata.DigestRollups#BUCKETS}
     * registers and drops every index they rule out -- no descriptor, head or manifest read for it at all --
     * counting its shards as skipped. At least one match is always kept, so a search that rules out everything
     * still has a shard to give its aggregations their shape.
     *
     * @param metadata the metadata plane
     * @param serving the node
     * @param index the names as the caller wrote them, comma-separated
     * @param ignoreUnavailable whether a name that is not there is an expectation rather than a mistake
     * @param throughRequestPath whether a plugin's index is out of reach for this caller
     * @param source the search, whose query the rollups judge; null to rule nothing out
     * @param nowMillis the request's single now, which the fan-out must use too
     * @return the indices and their shard counts, or the refusal
     * @throws IOException if a register cannot be read
     */
    public static SearchHandler.Resolution resolveIndices(
        MetadataPlane metadata,
        ServerlessNode serving,
        String index,
        boolean ignoreUnavailable,
        boolean throughRequestPath,
        SearchSourceBuilder source,
        long nowMillis
    ) throws IOException {
        final java.util.List<String> requested = java.util.List.of(index.split(",", -1));
        for (String name : requested) {
            if (name.isEmpty()) {
                return SearchHandler.Resolution.refuse(RestStatus.BAD_REQUEST, "unsupported_search", "an empty index name was given");
            }
            if (name.indexOf('?') >= 0 || isPrefixPattern(name) == false && name.indexOf('*') >= 0) {
                return SearchHandler.Resolution.refuse(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_search",
                    "only a trailing-wildcard pattern is supported, and ["
                        + name
                        + "] is not one: matching it would mean reading every index name in the "
                        + "deployment, which this system does not offer on a request path. Use a "
                        + "prefix like [logs-*], or name the indices you want."
                );
            }
        }

        // What each pattern matched, so a name that came from a pattern can be told from one the caller
        // typed. The distinction decides what a missing index means: a named index that is not there is a
        // mistake, and a pattern is a filter over what exists rather than an assertion that anything does.
        // Ordered and de-duplicated in one structure: a list's contains() made a five-thousand-name pattern
        // quadratic before a single register was read.
        final java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();
        final java.util.Set<String> fromPattern = new java.util.HashSet<>();
        int prunedShards = 0;
        final java.util.Set<String> judged = new java.util.HashSet<>();
        final java.util.Map<String, org.opensearch.serverless.metadata.DigestRollups.Entry> rolledUp = new java.util.HashMap<>();
        for (String name : requested) {
            if (isPrefixPattern(name) == false) {
                names.add(name);
                continue;
            }
            final String prefix = name.substring(0, name.length() - 1);
            final java.util.List<String> matched;
            java.util.Map<String, org.opensearch.serverless.metadata.DigestRollups.Entry> groupEntries = null;
            if (serving.searchSettings().resolveFromRollups() && org.opensearch.serverless.metadata.DigestRollups.coversGroup(prefix)) {
                // The group's entries are its name list: every index that can hold a document has one, so the
                // names the listing would add are indices with nothing to find. See DigestRollups.
                try {
                    groupEntries = readRollups(metadata, serving, prefix);
                } catch (Exception e) {
                    // Not answered from a partial list: a name missing from it would be a wrong answer.
                    return SearchHandler.Resolution.refuse(
                        RestStatus.SERVICE_UNAVAILABLE,
                        "rollups_unavailable",
                        "could not read the index names of [" + prefix + "*]: " + e.getMessage()
                    );
                }
                final java.util.List<String> fromRollups = new java.util.ArrayList<>();
                for (String entryName : groupEntries.keySet()) {
                    if (entryName.startsWith(prefix)) {
                        fromRollups.add(entryName);
                    }
                }
                java.util.Collections.sort(fromRollups);
                matched = fromRollups;
            } else {
                try {
                    matched = metadata.namesWithPrefix(prefix, serving.searchSettings().patternCap());
                } catch (org.opensearch.serverless.metadata.DescriptorStore.TooManyMatchesException e) {
                    return SearchHandler.Resolution.refuse(RestStatus.BAD_REQUEST, "too_many_indices", e.getMessage());
                }
            }
            final java.util.Set<String> ruledOut = ruledOutByRollups(
                metadata,
                serving,
                prefix,
                matched,
                source,
                nowMillis,
                rolledUp,
                judged,
                groupEntries
            );
            if (groupEntries != null && matched.size() - ruledOut.size() > serving.searchSettings().patternCap()) {
                // The cap bounds the work a search fans out to, so it counts what is left to search, not names a
                // digest has already ruled out.
                return SearchHandler.Resolution.refuse(
                    RestStatus.BAD_REQUEST,
                    "too_many_indices",
                    "more than "
                        + serving.searchSettings().patternCap()
                        + " indices begin with ["
                        + prefix
                        + "] and could match this query ("
                        + (matched.size() - ruledOut.size())
                        + "). Narrowing the pattern or naming the indices is the answer; returning the first "
                        + serving.searchSettings().patternCap()
                        + " would be a partial result that looks complete."
                );
            }
            for (String match : matched) {
                fromPattern.add(match);
                if (ruledOut.contains(match)) {
                    prunedShards += rolledUp.get(match).shards();
                    continue;
                }
                names.add(match);
            }
        }
        // Whether a name that is not there is a mistake or an expectation.
        //
        // Refusing is the default and stays the default: a search over three indices where one does not
        // exist, answering 200 and looking complete, is the confident empty answer this surface exists to
        // avoid, and a typo is far more likely than an absence somebody planned for.
        //
        // But a caller searching yesterday's and today's index, on a day that has only just started, is
        // saying something different -- they know one of these may not exist yet and they mean it. That is
        // exactly what ignore_unavailable means everywhere else, and refusing to offer it forces them to
        // either guess or make two requests. It has to be asked for explicitly, which is what makes the
        // distinction real rather than a default nobody chose.
        for (String name : requested) {
            if (isPrefixPattern(name) && fromPattern.stream().noneMatch(match -> match.startsWith(name.substring(0, name.length() - 1)))) {
                if (ignoreUnavailable) {
                    continue;
                }
                // A pattern that matches nothing is a search over no indices, and answering it with zero
                // hits and a complete flag is the confident empty answer this surface exists to avoid. A
                // caller who means "whatever is there, possibly nothing" says so with ignore_unavailable,
                // the same way they do for a named index that may not exist yet.
                return SearchHandler.Resolution.refuse(RestStatus.NOT_FOUND, "index_not_found", "no index matches [" + name + "]");
            }
        }
        // Every name's resolution at once, through the fan-out, before the loop below reads them in order: one
        // register read per name was a serial chain as long as the pattern was wide -- five thousand round trips
        // before the first shard was asked. Each read lands in the routing cache the loop then reads from.
        final java.util.Map<String, org.opensearch.serverless.metadata.DescriptorStore.Resolution> prefetched = prefetch(
            serving,
            metadata,
            names
        );
        final java.util.LinkedHashMap<String, IndexDescriptor> indices = new java.util.LinkedHashMap<>();
        final java.util.List<String> skipped = new java.util.ArrayList<>();
        // Pattern matches that belong to a plugin, left out of the answer and out of every message.
        final java.util.List<String> hidden = new java.util.ArrayList<>();
        for (String name : names) {
            // One read tells us whether the name is an index or an alias, because both live in the same
            // register. An alias is expanded here rather than deeper down, so everything below this line
            // deals only in indices and a shard count -- searching through an alias and searching the
            // indices it stands for are the same code path, which is the only way they cannot drift.
            // Through the routing cache: this was a register read per named index on every search. A stale
            // resolution routes to a shard that refuses to answer for a deleted incarnation; see
            // MetadataPlane#resolveForRouting.
            final org.opensearch.serverless.metadata.DescriptorStore.Resolution early = prefetched.get(name);
            final var resolved = early != null ? early : metadata.resolveForRouting(name);
            if (resolved.absent()) {
                if (fromPattern.contains(name)) {
                    // A name the listing returned and the register does not describe: a tombstone left by
                    // a deletion, or an index deleted between the listing and the read. A pattern filters
                    // what exists, so this is not a match rather than a missing index -- and reporting it
                    // as skipped would tell the caller something is absent from an answer they never asked
                    // to include.
                    continue;
                }
                if (ignoreUnavailable) {
                    skipped.add(name);
                    continue;
                }
                return SearchHandler.Resolution.refuse(RestStatus.NOT_FOUND, "index_not_found", "no such index: " + name);
            }
            if (resolved.index() != null) {
                if (throughRequestPath && serving.isSystemIndex(name)) {
                    if (fromPattern.contains(name)) {
                        // A pattern is a filter over what the caller may see, and a plugin's index is not
                        // in that set -- the listing endpoints already leave it out. Refusing the whole
                        // search here made GET /*/_search fail for everyone the moment a plugin created
                        // its index, and spelled the name in the refusal.
                        hidden.add(name);
                        continue;
                    }
                    // Checked here, on the resolved name, and not only on the path parameter: a comma list
                    // or a body index used to walk straight past the guard. A name the caller typed is
                    // refused as before; the name was theirs to begin with.
                    return SearchHandler.Resolution.refuse(
                        RestStatus.FORBIDDEN,
                        "system_index",
                        "[" + name + "] belongs to a plugin and is not reachable through the request path"
                    );
                }
                indices.put(name, resolved.index());
                continue;
            }
            for (String target : resolved.alias().indices()) {
                final Optional<IndexDescriptor> behind = metadata.describeForRouting(target);
                if (behind.isEmpty()) {
                    // An alias outliving one of its indices is ordinary: somebody deleted an index and did
                    // not update the alias. Skipping it silently would make a search over an alias quietly
                    // narrower than the caller believes, so it is refused unless they said to ignore what
                    // is missing -- the same rule a named index gets, since an alias is a way of naming
                    // indices and not a way of relaxing what naming one means.
                    if (ignoreUnavailable) {
                        skipped.add(target);
                        continue;
                    }
                    return SearchHandler.Resolution.refuse(
                        RestStatus.NOT_FOUND,
                        "index_not_found",
                        "alias [" + name + "] names [" + target + "], which does not exist"
                    );
                }
                if (throughRequestPath && serving.isSystemIndex(target)) {
                    return SearchHandler.Resolution.refuse(
                        RestStatus.FORBIDDEN,
                        "system_index",
                        "[" + target + "] belongs to a plugin and is not reachable through the request path"
                    );
                }
                indices.put(target, behind.get());
            }
        }
        if (indices.isEmpty()) {
            final java.util.List<String> visible = new java.util.ArrayList<>(names);
            visible.removeAll(hidden);
            if (visible.isEmpty()) {
                // Everything the patterns matched was a plugin's. To this caller that is a pattern that
                // matched nothing, and it is answered as one -- without the names, which are not theirs
                // to learn from an error message.
                return SearchHandler.Resolution.refuse(RestStatus.NOT_FOUND, "index_not_found", "no index matches [" + index + "]");
            }
            // Every name was absent. Answering 200 with no hits here would be the very thing the flag is
            // not for: the caller allowed for *some* of their indices to be missing, not all of them, and
            // an empty answer over nothing at all is indistinguishable from an empty answer over
            // everything.
            return SearchHandler.Resolution.refuse(
                RestStatus.NOT_FOUND,
                "index_not_found",
                "none of the indices named exist: " + String.join(", ", visible)
            );
        }
        return new SearchHandler.Resolution(indices, skipped, null, null, null, prunedShards, judged);
    }

    /**
     * Reports whether a name is a pattern this system can answer.
     *
     * <p>One trailing star and nothing else, because that is exactly what a prefix listing can do. A star
     * anywhere else needs the whole population read and matched, which is the operation a request path refuses; a
     * bare {@code *} is a prefix of nothing at all and is allowed, since the cap bounds it like any other.
     *
     * @param name the name as the caller wrote it
     * @return true if it is a trailing-wildcard pattern
     */
    static boolean isPrefixPattern(String name) {
        return name.endsWith("*") && name.indexOf('*') == name.length() - 1;
    }

    /**
     * The matches of one prefix pattern the digest rollups prove this search cannot match, all but one of them
     * at most. Empty when the rollups do not apply: pruning off, too few matches to be worth the group's reads,
     * a prefix shorter than a group, a query nothing could judge, a suggester or a global aggregation.
     */
    static java.util.Set<String> ruledOutByRollups(
        MetadataPlane metadata,
        ServerlessNode serving,
        String prefix,
        java.util.List<String> matched,
        SearchSourceBuilder source,
        long nowMillis,
        java.util.Map<String, org.opensearch.serverless.metadata.DigestRollups.Entry> rolledUp,
        java.util.Set<String> judged,
        java.util.Map<String, org.opensearch.serverless.metadata.DigestRollups.Entry> alreadyRead
    ) {
        if (source == null
            || serving.searchSettings().searchPruning() == false
            || matched.size() < serving.searchSettings().rollupMinIndices()
            || org.opensearch.serverless.metadata.DigestRollups.coversGroup(prefix) == false
            || org.opensearch.serverless.store.PruningDigest.mayPrune(source.query()) == false
            || source.suggest() != null
            || SearchFanout.hasGlobalAggregation(source)) {
            return java.util.Set.of();
        }
        final java.util.Map<String, org.opensearch.serverless.metadata.DigestRollups.Entry> entries;
        try {
            entries = alreadyRead != null ? alreadyRead : readRollups(metadata, serving, prefix);
        } catch (Exception e) {
            // The rollups are an optimisation over the per-index path, which is always correct.
            org.apache.logging.log4j.LogManager.getLogger(SearchHandler.class)
                .warn("could not read the digest rollups for [" + prefix + "*]; searching without them", e);
            return java.util.Set.of();
        }
        final java.util.Set<String> ruledOut = new java.util.LinkedHashSet<>();
        String kept = null;
        for (String name : matched) {
            final org.opensearch.serverless.metadata.DigestRollups.Entry entry = entries.get(name);
            if (entry != null && entry.rulesOut(source.query(), nowMillis)) {
                ruledOut.add(name);
                rolledUp.put(name, entry);
            } else {
                if (entry != null && entry.judgedMatchable(source.query(), nowMillis)) {
                    judged.add(name);
                }
                if (kept == null) {
                    kept = name;
                }
            }
        }
        if (kept == null && ruledOut.isEmpty() == false) {
            // Everything ruled out: one is searched anyway, so the answer has its aggregations' shape.
            final String first = ruledOut.iterator().next();
            ruledOut.remove(first);
            rolledUp.remove(first);
        }
        return ruledOut;
    }

    /** Reads a prefix's rollup group on GENERIC, reusing this node's copy within the routing cache's window. */
    static java.util.Map<String, org.opensearch.serverless.metadata.DigestRollups.Entry> readRollups(
        MetadataPlane metadata,
        ServerlessNode serving,
        String prefix
    ) throws java.io.IOException {
        final java.util.concurrent.Executor pool = serving.threadPool().executor(ThreadPool.Names.GENERIC);
        return metadata.rollups()
            .readGroup(
                org.opensearch.serverless.metadata.DigestRollups.group(prefix),
                new org.opensearch.serverless.metadata.DescriptorStore.Reads() {
                    @Override
                    public <T> java.util.List<T> runAll(java.util.List<java.util.concurrent.Callable<T>> tasks)
                        throws InterruptedException {
                        return Fanout.run(pool, serving.searchSettings().searchFanoutConcurrency(), tasks);
                    }
                },
                serving.searchSettings().rollupCacheMillis()
            );
    }

    /** Resolves many names concurrently; a name whose read failed is left for the caller to read and fail on. */
    static java.util.Map<String, org.opensearch.serverless.metadata.DescriptorStore.Resolution> prefetch(
        ServerlessNode serving,
        MetadataPlane metadata,
        java.util.Collection<String> names
    ) {
        final java.util.Map<String, org.opensearch.serverless.metadata.DescriptorStore.Resolution> resolved =
            new java.util.concurrent.ConcurrentHashMap<>();
        if (names.size() < 2) {
            return resolved;
        }
        final java.util.List<java.util.concurrent.Callable<Void>> reads = new java.util.ArrayList<>();
        for (String name : names) {
            reads.add(() -> {
                resolved.put(name, metadata.resolveForRouting(name));
                return null;
            });
        }
        try {
            Fanout.run(serving.threadPool().executor(ThreadPool.Names.GENERIC), serving.searchSettings().searchFanoutConcurrency(), reads);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return resolved;
    }
}
