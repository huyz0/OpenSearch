/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.schema;

import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.IndexAnalyzersProvider;
import org.opensearch.index.mapper.Mapping;
import org.opensearch.index.mapper.SharedMappings;
import org.opensearch.indices.IndicesService;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One set of analyzers, and one parsed copy of each mapping, for all the open indices on a node whose settings
 * differ only in what names and places an index.
 *
 * <p><b>Why analyzers and mappings go together.</b> A parsed mapping's field mappers hold the analyzers they were
 * built with, and an index closes its analyzers when it closes. Two indices can only share a parsed mapping if they
 * share the analyzers in it, and the analyzers may only be closed when the last index using them has closed. So the
 * unit shared is a {@link Group}: the analyzers built for one settings fingerprint, a count of the open indices
 * using them, and the parsed mappings those indices have offered. The group's analyzers close when its count reaches
 * zero, and the group is dropped.
 *
 * <p><b>The fingerprint.</b> Every index setting except the ones that only name, count or place an index: its uuid,
 * name, creation date, shard and replica counts, refresh interval, blocks, allocation and serverless placement
 * settings. Anything else, including a setting an installed plugin's mapper might read, keeps two indices apart, so
 * a setting this list does not know about can only cost sharing, never share wrongly.
 *
 * <p><b>Mappings.</b> Held weakly, keyed by the exact source bytes: a parsed mapping lives as long as some open index
 * uses it, and a source nobody uses any more is dropped on the next lookup. An index that updates its mapping merges
 * copy-on-write into a mapping of its own; the shared one is never changed. If the merged result is a mapping another
 * index already holds, as when many indices grow the same dynamic fields, the index adopts that one.
 *
 * <p><b>Reloading search analyzers.</b> Indices in a group share their analyzers, so reloading the updateable search
 * analyzers of one reloads them for every index in the group. They have equal analysis settings, so they read the
 * same files and would reload to the same result.
 */
public final class SharedAnalysis implements IndexAnalyzersProvider {

    /** Node setting that turns sharing on. */
    public static final String ENABLED = "serverless.schema.sharing.enabled";
    /**
     * On: at 3,000 open indices with one 150-field mapping, heap per open index went from 182 to 92 KB, opening them
     * took no longer (2-12% less over five runs), first-write p50 and p99 were within 1%, and no store request changes.
     */
    static final boolean ENABLED_DEFAULT = true;

    /** Settings that only name, count or place an index, so differ between indices that could share. */
    private static final Set<String> IDENTITY_SETTINGS = Set.of(
        "index.uuid",
        "index.provided_name",
        "index.creation_date",
        "index.history.uuid",
        "index.number_of_shards",
        "index.number_of_replicas",
        "index.auto_expand_replicas",
        "index.refresh_interval",
        "index.priority",
        "index.serverless.storage_uuid",
        "index.serverless.bucket",
        "index.serverless.bucket_group",
        "index.serverless.segment_placement",
        "index.serverless.tail_overlay"
    );

    private static final String[] IDENTITY_PREFIXES = { "index.blocks.", "index.routing.allocation." };

    private final Map<String, Group> groups = new HashMap<>();

    /**
     * Has every index this node opens from now on share analyzers and parsed mappings, if the node's settings say so.
     *
     * @param nodeSettings the node's settings
     * @param indicesService the node's indices service
     * @return the sharing installed, or null if it is off
     */
    public static SharedAnalysis install(Settings nodeSettings, IndicesService indicesService) {
        if (nodeSettings.getAsBoolean(ENABLED, ENABLED_DEFAULT) == false) {
            return null;
        }
        final SharedAnalysis shared = new SharedAnalysis();
        indicesService.setIndexAnalyzersProvider(shared);
        return shared;
    }

    @Override
    public IndexAnalyzers build(IndexSettings indexSettings, AnalysisRegistry registry) throws IOException {
        final String fingerprint = fingerprint(indexSettings.getSettings());
        Group group;
        synchronized (this) {
            group = groups.get(fingerprint);
            if (group != null) {
                group.users++;
            }
        }
        if (group == null) {
            // Built outside the lock: building analyzers can read files (synonyms, stopwords). Two openers racing on a
            // new fingerprint each build; the loser closes its own.
            final IndexAnalyzers built = registry.build(indexSettings);
            synchronized (this) {
                group = groups.get(fingerprint);
                if (group == null) {
                    group = new Group(fingerprint, built);
                    groups.put(fingerprint, group);
                }
                group.users++;
            }
            if (group.analyzers != built) {
                built.close();
            }
        }
        final Group held = group;
        final AtomicBoolean released = new AtomicBoolean();
        final Closeable release = () -> {
            if (released.compareAndSet(false, true)) {
                release(held);
            }
        };
        return held.analyzers.sharedView(release, held);
    }

    private void release(Group group) throws IOException {
        synchronized (this) {
            if (--group.users > 0) {
                return;
            }
            groups.remove(group.fingerprint);
        }
        group.analyzers.close();
    }

    /**
     * Returns how many distinct analyzer groups are open.
     *
     * @return the number of groups
     */
    public synchronized int groups() {
        return groups.size();
    }

    /**
     * Returns how many distinct parsed mappings are held across all groups.
     *
     * @return the number of mappings
     */
    public synchronized int mappings() {
        int count = 0;
        for (Group group : groups.values()) {
            count += group.size();
        }
        return count;
    }

    static String fingerprint(Settings settings) {
        final StringBuilder b = new StringBuilder();
        for (String key : settings.keySet()) {
            if (IDENTITY_SETTINGS.contains(key) || isIdentityPrefix(key)) {
                continue;
            }
            b.append(key).append('=');
            final var list = settings.getAsList(key, null);
            b.append(settings.hasValue(key) ? settings.get(key) : String.valueOf(list)).append('\n');
        }
        return b.toString();
    }

    private static boolean isIdentityPrefix(String key) {
        for (String prefix : IDENTITY_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /** The analyzers for one fingerprint, the indices using them, and the mappings they parsed. */
    private static final class Group implements SharedMappings {
        private final String fingerprint;
        private final IndexAnalyzers analyzers;
        private final ConcurrentHashMap<CompressedXContent, Held> mappings = new ConcurrentHashMap<>();
        private final ReferenceQueue<Mapping> cleared = new ReferenceQueue<>();
        private int users;

        Group(String fingerprint, IndexAnalyzers analyzers) {
            this.fingerprint = fingerprint;
            this.analyzers = analyzers;
        }

        @Override
        public Mapping get(CompressedXContent source) {
            purge();
            final Held held = mappings.get(source);
            return held == null ? null : held.get();
        }

        @Override
        public void put(CompressedXContent source, Mapping mapping) {
            purge();
            mappings.compute(
                source,
                (key, existing) -> existing != null && existing.get() != null ? existing : new Held(key, mapping, cleared)
            );
        }

        int size() {
            purge();
            return mappings.size();
        }

        private void purge() {
            for (var ref = cleared.poll(); ref != null; ref = cleared.poll()) {
                final Held held = (Held) ref;
                mappings.remove(held.source, held);
            }
        }
    }

    /** A parsed mapping, held only as long as an open index uses it. */
    private static final class Held extends WeakReference<Mapping> {
        private final CompressedXContent source;

        Held(CompressedXContent source, Mapping mapping, ReferenceQueue<Mapping> queue) {
            super(mapping, queue);
            this.source = source;
        }
    }
}
