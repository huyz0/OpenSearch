/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.index.Index;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An {@link IndexMetadataHolder} that keeps the descriptor resident and fetches the rest the first
 * time someone asks for it.
 *
 * <p>Nothing in core creates one. It exists for a metadata store that can fetch a single index --
 * OpenSearch's remote cluster state already writes one blob per index -- and would rather keep a few
 * hundred bytes per index resident than a few kilobytes. Install one with
 * {@link Metadata.Builder#putStub}.
 *
 * <p>The descriptor must agree with what the supplier eventually returns. Index name and UUID are
 * checked on every resolve because a mismatch there means the wrong index was loaded; the remaining
 * fields are checked under assertions, since by then the damage is a lookup table that disagrees with
 * the metadata it describes and the useful place to catch that is a test.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public final class LazyIndexMetadata implements IndexMetadataHolder {

    private final Index index;
    private final IndexMetadata.State state;
    private final Map<String, AliasMetadata> aliases;
    private final boolean system;
    private final boolean hidden;
    private final boolean remoteSnapshot;
    private final boolean warm;
    private final int totalNumberOfShards;
    private final Supplier<IndexMetadata> loader;

    private volatile IndexMetadata resolved;

    private LazyIndexMetadata(Builder builder) {
        this.index = Objects.requireNonNull(builder.index, "index");
        this.state = Objects.requireNonNull(builder.state, "state");
        this.aliases = builder.aliases == null ? Collections.emptyMap() : Map.copyOf(builder.aliases);
        this.system = builder.system;
        this.hidden = builder.hidden;
        this.remoteSnapshot = builder.remoteSnapshot;
        this.warm = builder.warm;
        this.totalNumberOfShards = builder.totalNumberOfShards;
        this.loader = Objects.requireNonNull(builder.loader, "loader");
    }

    /**
     * A stub whose descriptor is taken from an index that is already in hand, deferring only the
     * loading. Useful where the metadata is available at write time but should not stay resident.
     */
    public static LazyIndexMetadata of(IndexMetadata indexMetadata, Supplier<IndexMetadata> loader) {
        return builder(indexMetadata.getIndex()).state(indexMetadata.getState())
            .aliases(indexMetadata.getAliases())
            .system(indexMetadata.isSystem())
            .hidden(indexMetadata.isHidden())
            .remoteSnapshot(indexMetadata.isRemoteSnapshot())
            .warm(indexMetadata.isWarmIndex())
            .totalNumberOfShards(indexMetadata.getTotalNumberOfShards())
            .loader(loader)
            .build();
    }

    public static Builder builder(Index index) {
        return new Builder(index);
    }

    @Override
    public IndexMetadata get() {
        IndexMetadata local = resolved;
        if (local != null) {
            return local;
        }
        return load();
    }

    private synchronized IndexMetadata load() {
        if (resolved == null) {
            IndexMetadata loaded = loader.get();
            if (loaded == null) {
                throw new IllegalStateException("loader for index [" + index + "] returned null");
            }
            if (index.equals(loaded.getIndex()) == false) {
                throw new IllegalStateException("loader for index [" + index + "] returned metadata for [" + loaded.getIndex() + "]");
            }
            assert loaded.getState() == state : "state disagrees with descriptor for " + index;
            assert loaded.getAliases().equals(aliases) : "aliases disagree with descriptor for " + index;
            assert loaded.isSystem() == system : "system flag disagrees with descriptor for " + index;
            assert loaded.isHidden() == hidden : "hidden flag disagrees with descriptor for " + index;
            assert loaded.isRemoteSnapshot() == remoteSnapshot : "remote-snapshot flag disagrees with descriptor for " + index;
            assert loaded.isWarmIndex() == warm : "warm flag disagrees with descriptor for " + index;
            assert loaded.getTotalNumberOfShards() == totalNumberOfShards : "shard count disagrees with descriptor for " + index;
            resolved = loaded;
        }
        return resolved;
    }

    @Override
    public boolean isResolved() {
        return resolved != null;
    }

    @Override
    public Index getIndex() {
        return index;
    }

    @Override
    public IndexMetadata.State getState() {
        return state;
    }

    @Override
    public Map<String, AliasMetadata> getAliases() {
        return aliases;
    }

    @Override
    public boolean isSystem() {
        return system;
    }

    @Override
    public boolean isHidden() {
        return hidden;
    }

    @Override
    public boolean isRemoteSnapshot() {
        return remoteSnapshot;
    }

    @Override
    public boolean isWarmIndex() {
        return warm;
    }

    @Override
    public int getTotalNumberOfShards() {
        return totalNumberOfShards;
    }

    @Override
    public String toString() {
        return "LazyIndexMetadata[" + index + ", resolved=" + isResolved() + "]";
    }

    /**
     * Builds a {@link LazyIndexMetadata}. Deliberately does not derive anything: every descriptor field
     * has to be supplied by whoever wrote the index out, because deriving one would mean loading the
     * index, which is the thing being avoided.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.8.0")
    public static final class Builder {

        private final Index index;
        private IndexMetadata.State state = IndexMetadata.State.OPEN;
        private Map<String, AliasMetadata> aliases;
        private boolean system;
        private boolean hidden;
        private boolean remoteSnapshot;
        private boolean warm;
        private int totalNumberOfShards;
        private Supplier<IndexMetadata> loader;

        private Builder(Index index) {
            this.index = index;
        }

        public Builder state(IndexMetadata.State state) {
            this.state = state;
            return this;
        }

        public Builder aliases(Map<String, AliasMetadata> aliases) {
            this.aliases = aliases;
            return this;
        }

        public Builder system(boolean system) {
            this.system = system;
            return this;
        }

        public Builder hidden(boolean hidden) {
            this.hidden = hidden;
            return this;
        }

        public Builder remoteSnapshot(boolean remoteSnapshot) {
            this.remoteSnapshot = remoteSnapshot;
            return this;
        }

        public Builder warm(boolean warm) {
            this.warm = warm;
            return this;
        }

        public Builder totalNumberOfShards(int totalNumberOfShards) {
            this.totalNumberOfShards = totalNumberOfShards;
            return this;
        }

        /** Called at most once, on first read, however many threads race. */
        public Builder loader(Supplier<IndexMetadata> loader) {
            this.loader = loader;
            return this;
        }

        public LazyIndexMetadata build() {
            return new LazyIndexMetadata(this);
        }
    }
}
