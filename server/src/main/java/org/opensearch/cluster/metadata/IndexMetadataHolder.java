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

import java.util.Map;

/**
 * How {@link Metadata} stores one index: either the {@link IndexMetadata} itself, or something that
 * can produce it on demand.
 *
 * <p>{@link Metadata} needs far less from most of its indices than it holds. Building the derived
 * name arrays and the {@code indicesLookup} needs a name, a state, a hidden and a system flag, the
 * alias names, and enough to classify the index into a routing pool -- a few hundred bytes. The rest
 * of an {@link IndexMetadata} (settings, mappings, in-sync allocation ids, rollover info) is only
 * needed by whoever actually operates on the index. This interface is that split: everything above
 * {@link #get()} is the cheap part every node must hold, and {@link #get()} is the expensive part
 * that an implementation may defer.
 *
 * <p>{@link IndexMetadata} implements this itself and returns {@code this} from {@link #get()}, so an
 * index that is already materialized is stored directly in the map with no wrapper object and no
 * extra indirection. That matters: this is the storage type of the most-read map in the codebase, and
 * a design where the common case pays for the uncommon one would not be worth having. Deferred
 * loading is opt-in per index, via {@link Metadata.Builder#putStub}.
 *
 * <p>Implementations must be immutable in everything except the memoization of {@link #get()}, and
 * {@link #get()} must be safe to call concurrently and must always return the same instance.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public interface IndexMetadataHolder {

    /**
     * The full index metadata, loading it first if this holder was deferred. Never null.
     *
     * @throws RuntimeException if a deferred load fails; callers are not expected to handle this any
     *     differently from the cluster state being unreadable.
     */
    IndexMetadata get();

    /**
     * Whether {@link #get()} would return without doing work. Only for tests and diagnostics -- code
     * that needs the metadata should just call {@link #get()}.
     */
    default boolean isResolved() {
        return true;
    }

    /** Name and UUID. */
    Index getIndex();

    /** Open or closed. */
    IndexMetadata.State getState();

    /** Alias metadata by alias name. Empty when the index has no aliases. */
    Map<String, AliasMetadata> getAliases();

    /** Whether the index is a system index. */
    boolean isSystem();

    /** Whether {@code index.hidden} is set. */
    boolean isHidden();

    /** Whether the index is backed by a searchable snapshot. */
    boolean isRemoteSnapshot();

    /** Whether {@code index.warm} is set. */
    boolean isWarmIndex();

    /** Primaries plus all replicas, across every shard. */
    int getTotalNumberOfShards();
}
