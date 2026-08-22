/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;

import java.util.OptionalDouble;

/**
 * A plugin-supplied off-heap/native memory
 * pressure signal, generic across whatever allocator or pool taxonomy a plugin actually uses.
 *
 * <p>Before this interface existed, {@code NativeMemoryBasedAdmissionController} read {@code
 * org.opensearch.arrow.spi.PoolGroup} and {@code org.opensearch.plugin.stats.NativeAllocatorPoolStats}
 * directly -- core's generic admission-control decision logic had compiled-in knowledge of one specific
 * plugin's four-way pool taxonomy ({@code TRANSPORT}/{@code SEARCH}/{@code INDEXING}/{@code MERGE}).
 * With this seam, the controller only ever asks "what's the utilization for this action type," and any
 * plugin that tracks native memory pressure -- by any pool scheme, or none at all -- can answer it.
 *
 * <p>{@code null} (no supplier installed) or an empty {@link OptionalDouble} both mean "no signal
 * available for this action type"; the controller treats either identically to today's "supplier absent"
 * behavior -- the pool-based check is simply skipped.
 *
 * <p>Not {@code @ExperimentalApi}/{@code @PublicApi}-annotated, matching the rest of {@code
 * org.opensearch.ratelimitting.admissioncontrol} -- none of it carries those annotations today, and
 * retrofitting {@link AdmissionControlActionType} with one as a side effect of this interface was judged
 * a bigger, separate decision than this phase's scope.
 */
public interface NativeMemoryPressureSignal {

    /**
     * @param actionType the admission-control action type a request is being evaluated for.
     * @return the current utilization percentage (0-100+) this plugin tracks for that action type, or
     *         empty if this plugin has no signal for it (e.g. it only tracks one pool group and this
     *         action type maps to a different one).
     */
    OptionalDouble utilizationPercentFor(AdmissionControlActionType actionType);
}
