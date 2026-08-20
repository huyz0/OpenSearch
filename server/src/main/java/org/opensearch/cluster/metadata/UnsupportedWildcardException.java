/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;

/**
 * Thrown when a wildcard cannot be answered over a gated population, rather than answered incompletely.
 *
 * <p>Two patterns get this, and they fail for different reasons that are worth keeping apart in the message
 * even though they share a type.
 *
 * <p><b>A pattern that is not a prefix</b> ({@code *-logs}, {@code a*b}) has no range to scan. The
 * descriptor store lists by name, so {@code tenant-42-*} is a bounded listing and anything with a leading or
 * embedded star is a scan of every name in the population. At a hundred million indices that is not a slow
 * answer, it is no answer, and supporting it needs a second global structure keyed on reversed names.
 *
 * <p><b>A pattern that matches more than the cap</b> could be answered and should not be. T20 and T21
 * measured 118 KB and 3.06 file descriptors per awake shard, and with index per tenant most tenant indices
 * are asleep, so an expansion decides how many sleeping shards one request wakes. The cap bounds that, and
 * resolving the names was never the expensive part.
 *
 * <p><b>Why this is an error rather than a truncated answer.</b> Returning the first hundred of five
 * thousand matches reads exactly like a complete answer, and the caller has no way to tell. That is the
 * failure this whole area keeps producing, and it has now been measured four separate times: cluster stats
 * reporting a plausible wrong number (H19), eight clients each told they created the same index (T23), a
 * wildcard silently matching nothing (T25), and a hundred million indices listed as ten (T27).
 *
 * <p>Reported as {@link RestStatus#BAD_REQUEST} because the request is answerable only in a narrower form,
 * and narrowing it is the client's decision rather than something the cluster can do on their behalf.
 */
public class UnsupportedWildcardException extends OpenSearchStatusException {

    public UnsupportedWildcardException(String message) {
        super(message, RestStatus.BAD_REQUEST);
    }

    /**
     * Read from a stream, which every {@link org.opensearch.OpenSearchException} subclass must support.
     *
     * <p>Required by {@code ExceptionSerializationTests.testExceptionRegistration}, which walks every
     * subclass in the server jar and fails the build for any that cannot cross the wire as itself. Without
     * it the exception degrades to a generic wrapper between nodes, so a client would see the message
     * without the type and could not distinguish this from any other bad request.
     */
    public UnsupportedWildcardException(org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
        super(in);
    }

    /** A pattern whose star is not a single trailing one. */
    public static UnsupportedWildcardException notAPrefix(String expression) {
        return new UnsupportedWildcardException(
            "wildcard ["
                + expression
                + "] cannot be resolved over indices held outside cluster state. Only a trailing wildcard is "
                + "supported, because names are indexed in sorted order and a leading or embedded wildcard "
                + "has to read every name. Use a prefix such as [prefix*], or name the indices exactly."
        );
    }

    /**
     * A prefix that matches more indices than one request may expand to.
     *
     * <p>Phase J3 of {@code core-pluggability-refactor-plan.md}: {@code limitSettingName} is supplied by
     * whichever expander produced the limit, rather than core interpolating one specific plugin's setting
     * key. Null when the expander did not name one, in which case the sentence is simply omitted.
     */
    public static UnsupportedWildcardException tooManyMatches(String expression, int limit, String limitSettingName) {
        return new UnsupportedWildcardException(
            "wildcard ["
                + expression
                + "] matches more than ["
                + limit
                + "] indices. Expanding it would place every match in this request, so narrow the prefix or "
                + "name the indices exactly."
                + (limitSettingName == null ? "" : " The limit is controlled by [" + limitSettingName + "].")
        );
    }
}
