/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;

import java.util.List;

/**
 * The three descriptor operations that need an index over names rather than a key-value store.
 *
 * <p>Separate from {@link DescriptorBackend} because they do not have the same set of possible
 * implementations, and pretending otherwise would put the difference somewhere it can only be discovered
 * at runtime. S13 identified prefix and alias resolution as the one part of the partitioned design that
 * cannot be partitioned: answering {@code logs-*} requires knowing every name, which is global by
 * construction. Nothing derived from hashing a single name can serve it.
 *
 * <p>An object store cannot implement this interface usefully. Prefix-preserving keys make a LIST able to
 * enumerate a prefix, but LIST pages a thousand keys at a time behind a serial continuation token, so a
 * match set in the millions is minutes rather than the milliseconds S13 measured against an in-memory
 * structure. It also returns names without the uuid and state a resolver needs, and putting the uuid in
 * the key to fix that would break name uniqueness, since two creators of one name with different uuids
 * would write different keys and both conditional writes would succeed.
 *
 * <p>So the implementations are the system index today and the name index tier afterwards, and the reason
 * to name that in a type is the ordering it forces: the blob backend can be added while the system index
 * still answers these, but the system index cannot be removed until something else does.
 */
public interface DescriptorPrefixBackend {

    /**
     * Descriptors whose name starts with {@code prefix}, after {@code afterName} in name order.
     *
     * <p>Refresh-bound rather than realtime. A descriptor written a moment ago may not appear yet, which
     * is the documented wildcard contract rather than a defect, and is why a caller needing immediate
     * visibility has to name the index exactly and go through {@link DescriptorBackend#get}.
     */
    List<IndexDescriptor> findByPrefix(String prefix, String afterName, int size);

    /** One page of names in creation order, for pagination that cannot hold the population in memory. */
    List<AbsentIndexDescriptorSuppliers.PagedIndex> findNamesForPage(String afterName, long afterCreationDate, boolean ascending, int size);

    /**
     * Expands a wildcard prefix, or reports that it matched more than {@code limit}.
     *
     * <p>The bound is part of the answer rather than a truncation. A wildcard that matched five thousand
     * tenants and returned a hundred, reporting success, is the failure shape this contract exists to make
     * impossible: over the limit is an explicit outcome, not a short list.
     */
    AbsentIndexDescriptorSuppliers.PrefixExpansion expandPrefix(String prefix, int limit);
}
