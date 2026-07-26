/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.index.IndexNotFoundException;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A14. Resolves index expressions against a {@link NameIndex} with core's {@link IndicesOptions}
 * semantics.
 *
 * <p>This is the seam that replaces {@code Metadata.indicesLookup} for wildcard and alias resolution.
 * The structure underneath is the easy part; matching core exactly is not, and getting it wrong is
 * worse than not having it, because a query that silently selects a different set of indices produces
 * wrong answers rather than errors.
 *
 * <p>Four flags carry most of the behaviour, and each maps onto something the index already holds:
 *
 * <ul>
 *   <li>{@code expandWildcardsOpen} and {@code expandWildcardsClosed} filter by status. This is why
 *       closed indices are a status byte rather than an absence: an expression can legitimately want
 *       them and legitimately want them excluded.
 *   <li>{@code allowNoIndices} decides whether a <em>wildcard</em> matching nothing is empty or an
 *       error.
 *   <li>{@code ignoreUnavailable} decides whether a <em>concrete</em> name that is missing is skipped
 *       or an error. The two are distinct and are frequently conflated: a wildcard matching nothing and
 *       a named index that does not exist are different situations with different flags.
 * </ul>
 *
 * <p>Aliases expand to their targets, one level only, matching core: an alias naming another alias is
 * not something OpenSearch permits, and following it would turn malformed input into an unbounded walk.
 */
public final class NameIndexResolver {

    private final NameIndex nameIndex;

    public NameIndexResolver(NameIndex nameIndex) {
        this.nameIndex = Objects.requireNonNull(nameIndex, "nameIndex");
    }

    /**
     * Concrete index names for a set of expressions, deduplicated, in the order they were first
     * produced.
     *
     * <p>Order is insertion rather than sorted because an explicit list of names is expected to come
     * back in the order given, while each wildcard contributes its own matches in byte order. That is
     * what core does, and a caller comparing against it would notice any other choice.
     */
    public List<String> resolve(IndicesOptions options, String... expressions) {
        Objects.requireNonNull(options, "options");
        Set<String> resolved = new LinkedHashSet<>();

        // No expression at all means everything, subject to the same expansion flags. Core treats a
        // bare request as _all rather than as an error.
        if (expressions == null || expressions.length == 0) {
            collectPattern("*", options, resolved);
            return new ArrayList<>(resolved);
        }

        for (String expression : expressions) {
            if (NamePatterns.isPattern(expression)) {
                int before = resolved.size();
                collectPattern(expression, options, resolved);
                if (resolved.size() == before && options.allowNoIndices() == false) {
                    throw new IndexNotFoundException(expression);
                }
            } else {
                collectConcrete(expression, options, resolved);
            }
        }
        return new ArrayList<>(resolved);
    }

    private void collectPattern(String pattern, IndicesOptions options, Set<String> into) {
        nameIndex.forEachMatching(pattern, entry -> {
            if (entry.isAlias()) {
                // An alias matched by a wildcard contributes its targets, not itself. The targets are
                // names, so they are re-checked against the status flags rather than trusted.
                for (String target : entry.getTargets()) {
                    IndexNameEntry resolvedTarget = nameIndex.lookup(target);
                    if (resolvedTarget != null && admits(options, resolvedTarget)) {
                        into.add(target);
                    }
                }
            } else if (admits(options, entry)) {
                into.add(entry.getName());
            }
        });
    }

    private void collectConcrete(String name, IndicesOptions options, Set<String> into) {
        IndexNameEntry entry = nameIndex.lookup(name);
        if (entry == null) {
            // A named index that is absent is governed by ignoreUnavailable, not by allowNoIndices.
            if (options.ignoreUnavailable() == false) {
                throw new IndexNotFoundException(name);
            }
            return;
        }
        if (entry.isAlias()) {
            for (String target : entry.getTargets()) {
                IndexNameEntry resolvedTarget = nameIndex.lookup(target);
                if (resolvedTarget != null && admits(options, resolvedTarget)) {
                    into.add(target);
                }
            }
            return;
        }
        if (admits(options, entry) == false) {
            // Named explicitly but excluded by the expansion flags. Core treats this as unavailable
            // rather than as a silent omission, because the caller asked for it by name.
            if (options.ignoreUnavailable() == false) {
                throw new IndexNotFoundException(name);
            }
            return;
        }
        into.add(name);
    }

    /** Whether the expansion flags admit an entry, based on the status byte the index already carries. */
    private static boolean admits(IndicesOptions options, IndexNameEntry entry) {
        return entry.isClosed() ? options.expandWildcardsClosed() : options.expandWildcardsOpen();
    }
}
