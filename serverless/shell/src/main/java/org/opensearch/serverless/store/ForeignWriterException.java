/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

/**
 * Another node has already published at this writer's term.
 *
 * <p><b>This should be impossible, which is why it is worth throwing.</b> A term is taken by a
 * compare-and-swap on the shard-head, and one swap admits one winner — so two nodes holding the same term
 * means that swap did not do what the whole design assumes it does (see {@code r11-conformance.md}).
 *
 * <p>It is a subclass of {@link StaleWriterException} so that a caller which already knows how to give up
 * a shard it has lost does the right thing without being taught anything new. What it adds is a name and a
 * message: "you were fenced by a newer term" and "another node believes it is you" are the same instruction
 * and very different diagnoses, and an operator who sees this one is looking at their object store rather
 * than at their cluster.
 */
public final class ForeignWriterException extends StaleWriterException {

    /**
     * Creates the exception.
     *
     * @param term the term both writers hold
     * @param published the node that published first
     * @param attempting the node that tried to publish second
     */
    public ForeignWriterException(long term, String published, String attempting) {
        super(
            "refusing to publish at term "
                + term
                + ": node ["
                + published
                + "] has already published at that term and this node is ["
                + attempting
                + "]. Two nodes hold one term, which means the shard-head compare-and-swap admitted two "
                + "winners -- the assumption R11 exists to check."
        );
    }
}
