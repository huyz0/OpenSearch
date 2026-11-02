/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.serverless.log.LogCheckpoint;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/** Mutation-specific facade retaining exact resolution/checkpoint verification over the shared dependency gate. */
final class PreparationCommitSequencer {
    enum State {
        READY,
        PREPARING,
        APPENDING,
        APPLYING,
        FENCED,
        CLOSED
    }

    @FunctionalInterface
    interface Preparer {
        ResolvedMutation prepare() throws IOException;
    }

    @FunctionalInterface
    interface Appender {
        CompletionStage<LogCheckpoint> append(ResolvedMutation mutation) throws IOException;
    }

    @FunctionalInterface
    interface Applier {
        CommittedMutationReplayer.AppliedMutation apply(ResolvedMutation mutation, LogCheckpoint checkpoint) throws IOException;
    }

    private final PreparationDependencyGate<ResolvedMutation, CommittedMutationReplayer.AppliedMutation> gate;

    PreparationCommitSequencer(Executor executor, Appender appender, Applier applier) {
        Objects.requireNonNull(appender);
        Objects.requireNonNull(applier);
        gate = new PreparationDependencyGate<>(executor, appender::append, applier::apply, PreparationCommitSequencer::verifyMutation);
    }

    static void verifyMutation(ResolvedMutation mutation, LogCheckpoint checkpoint, CommittedMutationReplayer.AppliedMutation applied)
        throws IOException {
        if (mutation.sameResolution(applied.mutation()) == false || applied.checkpoint().equals(checkpoint) == false) {
            throw new IOException("serving application returned a different operation or committed checkpoint");
        }
    }

    CompletionStage<CommittedMutationReplayer.AppliedMutation> submit(Preparer preparer) {
        Objects.requireNonNull(preparer);
        return gate.submit(preparer::prepare);
    }

    State state() {
        return switch (gate.state()) {
            case READY -> State.READY;
            case PREPARING -> State.PREPARING;
            case APPENDING -> State.APPENDING;
            case APPLYING -> State.APPLYING;
            case FENCED -> State.FENCED;
            case CLOSED -> State.CLOSED;
        };
    }

    void fence(Throwable cause) {
        gate.fence(cause);
    }

    CompletionStage<Void> closeAsync() {
        return gate.closeAsync();
    }
}
