/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Bounded semantic replay of one activation from its complete local committed prefix. Only newly
 * accepted logical records are emitted as effects. This is historical replay, not current-read
 * authority, authenticated producer admission, an append acknowledgement or a serving-engine writer.
 * The host must bound the number and aggregate memory of resident replay instances and use the same
 * durable protocol limits on every replica. No state may be restored from a caller-supplied cursor.
 * The consumer must finish applying a returned batch before polling again; an application failure
 * requires discarding this cursor and rebuilding the consumer from a verified base.
 */
@InternalApi
public final class LogicalStreamReplay {
    /**
     * Disposition of a selected physically committed entry.
     * @param checkpoint physical entry inspected
     * @param decision logical disposition; duplicate results refer to their first accepted position
     */
    @InternalApi
    public record Receipt(LogCheckpoint checkpoint, LogicalStreamState.Decision decision) {
    }

    /**
     * A bounded identity lookup at one historical replay position, not latest-read or write authority.
     * @param outcome original result/refusal, or unknown in this folded prefix
     * @param throughIndex physical prefix inspected by this cursor
     */
    @InternalApi
    public record Lookup(Optional<LogicalStreamState.Decision> outcome, long throughIndex) {
    }

    /**
     * One bounded historical replay result.
     * @param effects newly accepted logical records, including controls; never duplicate/rejected entries
     * @param receipts dispositions for selected entries, including legacy refusals
     * @param throughIndex last inspected physical position, including other streams and configuration
     * @param committedIndex locally applied committed prefix observed by this scan, not latest authority
     * @param state immutable semantic state after this batch
     */
    @InternalApi
    public record Batch(List<RatisJournal.CommittedOperation> effects, List<Receipt> receipts, long throughIndex, long committedIndex,
        LogicalStreamState.View state) {
        /**
         * Owns immutable result lists.
         * @param effects newly accepted logical records
         * @param receipts all selected physical dispositions
         * @param throughIndex physical scan progress
         * @param committedIndex observed local committed prefix
         * @param state semantic projection
         */
        public Batch {
            effects = List.copyOf(effects);
            receipts = List.copyOf(receipts);
        }
    }

    @FunctionalInterface
    interface Source {
        RatisJournal.ScanBatch scan(long afterIndex, int maximumEntries, int maximumBytes) throws IOException;
    }

    private final Source source;
    private final ShardLogId stream;
    private final LogicalStreamState state;
    private long throughIndex = -1;
    private Throwable failure;

    /**
     * Replays using permanent lineage and semantic limits loaded from the authorized catalog.
     * The trusted host must bound resident cursors and separately authorize active storage mode.
     * @param journal authorized local journal
     * @param assignment permanent catalog assignment, not a client-supplied proof
     * @throws IOException on mismatched physical history or unsupported local frame capability
     */
    public LogicalStreamReplay(RatisJournal journal, LogicalStreamRecord assignment) throws IOException {
        this(boundSource(journal, assignment), assignment.stream(), assignment.maximumOutcomes(), assignment.maximumPayloadBytes());
    }

    private static Source boundSource(RatisJournal journal, LogicalStreamRecord assignment) throws IOException {
        journal.verifyLogicalStream(assignment);
        return source(journal, assignment.group(), assignment.stream());
    }

    /**
     * Starts from the beginning of one activated stream's retained committed history.
     * @param journal authorized local journal
     * @param group assigned physical group
     * @param stream exact logical activation identity
     * @param maximumOutcomes fixed protocol retry-retention count
     * @param maximumPayloadBytes fixed protocol frame limit
     */
    LogicalStreamReplay(RatisJournal journal, UUID group, ShardLogId stream, int maximumOutcomes, int maximumPayloadBytes) {
        this(source(journal, group, stream), stream, maximumOutcomes, maximumPayloadBytes);
    }

    LogicalStreamReplay(Source source, ShardLogId stream, int maximumOutcomes, int maximumPayloadBytes) {
        this.source = Objects.requireNonNull(source);
        this.stream = Objects.requireNonNull(stream);
        state = new LogicalStreamState(stream, maximumOutcomes, maximumPayloadBytes);
    }

    private static Source source(RatisJournal journal, UUID group, ShardLogId stream) {
        Objects.requireNonNull(journal);
        Objects.requireNonNull(group);
        final var selection = Set.of(stream);
        return (afterIndex, entries, bytes) -> journal.scan(group, afterIndex, selection, entries, bytes);
    }

    /**
     * Looks up an identity without decoding a payload or scanning the log. Unknown does not authorize
     * preparation: ownership fencing and exclusion of ambiguous in-flight proposals remain required.
     * @param requestId stable internal UUID
     * @param action original request domain
     * @param firstObservedIndex immutable initial observation
     * @param requestDigest original-input digest
     * @return immutable lookup and its exact replay progress
     * @throws IOException if partial replay failure has fenced this cursor
     */
    public synchronized Lookup knownOutcome(UUID requestId, LogicalCommand.Action action, long firstObservedIndex, byte[] requestDigest)
        throws IOException {
        if (failure != null) {
            throw new IOException("logical replay is stopped after an earlier failure", failure);
        }
        return new Lookup(state.knownOutcome(requestId, action, firstObservedIndex, requestDigest), throughIndex);
    }

    /**
     * Evaluates one bounded scan, advancing only from its own previously inspected prefix. Failure
     * permanently stops this instance; rebuild from verified history rather than continuing partial state.
     * Legacy records are never emitted as logical effects and prevent silent protocol migration.
     * @param maximumEntries physical positions inspected, at most 4096
     * @param maximumBytes encoded operation bytes inspected, within the journal's queue limit
     * @return immutable effects and physical receipts
     * @throws IOException on missing history or replay failure
     */
    public synchronized Batch poll(int maximumEntries, int maximumBytes) throws IOException {
        if (maximumEntries < 1 || maximumEntries > 4096 || maximumBytes < 1) {
            throw new IllegalArgumentException("invalid logical replay scan bounds");
        }
        if (failure != null) {
            throw new IOException("logical replay is stopped after an earlier failure", failure);
        }
        try {
            final var batch = source.scan(throughIndex, maximumEntries, maximumBytes);
            if (batch.throughIndex() < throughIndex
                || batch.throughIndex() > batch.committedIndex()
                || batch.throughIndex() - maximumEntries > throughIndex
                || batch.operations().size() > maximumEntries) {
                throw new IOException("logical scan returned invalid physical progress");
            }
            final List<RatisJournal.CommittedOperation> effects = new ArrayList<>();
            final List<Receipt> receipts = new ArrayList<>();
            long previous = throughIndex;
            for (var committed : batch.operations()) {
                final var checkpoint = committed.checkpoint();
                final var envelope = committed.operation();
                if (checkpoint.logId().equals(stream) == false
                    || envelope.logId().equals(stream) == false
                    || checkpoint.index() <= previous
                    || checkpoint.index() > batch.throughIndex()) {
                    throw new IOException("logical scan returned an unordered or unselected operation");
                }
                final LogicalStreamState.Decision decision;
                if (envelope.kind() == OperationEnvelope.Kind.LOGICAL) {
                    decision = state.apply(checkpoint, envelope);
                } else {
                    state.observeLegacy(checkpoint);
                    decision = new LogicalStreamState.Decision(
                        LogicalStreamState.Status.REJECTED,
                        LogicalStreamState.Rejection.LEGACY_HISTORY,
                        null
                    );
                }
                receipts.add(new Receipt(checkpoint, decision));
                if (decision.status() == LogicalStreamState.Status.ACCEPTED) {
                    effects.add(committed);
                }
                previous = checkpoint.index();
            }
            throughIndex = batch.throughIndex();
            return new Batch(effects, receipts, throughIndex, batch.committedIndex(), state.view());
        } catch (IOException | RuntimeException | Error stopped) {
            failure = stopped;
            if (stopped instanceof Error fatal) {
                throw fatal;
            }
            throw stopped instanceof IOException io ? io : new IOException("logical replay stopped", stopped);
        }
    }
}
