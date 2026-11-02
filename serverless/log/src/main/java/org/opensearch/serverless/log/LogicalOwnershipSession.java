/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * Caller-serialized ownership transition for one admitted logical activation. The host must use a
 * bounded worker, account for resident replay/outcome state and frame results, and finish native
 * work before closing this session. No thread, timer or cold-descriptor row is allocated here.
 * Every emitted effect is applied before the session exposes an installed ownership barrier.
 * The barrier is historical evidence, not a continuing lease or latest-read authority. Native
 * preparation still requires a verified rebuilt context with its exact sequence/schema/term.
 */
@InternalApi
public final class LogicalOwnershipSession implements Closeable {
    /** Session lifecycle; errors after proposal submission permanently stop this context. */
    public enum Stage {
        /** Reconstructing and applying the verified committed prefix. */
        REPLAYING,
        /** Awaiting physical commitment and semantic acceptance of a fresh claim. */
        CLAIMING,
        /** The ownership barrier and its preceding effects were applied. */
        OWNED,
        /** This context must be discarded and rebuilt behind another committed barrier. */
        FENCED,
        /** Closed without canceling an accepted physical proposal. */
        CLOSED
    }

    /**
     * A historical installed owner cut after successful application, not independent proof or a lease.
     * @param checkpoint this session's committed claim position
     * @param state exact accepted logical state after bounded replay/application
     */
    @InternalApi
    public record Ownership(LogCheckpoint checkpoint, LogicalStreamState.View state) {
    }

    /**
     * Small progress projection; applied payload batches are not retained or returned by the session.
     * @param stage current lifecycle
     * @param throughIndex physical positions inspected and successfully applied
     * @param committedIndex observed local applied/committed prefix, not latest-read authority
     * @param state logical state after successful consumer application
     */
    @InternalApi
    public record Progress(Stage stage, long throughIndex, long committedIndex, LogicalStreamState.View state) {
    }

    /** Host application runs synchronously on its admitted worker, never a journal callback. */
    @FunctionalInterface
    public interface EffectApplier {
        /**
         * Applies one newly accepted ordered control/data effect; failure requires consumer rebuild.
         * @param effect verified accepted historical record
         * @throws IOException if native application or schema/lifecycle handling fails
         */
        void apply(RatisJournal.CommittedOperation effect) throws IOException;
    }

    interface Source {
        RatisJournal.ScanBatch scan(long after, int entries, int bytes) throws IOException;

        CompletionStage<LogCheckpoint> append(LogicalCommand claim, long epoch, long term, long schema) throws IOException;

        boolean leaderReady() throws IOException;
    }

    private final LogicalStreamRecord assignment;
    private final UUID producer;
    private Source source;
    private EffectApplier applier;
    private LogicalStreamReplay replay;
    private LogicalStreamState.View view;
    private long through = -1;
    private long committed = -1;
    private Stage stage = Stage.REPLAYING;
    private CompletableFuture<LogCheckpoint> pending;
    private LogicalCommand claim;
    private long claimedEpoch;
    private long claimedTerm;
    private long claimedSchema;
    private Ownership ownership;
    private Throwable failure;
    private boolean applying;

    /**
     * Opens after host residency/admission and durable assignment validation.
     * @param journal authorized local journal
     * @param assignment catalog-loaded activation and fixed protocol limits
     * @param producer this journal's local voter UUID; proposal admission independently verifies it
     * @param applier host-owned ordered control/data consumer
     * @throws IOException on incompatible assignment or physical history
     */
    public LogicalOwnershipSession(RatisJournal journal, LogicalStreamRecord assignment, UUID producer, EffectApplier applier)
        throws IOException {
        this(source(journal, assignment), assignment, producer, applier);
    }

    LogicalOwnershipSession(Source source, LogicalStreamRecord assignment, UUID producer, EffectApplier applier) {
        this.assignment = Objects.requireNonNull(assignment);
        this.producer = Objects.requireNonNull(producer);
        this.source = Objects.requireNonNull(source);
        this.applier = Objects.requireNonNull(applier);
        replay = new LogicalStreamReplay(source::scan, assignment.stream(), assignment.maximumOutcomes(), assignment.maximumPayloadBytes());
    }

    private static Source source(RatisJournal journal, LogicalStreamRecord assignment) throws IOException {
        Objects.requireNonNull(journal).verifyLogicalStream(assignment);
        final var selection = java.util.Set.of(assignment.stream());
        return new Source() {
            @Override
            public RatisJournal.ScanBatch scan(long after, int entries, int bytes) throws IOException {
                return journal.scan(assignment.group(), after, selection, entries, bytes);
            }

            @Override
            public CompletionStage<LogCheckpoint> append(LogicalCommand claim, long epoch, long term, long schema) throws IOException {
                return journal.appendLogical(assignment, claim, epoch, term, schema);
            }

            @Override
            public boolean leaderReady() throws IOException {
                return journal.isLeaderReady(assignment.group());
            }
        };
    }

    /** Returns this caller-serialized session's current lifecycle. */
    public Stage stage() {
        return stage;
    }

    /** Returns the installed historical cut only after semantic acceptance and successful application. */
    public Optional<Ownership> ownership() {
        return applying ? Optional.empty() : Optional.ofNullable(ownership);
    }

    /**
     * Applies one bounded scan without waiting for an unresolved physical claim future. Failure
     * permanently fences this session; a new session must rebuild the consumer and claim again.
     * @param maximumEntries physical positions per scan, at most 4096
     * @param maximumBytes admitted encoded scan bytes
     * @return compact applied progress without retaining opaque frame payloads
     * @throws IOException on stopped context, authority loss, replay or application failure
     */
    public Progress advance(int maximumEntries, int maximumBytes) throws IOException {
        requireOpen();
        if (maximumEntries < 1 || maximumEntries > 4096 || maximumBytes < 1) {
            throw new IllegalArgumentException("invalid ownership replay bounds");
        }
        if (applying) {
            throw new IOException("ownership application cannot be reentered");
        }
        applying = true;
        try {
            if (stage == Stage.OWNED && source.leaderReady() == false) {
                throw new IOException("local leader readiness was lost after ownership installation");
            }
            final LogCheckpoint acknowledged = pending != null && pending.isDone()
                ? Objects.requireNonNull(pending.join(), "missing ownership checkpoint")
                : null;
            if (acknowledged != null && (acknowledged.logId().equals(assignment.stream()) == false || acknowledged.index() < 0)) {
                throw new IOException("ownership acknowledgement differs from assigned stream");
            }
            final var batch = replay.poll(maximumEntries, maximumBytes);
            for (var effect : batch.effects()) {
                applier.apply(effect);
            }
            view = batch.state();
            through = batch.throughIndex();
            committed = batch.committedIndex();
            if (stage == Stage.CLAIMING && acknowledged != null && through >= acknowledged.index()) {
                final var result = replay.knownOutcome(claim.requestId(), claim.action(), claim.firstObservedIndex(), claim.requestDigest())
                    .outcome()
                    .orElseThrow(() -> new IOException("ownership proposal has no retained accepted outcome"));
                if (result.status() != LogicalStreamState.Status.DUPLICATE
                    || acknowledged.equals(result.originalCheckpoint()) == false
                    || ownerMatches() == false
                    || source.leaderReady() == false) {
                    throw new IOException("ownership claim was refused, superseded or lost local leadership");
                }
                ownership = new Ownership(acknowledged, view);
                pending = null;
                claim = null;
                stage = Stage.OWNED;
            } else if (stage == Stage.OWNED && ownerMatches() == false) {
                throw new IOException("committed ownership changed after installation");
            }
            if (stage == Stage.OWNED) {
                ownership = new Ownership(ownership.checkpoint(), view);
            }
            return new Progress(stage, through, committed, view);
        } catch (IOException | RuntimeException | Error stopped) {
            fence(stopped);
            if (stopped instanceof Error fatal) {
                throw fatal;
            }
            throw stopped instanceof IOException io ? io : new IOException("ownership session stopped", stopped);
        } finally {
            applying = false;
        }
    }

    /**
     * Submits a fresh ownership CAS after the observed prefix has been applied. No native sequence
     * allocation may begin from the physical reply alone; continue advance until ownership exists.
     * Callers must schedule continuations on their bounded worker and choose a term compatible with
     * native serving/migration floors. Cancellation of the returned stage cannot cancel persistence.
     * @param primaryTerm new engine term, strictly above the committed logical term
     * @return detached physical completion used to schedule further bounded replay
     * @throws IOException on stale lifecycle, invalid ownership history or submission failure
     */
    public CompletionStage<LogCheckpoint> beginClaim(long primaryTerm) throws IOException {
        requireOpen();
        if (applying || stage != Stage.REPLAYING || view == null || through != committed) {
            throw new IOException("ownership claim requires a fully applied observed prefix");
        }
        if (view.producerEpoch() == Long.MAX_VALUE || primaryTerm <= view.primaryTerm()) {
            throw new IllegalArgumentException("ownership epoch/engine term cannot advance");
        }
        try {
            if (source.leaderReady() == false) {
                throw new IOException("ownership proposal requires the ready local leader");
            }
            final byte[] intent = ByteBuffer.allocate(40)
                .putLong(producer.getMostSignificantBits())
                .putLong(producer.getLeastSignificantBits())
                .putLong(view.producerEpoch())
                .putLong(primaryTerm)
                .putLong(view.schemaVersion())
                .array();
            final byte[] digest = MessageDigest.getInstance("SHA-256").digest(intent);
            claim = new LogicalCommand(
                LogicalCommand.Action.OWNERSHIP,
                UUID.randomUUID(),
                through,
                digest,
                producer,
                view.producerEpoch(),
                new byte[0]
            );
            if (replay.knownOutcome(claim.requestId(), claim.action(), claim.firstObservedIndex(), digest).outcome().isPresent()) {
                throw new IOException("logical history refuses a fresh ownership claim");
            }
            claimedEpoch = view.producerEpoch() + 1;
            claimedTerm = primaryTerm;
            claimedSchema = view.schemaVersion();
            stage = Stage.CLAIMING;
            pending = Objects.requireNonNull(source.append(claim, claimedEpoch, claimedTerm, claimedSchema)).toCompletableFuture();
            return pending.minimalCompletionStage();
        } catch (IOException | RuntimeException | NoSuchAlgorithmException | Error stopped) {
            fence(stopped);
            if (stopped instanceof Error fatal) {
                throw fatal;
            }
            throw stopped instanceof IOException io ? io : new IOException("ownership proposal stopped", stopped);
        }
    }

    private boolean ownerMatches() {
        return producer.equals(view.owner())
            && view.producerEpoch() == claimedEpoch
            && view.primaryTerm() == claimedTerm
            && view.retired() == false;
    }

    private void requireOpen() throws IOException {
        if (stage == Stage.FENCED || stage == Stage.CLOSED) {
            throw new IOException("ownership session is stopped: " + stage, failure);
        }
    }

    private void fence(Throwable stopped) {
        failure = stopped instanceof CompletionException && stopped.getCause() != null ? stopped.getCause() : stopped;
        stage = Stage.FENCED;
        clear();
    }

    private void clear() {
        ownership = null;
        pending = null;
        claim = null;
        replay = null;
        view = null;
        applier = null;
        source = null;
    }

    /** Releases resident references after host work drains; never waits for or cancels a physical proposal. */
    @Override
    public void close() {
        if (applying) {
            throw new IllegalStateException("finish ownership application before closing its session");
        }
        stage = Stage.CLOSED;
        clear();
    }
}
