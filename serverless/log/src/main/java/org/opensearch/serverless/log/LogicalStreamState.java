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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;

/**
 * Bounded deterministic fold of one stream's committed logical records. The embedding journal must
 * supply the verified ordered prefix and authenticate the actor; calling this object is not a commit
 * proof. All reads/updates require caller serialization. This is not a resident state for every cold
 * shard, a runtime cache, an archive snapshot codec or an enabled storage mode.
 */
@InternalApi
public final class LogicalStreamState {
    /** Semantic result of a physically committed record. */
    public enum Status {
        /** A new logical effect was accepted. */
        ACCEPTED,
        /** The original accepted request already exists in the retained outcome window. */
        DUPLICATE,
        /** The physical entry has no logical effect. */
        REJECTED
    }

    /** Explicit semantic refusals; these are not wire enum ordinals. */
    public enum Rejection {
        /** No refusal. */
        NONE,
        /** The logical payload was invalid. */
        MALFORMED,
        /** Legacy history requires an explicit fenced migration. */
        LEGACY_HISTORY,
        /** An internal request ID changed identity fields. */
        IDENTITY_CONFLICT,
        /** Its original observation is older than retained outcomes. */
        OBSERVATION_EXPIRED,
        /** A request cannot observe its own or a future physical position. */
        FUTURE_OBSERVATION,
        /** This stream activation is permanently retired. */
        RETIRED,
        /** No producer has been installed. */
        NO_OWNER,
        /** The expected/claimed producer epoch is stale. */
        STALE_EPOCH,
        /** The producer differs from the committed owner. */
        ACTOR_MISMATCH,
        /** The engine term is stale or cannot advance. */
        TERM_MISMATCH,
        /** The schema dependency/transition is not current. */
        SCHEMA_MISMATCH,
        /** No schema definition is installed. */
        MISSING_SCHEMA,
        /** The operation does not extend the accepted sequence prefix. */
        SEQUENCE_MISMATCH
    }

    /**
     * A logical effect, not independent evidence of physical commitment.
     * @param status accepted, duplicate or rejected
     * @param rejection refusal reason, or NONE
     * @param originalCheckpoint first accepted position, or null for rejection
     */
    @InternalApi
    public record Decision(Status status, Rejection rejection, LogCheckpoint originalCheckpoint) {
    }

    /**
     * Small immutable state projection; schema bytes and outcome payloads stay in the journal.
     * @param owner installed producer, or null
     * @param producerEpoch committed ownership epoch
     * @param primaryTerm installed engine term, independent of the Raft term
     * @param schemaVersion installed logical schema version
     * @param schemaDigest hex digest of exact definition bytes, empty before installation
     * @param schemaCheckpoint immutable definition location, or null
     * @param lastSequenceNumber last accepted engine operation sequence, starting at -1
     * @param lastIndex last observed physical position for this stream
     * @param minimumRequestIndex oldest original observation that can safely execute a cache miss
     * @param retainedOutcomes bounded accepted request identities
     * @param controlled whether logical records have selected this protocol
     * @param retired whether this activation is permanently closed
     */
    @InternalApi
    public record View(UUID owner, long producerEpoch, long primaryTerm, long schemaVersion, String schemaDigest,
        LogCheckpoint schemaCheckpoint, long lastSequenceNumber, long lastIndex, long minimumRequestIndex, int retainedOutcomes,
        boolean controlled, boolean retired) {

    }

    private record Seen(LogicalCommand.Action action, long observedIndex, byte[] digest, LogCheckpoint checkpoint) {
    }

    private final ShardLogId stream;
    private final int maximumOutcomes;
    private final int maximumPayloadBytes;
    private final LinkedHashMap<UUID, Seen> outcomes = new LinkedHashMap<>();
    private UUID owner;
    private long producerEpoch;
    private long primaryTerm;
    private long schemaVersion;
    private byte[] schemaDigest = new byte[0];
    private LogCheckpoint schemaCheckpoint;
    private long lastSequence = -1;
    private long lastIndex = -1;
    private long minimumRequestIndex = -1;
    private boolean legacyHistory;
    private boolean controlled;
    private boolean retired;

    /**
     * Creates an empty fold; recovery must feed its verified prefix in order.
     * @param stream exact logical activation identity
     * @param maximumOutcomes accepted identities retained, between 1 and 65,536
     * @param maximumPayloadBytes configured frame payload admission
     */
    public LogicalStreamState(ShardLogId stream, int maximumOutcomes, int maximumPayloadBytes) {
        this.stream = Objects.requireNonNull(stream);
        if (maximumOutcomes < 1
            || maximumOutcomes > 65536
            || maximumPayloadBytes < 1
            || maximumPayloadBytes > OperationEnvelope.MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("invalid logical state bounds");
        }
        this.maximumOutcomes = maximumOutcomes;
        this.maximumPayloadBytes = maximumPayloadBytes;
    }

    /** Returns a small immutable projection without copying the retained outcome table. */
    public View view() {
        return new View(
            owner,
            producerEpoch,
            primaryTerm,
            schemaVersion,
            HexFormat.of().formatHex(schemaDigest),
            schemaCheckpoint,
            lastSequence,
            lastIndex,
            minimumRequestIndex,
            outcomes.size(),
            controlled,
            retired
        );
    }

    /**
     * Observes an untyped old-protocol record while reconstructing the prefix.
     * @param checkpoint verified physical position for this stream
     * @return whether legacy application remains allowed; false after logical activation
     */
    public boolean observeLegacy(LogCheckpoint checkpoint) {
        advance(checkpoint);
        if (controlled) {
            return false;
        }
        legacyHistory = true;
        return true;
    }

    /**
     * Evaluates one verified, committed logical envelope in physical order.
     * Rejection cannot change owner/schema/sequence state; duplicates point to the original position.
     * @param checkpoint journal-supplied physical position
     * @param envelope verified enclosing frame for this stream
     * @return deterministic logical disposition
     * @throws IOException if hashing an accepted schema fails
     */
    public Decision apply(LogCheckpoint checkpoint, OperationEnvelope envelope) throws IOException {
        if (Objects.requireNonNull(envelope).logId().equals(stream) == false || envelope.kind() != OperationEnvelope.Kind.LOGICAL) {
            throw new IllegalArgumentException("logical fold requires its own stream and explicit logical framing");
        }
        advance(checkpoint);
        controlled = true;
        if (legacyHistory) {
            return reject(Rejection.LEGACY_HISTORY);
        }
        if (envelope.payloadLength() > maximumPayloadBytes) {
            return reject(Rejection.MALFORMED);
        }
        final LogicalCommand command;
        try {
            command = LogicalCommand.decode(envelope.payloadReader().readAllBytes(), maximumPayloadBytes);
        } catch (IOException malformed) {
            return reject(Rejection.MALFORMED);
        }
        final var previous = outcomes.get(command.requestId());
        if (previous != null) {
            if (retryActionMatches(previous.action(), command.action()) == false
                || previous.observedIndex() != command.firstObservedIndex()
                || Arrays.equals(previous.digest(), command.requestDigest()) == false) {
                return reject(Rejection.IDENTITY_CONFLICT);
            }
            return new Decision(Status.DUPLICATE, Rejection.NONE, previous.checkpoint());
        }
        if (retired) {
            return reject(Rejection.RETIRED);
        }
        if (command.firstObservedIndex() >= checkpoint.index()) {
            return reject(Rejection.FUTURE_OBSERVATION);
        }
        if (command.firstObservedIndex() < minimumRequestIndex) {
            return reject(Rejection.OBSERVATION_EXPIRED);
        }
        if (command.action() == LogicalCommand.Action.OWNERSHIP) {
            if (command.argument() != producerEpoch || producerEpoch == Long.MAX_VALUE || envelope.producerEpoch() != producerEpoch + 1) {
                return reject(Rejection.STALE_EPOCH);
            }
            if (envelope.primaryTerm() <= primaryTerm) {
                return reject(Rejection.TERM_MISMATCH);
            }
            if (envelope.schemaVersion() != schemaVersion) {
                return reject(Rejection.SCHEMA_MISMATCH);
            }
            owner = command.actor();
            producerEpoch = envelope.producerEpoch();
            primaryTerm = envelope.primaryTerm();
        } else {
            if (owner == null) {
                return reject(Rejection.NO_OWNER);
            }
            if (envelope.producerEpoch() != producerEpoch) {
                return reject(Rejection.STALE_EPOCH);
            }
            if (command.actor().equals(owner) == false) {
                return reject(Rejection.ACTOR_MISMATCH);
            }
            if (envelope.primaryTerm() != primaryTerm) {
                return reject(Rejection.TERM_MISMATCH);
            }
            if (command.action() == LogicalCommand.Action.SCHEMA) {
                if (command.argument() != schemaVersion
                    || schemaVersion == Long.MAX_VALUE
                    || envelope.schemaVersion() != schemaVersion + 1) {
                    return reject(Rejection.SCHEMA_MISMATCH);
                }
                final byte[] digest = definitionDigest(command);
                schemaVersion = envelope.schemaVersion();
                schemaDigest = digest;
                schemaCheckpoint = canonical(checkpoint);
            } else {
                if (envelope.schemaVersion() != schemaVersion) {
                    return reject(Rejection.SCHEMA_MISMATCH);
                }
                if (command.action() == LogicalCommand.Action.RETIRE) {
                    retired = true;
                } else {
                    if (schemaCheckpoint == null) {
                        return reject(Rejection.MISSING_SCHEMA);
                    }
                    if (command.action() == LogicalCommand.Action.MUTATION) {
                        if (lastSequence == Long.MAX_VALUE || command.argument() != lastSequence + 1) {
                            return reject(Rejection.SEQUENCE_MISMATCH);
                        }
                        lastSequence = command.argument();
                    }
                }
            }
        }
        final var original = canonical(checkpoint);
        outcomes.put(command.requestId(), new Seen(command.action(), command.firstObservedIndex(), command.requestDigest(), original));
        if (outcomes.size() > maximumOutcomes) {
            final var oldest = outcomes.entrySet().iterator();
            final var evicted = oldest.next().getValue();
            minimumRequestIndex = Math.max(minimumRequestIndex, evicted.checkpoint().index());
            oldest.remove();
        }
        return new Decision(Status.ACCEPTED, Rejection.NONE, original);
    }

    private void advance(LogCheckpoint checkpoint) {
        Objects.requireNonNull(checkpoint);
        if (checkpoint.logId().equals(stream) == false || checkpoint.index() < 0 || checkpoint.index() <= lastIndex) {
            throw new IllegalArgumentException("logical state requires the same stream in increasing physical order");
        }
        lastIndex = checkpoint.index();
    }

    private LogCheckpoint canonical(LogCheckpoint checkpoint) {
        return new LogCheckpoint(stream, checkpoint.index(), checkpoint.entryTerm());
    }

    private static boolean retryActionMatches(LogicalCommand.Action accepted, LogicalCommand.Action proposed) {
        if (accepted == proposed) {
            return true;
        }
        // A script/update can resolve differently after takeover. Its input identity, rather
        // than a later mutation/noop/error classification, determines the original outcome.
        return switch (accepted) {
            case MUTATION -> proposed == LogicalCommand.Action.OUTCOME_ONLY;
            case OUTCOME_ONLY -> proposed == LogicalCommand.Action.MUTATION;
            default -> false;
        };
    }

    private static Decision reject(Rejection rejection) {
        return new Decision(Status.REJECTED, rejection, null);
    }

    private static byte[] definitionDigest(LogicalCommand command) throws IOException {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IOException("SHA-256 is unavailable", unavailable);
        }
        try (var in = command.payloadReader()) {
            final byte[] buffer = new byte[4096];
            int count;
            while ((count = in.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
        }
        return digest.digest();
    }
}
