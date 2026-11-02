/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.log.JournalConfiguration;
import org.opensearch.serverless.log.JournalGroupRecord;
import org.opensearch.serverless.log.JournalMembershipRecord;
import org.opensearch.serverless.log.JournalReplacementReceipt;
import org.opensearch.serverless.log.LogicalStreamRecord;
import org.opensearch.serverless.log.RatisJournal;
import org.opensearch.serverless.log.ShardLogId;

import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Durable bootstrap authorization, using object-store conditional registers only on lifecycle paths.
 * One permanent register reserves each physical group; there is no per-mutation S3 operation and no
 * global mutable membership register. Group reservations are never deleted or reused. This catalog
 * serializes replacement intent but does not execute native membership changes, authorize shard
 * ownership, or permit replacement after quorum loss.
 *
 * All methods that touch storage must run on a bounded controller executor, never a Raft callback.
 */
@InternalApi
public final class JournalGroupCatalog {
    private static final String DEPLOYMENT = "deployment";
    private final BlobContainer container;
    private final UUID deploymentId;
    private volatile boolean namespaceVerified;

    /**
     * Binds a dedicated journal catalog prefix to an expected durable deployment UUID.
     * @param container dedicated journal catalog container, excluded from legacy shard enumeration
     * @param deploymentId controller-configured deployment namespace
     */
    public JournalGroupCatalog(BlobContainer container, UUID deploymentId) {
        this.container = Objects.requireNonNull(container);
        this.deploymentId = Objects.requireNonNull(deploymentId);
    }

    /**
     * Permanently reserves a group and its genesis voters before native bootstrap. Retrying the
     * same reservation returns its current state, including activation after a lost reply. A
     * different placement can never overwrite the reservation, even after every voter is lost.
     * @param group new unique physical group and three controller-verified AZ placements
     * @return durable reservation or its already-activated state
     * @throws IOException on namespace mismatch, conflicting placement, storage or format failure
     */
    public JournalGroupRecord reserve(JournalConfiguration.Group group) throws IOException {
        ensureNamespace(true);
        final var expected = new JournalGroupRecord(deploymentId, group, null);
        final var created = container.createRegisterIfAbsent(name(group.id()), new BytesArray(expected.encode()));
        if (created.applied()) {
            return expected;
        }
        final var current = readRequired(group.id());
        requirePlacement(current, expected);
        return current;
    }

    /**
     * Reads one group directly without listing or depending on the shard population.
     * @param group physical group identity
     * @return current reservation, or empty for an unreserved group
     * @throws IOException on missing deployment identity, corruption or storage failure
     */
    public Optional<JournalGroupRecord> read(UUID group) throws IOException {
        ensureNamespace(false);
        final var value = container.readRegister(name(group));
        return value.isEmpty() ? Optional.empty() : Optional.of(decode(group, value.get()));
    }

    /**
     * Permanently assigns one exact logical activation to an already activated physical history.
     * The first successful conditional creation fixes group lineage and semantic limits. Retrying
     * the identical assignment is idempotent; no later call can move or reconfigure it. This is a
     * lifecycle operation, not per-document S3 traffic or authorization to switch storage modes.
     * @param stream exact customer stream incarnation and activation
     * @param group durably activated physical group
     * @param maximumOutcomes fixed accepted-request retention count
     * @param maximumPayloadBytes fixed complete logical command limit
     * @return permanent assignment, including after an uncertain creation reply
     * @throws IOException on foreign namespace, inactive group, conflicting assignment or storage failure
     */
    public LogicalStreamRecord reserveStream(ShardLogId stream, UUID group, int maximumOutcomes, int maximumPayloadBytes)
        throws IOException {
        requireStreamNamespace(stream);
        final var genesis = readRequired(group);
        if (genesis.activation() == null) {
            throw new IOException("logical assignment requires a durably activated physical group");
        }
        final var expected = new LogicalStreamRecord(stream, group, genesis.activation(), maximumOutcomes, maximumPayloadBytes);
        final var result = container.createRegisterIfAbsent(streamName(stream), new BytesArray(expected.encode()));
        if (result.applied()) {
            return expected;
        }
        final var current = readStream(stream).orElseThrow(() -> new IOException("logical assignment disappeared after creation conflict"));
        if (current.equals(expected) == false) {
            throw new IOException("logical activation is permanently assigned to different history or protocol limits");
        }
        return current;
    }

    /**
     * Reads one immutable assignment by activation UUID without enumerating shards or groups.
     * The host must use a bounded working-set cache; no resident row is required for cold streams.
     * An absent assignment never authorizes recovery of a previously activated stream from scratch.
     * @param stream exact customer stream identity expected at this key
     * @return immutable assignment, or empty before initial reservation
     * @throws IOException on key/namespace mismatch, missing group lineage, corruption or storage failure
     */
    public Optional<LogicalStreamRecord> readStream(ShardLogId stream) throws IOException {
        requireStreamNamespace(stream);
        ensureNamespace(false);
        final var register = container.readRegister(streamName(stream));
        if (register.isEmpty()) {
            return Optional.empty();
        }
        final LogicalStreamRecord record;
        try (var in = register.get().value().streamInput()) {
            record = LogicalStreamRecord.read(in);
        }
        if (record.stream().equals(stream) == false) {
            throw new IOException("logical assignment key or namespace differs from its contents");
        }
        final var genesis = readRequired(record.group());
        if (record.groupActivation().equals(genesis.activation()) == false) {
            throw new IOException("logical assignment differs from durable physical activation");
        }
        return Optional.of(record);
    }

    private void requireStreamNamespace(ShardLogId stream) throws IOException {
        if (Objects.requireNonNull(stream).deploymentId().equals(deploymentId) == false) {
            throw new IOException("logical assignment differs from catalog deployment");
        }
    }

    private static String streamName(ShardLogId stream) {
        // Reusing an activation UUID for another shard is a conflict, not a second namespace.
        return "stream-" + stream.activationId();
    }

    /**
     * Validates every assigned reservation before touching a local volume or starting replication.
     * Bootstrap is refused once the catalog is active; recovery can use the original reserved
     * placement while the native journal checks its volume identity and persisted history.
     * @param configuration assigned static placement and startup option
     * @param keyManager journal member credentials
     * @param trustManager deployment-scoped trust
     * @param diskSpace actual-volume disk probe
     * @return local journal with an exclusively leased volume
     * @throws IOException on missing/conflicting reservation or journal startup failure
     */
    public RatisJournal openJournal(
        JournalConfiguration configuration,
        X509KeyManager keyManager,
        X509TrustManager trustManager,
        RatisJournal.DiskSpaceProbe diskSpace
    ) throws IOException {
        ensureNamespace(false);
        if (configuration.deploymentId().equals(deploymentId) == false) {
            throw new IOException("journal configuration differs from catalog deployment");
        }
        final Map<UUID, JournalMembershipRecord> memberships = new HashMap<>();
        for (var group : configuration.groups()) {
            final var current = readRequired(group.id());
            requirePlacement(current, new JournalGroupRecord(deploymentId, group, null));
            if (configuration.bootstrap() && current.activation() != null) {
                throw new IOException("an activated physical group cannot bootstrap a new independent history");
            }
            if (current.activation() != null) {
                final var membership = readMembership(current);
                if (membership.pending() != null) {
                    throw new IOException("journal membership requires replacement reconciliation before static startup");
                }
                memberships.put(group.id(), membership);
            }
        }
        return configuration.bootstrap()
            ? RatisJournal.open(configuration, keyManager, trustManager, diskSpace)
            : RatisJournal.recoverMembership(configuration, keyManager, trustManager, diskSpace, memberships);
    }

    /**
     * Publishes the original committed activation marker from a local authenticated journal.
     * Invoke only after its activation future completes, on a controller thread. The returned
     * active record is required before assigning logical shards to this group. A lost S3 reply
     * is reconciled by retrying this method; no new activation or voter identity is invented.
     * @param journal authorized running journal with its activation barrier committed
     * @param group physical group identity
     * @return durable active reservation
     * @throws IOException on missing activation, conflicting placement, CAS or storage failure
     */
    public JournalGroupRecord publishActivation(RatisJournal journal, UUID group) throws IOException {
        ensureNamespace(false);
        final var proof = journal.groupActivation(group);
        final var expected = new JournalGroupRecord(proof.deploymentId(), proof.initialPlacement(), null);
        final var register = container.readRegister(name(group))
            .orElseThrow(() -> new IOException("physical group has no durable bootstrap reservation"));
        final var current = decode(group, register);
        requirePlacement(current, expected);
        final JournalGroupRecord active;
        try {
            active = current.activate(proof.checkpoint());
        } catch (IllegalArgumentException failure) {
            throw new IOException("committed activation differs from reserved group lineage", failure);
        }
        if (current.activation() != null) {
            readMembership(active);
            return active;
        }
        // Publish the permanent membership head before activation becomes durable. Once ACTIVE,
        // absence must fail closed: recreating revision zero could erase an earlier replacement.
        final String membershipKey = membershipName(group);
        container.createRegisterIfAbsent(membershipKey, new BytesArray(JournalMembershipRecord.initial(active).encode()));
        readMembership(active);
        final var published = container.compareAndSwapRegister(name(group), register.generation(), new BytesArray(active.encode()));
        if (published.applied()) {
            return active;
        }
        final var observed = readRequired(group);
        if (observed.equals(active)) {
            return observed;
        }
        throw new IOException("journal activation publication conflicted; reconcile the durable record before retrying");
    }

    /**
     * Reads durable controller membership intent independently of native consensus state. An
     * activated group must have the head created before activation publication. A missing head
     * never reconstructs revision zero. This is not read authority.
     * @param group physical group identity
     * @return completed controller placement and any unresolved replacement
     * @throws IOException on missing activation, corruption or namespace/lineage mismatch
     */
    public JournalMembershipRecord membership(UUID group) throws IOException {
        return readMembership(readRequired(group));
    }

    /**
     * Persists a serialized replacement intent before any native membership operation. The full
     * source/target placement and nonce survive lost replies and controller restart. This method
     * does not start a learner or promote it. Pending intents cannot be canceled or overwritten;
     * native reconciliation and retirement fencing must complete before another change is allowed.
     * @param group physical group identity
     * @param expectedRevision last completed controller revision
     * @param operation unique replacement identity, reused on retries
     * @param target three-voter target with exactly one replacement
     * @return durable pending record
     * @throws IOException on stale/conflicting intent, inactive group, corruption or storage failure
     */
    public JournalMembershipRecord prepareReplacement(UUID group, long expectedRevision, UUID operation, JournalConfiguration.Group target)
        throws IOException {
        final var genesis = readRequired(group);
        final String key = membershipName(group);
        if (genesis.activation() == null) {
            throw new IOException("journal membership requires a durably published activation");
        }
        final var currentRegister = container.readRegister(key)
            .orElseThrow(() -> new IOException("activated journal membership head is missing; refusing revision reset"));
        final var current = decodeMembership(genesis, currentRegister);
        validateCompletedIdentities(current);
        final JournalMembershipRecord proposed;
        try {
            proposed = current.begin(expectedRevision, operation, target);
        } catch (IllegalArgumentException failure) {
            throw new IOException("journal replacement conflicts with durable membership", failure);
        }
        reserveReplacementIdentity(proposed, current.pending() != null);
        if (current.equals(proposed)) {
            return current;
        }
        if (container.compareAndSwapRegister(key, currentRegister.generation(), new BytesArray(proposed.encode())).applied()) {
            return proposed;
        }
        final var observed = readMembership(genesis);
        if (observed.equals(proposed)) {
            return observed;
        }
        throw new IOException("journal replacement publication conflicted; reconcile its durable intent before retrying");
    }

    /**
     * Opens assignments using current controller heads and permanent learner reservations. Unlike
     * static startup, this explicitly reconciles pending transitions without changing disk identity.
     * @param configuration original genesis assignments and optional learner placement
     * @param keyManager member credentials
     * @param trustManager deployment-scoped trust
     * @param diskSpace actual-volume reserve probe
     * @return running authenticated journal
     * @throws IOException on missing/conflicting authorization or startup failure
     */
    public RatisJournal openAssignedJournal(
        JournalConfiguration configuration,
        X509KeyManager keyManager,
        X509TrustManager trustManager,
        RatisJournal.DiskSpaceProbe diskSpace
    ) throws IOException {
        ensureNamespace(false);
        if (configuration.deploymentId().equals(deploymentId) == false) {
            throw new IOException("journal configuration differs from catalog deployment");
        }
        final var memberships = new HashMap<UUID, JournalMembershipRecord>();
        final var joins = new HashMap<UUID, JournalMembershipRecord>();
        for (var group : configuration.groups()) {
            final var genesis = readRequired(group.id());
            requirePlacement(genesis, new JournalGroupRecord(deploymentId, group, null));
            if (genesis.activation() == null) {
                continue;
            }
            final var head = readMembership(genesis);
            memberships.put(group.id(), head);
            if (group.peers().stream().noneMatch(peer -> peer.id().equals(configuration.localVoterId()))) {
                final String key = "replacement-" + group.id() + "-" + configuration.localVoterId();
                final var register = container.readRegister(key)
                    .orElseThrow(() -> new IOException("learner has no permanent original replacement reservation"));
                joins.put(group.id(), decodeMembership(genesis, register));
            }
        }
        return RatisJournal.openAssigned(configuration, keyManager, trustManager, diskSpace, memberships, joins);
    }

    /**
     * Publishes native completion proof before advancing the durable membership head. A lost reply
     * is reconciled from the permanent receipt, including after later replacements have completed.
     * No caller-supplied receipt is accepted. Run on the bounded controller executor.
     * @param journal authenticated journal containing the committed native transition
     * @param intent original durable pending head
     * @return permanent historical transition receipt
     * @throws IOException on missing proof, conflicting intent or conditional storage failure
     */
    public JournalReplacementReceipt publishReplacement(RatisJournal journal, JournalMembershipRecord intent) throws IOException {
        final UUID group = intent.genesis().initialPlacement().id();
        final var genesis = readRequired(group);
        if (intent.pending() == null || intent.genesis().equals(genesis) == false) {
            throw new IOException("replacement publication differs from the permanent activated group");
        }
        reserveReplacementIdentity(intent, true);
        final String receiptKey = completionName(group, intent.pending().id());
        final var existing = container.readRegister(receiptKey);
        final JournalReplacementReceipt receipt;
        if (existing.isPresent()) {
            receipt = decodeReceipt(genesis, intent.pending().id(), existing.get());
            if (receipt.intent().equals(intent) == false) {
                throw new IOException("replacement operation is permanently bound to another intent");
            }
        } else {
            if (readMembership(genesis).equals(intent) == false) {
                throw new IOException("replacement has no published receipt and is no longer the pending head");
            }
            final var proof = journal.replacementReceipt(intent)
                .orElseThrow(() -> new IOException("replacement target has not been committed and applied locally"));
            if (container.createRegisterIfAbsent(receiptKey, new BytesArray(proof.encode())).applied()) {
                receipt = proof;
            } else {
                final var observed = container.readRegister(receiptKey)
                    .orElseThrow(() -> new IOException("replacement receipt disappeared during publication"));
                receipt = decodeReceipt(genesis, intent.pending().id(), observed);
                if (receipt.intent().equals(proof.intent()) == false
                    || receipt.learnerBarrierIndex() != proof.learnerBarrierIndex()
                    || receipt.learnerBarrierTerm() != proof.learnerBarrierTerm()) {
                    throw new IOException("replacement proof conflicts with its permanent receipt");
                }
            }
        }
        final String key = membershipName(group);
        final var register = container.readRegister(key).orElseThrow(() -> new IOException("activated journal membership head is missing"));
        final var current = decodeMembership(genesis, register);
        final var completed = receipt.completedMembership();
        if (current.revision() >= completed.revision()) {
            readMembership(genesis);
            if (current.revision() == completed.revision() && current.voters().equals(completed.voters()) == false) {
                throw new IOException("completed replacement receipt conflicts with the current voters");
            }
            return finishReplacement(journal, receipt);
        }
        if (current.equals(intent) == false) {
            throw new IOException("replacement receipt cannot advance a different pending head");
        }
        if (container.compareAndSwapRegister(key, register.generation(), new BytesArray(completed.encode())).applied()) {
            return finishReplacement(journal, receipt);
        }
        final var observed = readMembership(genesis);
        if (observed.revision() >= completed.revision()
            && (observed.revision() > completed.revision() || observed.voters().equals(completed.voters()))) {
            return finishReplacement(journal, receipt);
        }
        throw new IOException("replacement completion conflicted; reconcile the durable head before retrying");
    }

    private static JournalReplacementReceipt finishReplacement(RatisJournal journal, JournalReplacementReceipt receipt) throws IOException {
        final var intent = receipt.intent();
        final var removed = intent.voters()
            .peers()
            .stream()
            .filter(peer -> intent.pending().target().peers().stream().noneMatch(current -> current.id().equals(peer.id())))
            .findFirst()
            .orElseThrow();
        journal.releaseUnusedPeer(removed.id());
        return receipt;
    }

    private static String completionName(UUID group, UUID operation) {
        return "completed-" + group + "-" + operation;
    }

    private static JournalReplacementReceipt decodeReceipt(JournalGroupRecord genesis, UUID operation, BlobRegister register)
        throws IOException {
        final JournalReplacementReceipt receipt;
        try (var in = register.value().streamInput()) {
            receipt = JournalReplacementReceipt.read(in);
        }
        if (receipt.intent().genesis().equals(genesis) == false || receipt.intent().pending().id().equals(operation) == false) {
            throw new IOException("replacement receipt differs from its immutable group or operation identity");
        }
        return receipt;
    }

    private void reserveReplacementIdentity(JournalMembershipRecord proposed, boolean alreadyPending) throws IOException {
        final String operationKey = "transition-" + proposed.voters().id() + "-" + proposed.pending().id();
        if (alreadyPending == false) {
            final var created = container.createRegisterIfAbsent(operationKey, new BytesArray(proposed.encode()));
            if (created.applied() == false) {
                requireOperationReservation(proposed, operationKey);
            }
        } else {
            requireOperationReservation(proposed, operationKey);
        }
        final var replacement = proposed.pending()
            .target()
            .peers()
            .stream()
            .filter(peer -> proposed.voters().peers().stream().noneMatch(old -> old.id().equals(peer.id())))
            .findFirst()
            .orElseThrow();
        final String key = "replacement-" + proposed.voters().id() + "-" + replacement.id();
        if (alreadyPending == false && container.createRegisterIfAbsent(key, new BytesArray(proposed.encode())).applied()) {
            return;
        }
        final var reservation = container.readRegister(key)
            .orElseThrow(() -> new IOException("pending replacement voter identity reservation is missing"));
        if (decodeMembership(proposed.genesis(), reservation).equals(proposed) == false) {
            throw new IOException("replacement voter identity is permanently reserved for a different intent");
        }
    }

    private void requireOperationReservation(JournalMembershipRecord proposed, String key) throws IOException {
        final var register = container.readRegister(key)
            .orElseThrow(() -> new IOException("replacement operation nonce reservation is missing"));
        if (decodeMembership(proposed.genesis(), register).equals(proposed) == false) {
            throw new IOException("replacement operation nonce is permanently reserved for another intent");
        }
    }

    private JournalMembershipRecord readMembership(JournalGroupRecord genesis) throws IOException {
        if (genesis.activation() == null) {
            throw new IOException("journal membership requires a durably published activation");
        }
        final var register = container.readRegister(membershipName(genesis.initialPlacement().id()))
            .orElseThrow(() -> new IOException("activated journal membership head is missing; refusing revision reset"));
        final var record = decodeMembership(genesis, register);
        validateCompletedIdentities(record);
        if (record.pending() != null) {
            reserveReplacementIdentity(record, true);
        }
        return record;
    }

    private void validateCompletedIdentities(JournalMembershipRecord current) throws IOException {
        for (var peer : current.voters().peers()) {
            if (current.genesis().initialPlacement().peers().stream().anyMatch(original -> original.id().equals(peer.id()))) {
                continue; // The codec verifies exact genesis placement; the immutable reservation binds it.
            }
            final String key = "replacement-" + current.voters().id() + "-" + peer.id();
            final var register = container.readRegister(key)
                .orElseThrow(() -> new IOException("completed membership voter identity reservation is missing"));
            // Decode only: recursively validating this historical source could grow with history.
            // Its immutable genesis, revision and exact new peer are sufficient for identity binding.
            final var reservation = decodeMembership(current.genesis(), register);
            if (reservation.pending() == null
                || reservation.revision() >= current.revision()
                || reservation.voters().peers().stream().anyMatch(old -> old.id().equals(peer.id()))
                || reservation.pending().target().peers().contains(peer) == false) {
                throw new IOException("completed membership voter differs from its permanent identity reservation");
            }
        }
    }

    private static JournalMembershipRecord decodeMembership(JournalGroupRecord genesis, BlobRegister register) throws IOException {
        final JournalMembershipRecord record;
        try (var in = register.value().streamInput()) {
            record = JournalMembershipRecord.read(in);
        }
        if (record.genesis().equals(genesis) == false) {
            throw new IOException("journal membership differs from permanent genesis reservation");
        }
        return record;
    }

    private static String membershipName(UUID group) {
        return "membership-" + Objects.requireNonNull(group);
    }

    private JournalGroupRecord readRequired(UUID group) throws IOException {
        return read(group).orElseThrow(() -> new IOException("physical group has no durable bootstrap reservation"));
    }

    private JournalGroupRecord decode(UUID group, BlobRegister register) throws IOException {
        final JournalGroupRecord record;
        try (var in = register.value().streamInput()) {
            record = JournalGroupRecord.read(in);
        }
        if (record.deploymentId().equals(deploymentId) == false || record.initialPlacement().id().equals(group) == false) {
            throw new IOException("journal reservation key or deployment differs from its contents");
        }
        return record;
    }

    private static void requirePlacement(JournalGroupRecord current, JournalGroupRecord expected) throws IOException {
        if (current.deploymentId().equals(expected.deploymentId()) == false
            || current.initialPlacement().equals(expected.initialPlacement()) == false) {
            throw new IOException("physical group identity is permanently reserved for a different voter placement");
        }
    }

    private void ensureNamespace(boolean create) throws IOException {
        if (namespaceVerified) {
            return;
        }
        final var expected = new BytesArray(deploymentId.toString());
        var current = container.readRegister(DEPLOYMENT);
        if (current.isEmpty() && create) {
            if (container.createRegisterIfAbsent(DEPLOYMENT, expected).applied()) {
                namespaceVerified = true;
                return;
            }
            current = container.readRegister(DEPLOYMENT);
        }
        if (current.isEmpty() || expected.equals(current.get().value()) == false) {
            throw new IOException("journal catalog deployment identity is missing or differs from configuration");
        }
        namespaceVerified = true;
    }

    private static String name(UUID group) {
        return "group-" + Objects.requireNonNull(group);
    }
}
