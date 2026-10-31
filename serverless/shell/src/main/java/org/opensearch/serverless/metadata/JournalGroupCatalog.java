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
import org.opensearch.serverless.log.LogCheckpoint;
import org.opensearch.serverless.log.RatisJournal;

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
        final Map<UUID, LogCheckpoint> requiredActivations = new HashMap<>();
        for (var group : configuration.groups()) {
            final var current = readRequired(group.id());
            requirePlacement(current, new JournalGroupRecord(deploymentId, group, null));
            if (configuration.bootstrap() && current.activation() != null) {
                throw new IOException("an activated physical group cannot bootstrap a new independent history");
            }
            if (current.activation() != null) {
                final var membership = readMembership(current);
                if (membership.revision() != 0 || membership.pending() != null) {
                    throw new IOException("journal membership requires replacement reconciliation before static startup");
                }
                requiredActivations.put(group.id(), current.activation());
            }
        }
        return RatisJournal.open(configuration, keyManager, trustManager, diskSpace, requiredActivations);
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

    private void reserveReplacementIdentity(JournalMembershipRecord proposed, boolean alreadyPending) throws IOException {
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

    private JournalMembershipRecord readMembership(JournalGroupRecord genesis) throws IOException {
        if (genesis.activation() == null) {
            throw new IOException("journal membership requires a durably published activation");
        }
        final var register = container.readRegister(membershipName(genesis.initialPlacement().id()))
            .orElseThrow(() -> new IOException("activated journal membership head is missing; refusing revision reset"));
        final var record = decodeMembership(genesis, register);
        if (record.pending() != null) {
            reserveReplacementIdentity(record, true);
        }
        return record;
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
