/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;

public class JournalMembershipRecordTests extends OpenSearchTestCase {
    public void testCanonicalBoundedRoundTripAndIdempotentRetry() throws Exception {
        final var initial = initial();
        final var target = replacement(initial);
        final UUID operation = UUID.randomUUID();
        final var pending = initial.begin(0, operation, target);
        final var reversed = new ArrayList<>(target.peers());
        Collections.reverse(reversed);
        assertEquals(pending, pending.begin(0, operation, new JournalConfiguration.Group(target.id(), reversed)));
        assertEquals(pending, JournalMembershipRecord.read(new ByteArrayInputStream(pending.encode())));
        assertEquals(initial, JournalMembershipRecord.read(new ByteArrayInputStream(initial.encode())));
        assertTrue(pending.encode().length <= JournalMembershipRecord.MAX_BYTES);
        assertEquals(initial.genesis(), pending.genesis());
        assertEquals(0, pending.revision());
        assertEquals(initial.voters(), pending.voters());
    }

    public void testUnresolvedIntentCannotBeOverwrittenByAnotherController() {
        final var initial = initial();
        final UUID operation = UUID.randomUUID();
        final var pending = initial.begin(0, operation, replacement(initial));
        expectThrows(IllegalArgumentException.class, () -> pending.begin(0, UUID.randomUUID(), pending.pending().target()));
        expectThrows(IllegalArgumentException.class, () -> pending.begin(0, operation, replacement(initial)));
        expectThrows(IllegalArgumentException.class, () -> pending.begin(1, operation, pending.pending().target()));
        expectThrows(IllegalArgumentException.class, () -> initial.begin(-1, operation, replacement(initial)));
    }

    public void testCannotChangeMultipleVotersOrRebindAnIdentityOrAddress() {
        final var initial = initial();
        final var target = replacement(initial);
        final var peers = new ArrayList<>(target.peers());
        final var retained = peers.getFirst();
        peers.set(0, new JournalConfiguration.Peer(retained.id(), "another-host", retained.port(), retained.availabilityZone()));
        expectThrows(
            IllegalArgumentException.class,
            () -> initial.begin(0, UUID.randomUUID(), new JournalConfiguration.Group(target.id(), peers))
        );
        peers.set(0, new JournalConfiguration.Peer(UUID.randomUUID(), retained.host(), retained.port(), retained.availabilityZone()));
        expectThrows(
            IllegalArgumentException.class,
            () -> initial.begin(0, UUID.randomUUID(), new JournalConfiguration.Group(target.id(), peers))
        );
        final var multiple = new ArrayList<>(target.peers());
        multiple.set(0, new JournalConfiguration.Peer(UUID.randomUUID(), "second-new-host", 9600, retained.availabilityZone()));
        expectThrows(
            IllegalArgumentException.class,
            () -> initial.begin(0, UUID.randomUUID(), new JournalConfiguration.Group(target.id(), multiple))
        );
        expectThrows(IllegalArgumentException.class, () -> initial.begin(0, UUID.randomUUID(), initial.voters()));
        expectThrows(
            IllegalArgumentException.class,
            () -> initial.begin(0, UUID.randomUUID(), new JournalConfiguration.Group(UUID.randomUUID(), target.peers()))
        );
    }

    public void testActivationAndRevisionAreRequiredAndCannotOverflow() {
        final var initial = initial();
        final var genesis = initial.genesis();
        expectThrows(
            IllegalArgumentException.class,
            () -> JournalMembershipRecord.initial(new JournalGroupRecord(genesis.deploymentId(), genesis.initialPlacement(), null))
        );
        expectThrows(IllegalArgumentException.class, () -> new JournalMembershipRecord(genesis, -1, initial.voters(), null));
        expectThrows(IllegalArgumentException.class, () -> new JournalMembershipRecord(genesis, 0, replacement(initial), null));
        final var exhausted = new JournalMembershipRecord(genesis, Long.MAX_VALUE, initial.voters(), null);
        expectThrows(IllegalArgumentException.class, () -> exhausted.begin(Long.MAX_VALUE, UUID.randomUUID(), replacement(initial)));
    }

    public void testCorruptionEveryTruncationAndOversizedFramesAreRejected() throws Exception {
        final var initial = initial();
        final byte[] encoded = initial.begin(0, UUID.randomUUID(), replacement(initial)).encode();
        for (int length = 0; length < encoded.length; length++) {
            final byte[] truncated = Arrays.copyOf(encoded, length);
            expectThrows(IOException.class, () -> JournalMembershipRecord.read(new ByteArrayInputStream(truncated)));
        }
        for (int offset : List.of(0, 4, 12, encoded.length / 2, encoded.length - 1)) {
            final byte[] corrupt = encoded.clone();
            corrupt[offset] ^= 1;
            expectThrows(IOException.class, () -> JournalMembershipRecord.read(new ByteArrayInputStream(corrupt)));
        }
        final var excessive = new ByteArrayInputStream(new byte[JournalMembershipRecord.MAX_BYTES + 100]);
        expectThrows(IOException.class, () -> JournalMembershipRecord.read(excessive));
        assertEquals(99, excessive.available());
    }

    public void testValidChecksumDoesNotPermitUnknownVersionsLengthsOrStates() throws Exception {
        final byte[] encoded = initial().encode();
        final byte[] version = encoded.clone();
        ByteBuffer.wrap(version).putInt(4, 2);
        checksum(version);
        expectThrows(IOException.class, () -> JournalMembershipRecord.read(new ByteArrayInputStream(version)));
        final byte[] length = encoded.clone();
        ByteBuffer.wrap(length).putInt(8, Integer.MAX_VALUE);
        checksum(length);
        expectThrows(IOException.class, () -> JournalMembershipRecord.read(new ByteArrayInputStream(length)));
        final byte[] state = encoded.clone();
        state[state.length - 5] = 2;
        checksum(state);
        expectThrows(IOException.class, () -> JournalMembershipRecord.read(new ByteArrayInputStream(state)));
        final byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        checksum(trailing);
        expectThrows(IOException.class, () -> JournalMembershipRecord.read(new ByteArrayInputStream(trailing)));
    }

    private static JournalMembershipRecord initial() {
        final var peers = new ArrayList<JournalConfiguration.Peer>();
        for (int i = 0; i < 3; i++) {
            peers.add(new JournalConfiguration.Peer(UUID.randomUUID(), "h".repeat(253), 9000 + i, "界".repeat(255) + i));
        }
        final UUID deployment = UUID.randomUUID();
        final var placement = new JournalConfiguration.Group(UUID.randomUUID(), peers);
        final var activation = new LogCheckpoint(JournalGroupRecord.controlIdentity(deployment, placement.id()), 2, 1);
        return JournalMembershipRecord.initial(new JournalGroupRecord(deployment, placement, activation));
    }

    private static JournalConfiguration.Group replacement(JournalMembershipRecord record) {
        final var peers = new ArrayList<>(record.voters().peers());
        final var removed = peers.removeLast();
        peers.add(new JournalConfiguration.Peer(UUID.randomUUID(), "replacement-host", 9500, removed.availabilityZone()));
        return new JournalConfiguration.Group(record.voters().id(), peers);
    }

    private static void checksum(byte[] frame) {
        final var crc = new CRC32C();
        crc.update(frame, 0, frame.length - 4);
        ByteBuffer.wrap(frame).putInt(frame.length - 4, (int) crc.getValue());
    }
}
