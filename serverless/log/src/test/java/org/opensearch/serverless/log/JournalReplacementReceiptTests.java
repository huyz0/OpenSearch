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
import java.util.Arrays;

public class JournalReplacementReceiptTests extends OpenSearchTestCase {
    public void testBoundedReceiptRetainsExactIntentAndAdvancesOnlyOneRevision() throws Exception {
        final var pending = JournalStartupTests.pending();
        final var receipt = new JournalReplacementReceipt(pending, 10, 2, 12, 2);
        assertEquals(receipt, JournalReplacementReceipt.read(new ByteArrayInputStream(receipt.encode())));
        assertTrue(receipt.encode().length <= JournalReplacementReceipt.MAX_BYTES);
        final var completed = receipt.completedMembership();
        assertEquals(pending.genesis(), completed.genesis());
        assertEquals(pending.revision() + 1, completed.revision());
        assertEquals(pending.pending().target(), completed.voters());
        assertNull(completed.pending());
    }

    public void testInvalidChronologyOrUnresolvedIntentIsRefused() {
        final var pending = JournalStartupTests.pending();
        expectThrows(IllegalArgumentException.class, () -> new JournalReplacementReceipt(pending, 2, 1, 4, 1));
        expectThrows(IllegalArgumentException.class, () -> new JournalReplacementReceipt(pending, 3, 0, 4, 1));
        expectThrows(IllegalArgumentException.class, () -> new JournalReplacementReceipt(pending, 3, 1, 3, 1));
        expectThrows(IllegalArgumentException.class, () -> new JournalReplacementReceipt(pending, 3, 2, 4, 1));
        expectThrows(
            IllegalArgumentException.class,
            () -> new JournalReplacementReceipt(JournalMembershipRecord.initial(pending.genesis()), 3, 1, 4, 1)
        );
    }

    public void testCorruptionTruncationAndExcessiveFramesFailClosed() throws Exception {
        final byte[] frame = new JournalReplacementReceipt(JournalStartupTests.pending(), 10, 2, 12, 3).encode();
        for (int length = 0; length < frame.length; length++) {
            final byte[] truncated = Arrays.copyOf(frame, length);
            expectThrows(IOException.class, () -> JournalReplacementReceipt.read(new ByteArrayInputStream(truncated)));
        }
        frame[frame.length / 2] ^= 1;
        expectThrows(IOException.class, () -> JournalReplacementReceipt.read(new ByteArrayInputStream(frame)));
        final var excessive = new ByteArrayInputStream(new byte[JournalReplacementReceipt.MAX_BYTES + 100]);
        expectThrows(IOException.class, () -> JournalReplacementReceipt.read(excessive));
        assertEquals(99, excessive.available());
    }
}
