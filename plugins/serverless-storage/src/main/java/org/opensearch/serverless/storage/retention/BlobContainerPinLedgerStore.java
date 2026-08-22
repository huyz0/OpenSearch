/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Where {@link PinLedger} entries live: one blob per pin id, in the index's shard 0 container.
 *
 * <h2>Shard 0, and why that is a wart rather than a design</h2>
 *
 * The ledger is a per-index record and it is stored in a per-shard container, because a per-shard container
 * is the only stable location this plugin exposes for an index. Named here rather than hidden: an index
 * whose shard 0 container is unreachable cannot have its ledger read even though its other shards are fine.
 * The alternative -- a per-index container -- is a storage-layout change, and the ledger is worth having
 * before that is worth doing.
 *
 * <h2>Atomic writes, unconditional deletes</h2>
 *
 * Written with {@code writeBlobAtomic} so a reader never sees a half-written entry, for the same reason the
 * manifest store does it. Deleted with the ignoring-if-absent form, because deleting a ledger that is
 * already gone is the ordinary outcome of a retried release, not an error.
 */
public class BlobContainerPinLedgerStore {

    /** Prefix every ledger blob's name carries, so the set can be listed without knowing any pin id. */
    public static final String LEDGER_PREFIX = "pin-ledger-";

    private final BlobContainer blobContainer;

    /**
     * @param blobContainer the index's shard 0 container, which is where ledger entries are kept.
     */
    public BlobContainerPinLedgerStore(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /** Records that a pin exists. Overwrites any entry under the same pin id, which is what a re-pin means. */
    public void write(PinLedger ledger) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ledger.writeTo(out);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            try (InputStream in = new ByteArrayInputStream(bytes)) {
                blobContainer.writeBlobAtomic(blobName(ledger.pinId()), in, bytes.length, false);
            }
        }
    }

    /** The entry for this pin id, or empty if none was ever written or it has already been released. */
    public Optional<PinLedger> read(String pinId) throws IOException {
        try (InputStream in = blobContainer.readBlob(blobName(pinId)); StreamInput stream = StreamInput.wrap(in.readAllBytes())) {
            return Optional.of(new PinLedger(stream));
        } catch (java.nio.file.NoSuchFileException absent) {
            // The same discrimination BlobContainerManifestStore makes: an absent blob is an answer, and
            // only NoSuchFileException means absent -- anything else is a store problem and must surface.
            return Optional.empty();
        }
    }

    /**
     * Every ledger entry this index has, which is what a sweeper looking for abandoned pins reads.
     *
     * <p>An entry that cannot be deserialized is skipped rather than failing the listing: one unreadable
     * record must not hide every other outstanding pin from whoever is trying to clean up.
     */
    public List<PinLedger> list() throws IOException {
        List<PinLedger> ledgers = new ArrayList<>();
        for (String name : blobContainer.listBlobsByPrefix(LEDGER_PREFIX).keySet()) {
            try (InputStream in = blobContainer.readBlob(name); StreamInput stream = StreamInput.wrap(in.readAllBytes())) {
                ledgers.add(new PinLedger(stream));
            } catch (Exception skipUnreadable) {
                // Deliberately swallowed. See the javadoc: a listing that fails on one bad record is a
                // listing that cannot be used for cleanup, which is the only thing it is for.
            }
        }
        return ledgers;
    }

    /**
     * Removes the record, which must happen <em>after</em> the pins it names are released.
     *
     * <p>That ordering is the whole point of the ledger and is the same one core's shallow-copy delete
     * uses: if releasing fails partway, the record survives and the next attempt can finish the job. Delete
     * first and a failed release becomes an invisible leak.
     */
    public void delete(String pinId) throws IOException {
        blobContainer.deleteBlobsIgnoringIfNotExists(List.of(blobName(pinId)));
    }

    private static String blobName(String pinId) {
        return LEDGER_PREFIX + pinId;
    }
}
