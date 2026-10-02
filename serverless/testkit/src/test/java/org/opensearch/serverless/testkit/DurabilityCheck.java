/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.WalRecord;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Decides from the object store alone whether acknowledged writes are durable: in the published commit, or in the log
 * a successor would replay.
 *
 * <p><b>Why the store and not a node.</b> A read through the fleet asks whoever owns the shard, and a shard changing
 * hands answers "retry" -- or a reader behind its log refuses -- for as long as the move takes. A fleet run's read-back
 * gave up on shards in that state and reported their writes lost: 16 in one run, 399 in another, every one of which was
 * later found by taking the shard and replaying it by hand. What survives a crash is the commit and the log, so that is
 * what is read, and it needs no ownership: a handover cannot hide a write from it.
 *
 * <p><b>The order is the guarantee.</b> The log is read before the commit. A publish writes its manifest before it
 * truncates the log, so a record truncated before the log was read is covered by a manifest at least as new as the one
 * read afterwards; a record truncated after is in the log as read. A blob that vanishes between a listing and its read
 * fails the attempt, which is retried.
 *
 * <p><b>Three answers, never two.</b> {@link Status#VERIFIED} when the write was found; {@link Status#LOST} only when
 * both the log and the commit were read and it is in neither -- with what was read; {@link Status#UNREACHED} when the
 * store could not be read by the deadline. Unreached is not lost: a run that has any is inconclusive, never failed for
 * loss.
 */
final class DurabilityCheck {

    enum Status {
        VERIFIED,
        LOST,
        UNREACHED
    }

    /** One write's verdict, and what it rests on. */
    record Finding(String index, String id, Status status, String evidence) {
    }

    private final MetadataPlane plane;
    private final Path scratch;

    DurabilityCheck(MetadataPlane plane, Path scratch) {
        this.plane = plane;
        this.scratch = scratch;
    }

    /**
     * Checks one index's writes, retrying a failed read until the deadline.
     *
     * @param index the index
     * @param ids the acknowledged writes' ids
     * @param timeoutMillis how long a read may keep failing before the answer is unreached
     * @return one finding per id
     */
    Map<String, Finding> check(String index, Collection<String> ids, long timeoutMillis) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        Exception last = null;
        for (int attempt = 0; System.nanoTime() < deadline; attempt++) {
            try {
                return checkOnce(index, ids);
            } catch (Exception e) {
                last = e;
                final long backoff = Math.min(5_000L, 100L << Math.min(attempt, 6));
                try {
                    Thread.sleep(Math.max(1L, Math.min(backoff, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()))));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        final Map<String, Finding> unreached = new LinkedHashMap<>();
        final String why = "the store could not be read within " + timeoutMillis + " ms: " + last;
        for (String id : ids) {
            unreached.put(id, new Finding(index, id, Status.UNREACHED, why));
        }
        return unreached;
    }

    private Map<String, Finding> checkOnce(String index, Collection<String> ids) throws Exception {
        final Optional<IndexDescriptor> descriptor = plane.describe(index);
        final Map<String, Finding> findings = new LinkedHashMap<>();
        if (descriptor.isEmpty()) {
            for (String id : ids) {
                findings.put(id, new Finding(index, id, Status.LOST, "the index no longer exists"));
            }
            return findings;
        }
        final String uuid = descriptor.get().uuid();
        final int shards = descriptor.get().numberOfShards();
        final Set<String> remaining = new HashSet<>(ids);
        final List<String> read = new ArrayList<>();

        // The log first, every shard: what a successor would replay.
        for (int shard = 0; shard < shards; shard++) {
            final List<WalRecord> records = plane.walStore(index, uuid, shard).replayableAfterFencing();
            long minSeqNo = Long.MAX_VALUE;
            long maxSeqNo = -1L;
            final Set<Long> terms = new java.util.TreeSet<>();
            for (WalRecord record : records) {
                if (record.isDeletion() == false && remaining.remove(record.id())) {
                    findings.put(record.id(), new Finding(index, record.id(), Status.VERIFIED, "in the log of shard " + shard));
                }
                if (record.hasSequenceIdentity()) {
                    minSeqNo = Math.min(minSeqNo, record.seqNo());
                    maxSeqNo = Math.max(maxSeqNo, record.seqNo());
                    terms.add(record.primaryTerm());
                }
            }
            read.add(
                "shard "
                    + shard
                    + " log: "
                    + records.size()
                    + " replayable records"
                    + (records.isEmpty() ? "" : ", seqNo " + minSeqNo + ".." + maxSeqNo + " in terms " + terms)
            );
        }

        // Then the published commit of every shard, for what the log no longer holds.
        for (int shard = 0; shard < shards && remaining.isEmpty() == false; shard++) {
            final var publisher = plane.segmentPublisher(index, uuid, shard);
            final Optional<CommitManifest> manifest = publisher.readManifest();
            final var head = plane.heads().read(index, shard);
            if (manifest.isEmpty()) {
                read.add("shard " + shard + " commit: none published; head " + head);
                continue;
            }
            read.add(
                "shard "
                    + shard
                    + " commit: term "
                    + manifest.get().term()
                    + ", log ordinal "
                    + manifest.get().walOrdinal()
                    + ", "
                    + manifest.get().files().size()
                    + " files; head "
                    + head
            );
            final Path dir = Files.createTempDirectory(scratch, "commit-");
            try {
                try (FSDirectory directory = FSDirectory.open(dir)) {
                    publisher.restoreInto(directory, new org.opensearch.core.index.shard.ShardId(index, uuid, shard));
                    try (DirectoryReader reader = DirectoryReader.open(directory)) {
                        final IndexSearcher searcher = new IndexSearcher(reader);
                        for (String id : new ArrayList<>(remaining)) {
                            if (searcher.count(new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(id)))) > 0) {
                                remaining.remove(id);
                                findings.put(id, new Finding(index, id, Status.VERIFIED, "in the published commit of shard " + shard));
                            }
                        }
                    }
                }
            } finally {
                IOUtils.rm(dir);
            }
        }

        final String evidence = String.join("; ", read);
        for (String id : remaining) {
            findings.put(id, new Finding(index, id, Status.LOST, "in neither the log nor the commit: " + evidence));
        }
        return findings;
    }

}
