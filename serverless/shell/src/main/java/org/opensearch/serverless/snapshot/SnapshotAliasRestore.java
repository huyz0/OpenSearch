/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.snapshot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.Names;
import org.opensearch.serverless.metadata.DescriptorResolution;
import org.opensearch.serverless.metadata.IndexAlreadyExistsException;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.repository.SnapshotAliases;
import org.opensearch.serverless.repository.SnapshotRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Brings back the aliases a snapshot recorded for the indices a restore created.
 *
 * <p>Planned before anything is written, applied after every restored index exists: one alias mutation for each
 * distinct alias, adding all of its restored members at once, so a later index's rollback never leaves an alias
 * pointing at a deleted index. An alias that cannot be added is reported, never a reason to delete an index whose data
 * is whole.
 *
 * <p>A restored member is an alias's write index only when the snapshot said it was <em>and</em> the alias has no write
 * target when the mutation reads it, explicit or implicit; of several restored members claiming it, the first in plan
 * order keeps it.
 */
public final class SnapshotAliasRestore {

    private static final Logger LOGGER = LogManager.getLogger(SnapshotAliasRestore.class);

    /** The most distinct aliases one restore adds: the take's bound, 100 indices of 64 aliases. */
    public static final int MAX_DISTINCT = 6_400;

    private static final int ATTEMPTS = 4;

    /** How long a foreground restore spends adding aliases; those not yet added when it ends are reported as failed. */
    private static final long BUDGET_NANOS = java.util.concurrent.TimeUnit.MINUTES.toNanos(2);

    /**
     * One restored index in one alias.
     *
     * @param index the restored index's name
     * @param write whether the snapshot recorded it as the alias's explicit write member
     */
    public record Member(String index, boolean write) {
    }

    /**
     * What a restore will add.
     *
     * @param byAlias each distinct alias name, after renaming, with its restored members in plan order
     * @param truncated the restored indices whose snapshot entry says it recorded only some of their aliases
     */
    record Plan(Map<String, List<Member>> byAlias, List<String> truncated) {
    }

    private SnapshotAliasRestore() {}

    /**
     * Plans the aliases of a restore, reading the record only; nothing is read from the store.
     *
     * @param record the snapshot
     * @param targets each restored index's name in the snapshot and the name it restores under
     * @param pattern the alias rename pattern, or null to keep the names
     * @param replacement the alias rename replacement
     * @return the plan
     * @throws IOException if a recorded alias field is not what a writer produces
     * @throws SnapshotHandler.InvalidAliasNameException if an alias name is not a name, or is a restored index's name
     * @throws SnapshotHandler.TooManyAliasesException if the restore would add more than {@link #MAX_DISTINCT} aliases
     */
    static Plan plan(SnapshotRecord record, Map<String, String> targets, Pattern pattern, String replacement) throws IOException,
        SnapshotHandler.InvalidAliasNameException, SnapshotHandler.TooManyAliasesException {
        final Map<String, List<Member>> byAlias = new LinkedHashMap<>();
        final List<String> truncated = new ArrayList<>();
        for (Map.Entry<String, String> restoring : targets.entrySet()) {
            final SnapshotAliases recorded = SnapshotAliases.read(record.indices().get(restoring.getKey()));
            if (recorded.truncated()) {
                truncated.add(restoring.getValue());
            }
            for (Map.Entry<String, Boolean> flag : recorded.writeFlags().entrySet()) {
                final String name;
                try {
                    name = pattern == null ? flag.getKey() : pattern.matcher(flag.getKey()).replaceAll(replacement);
                } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
                    throw new SnapshotHandler.InvalidAliasNameException("rename_alias_replacement cannot be applied: " + e.getMessage());
                }
                if (targets.containsValue(name)) {
                    throw new SnapshotHandler.InvalidAliasNameException(
                        "alias [" + name + "] has the name of an index this restore creates; a name cannot be both"
                    );
                }
                try {
                    Names.validateIndexOrAlias(name);
                } catch (IllegalArgumentException e) {
                    throw new SnapshotHandler.InvalidAliasNameException(e.getMessage());
                }
                if (name.getBytes(StandardCharsets.UTF_8).length > SnapshotAliases.MAX_NAME_BYTES) {
                    throw new SnapshotHandler.InvalidAliasNameException("alias name [" + name + "] is longer than 255 bytes");
                }
                final List<Member> members = byAlias.computeIfAbsent(name, key -> new ArrayList<>());
                final int existing = indexOf(members, restoring.getValue());
                if (existing >= 0) {
                    members.set(existing, new Member(restoring.getValue(), members.get(existing).write() || flag.getValue()));
                } else {
                    members.add(new Member(restoring.getValue(), flag.getValue()));
                }
                if (byAlias.size() > MAX_DISTINCT) {
                    throw new SnapshotHandler.TooManyAliasesException(MAX_DISTINCT);
                }
            }
        }
        return new Plan(byAlias, truncated);
    }

    private static int indexOf(List<Member> members, String index) {
        for (int i = 0; i < members.size(); i++) {
            if (members.get(i).index().equals(index)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Reads each planned alias once, before the first index is created, and refuses one that names an index or a data
     * stream: the mutation would refuse it later, with the restored indices already written.
     *
     * @param metadata the metadata plane
     * @param byAlias the planned aliases
     * @return what each alias is now, kept as the mutation's {@code before}
     * @throws IOException if a register cannot be read
     * @throws SnapshotHandler.InvalidAliasNameException if a planned alias is an index or a data stream
     */
    static Map<String, DescriptorResolution> resolve(MetadataPlane metadata, Map<String, List<Member>> byAlias) throws IOException,
        SnapshotHandler.InvalidAliasNameException {
        final Map<String, DescriptorResolution> before = new LinkedHashMap<>();
        for (String name : byAlias.keySet()) {
            final DescriptorResolution resolved = metadata.resolveForAliasMutation(name);
            if (resolved.index() != null || (resolved.alias() != null && resolved.alias().dataStream())) {
                throw new SnapshotHandler.InvalidAliasNameException(
                    "[" + name + "] is " + (resolved.index() != null ? "an index" : "a data stream") + ", not an alias"
                );
            }
            before.put(name, resolved);
        }
        return before;
    }

    /**
     * Adds every planned alias, one mutation each. Never throws: an alias that fails is named in the result.
     *
     * @param metadata the metadata plane
     * @param byAlias each alias with the restored members to add
     * @param before what {@link #resolve} read, used for each alias's first attempt; a retry reads again
     * @return the aliases that could not be added
     */
    public static List<String> apply(MetadataPlane metadata, Map<String, List<Member>> byAlias, Map<String, DescriptorResolution> before) {
        final List<String> failed = new ArrayList<>();
        final long deadline = System.nanoTime() + BUDGET_NANOS;
        for (Map.Entry<String, List<Member>> alias : byAlias.entrySet()) {
            boolean applied = false;
            try {
                if (System.nanoTime() - deadline > 0) {
                    failed.add(alias.getKey());
                    continue;
                }
                for (int attempt = 0; attempt < ATTEMPTS && applied == false; attempt++) {
                    final DescriptorResolution resolved = attempt == 0 && before.containsKey(alias.getKey())
                        ? before.get(alias.getKey())
                        : metadata.resolveForAliasMutation(alias.getKey());
                    applied = mutate(metadata, alias.getKey(), alias.getValue(), resolved);
                }
            } catch (IOException | RuntimeException e) {
                LOGGER.warn("restore could not add alias [" + alias.getKey() + "]", e);
            }
            if (applied == false) {
                LOGGER.warn("restore left alias [{}] unadded; add it with the alias API", alias.getKey());
                failed.add(alias.getKey());
            }
        }
        return failed;
    }

    private static boolean mutate(MetadataPlane metadata, String name, List<Member> members, DescriptorResolution resolved)
        throws IOException {
        if (resolved.index() != null || (resolved.alias() != null && resolved.alias().dataStream())) {
            return false;
        }
        final AliasRecord current = resolved.alias();
        final List<String> now = new ArrayList<>(current == null ? List.of() : current.indices());
        String writeMember = current == null ? null : current.writeMember();
        boolean hasWriteTarget = current != null && current.writesTo() != null;
        for (Member member : members) {
            if (now.contains(member.index()) == false) {
                now.add(member.index());
            }
            if (member.write() && hasWriteTarget == false) {
                writeMember = member.index();
                hasWriteTarget = true;
            }
        }
        if (current == null) {
            try {
                metadata.createAlias(new AliasRecord(name, now).withWriteMember(writeMember));
                return true;
            } catch (IndexAlreadyExistsException e) {
                return false;
            }
        }
        return metadata.updateAlias(current, MetadataPlane.withIndices(current, now).withWriteMember(writeMember), resolved.generation())
            .isPresent();
    }
}
