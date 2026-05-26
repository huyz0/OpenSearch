/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.coord.LuceneVersionConverter;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.MetadataFilenameUtils;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.store.remote.metadata.DefaultUploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandlerFactory;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Default implementation of RemoteStoreSegmentStrategy which uploads segments one-by-one and manages separate metadata files.
 */
public final class DefaultRemoteStoreSegmentStrategy implements RemoteStoreSegmentStrategy {

    private static final Logger logger = LogManager.getLogger(DefaultRemoteStoreSegmentStrategy.class);

    private static final VersionedCodecStreamWrapper<RemoteSegmentMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new RemoteSegmentMetadataHandlerFactory(),
        RemoteSegmentMetadata.VERSION_ONE,
        RemoteSegmentMetadata.CURRENT_VERSION,
        RemoteSegmentMetadata.METADATA_CODEC
    );

    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final RemoteDirectory remoteDataDirectory;
    private final RemoteDirectory remoteMetadataDirectory;
    private final ShardId shardId;

    public DefaultRemoteStoreSegmentStrategy() {
        this.remoteDirectory = null;
        this.remoteDataDirectory = null;
        this.remoteMetadataDirectory = null;
        this.shardId = null;
    }

    private DefaultRemoteStoreSegmentStrategy(
        RemoteSegmentStoreDirectory remoteDirectory,
        RemoteDirectory remoteMetadataDirectory,
        ShardId shardId
    ) {
        this.remoteDirectory = remoteDirectory;
        this.remoteDataDirectory = (RemoteDirectory) remoteDirectory.getDelegate();
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        this.shardId = shardId;
    }

    @Override
    public RemoteStoreSegmentStrategy getShardInstance(
        RemoteSegmentStoreDirectory remoteDirectory,
        RemoteDirectory remoteMetadataDirectory,
        ShardId shardId
    ) throws IOException {
        return new DefaultRemoteStoreSegmentStrategy(remoteDirectory, remoteMetadataDirectory, shardId);
    }

    @Override
    public void upload(UploadContext context, ActionListener<Void> listener) throws IOException {
        final Collection<String> filesToUpload = context.getFilesToUpload();
        final Directory storeDirectory = context.getStoreDirectory();
        final RemoteStoreSegmentStrategy.UploadCallback callback = context.getCallback();
        final boolean isLowPriorityUpload = context.isLowPriorityUpload();

        final org.opensearch.action.support.GroupedActionListener<Void> batchUploadListener =
            new org.opensearch.action.support.GroupedActionListener<>(ActionListener.map(listener, resp -> null), filesToUpload.size());

        for (final String localSegment : filesToUpload) {
            final ActionListener<Void> aggregatedListener = ActionListener.wrap(resp -> {
                callback.onUploadSuccess(localSegment);
                batchUploadListener.onResponse(resp);
            }, ex -> {
                callback.onUploadFailure(localSegment, ex);
                batchUploadListener.onFailure(ex);
            });

            callback.onUploadStart(localSegment);

            // Register DefaultUploadedSegmentMetadata once the copy completes
            final Runnable postUpload = () -> {
                try {
                    String remoteName = remoteDirectory.getExistingRemoteFilename(localSegment);
                    String checksum = remoteDirectory.getChecksumOfLocalFile(storeDirectory, localSegment);
                    long length = storeDirectory.fileLength(localSegment);
                    UploadedSegmentMetadata meta = new DefaultUploadedSegmentMetadata(
                        localSegment,
                        remoteName,
                        checksum,
                        length,
                        remoteDataDirectory
                    );
                    meta.setWrittenByMajor(org.apache.lucene.util.Version.LATEST.major);
                    remoteDirectory.addUploadedSegment(localSegment, meta);
                } catch (Exception ex) {
                    logger.error("Failed to register uploaded segment: " + localSegment, ex);
                }
            };

            boolean uploaded = remoteDataDirectory.copyFrom(
                storeDirectory,
                localSegment,
                remoteDirectory.getNewRemoteSegmentFilename(localSegment),
                IOContext.DEFAULT,
                postUpload,
                aggregatedListener,
                isLowPriorityUpload,
                context.getCryptoMetadata()
            );

            if (uploaded == false) {
                try {
                    String remoteName = remoteDirectory.getNewRemoteSegmentFilename(localSegment);
                    remoteDataDirectory.copyFrom(storeDirectory, localSegment, remoteName, IOContext.DEFAULT);
                    postUpload.run();
                    aggregatedListener.onResponse(null);
                } catch (final Exception e) {
                    aggregatedListener.onFailure(e);
                }
            }
        }
    }

    @Override
    public void uploadMetadata(MetadataUploadContext context) throws IOException {
        String metadataFilename = MetadataFilenameUtils.getMetadataFilename(
            context.getCheckpoint().getPrimaryTerm(),
            context.getCatalogSnapshot().getGeneration(),
            context.getTranslogGeneration(),
            remoteDirectory.getMetadataUploadCounter().incrementAndGet(),
            RemoteSegmentMetadata.CURRENT_VERSION,
            context.getNodeId()
        );
        try {
            try (IndexOutput indexOutput = context.getStoreDirectory().createOutput(metadataFilename, IOContext.DEFAULT)) {
                Map<String, String> uploadedSegmentsMap = new java.util.HashMap<>();
                for (String file : context.getActiveSegmentFiles()) {
                    if (remoteDirectory.getSegmentsUploadedToRemoteStore().containsKey(file)) {
                        UploadedSegmentMetadata metadata = remoteDirectory.getSegmentsUploadedToRemoteStore().get(file);
                        metadata.setWrittenByMajor(
                            LuceneVersionConverter.toLuceneOrLatest(
                                context.getCatalogSnapshot().getFormatVersionForFile(metadata.getOriginalFilename())
                            ).major
                        );
                        uploadedSegmentsMap.put(file, metadata.toString());
                    } else {
                        throw new NoSuchFileException(file);
                    }
                }

                Objects.requireNonNull(
                    context.getCatalogSnapshotToCommitSerializer(),
                    "catalogSnapshotToCommitSerializer must be supplied"
                );
                final byte[] segmentInfoSnapshotByteArray = context.getCatalogSnapshotToCommitSerializer()
                    .apply(context.getCatalogSnapshot());

                metadataStreamWrapper.writeStream(
                    indexOutput,
                    new RemoteSegmentMetadata(
                        RemoteSegmentMetadata.fromMapOfStrings(uploadedSegmentsMap),
                        segmentInfoSnapshotByteArray,
                        context.getCheckpoint()
                    )
                );
            }
            context.getStoreDirectory().sync(Collections.singleton(metadataFilename));
            remoteMetadataDirectory.copyFrom(context.getStoreDirectory(), metadataFilename, metadataFilename, IOContext.DEFAULT);
        } finally {
            tryAndDeleteLocalFile(metadataFilename, context.getStoreDirectory());
        }
    }

    @Override
    public RemoteSegmentMetadata init() throws IOException {
        logger.debug("Start initialisation of remote segment metadata");
        RemoteSegmentMetadata remoteSegmentMetadata = null;

        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            RemoteSegmentStoreDirectory.METADATA_FILES_TO_FETCH
        );

        RemoteStoreUtils.verifyNoMultipleWriters(metadataFiles, MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen);

        if (!metadataFiles.isEmpty()) {
            String latestMetadataFile = metadataFiles.get(0);
            logger.trace("Reading latest Metadata file {}", latestMetadataFile);
            remoteSegmentMetadata = readMetadata(latestMetadataFile);
        } else {
            logger.trace("No metadata file found, this can happen for new index with no data uploaded to remote segment store");
        }

        logger.debug("Initialisation of remote segment metadata completed");
        return remoteSegmentMetadata;
    }

    private RemoteSegmentMetadata readMetadataFile(String metadataFilename) throws IOException {
        try (InputStream inputStream = remoteMetadataDirectory.getBlobStream(metadataFilename)) {
            byte[] metadataBytes = inputStream.readAllBytes();
            return postProcessMetadata(metadataStreamWrapper.readStream(new ByteArrayIndexInput(metadataFilename, metadataBytes)));
        }
    }

    private RemoteSegmentMetadata postProcessMetadata(RemoteSegmentMetadata rawMetadata) {
        if (rawMetadata == null) {
            return null;
        }
        Map<String, RemoteSegmentFile> processedMap = new HashMap<>();
        for (Map.Entry<String, RemoteSegmentFile> entry : rawMetadata.getMetadata().entrySet()) {
            UploadedSegmentMetadata raw = (UploadedSegmentMetadata) entry.getValue();
            DefaultUploadedSegmentMetadata processed = new DefaultUploadedSegmentMetadata(
                raw.getOriginalFilename(),
                raw.getUploadedFilename(),
                raw.getChecksum(),
                raw.getLength(),
                remoteDataDirectory
            );
            processed.setWrittenByMajor(raw.getWrittenByMajor());
            processedMap.put(entry.getKey(), processed);
        }
        return new RemoteSegmentMetadata(processedMap, rawMetadata.getSegmentInfosBytes(), rawMetadata.getReplicationCheckpoint());
    }

    @Override
    public void deleteStaleSegments(int minCommitsToKeep) throws IOException {
        if (minCommitsToKeep == -1) {
            logger.info(
                "Stale segment deletion is disabled if cluster.remote_store.index.segment_metadata.retention.max_count is set to -1"
            );
            return;
        }

        List<String> sortedMetadataFileList = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            Integer.MAX_VALUE
        );
        if (sortedMetadataFileList.size() <= minCommitsToKeep) {
            logger.debug(
                "Number of commits in remote segment store={}, lastNMetadataFilesToKeep={}",
                sortedMetadataFileList.size(),
                minCommitsToKeep
            );
            return;
        }

        // Check last fetch status of pinned timestamps. If stale, return.
        if (minCommitsToKeep != 0 && RemoteStoreUtils.isPinnedTimestampStateStale()) {
            logger.warn("Skipping remote segment store garbage collection as last fetch of pinned timestamp is stale");
            return;
        }

        Tuple<Long, Set<Long>> pinnedTimestampsState = RemoteStorePinnedTimestampService.getPinnedTimestamps();

        Set<Long> pinnedTimestamps = new HashSet<>(pinnedTimestampsState.v2());
        pinnedTimestamps.add(pinnedTimestampsState.v1());
        Set<String> implicitLockedFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            sortedMetadataFileList,
            pinnedTimestamps,
            remoteDirectory.getMetadataFilePinnedTimestampMap(),
            MetadataFilenameUtils::getTimestamp,
            MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen
        );
        final Set<String> allLockFiles = new HashSet<>(implicitLockedFiles);

        try {
            allLockFiles.addAll(
                ((RemoteStoreMetadataLockManager) remoteDirectory.getMetadataLockManager()).fetchLockedMetadataFiles(
                    MetadataFilenameUtils.METADATA_PREFIX
                )
            );
        } catch (Exception e) {
            logger.error("Exception while fetching segment metadata lock files, skipping deleteStaleSegments", e);
            return;
        }

        List<String> metadataFilesEligibleToDelete = new ArrayList<>(
            sortedMetadataFileList.subList(minCommitsToKeep, sortedMetadataFileList.size())
        );

        // Along with last N files, we need to keep files since last successful run of scheduler
        long lastSuccessfulFetchOfPinnedTimestamps = pinnedTimestampsState.v1();
        metadataFilesEligibleToDelete = RemoteStoreUtils.filterOutMetadataFilesBasedOnAge(
            metadataFilesEligibleToDelete,
            MetadataFilenameUtils::getTimestamp,
            lastSuccessfulFetchOfPinnedTimestamps
        );

        if (metadataFilesEligibleToDelete.isEmpty()) {
            logger.debug("No metadata files are eligible to be deleted based on lastNMetadataFilesToKeep and age");
            return;
        }

        List<String> metadataFilesToBeDeleted = metadataFilesEligibleToDelete.stream()
            .filter(metadataFile -> allLockFiles.contains(metadataFile) == false)
            .collect(Collectors.toList());

        logger.debug(
            "metadataFilesEligibleToDelete={} metadataFilesToBeDeleted={}",
            metadataFilesEligibleToDelete,
            metadataFilesToBeDeleted
        );

        Map<String, RemoteSegmentFile> activeSegmentFilesMetadataMap = new HashMap<>();
        Set<String> activeSegmentRemoteFilenames = new HashSet<>();

        final Set<String> metadataFilesToFilterActiveSegments = remoteDirectory.getMetadataFilesToFilterActiveSegments(
            sortedMetadataFileList.indexOf(metadataFilesEligibleToDelete.get(0)),
            sortedMetadataFileList,
            allLockFiles
        );

        for (String metadataFile : metadataFilesToFilterActiveSegments) {
            Map<String, RemoteSegmentFile> segmentMetadataMap = readMetadata(metadataFile).getMetadata();
            activeSegmentFilesMetadataMap.putAll(segmentMetadataMap);
            activeSegmentRemoteFilenames.addAll(
                segmentMetadataMap.values()
                    .stream()
                    .map(metadata -> ((UploadedSegmentMetadata) metadata).getUploadedFilename())
                    .collect(Collectors.toSet())
            );
        }
        final Set<String> activeBaseNames = activeSegmentRemoteFilenames.stream()
            .map(f -> f.contains("#") ? f.split("#")[0] : f)
            .collect(Collectors.toSet());

        final Set<String> deletedSegmentFiles = new HashSet<>();
        for (final String metadataFile : metadataFilesToBeDeleted) {
            final Map<String, RemoteSegmentFile> staleSegmentFilesMetadataMap = readMetadata(metadataFile).getMetadata();
            final Set<String> staleSegmentRemoteFilenames = staleSegmentFilesMetadataMap.values()
                .stream()
                .map(metadata -> ((UploadedSegmentMetadata) metadata).getUploadedFilename())
                .collect(Collectors.toSet());

            // Collect all files to delete for this metadata file
            final List<String> filesToDelete = staleSegmentRemoteFilenames.stream()
                .map(file -> file.contains("#") ? file.split("#")[0] : file)
                .filter(file -> activeBaseNames.contains(file) == false)
                .filter(file -> deletedSegmentFiles.contains(file) == false)
                .distinct()
                .collect(Collectors.toList());

            final AtomicBoolean deletionSuccessful = new AtomicBoolean(true);
            try {
                if (filesToDelete.isEmpty() == false) {
                    // Batch delete all stale segment files
                    remoteDataDirectory.deleteFiles(filesToDelete);
                    deletedSegmentFiles.addAll(filesToDelete);
                }

                // Update cache after successful batch deletion
                for (final Map.Entry<String, RemoteSegmentFile> entry : staleSegmentFilesMetadataMap.entrySet()) {
                    final String localFile = entry.getKey();
                    final String remoteFile = ((UploadedSegmentMetadata) entry.getValue()).getUploadedFilename();
                    final String baseRemoteFile = remoteFile.contains("#") ? remoteFile.split("#")[0] : remoteFile;
                    if (filesToDelete.contains(baseRemoteFile) || deletedSegmentFiles.contains(baseRemoteFile)) {
                        if (activeSegmentFilesMetadataMap.containsKey(localFile) == false) {
                            remoteDirectory.removeUploadedSegment(localFile);
                        }
                    }
                }
            } catch (IOException e) {
                deletionSuccessful.set(false);
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Exception while deleting segment files corresponding to metadata file {}. Deletion will be re-tried",
                        metadataFile
                    ),
                    e
                );
            }
            if (deletionSuccessful.get()) {
                logger.debug("Deleting stale metadata file {} from remote segment store", metadataFile);
                remoteMetadataDirectory.deleteFile(metadataFile);
            }
        }
        logger.debug("deletedSegmentFiles={}", deletedSegmentFiles);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        final String remoteFilename = remoteDirectory.getExistingRemoteFilename(name);
        if (remoteFilename != null) {
            remoteDataDirectory.deleteFile(remoteFilename);
            remoteDirectory.removeUploadedSegment(name);
        }
    }

    private void tryAndDeleteLocalFile(String filename, Directory directory) {
        try {
            logger.debug("Deleting file: " + filename);
            directory.deleteFile(filename);
        } catch (NoSuchFileException | FileNotFoundException e) {
            logger.trace("Exception while deleting. Missing file : " + filename, e);
        } catch (IOException e) {
            logger.warn("Exception while deleting: " + filename, e);
        }
    }

    @Override
    public String getMetadataFileForCommit(long primaryTerm, long generation) throws IOException {
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation),
            1
        );

        if (metadataFiles.isEmpty()) {
            throw new NoSuchFileException(
                "Metadata file is not present for given primary term " + primaryTerm + " and generation " + generation
            );
        }
        return metadataFiles.get(0);
    }

    @Override
    public RemoteSegmentMetadata readMetadata(String filename) throws IOException {
        return readMetadataFile(filename);
    }

    @Override
    public RemoteSegmentMetadata initializeToSpecificTimestamp(long timestamp) throws IOException {
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            Integer.MAX_VALUE
        );
        Set<String> lockedMetadataFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            metadataFiles,
            Set.of(timestamp),
            MetadataFilenameUtils::getTimestamp,
            MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen,
            true
        );
        if (lockedMetadataFiles.isEmpty()) {
            return null;
        }
        if (lockedMetadataFiles.size() > 1) {
            throw new IOException(
                "Expected exactly one metadata file matching timestamp: " + timestamp + " but got " + lockedMetadataFiles
            );
        }
        String metadataFile = lockedMetadataFiles.iterator().next();
        return readMetadata(metadataFile);
    }

    @Override
    public RemoteSegmentMetadata initializeToSpecificCommit(
        long primaryTerm,
        long commitGeneration,
        String acquirerId,
        RemoteStoreLockManager mdLockManager
    ) throws IOException {
        String metadataFilePrefix = MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, commitGeneration);
        String metadataFile = ((RemoteStoreMetadataLockManager) mdLockManager).fetchLockedMetadataFile(metadataFilePrefix, acquirerId);
        return readMetadata(metadataFile);
    }

    @Override
    public Map<String, RemoteSegmentMetadata> readLatestNMetadataFiles(int count) throws IOException {
        Map<String, RemoteSegmentMetadata> metadataMap = new java.util.LinkedHashMap<>();
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            count
        );
        for (String file : metadataFiles) {
            metadataMap.put(file, readMetadata(file));
        }
        return metadataMap;
    }
}
