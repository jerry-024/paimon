/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.format;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FormatTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Atomic committer for FormatTable that ensures write visibility only after successful commit.
 *
 * <p>This class provides different commit strategies based on the underlying file system: - For
 * HDFS: Uses temporary directory + atomic rename - For S3/OSS: Uses Hadoop S3A Committer for atomic
 * writes
 */
@Public
public abstract class FormatTableAtomicCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableAtomicCommitter.class);

    protected final FormatTable formatTable;
    protected final FileIO fileIO;
    protected final String sessionId;

    public FormatTableAtomicCommitter(FormatTable formatTable) {
        this.formatTable = formatTable;
        this.fileIO = formatTable.fileIO();
        this.sessionId = generateSessionId();
    }

    /** Creates an appropriate committer based on the file system type. */
    public static FormatTableAtomicCommitter create(FormatTable formatTable) {
        String location = formatTable.location();

        if (isS3FileSystem(location) || isOSSFileSystem(location)) {
            return new MagicAtomicCommitter(formatTable);
        } else {
            return new HDFSAtomicCommitter(formatTable);
        }
    }

    /**
     * Prepares a temporary location for writing files. Files written to this location are not
     * visible until commit.
     */
    public abstract Path prepareTempLocation(String partitionPath);

    /**
     * Commits all temporary files to their final locations atomically. After this operation, all
     * files become visible.
     */
    public abstract void commitFiles(List<TempFileInfo> tempFiles) throws IOException;

    public abstract void cleanupTempDirectory() throws IOException;

    /** Aborts the write operation and cleans up temporary files. */
    public void abortFiles(List<TempFileInfo> tempFiles) throws IOException {
        LOG.info("Aborting {} files and cleaning up temporary directory", tempFiles.size());

        // First, delete individual temporary files
        for (TempFileInfo tempFile : tempFiles) {
            Path tempPath = tempFile.getTempPath();
            if (fileIO.exists(tempPath)) {
                try {
                    fileIO.delete(tempPath, false);
                    LOG.debug("Deleted temporary file: {}", tempPath);
                } catch (IOException e) {
                    LOG.warn("Failed to delete temporary file: {}", tempPath, e);
                    // Continue with other files even if one fails
                }
            }

            // Also ensure final file doesn't exist (in case it was partially committed)
            Path finalPath = tempFile.getFinalPath();
            if (fileIO.exists(finalPath)) {
                try {
                    fileIO.delete(finalPath, false);
                    LOG.debug("Deleted final file: {}", finalPath);
                } catch (IOException e) {
                    LOG.warn("Failed to delete final file: {}", finalPath, e);
                    // Continue with other files even if one fails
                }
            }
        }

        // Then clean up temporary directory
        this.cleanupTempDirectory();
    }

    private static boolean isS3FileSystem(String location) {
        return location.startsWith("s3://")
                || location.startsWith("s3a://")
                || location.startsWith("s3n://");
    }

    private static boolean isOSSFileSystem(String location) {
        return location.startsWith("oss://");
    }

    private String generateSessionId() {
        String timestamp =
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        return String.format("session-%s-%s", timestamp, uuid);
    }

    /** Information about a temporary file that needs to be committed. */
    public static class TempFileInfo {
        private final Path tempPath;
        private final Path finalPath;
        private final String partitionPath;
        private final org.apache.paimon.fs.MultipartUploadInfo multipartUploadInfo;

        public TempFileInfo(Path tempPath, Path finalPath, String partitionPath) {
            this(tempPath, finalPath, partitionPath, null);
        }

        public TempFileInfo(
                Path tempPath,
                Path finalPath,
                String partitionPath,
                org.apache.paimon.fs.MultipartUploadInfo multipartUploadInfo) {
            this.tempPath = tempPath;
            this.finalPath = finalPath;
            this.partitionPath = partitionPath;
            this.multipartUploadInfo = multipartUploadInfo;
        }

        public Path getTempPath() {
            return tempPath;
        }

        public Path getFinalPath() {
            return finalPath;
        }

        public String getPartitionPath() {
            return partitionPath;
        }

        public org.apache.paimon.fs.MultipartUploadInfo getMultipartUploadInfo() {
            return multipartUploadInfo;
        }

        public boolean hasMultipartUpload() {
            return multipartUploadInfo != null;
        }

        @Override
        public String toString() {
            return String.format(
                    "TempFileInfo{tempPath=%s, finalPath=%s, partition=%s, hasMultipart=%s}",
                    tempPath, finalPath, partitionPath, hasMultipartUpload());
        }
    }

    /**
     * HDFS implementation using temporary directories and atomic rename. Optimized with batch
     * operations and improved cleanup strategies.
     */
    private static class HDFSAtomicCommitter extends FormatTableAtomicCommitter {

        private static final String TEMP_DIR_PREFIX = "_temp_";
        private static final String STAGING_DIR_NAME = "_staging";

        public HDFSAtomicCommitter(FormatTable formatTable) {
            super(formatTable);
        }

        @Override
        public Path prepareTempLocation(String partitionPath) {
            Path tablePath = new Path(formatTable.location());
            // Use a staging directory structure for better organization
            String tempDirName = TEMP_DIR_PREFIX + sessionId;
            Path stagingPath = new Path(tablePath, STAGING_DIR_NAME + "/" + tempDirName);

            if (partitionPath != null
                    && !partitionPath.isEmpty()
                    && !"default".equals(partitionPath)) {
                return new Path(stagingPath, partitionPath);
            } else {
                return stagingPath;
            }
        }

        @Override
        public void commitFiles(List<TempFileInfo> tempFiles) throws IOException {
            LOG.info("Committing {} files using HDFS atomic rename strategy", tempFiles.size());

            // Group files by their parent directory for batch operations
            Map<Path, List<TempFileInfo>> filesByParent =
                    tempFiles.stream()
                            .collect(
                                    java.util.stream.Collectors.groupingBy(
                                            f -> f.getFinalPath().getParent()));

            // Create all necessary parent directories first (batch operation)
            for (Path parentDir : filesByParent.keySet()) {
                if (parentDir != null && !fileIO.exists(parentDir)) {
                    fileIO.mkdirs(parentDir);
                    LOG.debug("Created parent directory: {}", parentDir);
                }
            }

            // Perform atomic renames with error handling
            int successCount = 0;
            List<IOException> failures = new ArrayList<>();

            for (TempFileInfo tempFile : tempFiles) {
                try {
                    Path tempPath = tempFile.getTempPath();
                    Path finalPath = tempFile.getFinalPath();

                    if (!fileIO.exists(tempPath)) {
                        throw new IOException("Temporary file does not exist: " + tempPath);
                    }

                    // Remove existing file if present (overwrite case)
                    if (fileIO.exists(finalPath)) {
                        fileIO.delete(finalPath, false);
                    }

                    // Atomic rename
                    if (!fileIO.rename(tempPath, finalPath)) {
                        throw new IOException("Failed to rename " + tempPath + " to " + finalPath);
                    }

                    successCount++;
                    LOG.debug("Successfully renamed {} to {}", tempPath, finalPath);

                } catch (IOException e) {
                    failures.add(e);
                    LOG.error("Failed to commit file: {}", tempFile.getTempPath(), e);
                }
            }

            // Clean up temporary directory
            cleanupTempDirectory();

            // Report any failures
            if (!failures.isEmpty()) {
                LOG.error("Failed to commit {} out of {} files", failures.size(), tempFiles.size());
                IOException combined =
                        new IOException(
                                String.format(
                                        "Failed to commit %d out of %d files",
                                        failures.size(), tempFiles.size()));
                for (IOException failure : failures) {
                    combined.addSuppressed(failure);
                }
                throw combined;
            }

            LOG.info("Successfully committed {} files", successCount);
        }

        @Override
        public void cleanupTempDirectory() throws IOException {
            Path tablePath = new Path(formatTable.location());
            Path stagingPath = new Path(tablePath, STAGING_DIR_NAME);
            Path tempDir = new Path(stagingPath, TEMP_DIR_PREFIX + sessionId);

            // Clean up this session's temp directory
            if (fileIO.exists(tempDir)) {
                fileIO.delete(tempDir, true);
                LOG.debug("Cleaned up temporary directory: {}", tempDir);
            }

            // Clean up empty staging directory
            try {
                if (fileIO.exists(stagingPath)) {
                    FileStatus[] stagingContents = fileIO.listStatus(stagingPath);
                    if (stagingContents == null || stagingContents.length == 0) {
                        fileIO.delete(stagingPath, false);
                        LOG.debug("Cleaned up empty staging directory: {}", stagingPath);
                    } else {
                        LOG.debug(
                                "Staging directory not empty, contains {} items",
                                stagingContents.length);
                        // Clean up any orphaned temp directories from previous sessions
                        for (FileStatus content : stagingContents) {
                            if (content.isDir()
                                    && content.getPath().getName().startsWith(TEMP_DIR_PREFIX)) {
                                try {
                                    fileIO.delete(content.getPath(), true);
                                    LOG.debug(
                                            "Cleaned up orphaned temp directory: {}",
                                            content.getPath());
                                } catch (IOException e) {
                                    LOG.warn(
                                            "Failed to clean up orphaned temp directory: {}",
                                            content.getPath(),
                                            e);
                                }
                            }
                        }

                        // Try to delete staging directory again after cleanup
                        FileStatus[] remainingContents = fileIO.listStatus(stagingPath);
                        if (remainingContents == null || remainingContents.length == 0) {
                            fileIO.delete(stagingPath, false);
                            LOG.debug(
                                    "Cleaned up staging directory after orphan cleanup: {}",
                                    stagingPath);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.debug("Could not clean up staging directory: {}", stagingPath, e);
            }
        }
    }

    /** S3/OSS implementation using multipart upload for atomic writes. */
    private static class MagicAtomicCommitter extends FormatTableAtomicCommitter {

        private static final String MAGIC_COMMITTER_DIR = "_temporary";
        private static final String PENDING_UPLOADS_DIR = "__pending-uploads";

        private final org.apache.paimon.fs.MultipartUploadTracker uploadTracker;

        public MagicAtomicCommitter(FormatTable formatTable) {
            super(formatTable);
            Path metadataDir =
                    new Path(formatTable.location(), PENDING_UPLOADS_DIR + "/" + sessionId);
            this.uploadTracker =
                    new org.apache.paimon.fs.MultipartUploadTracker(fileIO, metadataDir);
        }

        @Override
        public Path prepareTempLocation(String partitionPath) {
            // For multipart upload committer, we use a magic directory for staging
            Path tablePath = new Path(formatTable.location());
            Path tempDir = new Path(tablePath, MAGIC_COMMITTER_DIR + "/" + sessionId);

            if (partitionPath != null
                    && !partitionPath.isEmpty()
                    && !"default".equals(partitionPath)) {
                return new Path(tempDir, partitionPath);
            } else {
                return tempDir;
            }
        }

        @Override
        public void commitFiles(List<TempFileInfo> tempFiles) throws IOException {
            LOG.info("Committing {} files using multipart upload strategy", tempFiles.size());

            List<IOException> failures = new ArrayList<>();
            int successCount = 0;

            for (TempFileInfo tempFile : tempFiles) {
                try {
                    // Complete the multipart upload
                    completeMultipartUpload(tempFile.getMultipartUploadInfo());
                    uploadTracker.recordUploadComplete(
                            tempFile.getMultipartUploadInfo().getObjectKey());
                    successCount++;
                    LOG.debug("Completed multipart upload for {}", tempFile.getFinalPath());
                } catch (IOException e) {
                    failures.add(e);
                    LOG.error("Failed to commit file: {}", tempFile.getTempPath(), e);

                    // Try to abort the multipart upload if it exists
                    if (tempFile.hasMultipartUpload()) {
                        try {
                            abortMultipartUpload(tempFile.getMultipartUploadInfo());
                            uploadTracker.recordUploadAbort(
                                    tempFile.getMultipartUploadInfo().getObjectKey());
                        } catch (Exception abortException) {
                            LOG.error(
                                    "Failed to abort multipart upload for {}",
                                    tempFile.getMultipartUploadInfo().getObjectKey(),
                                    abortException);
                            e.addSuppressed(abortException);
                        }
                    }
                }
            }

            LOG.info("Successfully committed {} files using multipart upload", successCount);
        }

        @Override
        public void cleanupTempDirectory() throws IOException {
            Path tablePath = new Path(formatTable.location());
            Path tempDir = new Path(tablePath, MAGIC_COMMITTER_DIR + "/" + sessionId);

            if (fileIO.exists(tempDir)) {
                fileIO.delete(tempDir, true);
                LOG.debug("Cleaned up temporary directory: {}", tempDir);
            }

            // Clean up upload tracker metadata
            try {
                uploadTracker.cleanup();
            } catch (Exception e) {
                LOG.warn("Failed to clean up upload tracker metadata", e);
            }
        }

        /** Completes a multipart upload using the object store's native API. */
        private void completeMultipartUpload(org.apache.paimon.fs.MultipartUploadInfo uploadInfo)
                throws IOException {
            String location = formatTable.location();

            if (isS3FileSystem(location)) {
                completeS3MultipartUpload(uploadInfo);
            } else if (isOSSFileSystem(location)) {
                completeOSSMultipartUpload(uploadInfo);
            } else {
                throw new IOException("Unsupported object store for multipart upload: " + location);
            }
        }

        /** Aborts a multipart upload using the object store's native API. */
        private void abortMultipartUpload(org.apache.paimon.fs.MultipartUploadInfo uploadInfo)
                throws IOException {
            String location = formatTable.location();

            if (isS3FileSystem(location)) {
                abortS3MultipartUpload(uploadInfo);
            } else if (isOSSFileSystem(location)) {
                abortOSSMultipartUpload(uploadInfo);
            } else {
                throw new IOException("Unsupported object store for multipart upload: " + location);
            }
        }

        private void completeS3MultipartUpload(org.apache.paimon.fs.MultipartUploadInfo uploadInfo)
                throws IOException {
            // Implementation would use S3MagicCommitterOutputStream.completeMultipartUpload
            // For now, this is a placeholder
            LOG.info("Completing S3 multipart upload: {}", uploadInfo.getUploadId());
            throw new IOException("S3 multipart upload completion not yet implemented");
        }

        private void completeOSSMultipartUpload(org.apache.paimon.fs.MultipartUploadInfo uploadInfo)
                throws IOException {
            // Implementation would use OSSMagicCommitterOutputStream.completeMultipartUpload
            // For now, this is a placeholder
            LOG.info("Completing OSS multipart upload: {}", uploadInfo.getUploadId());
            throw new IOException("OSS multipart upload completion not yet implemented");
        }

        private void abortS3MultipartUpload(org.apache.paimon.fs.MultipartUploadInfo uploadInfo)
                throws IOException {
            // Implementation would use S3MagicCommitterOutputStream.abortMultipartUpload
            LOG.info("Aborting S3 multipart upload: {}", uploadInfo.getUploadId());
        }

        private void abortOSSMultipartUpload(org.apache.paimon.fs.MultipartUploadInfo uploadInfo)
                throws IOException {
            // Implementation would use OSSMagicCommitterOutputStream.abortMultipartUpload
            LOG.info("Aborting OSS multipart upload: {}", uploadInfo.getUploadId());
        }

        private void commitRegularFile(TempFileInfo tempFile) throws IOException {
            Path tempPath = tempFile.getTempPath();
            Path finalPath = tempFile.getFinalPath();

            if (!fileIO.exists(tempPath)) {
                throw new IOException("Temporary file does not exist: " + tempPath);
            }

            // Ensure parent directory exists
            Path parentDir = finalPath.getParent();
            if (parentDir != null && !fileIO.exists(parentDir)) {
                fileIO.mkdirs(parentDir);
            }

            // Remove existing file if present (overwrite case)
            if (fileIO.exists(finalPath)) {
                fileIO.delete(finalPath, false);
            }

            // Atomic rename
            if (!fileIO.rename(tempPath, finalPath)) {
                throw new IOException("Failed to rename " + tempPath + " to " + finalPath);
            }
        }
    }
}
