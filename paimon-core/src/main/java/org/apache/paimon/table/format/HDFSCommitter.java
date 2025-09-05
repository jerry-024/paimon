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

import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.CommitMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HDFS committer implementation that uses temporary files and atomic rename operations. This
 * committer creates temporary files in a staging directory and atomically moves them to their final
 * destinations during commit.
 */
public class HDFSCommitter extends FormatTableCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(HDFSCommitter.class);

    private static final String TEMP_DIR_PREFIX = "_temp_";
    private static final String STAGING_DIR_NAME = "_staging";

    private final Map<Path, Path> tempToFinalMapping;

    public HDFSCommitter(FormatTable formatTable) {
        super(formatTable);
        this.tempToFinalMapping = new HashMap<>();
    }

    @Override
    public PositionOutputStream createWriter(Path filePath) throws IOException {
        // Create temporary file path
        Path tempFilePath = createTempPath(filePath);

        // Ensure temporary directory exists
        Path tempDir = tempFilePath.getParent();
        if (!fileIO.exists(tempDir)) {
            fileIO.mkdirs(tempDir);
        }

        // Store mapping for later commit
        tempToFinalMapping.put(tempFilePath, filePath);

        LOG.debug("Created temporary file {} for final path {}", tempFilePath, filePath);

        return fileIO.newOutputStream(tempFilePath, false);
    }

    @Override
    public void commitFiles(List<CommitMessage> commitMessages) throws IOException {
        LOG.info("Committing {} files using HDFS rename strategy", commitMessages.size());

        List<HDFSTempFileInfo> tempFiles = extractTempFileInfos(commitMessages);

        // Group files by their parent directory for batch operations
        Map<Path, List<HDFSTempFileInfo>> filesByParent = groupFilesByParent(tempFiles);

        // Create all necessary parent directories first
        createParentDirectories(filesByParent.keySet());

        // Perform atomic renames with error handling
        int successCount = performAtomicRenames(tempFiles);

        // Clean up temporary directories
        cleanupTempDirectories();

        LOG.info("Successfully committed {} files", successCount);
    }

    @Override
    public void abortFiles(List<CommitMessage> commitMessages) throws IOException {
        LOG.info("Aborting {} files and cleaning up temporary files", commitMessages.size());

        List<HDFSTempFileInfo> tempFiles = extractTempFileInfos(commitMessages);

        // Delete individual temporary files
        for (HDFSTempFileInfo tempFile : tempFiles) {
            Path tempPath = tempFile.getTempPath();
            if (fileIO.exists(tempPath)) {
                try {
                    fileIO.delete(tempPath, false);
                    LOG.debug("Deleted temporary file: {}", tempPath);
                } catch (IOException e) {
                    LOG.warn("Failed to delete temporary file: {}", tempPath, e);
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
                }
            }
        }

        // Clean up temporary directories
        cleanupTempDirectories();
    }

    private Path createTempPath(Path finalPath) {
        Path tablePath = new Path(formatTable.location());
        String tempDirName = TEMP_DIR_PREFIX + sessionId;
        Path stagingPath = new Path(tablePath, STAGING_DIR_NAME + "/" + tempDirName);

        // Calculate relative path from table location to final path
        String relativePath = relativize(new Path(formatTable.location()), finalPath);

        return new Path(stagingPath, relativePath);
    }

    private String relativize(Path base, Path target) {
        String baseStr = base.toString();
        String targetStr = target.toString();

        if (targetStr.startsWith(baseStr)) {
            String relative = targetStr.substring(baseStr.length());
            if (relative.startsWith("/")) {
                relative = relative.substring(1);
            }
            return relative;
        }

        // Fallback: use the full target path
        return target.getName();
    }

    private List<HDFSTempFileInfo> extractTempFileInfos(List<CommitMessage> commitMessages) {
        List<HDFSTempFileInfo> tempFiles = new ArrayList<>();

        for (CommitMessage message : commitMessages) {
            if (message instanceof HDFSCommitMessage) {
                HDFSCommitMessage hdfsMessage = (HDFSCommitMessage) message;
                tempFiles.add(hdfsMessage.getTempFileInfo());
            }
        }

        return tempFiles;
    }

    private Map<Path, List<HDFSTempFileInfo>> groupFilesByParent(List<HDFSTempFileInfo> tempFiles) {
        Map<Path, List<HDFSTempFileInfo>> filesByParent = new HashMap<>();

        for (HDFSTempFileInfo tempFile : tempFiles) {
            Path parentDir = tempFile.getFinalPath().getParent();
            filesByParent.computeIfAbsent(parentDir, k -> new ArrayList<>()).add(tempFile);
        }

        return filesByParent;
    }

    private void createParentDirectories(Iterable<Path> parentDirs) throws IOException {
        for (Path parentDir : parentDirs) {
            if (parentDir != null && !fileIO.exists(parentDir)) {
                fileIO.mkdirs(parentDir);
                LOG.debug("Created parent directory: {}", parentDir);
            }
        }
    }

    private int performAtomicRenames(List<HDFSTempFileInfo> tempFiles) throws IOException {
        int successCount = 0;
        List<IOException> failures = new ArrayList<>();

        for (HDFSTempFileInfo tempFile : tempFiles) {
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

        return successCount;
    }

    private void cleanupTempDirectories() throws IOException {
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
                    // Clean up any orphaned temp directories from previous sessions
                    cleanupOrphanedTempDirectories(stagingPath, stagingContents);
                }
            }
        } catch (Exception e) {
            LOG.debug("Could not clean up staging directory: {}", stagingPath, e);
        }
    }

    private void cleanupOrphanedTempDirectories(Path stagingPath, FileStatus[] contents) {
        for (FileStatus content : contents) {
            if (content.isDir() && content.getPath().getName().startsWith(TEMP_DIR_PREFIX)) {
                try {
                    fileIO.delete(content.getPath(), true);
                    LOG.debug("Cleaned up orphaned temp directory: {}", content.getPath());
                } catch (IOException e) {
                    LOG.warn(
                            "Failed to clean up orphaned temp directory: {}", content.getPath(), e);
                }
            }
        }

        // Try to delete staging directory again after cleanup
        try {
            FileStatus[] remainingContents = fileIO.listStatus(stagingPath);
            if (remainingContents == null || remainingContents.length == 0) {
                fileIO.delete(stagingPath, false);
                LOG.debug("Cleaned up staging directory after orphan cleanup: {}", stagingPath);
            }
        } catch (Exception e) {
            LOG.debug(
                    "Could not clean up staging directory after orphan cleanup: {}",
                    stagingPath,
                    e);
        }
    }

    /** Information about a temporary file that needs to be committed using rename. */
    public static class HDFSTempFileInfo {
        private final Path tempPath;
        private final Path finalPath;

        public HDFSTempFileInfo(Path tempPath, Path finalPath) {
            this.tempPath = tempPath;
            this.finalPath = finalPath;
        }

        public Path getTempPath() {
            return tempPath;
        }

        public Path getFinalPath() {
            return finalPath;
        }

        @Override
        public String toString() {
            return String.format(
                    "HDFSTempFileInfo{tempPath=%s, finalPath=%s}", tempPath, finalPath);
        }
    }
}
