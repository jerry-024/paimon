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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.MagicS3ACommitMessage.PartETag;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MagicS3ACommitter implementation that uses multipart upload for atomic commits on object stores.
 *
 * <p>This committer writes files directly to their final destinations using multipart upload,
 * eliminating the need for temporary directories. During the write phase, files are uploaded as
 * multipart uploads and their upload metadata (uploadId, partETags) is stored in commit messages.
 * During the commit phase, all multipart uploads are completed atomically using
 * completeMultipartUpload.
 *
 * <p>This approach provides true atomic commits on object stores without relying on rename
 * operations, which are not atomic on most object storage systems.
 */
public class MagicS3ACommitter extends FormatTableCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(MagicS3ACommitter.class);

    private final Map<Path, MultipartUploadOutputStream> activeUploads;
    private final boolean isS3FileSystem;

    public MagicS3ACommitter(FormatTable formatTable) {
        super(formatTable);
        this.activeUploads = new HashMap<>();

        String location = formatTable.location();
        this.isS3FileSystem =
                location.startsWith("s3://")
                        || location.startsWith("s3a://")
                        || location.startsWith("s3n://");

        LOG.info("Initialized MagicS3ACommitter for {} file system", isS3FileSystem ? "S3" : "OSS");
    }

    @Override
    public PositionOutputStream createWriter(Path filePath) throws IOException {
        LOG.debug("Creating multipart upload writer for {}", filePath);

        // Get the underlying Hadoop FileSystem
        FileSystem hadoopFS = getHadoopFileSystem(filePath);

        // Create multipart upload output stream that writes directly to final destination
        MultipartUploadOutputStream outputStream =
                new MultipartUploadOutputStream(filePath, hadoopFS);

        // Track active uploads for cleanup if needed
        activeUploads.put(filePath, outputStream);

        return outputStream;
    }

    @Override
    public void commitFiles(List<CommitMessage> commitMessages) throws IOException {
        LOG.info("Committing {} files using MagicS3ACommitter strategy", commitMessages.size());

        List<MagicS3ACommitMessage> s3aMessages = extractS3AMessages(commitMessages);

        if (s3aMessages.isEmpty()) {
            LOG.warn("No MagicS3ACommitMessage found in commit messages");
            return;
        }

        List<IOException> failures = new ArrayList<>();
        int successCount = 0;

        for (MagicS3ACommitMessage message : s3aMessages) {
            try {
                completeMultipartUpload(message);
                successCount++;
                LOG.debug("Completed multipart upload for object: {}", message.getObjectName());
            } catch (Exception e) {
                IOException ioException =
                        new IOException(
                                "Failed to complete multipart upload for object: "
                                        + message.getObjectName(),
                                e);
                failures.add(ioException);
                LOG.error(
                        "Failed to complete multipart upload for object: {}",
                        message.getObjectName(),
                        e);
            }
        }

        // Clear active uploads as they're now committed or failed
        activeUploads.clear();

        if (!failures.isEmpty()) {
            LOG.error("Failed to commit {} out of {} files", failures.size(), s3aMessages.size());
            IOException combined =
                    new IOException(
                            String.format(
                                    "Failed to commit %d out of %d files via multipart upload",
                                    failures.size(), s3aMessages.size()));
            for (IOException failure : failures) {
                combined.addSuppressed(failure);
            }
            throw combined;
        }

        LOG.info("Successfully committed {} files using multipart upload", successCount);
    }

    @Override
    public void abortFiles(List<CommitMessage> commitMessages) throws IOException {
        LOG.info("Aborting {} files and cleaning up multipart uploads", commitMessages.size());

        List<MagicS3ACommitMessage> s3aMessages = extractS3AMessages(commitMessages);

        // Abort all active uploads first
        for (MultipartUploadOutputStream stream : activeUploads.values()) {
            try {
                stream.abort();
            } catch (Exception e) {
                LOG.warn("Failed to abort active upload: {}", stream.getObjectName(), e);
            }
        }
        activeUploads.clear();

        // Abort multipart uploads based on commit messages
        for (MagicS3ACommitMessage message : s3aMessages) {
            try {
                abortMultipartUpload(message);
                LOG.debug("Aborted multipart upload for object: {}", message.getObjectName());
            } catch (Exception e) {
                LOG.warn(
                        "Failed to abort multipart upload for object: {}",
                        message.getObjectName(),
                        e);
            }
        }
    }

    private List<MagicS3ACommitMessage> extractS3AMessages(List<CommitMessage> commitMessages) {
        List<MagicS3ACommitMessage> s3aMessages = new ArrayList<>();

        for (CommitMessage message : commitMessages) {
            if (message instanceof MagicS3ACommitMessage) {
                s3aMessages.add((MagicS3ACommitMessage) message);
            }
        }

        return s3aMessages;
    }

    private FileSystem getHadoopFileSystem(Path path) throws IOException {
        try {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path.toUri());
            return hadoopPath.getFileSystem(new org.apache.hadoop.conf.Configuration());
        } catch (Exception e) {
            throw new IOException("Failed to get Hadoop FileSystem for path: " + path, e);
        }
    }

    private void completeMultipartUpload(MagicS3ACommitMessage message) throws Exception {
        String objectName = message.getObjectName();
        String uploadId = message.getUploadId();
        List<PartETag> partETags = message.getPartETags();

        if (partETags == null || partETags.isEmpty()) {
            throw new IllegalArgumentException(
                    "No part ETags found for multipart upload: " + objectName);
        }

        LOG.debug("Completing multipart upload for {} with {} parts", objectName, partETags.size());

        FileSystem hadoopFS = getHadoopFileSystemForObject(objectName);

        if (isS3FileSystem) {
            completeS3MultipartUpload(hadoopFS, objectName, uploadId, partETags);
        } else {
            completeOSSMultipartUpload(hadoopFS, objectName, uploadId, partETags);
        }
    }

    private void abortMultipartUpload(MagicS3ACommitMessage message) throws Exception {
        String objectName = message.getObjectName();
        String uploadId = message.getUploadId();

        FileSystem hadoopFS = getHadoopFileSystemForObject(objectName);

        if (isS3FileSystem) {
            abortS3MultipartUpload(hadoopFS, objectName, uploadId);
        } else {
            abortOSSMultipartUpload(hadoopFS, objectName, uploadId);
        }
    }

    private FileSystem getHadoopFileSystemForObject(String objectName) throws IOException {
        String location = formatTable.location();
        String fullPath =
                location.endsWith("/") ? location + objectName : location + "/" + objectName;
        return getHadoopFileSystem(new Path(fullPath));
    }

    // S3-specific methods using reflection to avoid direct dependencies
    private void completeS3MultipartUpload(
            FileSystem fs, String objectName, String uploadId, List<PartETag> partETags)
            throws Exception {
        // Convert PartETag list to the format expected by S3AFileSystem
        List<Object> s3PartETags = new ArrayList<>();
        for (PartETag partETag : partETags) {
            // Create S3A PartETag object using reflection
            Class<?> s3aPartETagClass =
                    Class.forName("org.apache.hadoop.fs.s3a.commit.files.PartETag");
            Object s3aPartETag =
                    s3aPartETagClass
                            .getConstructor(int.class, String.class)
                            .newInstance(partETag.getPartNumber(), partETag.getETag());
            s3PartETags.add(s3aPartETag);
        }

        Method method =
                fs.getClass()
                        .getMethod(
                                "completeMultipartUpload", String.class, String.class, List.class);
        method.invoke(fs, objectName, uploadId, s3PartETags);
    }

    private void abortS3MultipartUpload(FileSystem fs, String objectName, String uploadId)
            throws Exception {
        Method method = fs.getClass().getMethod("abortMultipartUpload", String.class, String.class);
        method.invoke(fs, objectName, uploadId);
    }

    // OSS-specific methods using reflection to avoid direct dependencies
    private void completeOSSMultipartUpload(
            FileSystem fs, String objectName, String uploadId, List<PartETag> partETags)
            throws Exception {
        // Convert PartETag list to the format expected by AliyunOSSFileSystem
        List<Object> ossPartETags = new ArrayList<>();
        for (PartETag partETag : partETags) {
            // Create OSS PartETag object using reflection
            Class<?> ossPartETagClass = Class.forName("com.aliyun.oss.model.PartETag");
            Object ossPartETag =
                    ossPartETagClass
                            .getConstructor(int.class, String.class)
                            .newInstance(partETag.getPartNumber(), partETag.getETag());
            ossPartETags.add(ossPartETag);
        }

        Method method =
                fs.getClass()
                        .getMethod(
                                "completeMultipartUpload", String.class, String.class, List.class);
        method.invoke(fs, objectName, uploadId, ossPartETags);
    }

    private void abortOSSMultipartUpload(FileSystem fs, String objectName, String uploadId)
            throws Exception {
        Method method = fs.getClass().getMethod("abortMultipartUpload", String.class, String.class);
        method.invoke(fs, objectName, uploadId);
    }
}
