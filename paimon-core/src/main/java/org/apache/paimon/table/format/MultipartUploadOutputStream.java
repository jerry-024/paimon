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
import org.apache.paimon.table.format.MagicS3ACommitMessage.PartETag;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * A PositionOutputStream that uses multipart upload for S3 and OSS file systems. This stream writes
 * directly to the final destination without using temporary files. It collects uploadId and
 * partETags which are later used to complete the multipart upload.
 */
public class MultipartUploadOutputStream extends PositionOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(MultipartUploadOutputStream.class);

    // Minimum part size for multipart upload (5MB)
    private static final int MIN_PART_SIZE = 5 * 1024 * 1024;
    // Maximum part size for efficient upload (64MB)
    private static final int MAX_PART_SIZE = 64 * 1024 * 1024;

    private final FileSystem fileSystem;
    private final String objectName;
    private final ByteArrayOutputStream buffer;
    private final List<PartETag> partETags;
    private final boolean isS3FileSystem;

    private String uploadId;
    private long position;
    private int partNumber;
    private boolean closed;

    public MultipartUploadOutputStream(Path filePath, FileSystem fileSystem) throws IOException {
        this.fileSystem = fileSystem;
        this.objectName = extractObjectName(filePath);
        this.buffer = new ByteArrayOutputStream(MAX_PART_SIZE);
        this.partETags = new ArrayList<>();
        this.isS3FileSystem = isS3FileSystemType(fileSystem);
        this.position = 0;
        this.partNumber = 1;
        this.closed = false;

        initializeMultipartUpload();
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        checkClosed();
        buffer.write(b);
        position++;

        if (buffer.size() >= MAX_PART_SIZE) {
            flushCurrentPart();
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkClosed();

        int remaining = len;
        int offset = off;

        while (remaining > 0) {
            int spaceInBuffer = MAX_PART_SIZE - buffer.size();
            int toWrite = Math.min(remaining, spaceInBuffer);

            buffer.write(b, offset, toWrite);
            position += toWrite;
            offset += toWrite;
            remaining -= toWrite;

            if (buffer.size() >= MAX_PART_SIZE) {
                flushCurrentPart();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        // For multipart upload, we flush when buffer reaches threshold
        // Final flush happens in close()
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            // Upload the final part if there's data in buffer
            if (buffer.size() > 0) {
                flushCurrentPart();
            }

            LOG.info(
                    "Multipart upload completed for {} with {} parts",
                    objectName,
                    partETags.size());
        } finally {
            closed = true;
        }
    }

    /** Gets the upload ID for this multipart upload. */
    public String getUploadId() {
        return uploadId;
    }

    /** Gets the list of part ETags for this multipart upload. */
    public List<PartETag> getPartETags() {
        return new ArrayList<>(partETags);
    }

    /** Gets the object name (key) for this upload. */
    public String getObjectName() {
        return objectName;
    }

    /** Aborts the multipart upload, cleaning up all uploaded parts. */
    public void abort() throws IOException {
        if (uploadId == null) {
            return;
        }

        try {
            if (isS3FileSystem) {
                abortS3MultipartUpload();
            } else {
                abortOSSMultipartUpload();
            }
            LOG.info("Aborted multipart upload for {} with uploadId {}", objectName, uploadId);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to abort multipart upload for {} with uploadId {}",
                    objectName,
                    uploadId,
                    e);
        }
    }

    private void initializeMultipartUpload() throws IOException {
        try {
            if (isS3FileSystem) {
                uploadId = initializeS3MultipartUpload();
            } else {
                uploadId = initializeOSSMultipartUpload();
            }
            LOG.info("Initialized multipart upload for {} with uploadId {}", objectName, uploadId);
        } catch (Exception e) {
            throw new IOException("Failed to initialize multipart upload for " + objectName, e);
        }
    }

    private void flushCurrentPart() throws IOException {
        if (buffer.size() == 0) {
            return;
        }

        // For the last part, we allow smaller sizes
        if (buffer.size() < MIN_PART_SIZE && partNumber > 1) {
            // This is fine for the last part
        }

        byte[] partData = buffer.toByteArray();
        buffer.reset();

        try {
            String etag;
            if (isS3FileSystem) {
                etag = uploadS3Part(partData, partNumber);
            } else {
                etag = uploadOSSPart(partData, partNumber);
            }

            partETags.add(new PartETag(partNumber, etag));
            LOG.debug("Uploaded part {} for {} with ETag {}", partNumber, objectName, etag);
            partNumber++;
        } catch (Exception e) {
            throw new IOException("Failed to upload part " + partNumber + " for " + objectName, e);
        }
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Stream has been closed");
        }
    }

    private String extractObjectName(Path path) {
        String pathStr = path.toString();
        if (pathStr.startsWith("s3://")
                || pathStr.startsWith("s3a://")
                || pathStr.startsWith("s3n://")) {
            // Remove s3:// prefix and extract bucket/key
            int idx = pathStr.indexOf('/', 5); // Skip s3://
            return idx >= 0 ? pathStr.substring(idx + 1) : pathStr;
        } else if (pathStr.startsWith("oss://")) {
            // Remove oss:// prefix and extract bucket/key
            int idx = pathStr.indexOf('/', 6); // Skip oss://
            return idx >= 0 ? pathStr.substring(idx + 1) : pathStr;
        }
        return pathStr;
    }

    // Check if the filesystem is S3-compatible
    private boolean isS3FileSystemType(FileSystem fs) {
        String className = fs.getClass().getName();
        return className.contains("S3AFileSystem") || className.contains("s3a");
    }

    // S3-specific methods using reflection to avoid direct dependencies
    private String initializeS3MultipartUpload() throws Exception {
        Method method = fileSystem.getClass().getMethod("initiateMultipartUpload", String.class);
        return (String) method.invoke(fileSystem, objectName);
    }

    private String uploadS3Part(byte[] data, int partNum) throws Exception {
        Method method =
                fileSystem
                        .getClass()
                        .getMethod(
                                "uploadPart",
                                String.class,
                                String.class,
                                int.class,
                                ByteArrayInputStream.class,
                                long.class);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        return (String)
                method.invoke(
                        fileSystem, objectName, uploadId, partNum, inputStream, (long) data.length);
    }

    private void abortS3MultipartUpload() throws Exception {
        Method method =
                fileSystem.getClass().getMethod("abortMultipartUpload", String.class, String.class);
        method.invoke(fileSystem, objectName, uploadId);
    }

    // OSS-specific methods using reflection to avoid direct dependencies
    private String initializeOSSMultipartUpload() throws Exception {
        Method method = fileSystem.getClass().getMethod("initiateMultipartUpload", String.class);
        return (String) method.invoke(fileSystem, objectName);
    }

    private String uploadOSSPart(byte[] data, int partNum) throws Exception {
        Method method =
                fileSystem
                        .getClass()
                        .getMethod(
                                "uploadPart",
                                String.class,
                                String.class,
                                int.class,
                                ByteArrayInputStream.class,
                                long.class);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        return (String)
                method.invoke(
                        fileSystem, objectName, uploadId, partNum, inputStream, (long) data.length);
    }

    private void abortOSSMultipartUpload() throws Exception {
        Method method =
                fileSystem.getClass().getMethod("abortMultipartUpload", String.class, String.class);
        method.invoke(fileSystem, objectName, uploadId);
    }
}
