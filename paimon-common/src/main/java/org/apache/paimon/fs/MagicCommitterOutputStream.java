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

package org.apache.paimon.fs;

import org.apache.paimon.annotation.Public;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An output stream that supports magic committer pattern using multipart uploads.
 *
 * <p>This stream buffers data in memory and uploads it as parts when the buffer reaches a certain
 * size. The upload metadata is tracked and can be used later for atomic commit operations.
 *
 * <p>This implementation is designed for object stores like S3 and OSS that support multipart
 * upload operations.
 */
@Public
public abstract class MagicCommitterOutputStream extends PositionOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(MagicCommitterOutputStream.class);

    /** Default part size for multipart uploads (5MB). */
    public static final int DEFAULT_PART_SIZE = 5 * 1024 * 1024;

    /** Minimum part size allowed by most object stores (5MB). */
    public static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    /** Maximum number of parts allowed by most object stores. */
    public static final int MAX_PARTS = 10000;

    protected final String bucketName;
    protected final String objectKey;
    protected final Path tempPath;
    protected final Path finalPath;
    protected final String partitionPath;
    protected final int partSize;

    private final ByteArrayOutputStream buffer;
    private final List<MultipartUploadInfo.PartETag> completedParts;

    private String uploadId;
    private long position;
    private int nextPartNumber;
    private boolean closed;

    protected MagicCommitterOutputStream(
            String bucketName,
            String objectKey,
            Path tempPath,
            Path finalPath,
            String partitionPath,
            int partSize) {
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.tempPath = tempPath;
        this.finalPath = finalPath;
        this.partitionPath = partitionPath;
        this.partSize = Math.max(partSize, MIN_PART_SIZE);
        this.buffer = new ByteArrayOutputStream();
        this.completedParts = new ArrayList<>();
        this.nextPartNumber = 1;
        this.position = 0;
        this.closed = false;
    }

    /**
     * Initializes the multipart upload and returns the upload ID. This method should be implemented
     * by subclasses for specific object stores.
     */
    protected abstract String initiateMultipartUpload() throws IOException;

    /**
     * Uploads a part and returns the ETag. This method should be implemented by subclasses for
     * specific object stores.
     */
    protected abstract String uploadPart(
            String uploadId, int partNumber, byte[] data, int offset, int length)
            throws IOException;

    /**
     * Completes the multipart upload with the given parts. This method should be implemented by
     * subclasses for specific object stores.
     */
    protected abstract void completeMultipartUpload(
            String uploadId, List<MultipartUploadInfo.PartETag> parts) throws IOException;

    /**
     * Aborts the multipart upload. This method should be implemented by subclasses for specific
     * object stores.
     */
    protected abstract void abortMultipartUpload(String uploadId) throws IOException;

    @Override
    public void write(int b) throws IOException {
        ensureOpen();
        ensureUploadInitialized();

        buffer.write(b);
        position++;

        if (buffer.size() >= partSize) {
            flushBuffer();
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        ensureUploadInitialized();

        int remaining = len;
        int offset = off;

        while (remaining > 0) {
            int spaceInBuffer = partSize - buffer.size();
            int toWrite = Math.min(remaining, spaceInBuffer);

            buffer.write(b, offset, toWrite);
            position += toWrite;
            offset += toWrite;
            remaining -= toWrite;

            if (buffer.size() >= partSize) {
                flushBuffer();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();
        // For multipart uploads, we don't flush incomplete parts
        // They will be uploaded when the buffer is full or on close
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            if (uploadId != null) {
                // Upload any remaining data in the buffer
                if (buffer.size() > 0) {
                    flushBuffer();
                }

                // Complete the multipart upload
                completeMultipartUpload(uploadId, completedParts);
                LOG.info(
                        "Completed multipart upload for {} with {} parts",
                        objectKey,
                        completedParts.size());
            }
        } catch (Exception e) {
            // If completion fails, try to abort the upload
            if (uploadId != null) {
                try {
                    abortMultipartUpload(uploadId);
                    LOG.warn(
                            "Aborted multipart upload {} due to error during completion", uploadId);
                } catch (Exception abortException) {
                    LOG.error("Failed to abort multipart upload {}", uploadId, abortException);
                    e.addSuppressed(abortException);
                }
            }
            throw e;
        } finally {
            closed = true;
        }
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    /**
     * Returns the multipart upload information for this stream. This can be used by the committer
     * to track and manage the upload.
     */
    public MultipartUploadInfo getUploadInfo() {
        return new MultipartUploadInfo(
                uploadId,
                bucketName,
                objectKey,
                tempPath,
                finalPath,
                partitionPath,
                new ArrayList<>(completedParts),
                position,
                null);
    }

    /**
     * Aborts the current upload. This should be called if the write operation needs to be
     * cancelled.
     */
    public void abort() throws IOException {
        if (uploadId != null && !closed) {
            try {
                abortMultipartUpload(uploadId);
                LOG.info("Aborted multipart upload {}", uploadId);
            } finally {
                closed = true;
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    private void ensureUploadInitialized() throws IOException {
        if (uploadId == null) {
            uploadId = initiateMultipartUpload();
            LOG.info("Initiated multipart upload {} for object {}", uploadId, objectKey);
        }
    }

    private void flushBuffer() throws IOException {
        if (buffer.size() == 0) {
            return;
        }

        byte[] data = buffer.toByteArray();
        String eTag = uploadPart(uploadId, nextPartNumber, data, 0, data.length);

        completedParts.add(new MultipartUploadInfo.PartETag(nextPartNumber, eTag));

        LOG.debug(
                "Uploaded part {} for {} (size: {} bytes, eTag: {})",
                nextPartNumber,
                objectKey,
                data.length,
                eTag);

        nextPartNumber++;
        buffer.reset();
    }
}
