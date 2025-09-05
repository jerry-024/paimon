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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Information about a multipart upload operation for object stores (S3, OSS, etc.). This class
 * stores the metadata required to track and complete multipart uploads.
 */
@Public
public class MultipartUploadInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The unique upload identifier assigned by the object store. */
    private final String uploadId;

    /** The bucket name in the object store. */
    private final String bucketName;

    /** The object key/name in the object store. */
    private final String objectKey;

    /** The temporary/magic path where data is initially written. */
    private final Path tempPath;

    /** The final path where the object should appear after commit. */
    private final Path finalPath;

    /** The partition path (optional). */
    private final String partitionPath;

    /** List of completed parts with their ETags. */
    private final List<PartETag> completedParts;

    /** Total size of the upload in bytes. */
    private final Long totalSize;

    /** Optional metadata for the upload. */
    private final Object metadata;

    public MultipartUploadInfo(
            String uploadId,
            String bucketName,
            String objectKey,
            Path tempPath,
            Path finalPath,
            String partitionPath,
            List<PartETag> completedParts,
            Long totalSize,
            Object metadata) {
        this.uploadId = uploadId;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.tempPath = tempPath;
        this.finalPath = finalPath;
        this.partitionPath = partitionPath;
        this.completedParts = completedParts;
        this.totalSize = totalSize;
        this.metadata = metadata;
    }

    public String getUploadId() {
        return uploadId;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectKey() {
        return objectKey;
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

    public List<PartETag> getCompletedParts() {
        return completedParts;
    }

    public Long getTotalSize() {
        return totalSize;
    }

    public Object getMetadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultipartUploadInfo that = (MultipartUploadInfo) o;
        return Objects.equals(uploadId, that.uploadId)
                && Objects.equals(objectKey, that.objectKey)
                && Objects.equals(bucketName, that.bucketName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uploadId, objectKey, bucketName);
    }

    @Override
    public String toString() {
        return String.format(
                "MultipartUploadInfo{uploadId='%s', bucket='%s', key='%s', parts=%d}",
                uploadId,
                bucketName,
                objectKey,
                completedParts != null ? completedParts.size() : 0);
    }

    /** Represents a completed part of a multipart upload with its ETag. */
    public static class PartETag implements Serializable {

        private static final long serialVersionUID = 1L;

        /** The part number (1-based). */
        private final int partNumber;

        /** The ETag returned by the object store for this part. */
        private final String eTag;

        public PartETag(int partNumber, String eTag) {
            this.partNumber = partNumber;
            this.eTag = eTag;
        }

        public int getPartNumber() {
            return partNumber;
        }

        public String getETag() {
            return eTag;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartETag partETag = (PartETag) o;
            return partNumber == partETag.partNumber && Objects.equals(eTag, partETag.eTag);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partNumber, eTag);
        }

        @Override
        public String toString() {
            return String.format("PartETag{partNumber=%d, eTag='%s'}", partNumber, eTag);
        }
    }
}
