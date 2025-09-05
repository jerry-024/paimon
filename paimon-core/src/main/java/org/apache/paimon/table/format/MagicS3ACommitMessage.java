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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;
import java.util.Objects;

/**
 * Commit message for MagicS3ACommitter that contains multipart upload information. This message
 * stores the objectName, uploadId, and partETags required to complete the multipart upload during
 * the commit phase.
 */
@Public
public class MagicS3ACommitMessage implements CommitMessage {

    private static final long serialVersionUID = 1L;

    private final BinaryRow partition;
    private final int bucket;
    private final String objectName;
    private final String uploadId;
    private final List<PartETag> partETags;
    private final long fileSize;

    public MagicS3ACommitMessage(
            BinaryRow partition,
            int bucket,
            String objectName,
            String uploadId,
            List<PartETag> partETags,
            long fileSize) {
        this.partition = partition;
        this.bucket = bucket;
        this.objectName = objectName;
        this.uploadId = uploadId;
        this.partETags = partETags;
        this.fileSize = fileSize;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public Integer totalBuckets() {
        return 1; // Format tables don't use bucketing
    }

    public String getObjectName() {
        return objectName;
    }

    public String getUploadId() {
        return uploadId;
    }

    public List<PartETag> getPartETags() {
        return partETags;
    }

    public long getFileSize() {
        return fileSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MagicS3ACommitMessage that = (MagicS3ACommitMessage) o;
        return bucket == that.bucket
                && fileSize == that.fileSize
                && Objects.equals(partition, that.partition)
                && Objects.equals(objectName, that.objectName)
                && Objects.equals(uploadId, that.uploadId)
                && Objects.equals(partETags, that.partETags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, objectName, uploadId, partETags, fileSize);
    }

    @Override
    public String toString() {
        return String.format(
                "MagicS3ACommitMessage{objectName='%s', uploadId='%s', partCount=%d, fileSize=%d}",
                objectName, uploadId, partETags != null ? partETags.size() : 0, fileSize);
    }

    /** Represents an ETag for a part of a multipart upload. */
    public static class PartETag implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final int partNumber;
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
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
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
