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
import org.apache.paimon.table.format.HDFSCommitter.HDFSTempFileInfo;
import org.apache.paimon.table.sink.CommitMessage;

import java.util.Objects;

/**
 * Commit message for HDFS committer that contains temporary file information for rename-based
 * atomic commit operations.
 */
@Public
public class HDFSCommitMessage implements CommitMessage {

    private static final long serialVersionUID = 1L;

    private final BinaryRow partition;
    private final int bucket;
    private final HDFSTempFileInfo tempFileInfo;

    public HDFSCommitMessage(BinaryRow partition, int bucket, HDFSTempFileInfo tempFileInfo) {
        this.partition = partition;
        this.bucket = bucket;
        this.tempFileInfo = tempFileInfo;
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

    public HDFSTempFileInfo getTempFileInfo() {
        return tempFileInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HDFSCommitMessage that = (HDFSCommitMessage) o;
        return bucket == that.bucket
                && Objects.equals(partition, that.partition)
                && Objects.equals(tempFileInfo, that.tempFileInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, tempFileInfo);
    }

    @Override
    public String toString() {
        return String.format(
                "HDFSCommitMessage{partition=%s, bucket=%d, tempFileInfo=%s}",
                partition, bucket, tempFileInfo);
    }
}
