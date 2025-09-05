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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.sink.CommitMessage;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Format table committer that provides different commit strategies based on the underlying file
 * system. For HDFS: Uses rename strategy. For S3/OSS: Uses MagicS3ACommitter approach with
 * multipart upload.
 */
@Public
public abstract class FormatTableCommitter {

    protected final FormatTable formatTable;
    protected final FileIO fileIO;
    protected final String sessionId;

    public FormatTableCommitter(FormatTable formatTable) {
        this.formatTable = formatTable;
        this.fileIO = formatTable.fileIO();
        this.sessionId = generateSessionId();
    }

    /** Creates an appropriate committer based on the file system type. */
    public static FormatTableCommitter create(FormatTable formatTable) {
        String location = formatTable.location();

        if (isS3FileSystem(location) || isOSSFileSystem(location)) {
            return new MagicS3ACommitter(formatTable);
        } else {
            return new HDFSCommitter(formatTable);
        }
    }

    /**
     * Creates a writer for the specified path. For object stores, this may create a multipart
     * upload writer that writes directly to the final destination.
     */
    public abstract PositionOutputStream createWriter(Path filePath) throws IOException;

    /**
     * Commits all files atomically. After this operation, all files become visible.
     *
     * @param commitMessages List of commit messages containing file information
     * @throws IOException if the commit operation fails
     */
    public abstract void commitFiles(List<CommitMessage> commitMessages) throws IOException;

    /**
     * Aborts the write operation and cleans up any temporary resources.
     *
     * @param commitMessages List of commit messages to abort
     * @throws IOException if cleanup fails
     */
    public abstract void abortFiles(List<CommitMessage> commitMessages) throws IOException;

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
}
