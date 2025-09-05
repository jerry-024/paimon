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
import org.apache.paimon.table.format.MagicS3ACommitMessage.PartETag;
import org.apache.paimon.utils.SerializationUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link MagicS3ACommitMessage} that provides consistent serialization of multipart
 * upload metadata including objectName, uploadId, and partETags.
 */
@Public
public class MagicS3ACommitMessageSerializer {

    private static final int VERSION = 1;

    /**
     * Serializes a MagicS3ACommitMessage to byte array.
     *
     * @param message the commit message to serialize
     * @return serialized bytes
     * @throws IOException if serialization fails
     */
    public byte[] serialize(MagicS3ACommitMessage message) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try (DataOutputStream dataOut = new DataOutputStream(outputStream)) {
            // Write version for future compatibility
            dataOut.writeInt(VERSION);

            // Write partition information
            BinaryRow partition = message.partition();
            dataOut.writeInt(partition.getFieldCount());
            if (partition.getFieldCount() > 0) {
                byte[] partitionBytes = partition.toBytes();
                dataOut.writeInt(partitionBytes.length);
                dataOut.write(partitionBytes);
            }

            // Write bucket
            dataOut.writeInt(message.bucket());

            // Write multipart upload metadata
            dataOut.writeUTF(message.getObjectName());
            dataOut.writeUTF(message.getUploadId());
            dataOut.writeLong(message.getFileSize());

            // Write part ETags
            List<PartETag> partETags = message.getPartETags();
            dataOut.writeInt(partETags.size());
            for (PartETag partETag : partETags) {
                dataOut.writeInt(partETag.getPartNumber());
                dataOut.writeUTF(partETag.getETag());
            }

            dataOut.flush();
        }

        return outputStream.toByteArray();
    }

    /**
     * Deserializes a MagicS3ACommitMessage from byte array.
     *
     * @param bytes the serialized bytes
     * @return deserialized commit message
     * @throws IOException if deserialization fails
     */
    public MagicS3ACommitMessage deserialize(byte[] bytes) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

        try (DataInputStream dataIn = new DataInputStream(inputStream)) {
            // Read and validate version
            int version = dataIn.readInt();
            if (version != VERSION) {
                throw new IOException(
                        "Unsupported serialization version: " + version + ", expected: " + VERSION);
            }

            // Read partition information
            int partitionFieldCount = dataIn.readInt();
            BinaryRow partition;
            if (partitionFieldCount > 0) {
                int partitionBytesLength = dataIn.readInt();
                byte[] partitionBytes = new byte[partitionBytesLength];
                dataIn.readFully(partitionBytes);
                partition = SerializationUtils.deserializeBinaryRow(partitionBytes);
            } else {
                partition = new BinaryRow(0);
            }

            // Read bucket
            int bucket = dataIn.readInt();

            // Read multipart upload metadata
            String objectName = dataIn.readUTF();
            String uploadId = dataIn.readUTF();
            long fileSize = dataIn.readLong();

            // Read part ETags
            int partCount = dataIn.readInt();
            List<PartETag> partETags = new ArrayList<>(partCount);
            for (int i = 0; i < partCount; i++) {
                int partNumber = dataIn.readInt();
                String eTag = dataIn.readUTF();
                partETags.add(new PartETag(partNumber, eTag));
            }

            return new MagicS3ACommitMessage(
                    partition, bucket, objectName, uploadId, partETags, fileSize);
        }
    }

    /**
     * Gets the version of this serializer.
     *
     * @return serializer version
     */
    public int getVersion() {
        return VERSION;
    }
}
