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

package org.apache.paimon.s3;

import org.apache.paimon.fs.MagicCommitterOutputStream;
import org.apache.paimon.fs.MultipartUploadInfo;
import org.apache.paimon.fs.Path;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * S3-specific implementation of MagicCommitterOutputStream that uses S3A FileSystem for multipart
 * upload operations.
 *
 * <p>This implementation leverages the Hadoop S3A filesystem's underlying AWS SDK to perform
 * multipart upload operations directly against S3.
 */
public class S3MagicCommitterOutputStream extends MagicCommitterOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(S3MagicCommitterOutputStream.class);

    private final S3AFileSystem s3aFileSystem;

    public S3MagicCommitterOutputStream(
            S3AFileSystem s3aFileSystem,
            String bucketName,
            String objectKey,
            Path tempPath,
            Path finalPath,
            String partitionPath,
            int partSize) {
        super(bucketName, objectKey, tempPath, finalPath, partitionPath, partSize);
        this.s3aFileSystem = s3aFileSystem;
    }

    @Override
    protected String initiateMultipartUpload() throws IOException {
        try {
            // Use reflection to access the underlying S3 client
            // This is necessary because S3AFileSystem doesn't expose multipart upload APIs directly
            Object s3Client = getS3Client(s3aFileSystem);

            Class<?> initiateRequestClass =
                    Class.forName("com.amazonaws.services.s3.model.InitiateMultipartUploadRequest");
            Object initiateRequest =
                    initiateRequestClass
                            .getConstructor(String.class, String.class)
                            .newInstance(bucketName, objectKey);

            Object initiateResult =
                    s3Client.getClass()
                            .getMethod("initiateMultipartUpload", initiateRequestClass)
                            .invoke(s3Client, initiateRequest);

            String uploadId =
                    (String)
                            initiateResult
                                    .getClass()
                                    .getMethod("getUploadId")
                                    .invoke(initiateResult);

            LOG.info(
                    "Initiated S3 multipart upload: bucket={}, key={}, uploadId={}",
                    bucketName,
                    objectKey,
                    uploadId);
            return uploadId;

        } catch (Exception e) {
            throw new IOException("Failed to initiate S3 multipart upload for " + objectKey, e);
        }
    }

    @Override
    protected String uploadPart(
            String uploadId, int partNumber, byte[] data, int offset, int length)
            throws IOException {
        try {
            Object s3Client = getS3Client(s3aFileSystem);

            Class<?> uploadPartRequestClass =
                    Class.forName("com.amazonaws.services.s3.model.UploadPartRequest");
            Object uploadPartRequest = uploadPartRequestClass.newInstance();

            // Set request properties
            uploadPartRequestClass
                    .getMethod("setBucketName", String.class)
                    .invoke(uploadPartRequest, bucketName);
            uploadPartRequestClass
                    .getMethod("setKey", String.class)
                    .invoke(uploadPartRequest, objectKey);
            uploadPartRequestClass
                    .getMethod("setUploadId", String.class)
                    .invoke(uploadPartRequest, uploadId);
            uploadPartRequestClass
                    .getMethod("setPartNumber", int.class)
                    .invoke(uploadPartRequest, partNumber);
            uploadPartRequestClass
                    .getMethod("setPartSize", long.class)
                    .invoke(uploadPartRequest, (long) length);

            // Create input stream for the part data
            ByteArrayInputStream partStream = new ByteArrayInputStream(data, offset, length);
            uploadPartRequestClass
                    .getMethod("setInputStream", java.io.InputStream.class)
                    .invoke(uploadPartRequest, partStream);

            // Upload the part
            Object uploadResult =
                    s3Client.getClass()
                            .getMethod("uploadPart", uploadPartRequestClass)
                            .invoke(s3Client, uploadPartRequest);

            // Get the ETag
            String eTag =
                    (String) uploadResult.getClass().getMethod("getETag").invoke(uploadResult);

            LOG.debug("Uploaded S3 part {}: eTag={}, size={}", partNumber, eTag, length);
            return eTag;

        } catch (Exception e) {
            throw new IOException(
                    "Failed to upload S3 part " + partNumber + " for " + objectKey, e);
        }
    }

    @Override
    protected void completeMultipartUpload(
            String uploadId, List<MultipartUploadInfo.PartETag> parts) throws IOException {
        try {
            Object s3Client = getS3Client(s3aFileSystem);

            // Create PartETag objects
            Class<?> partETagClass = Class.forName("com.amazonaws.services.s3.model.PartETag");
            java.util.List<Object> s3PartETags = new java.util.ArrayList<>();

            for (MultipartUploadInfo.PartETag part : parts) {
                Object partETag =
                        partETagClass
                                .getConstructor(int.class, String.class)
                                .newInstance(part.getPartNumber(), part.getETag());
                s3PartETags.add(partETag);
            }

            // Create complete request
            Class<?> completeRequestClass =
                    Class.forName("com.amazonaws.services.s3.model.CompleteMultipartUploadRequest");
            Object completeRequest =
                    completeRequestClass
                            .getConstructor(
                                    String.class, String.class, String.class, java.util.List.class)
                            .newInstance(bucketName, objectKey, uploadId, s3PartETags);

            // Complete the upload
            Object completeResult =
                    s3Client.getClass()
                            .getMethod("completeMultipartUpload", completeRequestClass)
                            .invoke(s3Client, completeRequest);

            String resultETag =
                    (String) completeResult.getClass().getMethod("getETag").invoke(completeResult);

            LOG.info(
                    "Completed S3 multipart upload: uploadId={}, parts={}, eTag={}",
                    uploadId,
                    parts.size(),
                    resultETag);

        } catch (Exception e) {
            throw new IOException(
                    "Failed to complete S3 multipart upload " + uploadId + " for " + objectKey, e);
        }
    }

    @Override
    protected void abortMultipartUpload(String uploadId) throws IOException {
        try {
            Object s3Client = getS3Client(s3aFileSystem);

            Class<?> abortRequestClass =
                    Class.forName("com.amazonaws.services.s3.model.AbortMultipartUploadRequest");
            Object abortRequest =
                    abortRequestClass
                            .getConstructor(String.class, String.class, String.class)
                            .newInstance(bucketName, objectKey, uploadId);

            s3Client.getClass()
                    .getMethod("abortMultipartUpload", abortRequestClass)
                    .invoke(s3Client, abortRequest);

            LOG.info("Aborted S3 multipart upload: uploadId={}", uploadId);

        } catch (Exception e) {
            throw new IOException(
                    "Failed to abort S3 multipart upload " + uploadId + " for " + objectKey, e);
        }
    }

    /**
     * Extracts the underlying S3 client from the S3AFileSystem using reflection. This is necessary
     * because S3AFileSystem doesn't expose the S3 client directly.
     */
    private Object getS3Client(S3AFileSystem s3aFileSystem) throws Exception {
        // Access the S3 client through reflection
        // This approach may need to be updated if the S3AFileSystem internals change
        java.lang.reflect.Field s3Field = s3aFileSystem.getClass().getDeclaredField("s3");
        s3Field.setAccessible(true);
        return s3Field.get(s3aFileSystem);
    }

    /** Extracts bucket name from S3 path. */
    public static String extractBucketName(String s3Path) {
        if (s3Path.startsWith("s3://")
                || s3Path.startsWith("s3a://")
                || s3Path.startsWith("s3n://")) {
            URI uri = URI.create(s3Path);
            return uri.getHost();
        }
        throw new IllegalArgumentException("Invalid S3 path: " + s3Path);
    }

    /** Extracts object key from S3 path. */
    public static String extractObjectKey(String s3Path) {
        if (s3Path.startsWith("s3://")
                || s3Path.startsWith("s3a://")
                || s3Path.startsWith("s3n://")) {
            URI uri = URI.create(s3Path);
            String path = uri.getPath();
            return path.startsWith("/") ? path.substring(1) : path;
        }
        throw new IllegalArgumentException("Invalid S3 path: " + s3Path);
    }
}
