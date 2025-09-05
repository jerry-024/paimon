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

package org.apache.paimon.oss;

import org.apache.paimon.fs.MagicCommitterOutputStream;
import org.apache.paimon.fs.MultipartUploadInfo;
import org.apache.paimon.fs.Path;

import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * OSS-specific implementation of MagicCommitterOutputStream that uses AliyunOSSFileSystem for
 * multipart upload operations.
 *
 * <p>This implementation leverages the Hadoop OSS filesystem's underlying Aliyun OSS SDK to perform
 * multipart upload operations directly against OSS.
 */
public class OSSMagicCommitterOutputStream extends MagicCommitterOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(OSSMagicCommitterOutputStream.class);

    private final AliyunOSSFileSystem ossFileSystem;

    public OSSMagicCommitterOutputStream(
            AliyunOSSFileSystem ossFileSystem,
            String bucketName,
            String objectKey,
            Path tempPath,
            Path finalPath,
            String partitionPath,
            int partSize) {
        super(bucketName, objectKey, tempPath, finalPath, partitionPath, partSize);
        this.ossFileSystem = ossFileSystem;
    }

    @Override
    protected String initiateMultipartUpload() throws IOException {
        try {
            // Use reflection to access the underlying OSS client
            Object ossClient = getOSSClient(ossFileSystem);

            Class<?> initiateRequestClass =
                    Class.forName("com.aliyun.oss.model.InitiateMultipartUploadRequest");
            Object initiateRequest =
                    initiateRequestClass
                            .getConstructor(String.class, String.class)
                            .newInstance(bucketName, objectKey);

            Object initiateResult =
                    ossClient
                            .getClass()
                            .getMethod("initiateMultipartUpload", initiateRequestClass)
                            .invoke(ossClient, initiateRequest);

            String uploadId =
                    (String)
                            initiateResult
                                    .getClass()
                                    .getMethod("getUploadId")
                                    .invoke(initiateResult);

            LOG.info(
                    "Initiated OSS multipart upload: bucket={}, key={}, uploadId={}",
                    bucketName,
                    objectKey,
                    uploadId);
            return uploadId;

        } catch (Exception e) {
            throw new IOException("Failed to initiate OSS multipart upload for " + objectKey, e);
        }
    }

    @Override
    protected String uploadPart(
            String uploadId, int partNumber, byte[] data, int offset, int length)
            throws IOException {
        try {
            Object ossClient = getOSSClient(ossFileSystem);

            Class<?> uploadPartRequestClass =
                    Class.forName("com.aliyun.oss.model.UploadPartRequest");
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
                    ossClient
                            .getClass()
                            .getMethod("uploadPart", uploadPartRequestClass)
                            .invoke(ossClient, uploadPartRequest);

            // Get the ETag
            String eTag =
                    (String) uploadResult.getClass().getMethod("getETag").invoke(uploadResult);

            LOG.debug("Uploaded OSS part {}: eTag={}, size={}", partNumber, eTag, length);
            return eTag;

        } catch (Exception e) {
            throw new IOException(
                    "Failed to upload OSS part " + partNumber + " for " + objectKey, e);
        }
    }

    @Override
    protected void completeMultipartUpload(
            String uploadId, List<MultipartUploadInfo.PartETag> parts) throws IOException {
        try {
            Object ossClient = getOSSClient(ossFileSystem);

            // Create PartETag objects
            Class<?> partETagClass = Class.forName("com.aliyun.oss.model.PartETag");
            java.util.List<Object> ossPartETags = new java.util.ArrayList<>();

            for (MultipartUploadInfo.PartETag part : parts) {
                Object partETag =
                        partETagClass
                                .getConstructor(int.class, String.class)
                                .newInstance(part.getPartNumber(), part.getETag());
                ossPartETags.add(partETag);
            }

            // Create complete request
            Class<?> completeRequestClass =
                    Class.forName("com.aliyun.oss.model.CompleteMultipartUploadRequest");
            Object completeRequest =
                    completeRequestClass
                            .getConstructor(
                                    String.class, String.class, String.class, java.util.List.class)
                            .newInstance(bucketName, objectKey, uploadId, ossPartETags);

            // Complete the upload
            Object completeResult =
                    ossClient
                            .getClass()
                            .getMethod("completeMultipartUpload", completeRequestClass)
                            .invoke(ossClient, completeRequest);

            String resultETag =
                    (String) completeResult.getClass().getMethod("getETag").invoke(completeResult);

            LOG.info(
                    "Completed OSS multipart upload: uploadId={}, parts={}, eTag={}",
                    uploadId,
                    parts.size(),
                    resultETag);

        } catch (Exception e) {
            throw new IOException(
                    "Failed to complete OSS multipart upload " + uploadId + " for " + objectKey, e);
        }
    }

    @Override
    protected void abortMultipartUpload(String uploadId) throws IOException {
        try {
            Object ossClient = getOSSClient(ossFileSystem);

            Class<?> abortRequestClass =
                    Class.forName("com.aliyun.oss.model.AbortMultipartUploadRequest");
            Object abortRequest =
                    abortRequestClass
                            .getConstructor(String.class, String.class, String.class)
                            .newInstance(bucketName, objectKey, uploadId);

            ossClient
                    .getClass()
                    .getMethod("abortMultipartUpload", abortRequestClass)
                    .invoke(ossClient, abortRequest);

            LOG.info("Aborted OSS multipart upload: uploadId={}", uploadId);

        } catch (Exception e) {
            throw new IOException(
                    "Failed to abort OSS multipart upload " + uploadId + " for " + objectKey, e);
        }
    }

    /**
     * Extracts the underlying OSS client from the AliyunOSSFileSystem using reflection. This is
     * necessary because AliyunOSSFileSystem doesn't expose the OSS client directly.
     */
    private Object getOSSClient(AliyunOSSFileSystem ossFileSystem) throws Exception {
        // Access the OSS client through reflection
        // This approach may need to be updated if the AliyunOSSFileSystem internals change
        java.lang.reflect.Field ossField = ossFileSystem.getClass().getDeclaredField("ossClient");
        ossField.setAccessible(true);
        return ossField.get(ossFileSystem);
    }

    /** Extracts bucket name from OSS path. */
    public static String extractBucketName(String ossPath) {
        if (ossPath.startsWith("oss://")) {
            URI uri = URI.create(ossPath);
            return uri.getHost();
        }
        throw new IllegalArgumentException("Invalid OSS path: " + ossPath);
    }

    /** Extracts object key from OSS path. */
    public static String extractObjectKey(String ossPath) {
        if (ossPath.startsWith("oss://")) {
            URI uri = URI.create(ossPath);
            String path = uri.getPath();
            return path.startsWith("/") ? path.substring(1) : path;
        }
        throw new IllegalArgumentException("Invalid OSS path: " + ossPath);
    }
}
