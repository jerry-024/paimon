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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks multipart upload operations and persists their metadata for recovery purposes.
 *
 * <p>This class provides functionality to:
 *
 * <ul>
 *   <li>Record when multipart uploads are started
 *   <li>Track upload progress and metadata
 *   <li>Persist upload state to disk for recovery
 *   <li>Load pending uploads after restart/failure
 *   <li>Clean up completed or aborted uploads
 * </ul>
 *
 * <p>The tracker uses JSON files to persist upload metadata, allowing for recovery even after
 * process restart or failure.
 */
@Public
public class MultipartUploadTracker {

    private static final Logger LOG = LoggerFactory.getLogger(MultipartUploadTracker.class);

    private static final String PENDING_UPLOADS_FILE = "pending-uploads.json";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final FileIO fileIO;
    private final Path metadataDir;
    private final Map<String, MultipartUploadInfo> pendingUploads;

    /**
     * Creates a new tracker that stores metadata in the specified directory.
     *
     * @param fileIO the file I/O implementation to use
     * @param metadataDir the directory where metadata files will be stored
     */
    public MultipartUploadTracker(FileIO fileIO, Path metadataDir) {
        this.fileIO = fileIO;
        this.metadataDir = metadataDir;
        this.pendingUploads = new ConcurrentHashMap<>();
    }

    /**
     * Records that a multipart upload has started.
     *
     * @param uploadInfo the upload information to track
     * @throws IOException if the metadata cannot be persisted
     */
    public void recordUploadStart(MultipartUploadInfo uploadInfo) throws IOException {
        String key = createUploadKey(uploadInfo);
        pendingUploads.put(key, uploadInfo);
        persistPendingUploads();

        LOG.debug("Recorded start of multipart upload: {}", uploadInfo);
    }

    /**
     * Updates the progress of an existing multipart upload.
     *
     * @param uploadInfo the updated upload information
     * @throws IOException if the metadata cannot be persisted
     */
    public void updateUploadProgress(MultipartUploadInfo uploadInfo) throws IOException {
        String key = createUploadKey(uploadInfo);
        if (pendingUploads.containsKey(key)) {
            pendingUploads.put(key, uploadInfo);
            persistPendingUploads();

            LOG.debug("Updated multipart upload progress: {}", uploadInfo);
        } else {
            LOG.warn("Attempted to update unknown upload: {}", uploadInfo);
        }
    }

    /**
     * Records that a multipart upload has been completed successfully.
     *
     * @param objectKey the object key of the completed upload
     * @throws IOException if the metadata cannot be persisted
     */
    public void recordUploadComplete(String objectKey) throws IOException {
        MultipartUploadInfo removed = pendingUploads.remove(objectKey);
        if (removed != null) {
            persistPendingUploads();
            LOG.info("Recorded completion of multipart upload: {}", removed);
        } else {
            LOG.warn("Attempted to complete unknown upload for object: {}", objectKey);
        }
    }

    /**
     * Records that a multipart upload has been aborted.
     *
     * @param objectKey the object key of the aborted upload
     * @throws IOException if the metadata cannot be persisted
     */
    public void recordUploadAbort(String objectKey) throws IOException {
        MultipartUploadInfo removed = pendingUploads.remove(objectKey);
        if (removed != null) {
            persistPendingUploads();
            LOG.info("Recorded abort of multipart upload: {}", removed);
        } else {
            LOG.warn("Attempted to abort unknown upload for object: {}", objectKey);
        }
    }

    /**
     * Loads all pending uploads from persistent storage.
     *
     * @return a map of object keys to upload information
     * @throws IOException if the metadata cannot be loaded
     */
    public Map<String, MultipartUploadInfo> loadPendingUploads() throws IOException {
        Path metadataFile = new Path(metadataDir, PENDING_UPLOADS_FILE);

        if (!fileIO.exists(metadataFile)) {
            LOG.debug("No pending uploads metadata file found: {}", metadataFile);
            return new HashMap<>();
        }

        try (Reader reader =
                new InputStreamReader(
                        fileIO.newInputStream(metadataFile), StandardCharsets.UTF_8)) {

            @SuppressWarnings("unchecked")
            Map<String, Object> rawData = OBJECT_MAPPER.readValue(reader, Map.class);
            Map<String, MultipartUploadInfo> result = new HashMap<>();

            for (Map.Entry<String, Object> entry : rawData.entrySet()) {
                try {
                    String json = OBJECT_MAPPER.writeValueAsString(entry.getValue());
                    MultipartUploadInfo uploadInfo =
                            OBJECT_MAPPER.readValue(json, MultipartUploadInfo.class);
                    result.put(entry.getKey(), uploadInfo);
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to deserialize upload info for key {}: {}",
                            entry.getKey(),
                            e.getMessage());
                }
            }

            // Update in-memory state
            pendingUploads.clear();
            pendingUploads.putAll(result);

            LOG.info("Loaded {} pending uploads from metadata", result.size());
            return new HashMap<>(result);

        } catch (JsonProcessingException e) {
            throw new IOException("Failed to parse pending uploads metadata", e);
        }
    }

    /**
     * Returns the current set of pending uploads.
     *
     * @return a copy of the pending uploads map
     */
    public Map<String, MultipartUploadInfo> getPendingUploads() {
        return new HashMap<>(pendingUploads);
    }

    /**
     * Clears all pending uploads from memory and persistent storage.
     *
     * @throws IOException if the metadata cannot be cleared
     */
    public void clearPendingUploads() throws IOException {
        pendingUploads.clear();
        persistPendingUploads();
        LOG.info("Cleared all pending uploads");
    }

    /**
     * Cleans up old metadata files and directories.
     *
     * @throws IOException if cleanup fails
     */
    public void cleanup() throws IOException {
        if (fileIO.exists(metadataDir)) {
            fileIO.delete(metadataDir, true);
            LOG.debug("Cleaned up metadata directory: {}", metadataDir);
        }
    }

    private String createUploadKey(MultipartUploadInfo uploadInfo) {
        return uploadInfo.getObjectKey();
    }

    private void persistPendingUploads() throws IOException {
        // Ensure metadata directory exists
        if (!fileIO.exists(metadataDir)) {
            fileIO.mkdirs(metadataDir);
        }

        Path metadataFile = new Path(metadataDir, PENDING_UPLOADS_FILE);

        try (Writer writer =
                new OutputStreamWriter(
                        fileIO.newOutputStream(metadataFile, true), StandardCharsets.UTF_8)) {

            OBJECT_MAPPER.writeValue(writer, pendingUploads);

        } catch (JsonProcessingException e) {
            throw new IOException("Failed to serialize pending uploads metadata", e);
        }

        LOG.debug("Persisted {} pending uploads to {}", pendingUploads.size(), metadataFile);
    }
}
