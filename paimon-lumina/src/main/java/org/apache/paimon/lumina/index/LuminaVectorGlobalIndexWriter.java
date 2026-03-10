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

package org.apache.paimon.lumina.index;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import org.aliyun.lumina.LuminaFileOutput;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Vector global index writer using Lumina. */
public class LuminaVectorGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

    private static final String FILE_NAME_PREFIX = "lumina";

    private final GlobalIndexFileWriter fileWriter;
    private final LuminaVectorIndexOptions options;
    private final int sizePerIndex;
    private final int dim;

    private long count = 0; // monotonically increasing global row ID across all index files
    private long currentIndexMinId = Long.MAX_VALUE;
    private long currentIndexMaxId = Long.MIN_VALUE;
    private List<float[]> pendingVectors;
    private final List<ResultEntry> results;

    public LuminaVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            LuminaVectorIndexOptions options) {
        this.fileWriter = fileWriter;
        this.options = options;
        this.dim = options.dimension();
        int configuredSize = options.sizePerIndex();
        long buildMemoryLimit = options.buildMemoryLimit();
        int maxByDim =
                (int) Math.min(configuredSize, buildMemoryLimit / ((long) dim * Float.BYTES));
        this.sizePerIndex = Math.max(maxByDim, 1);
        this.pendingVectors = new ArrayList<>();
        this.results = new ArrayList<>();

        validateFieldType(fieldType);
    }

    private void validateFieldType(DataType dataType) {
        if (!(dataType instanceof ArrayType)) {
            throw new IllegalArgumentException(
                    "Lumina vector index requires ArrayType, but got: " + dataType);
        }
        DataType elementType = ((ArrayType) dataType).getElementType();
        if (!(elementType instanceof FloatType)) {
            throw new IllegalArgumentException(
                    "Lumina vector index requires float array, but got: " + elementType);
        }
    }

    @Override
    public void write(Object fieldData) {
        float[] vector;
        if (fieldData == null) {
            throw new IllegalArgumentException("Field data must not be null");
        }
        if (fieldData instanceof float[]) {
            vector = ((float[]) fieldData).clone();
        } else if (fieldData instanceof InternalArray) {
            vector = ((InternalArray) fieldData).toFloatArray();
        } else {
            throw new RuntimeException(
                    "Unsupported vector type: " + fieldData.getClass().getName());
        }
        checkDimension(vector);

        pendingVectors.add(vector);
        currentIndexMinId = Math.min(currentIndexMinId, count);
        currentIndexMaxId = Math.max(currentIndexMaxId, count);
        count++;

        try {
            if (pendingVectors.size() >= sizePerIndex) {
                buildAndFlushIndex();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (!pendingVectors.isEmpty()) {
                buildAndFlushIndex();
            }
            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Lumina vector global index", e);
        }
    }

    /**
     * Build a complete DiskANN index from the accumulated vectors: create DirectByteBuffers on
     * demand, pretrain, insert all vectors in a single batch, dump directly to the output stream,
     * and release buffers.
     */
    private void buildAndFlushIndex() throws IOException {
        int n = pendingVectors.size();
        if (n == 0) {
            return;
        }

        try (LuminaIndex index = createIndex()) {
            // Build the full vector buffer from accumulated vectors.
            ByteBuffer vectorBuffer = buildVectorBuffer(pendingVectors);
            ByteBuffer idBuffer = buildIdBuffer(n, currentIndexMinId);

            // Pretrain phase.
            int trainingSize = Math.min(n, options.trainingSize());
            if (trainingSize == n) {
                index.pretrain(vectorBuffer, n);
            } else {
                int[] sampleIndices = reservoirSample(n, trainingSize);
                ByteBuffer trainingBuffer = LuminaIndex.allocateVectorBuffer(trainingSize, dim);
                FloatBuffer trainingView = trainingBuffer.asFloatBuffer();
                for (int i = 0; i < trainingSize; i++) {
                    trainingView.put(pendingVectors.get(sampleIndices[i]));
                }
                index.pretrain(trainingBuffer, trainingSize);
            }

            // Insert phase.
            index.insertBatch(vectorBuffer, idBuffer, n);

            // Dump to output stream — direct streaming, no temp files.
            String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                index.dump(new OutputStreamFileOutput(out));
                out.flush();
            }

            LuminaIndexMeta meta =
                    new LuminaIndexMeta(
                            dim,
                            options.metric().getValue(),
                            "DISKANN",
                            n,
                            currentIndexMinId,
                            currentIndexMaxId);
            results.add(new ResultEntry(fileName, n, meta.serialize()));
        }

        // Release accumulated vectors.
        pendingVectors.clear();
        currentIndexMinId = Long.MAX_VALUE;
        currentIndexMaxId = Long.MIN_VALUE;
    }

    /**
     * Build a DirectByteBuffer from accumulated vectors. Each float[] is copied into a contiguous
     * native-order buffer suitable for JNI. The source arrays remain valid until {@link
     * #pendingVectors} is cleared after a successful build.
     */
    private ByteBuffer buildVectorBuffer(List<float[]> vectors) {
        ByteBuffer buffer = LuminaIndex.allocateVectorBuffer(vectors.size(), dim);
        FloatBuffer floatView = buffer.asFloatBuffer();
        for (float[] vec : vectors) {
            floatView.put(vec);
        }
        return buffer;
    }

    /** Build an ID buffer with monotonically increasing IDs starting from {@code startId}. */
    private static ByteBuffer buildIdBuffer(int count, long startId) {
        ByteBuffer buffer = LuminaIndex.allocateIdBuffer(count);
        LongBuffer longView = buffer.asLongBuffer();
        for (int i = 0; i < count; i++) {
            longView.put(i, startId + i);
        }
        return buffer;
    }

    private LuminaIndex createIndex() {
        Map<String, String> extraOptions = new LinkedHashMap<>();
        extraOptions.put("encoding.type", options.encodingType());

        if (options.pretrainSampleRatio() != 1.0) {
            extraOptions.put(
                    "pretrain.sample_ratio", String.valueOf(options.pretrainSampleRatio()));
        }

        if (options.diskannEfConstruction() != null) {
            extraOptions.put(
                    "diskann.build.ef_construction",
                    String.valueOf(options.diskannEfConstruction()));
        }
        if (options.diskannNeighborCount() != null) {
            extraOptions.put(
                    "diskann.build.neighbor_count", String.valueOf(options.diskannNeighborCount()));
        }
        if (options.diskannBuildThreadCount() != null) {
            extraOptions.put(
                    "diskann.build.thread_count",
                    String.valueOf(options.diskannBuildThreadCount()));
        }

        return LuminaIndex.createForBuild(dim, options.metric(), extraOptions);
    }

    /**
     * Selects {@code k} indices from [0, n) using reservoir sampling (Algorithm R).
     *
     * <p>When {@code k >= n} all indices are returned in order. Otherwise a random representative
     * subset is chosen, ensuring training data covers the full vector distribution instead of being
     * biased toward the first {@code k} inserted vectors.
     */
    private static int[] reservoirSample(int n, int k) {
        int[] reservoir = new int[k];
        for (int i = 0; i < k; i++) {
            reservoir[i] = i;
        }
        if (k < n) {
            Random random = new Random(42);
            for (int i = k; i < n; i++) {
                int j = random.nextInt(i + 1);
                if (j < k) {
                    reservoir[j] = i;
                }
            }
        }
        return reservoir;
    }

    private void checkDimension(float[] vector) {
        if (vector.length != dim) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector dimension mismatch: expected %d, but got %d",
                            dim, vector.length));
        }
    }

    @Override
    public void close() {
        if (pendingVectors != null) {
            pendingVectors.clear();
            pendingVectors = null;
        }
    }

    /** Adapts a {@link PositionOutputStream} to the {@link LuminaFileOutput} JNI callback API. */
    static class OutputStreamFileOutput implements LuminaFileOutput {
        private final PositionOutputStream out;

        OutputStreamFileOutput(PositionOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public long getPos() throws IOException {
            return out.getPos();
        }

        @Override
        public void close() {
            // Lifecycle managed by the caller's try-with-resources.
        }
    }
}
