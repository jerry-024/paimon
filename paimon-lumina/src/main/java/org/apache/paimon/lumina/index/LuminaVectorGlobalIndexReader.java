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

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.aliyun.lumina.LuminaFileInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * Vector global index reader using Lumina.
 *
 * <p>This reader loads Lumina indices from global index files and performs vector similarity
 * search.
 */
public class LuminaVectorGlobalIndexReader implements GlobalIndexReader {

    private final LuminaIndex[] indices;
    private final LuminaIndexMeta[] indexMetas;
    private final List<SeekableInputStream> openStreams;
    private final List<GlobalIndexIOMeta> ioMetas;
    private final GlobalIndexFileReader fileReader;
    private final DataType fieldType;
    private final LuminaVectorIndexOptions options;
    private volatile boolean metasLoaded = false;
    private volatile boolean indicesLoaded = false;

    public LuminaVectorGlobalIndexReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> ioMetas,
            DataType fieldType,
            LuminaVectorIndexOptions options) {
        this.fileReader = fileReader;
        this.ioMetas = ioMetas;
        this.fieldType = fieldType;
        this.options = options;
        this.indices = new LuminaIndex[ioMetas.size()];
        this.indexMetas = new LuminaIndexMeta[ioMetas.size()];
        this.openStreams = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public Optional<GlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        try {
            ensureLoadMetas();

            RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();

            LuminaIndex[] loadedIndices;
            LuminaIndexMeta[] loadedMetas;

            if (includeRowIds != null) {
                List<Integer> matchingIndices = new ArrayList<>();
                for (int i = 0; i < indexMetas.length; i++) {
                    LuminaIndexMeta meta = indexMetas[i];
                    if (includeRowIds.intersectsRange(meta.minId(), meta.maxId())) {
                        matchingIndices.add(i);
                    }
                }
                if (matchingIndices.isEmpty()) {
                    return Optional.empty();
                }
                ensureLoadIndices(matchingIndices);
            } else {
                ensureLoadAllIndices();
            }

            synchronized (this) {
                loadedIndices = indices.clone();
                loadedMetas = indexMetas.clone();
            }

            return Optional.ofNullable(search(vectorSearch, loadedIndices, loadedMetas));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to search Lumina vector index with fieldName=%s, limit=%d",
                            vectorSearch.fieldName(), vectorSearch.limit()),
                    e);
        }
    }

    private GlobalIndexResult search(
            VectorSearch vectorSearch, LuminaIndex[] loadedIndices, LuminaIndexMeta[] loadedMetas)
            throws IOException {
        validateSearchVector(vectorSearch.vector(), loadedMetas);
        float[] queryVector = ((float[]) vectorSearch.vector()).clone();
        int limit = vectorSearch.limit();

        // Min-heap: smallest score at head, so we can evict the weakest candidate efficiently.
        PriorityQueue<ScoredRow> topK =
                new PriorityQueue<>(limit + 1, Comparator.comparingDouble(s -> s.score));

        RoaringNavigableMap64 includeRowIds = vectorSearch.includeRowIds();

        Map<String, String> filterSearchOptions = null;
        Map<String, String> plainSearchOptions = null;
        if (includeRowIds != null) {
            filterSearchOptions = new LinkedHashMap<>();
            int listSize = (int) Math.min((long) limit * options.searchFactor(), Integer.MAX_VALUE);
            listSize = Math.max(listSize, options.searchListSize());
            filterSearchOptions.put("diskann.search.list_size", String.valueOf(listSize));
            filterSearchOptions.put("search.thread_safe_filter", "true");
        } else {
            plainSearchOptions = new LinkedHashMap<>();
            int listSize = Math.max(limit, options.searchListSize());
            plainSearchOptions.put("diskann.search.list_size", String.valueOf(listSize));
        }

        for (int i = 0; i < loadedIndices.length; i++) {
            LuminaIndex index = loadedIndices[i];
            if (index == null) {
                continue;
            }

            LuminaIndexMeta meta = loadedMetas[i];
            LuminaVectorMetric indexMetric = LuminaVectorMetric.fromValue(meta.metricValue());

            int effectiveK = (int) Math.min(limit, index.size());
            if (effectiveK <= 0) {
                continue;
            }

            if (includeRowIds != null) {
                // scopedIds from the bitmap that fall within [minId, maxId] without materializing
                // the entire
                //     * bitmap. Uses the bitmap iterator and collects only IDs in the given range.
                long[] scopedIds = includeRowIds.toArrayInRange(meta.minId(), meta.maxId());
                if (scopedIds.length == 0) {
                    continue;
                }
                effectiveK = Math.min(effectiveK, scopedIds.length);

                float[] distances = new float[effectiveK];
                long[] labels = new long[effectiveK];
                index.searchWithFilter(
                        queryVector,
                        1,
                        effectiveK,
                        distances,
                        labels,
                        scopedIds,
                        filterSearchOptions);
                collectResults(distances, labels, effectiveK, limit, topK, indexMetric);
            } else {
                float[] distances = new float[effectiveK];
                long[] labels = new long[effectiveK];
                index.search(queryVector, 1, effectiveK, distances, labels, plainSearchOptions);
                collectResults(distances, labels, effectiveK, limit, topK, indexMetric);
            }
        }

        RoaringNavigableMap64 roaringBitmap64 = new RoaringNavigableMap64();
        HashMap<Long, Float> id2scores = new HashMap<>(topK.size());
        for (ScoredRow row : topK) {
            roaringBitmap64.add(row.rowId);
            id2scores.put(row.rowId, row.score);
        }
        return new LuminaScoredGlobalIndexResult(roaringBitmap64, id2scores);
    }

    /**
     * Collect search results into a min-heap of size {@code limit}. The heap keeps the top-k
     * highest-scoring rows; rows with score lower than the current minimum are discarded once the
     * heap is full. The metric from the index file's metadata is used for distance→score conversion
     * so that results remain correct even if table-level options have changed since the index was
     * built.
     */
    private static void collectResults(
            float[] distances,
            long[] labels,
            int count,
            int limit,
            PriorityQueue<ScoredRow> topK,
            LuminaVectorMetric metric) {
        for (int i = 0; i < count; i++) {
            long rowId = labels[i];
            if (rowId < 0) {
                continue;
            }
            float score = convertDistanceToScore(distances[i], metric);
            if (topK.size() < limit) {
                topK.offer(new ScoredRow(rowId, score));
            } else if (score > topK.peek().score) {
                topK.poll();
                topK.offer(new ScoredRow(rowId, score));
            }
        }
    }

    /**
     * Convert a raw distance returned by Lumina into a similarity score using the metric recorded
     * in the index file's metadata, not the current table options. This ensures correct scoring
     * even when table options have been changed after the index was built.
     */
    private static float convertDistanceToScore(float distance, LuminaVectorMetric metric) {
        if (metric == LuminaVectorMetric.L2) {
            return 1.0f / (1.0f + distance);
        } else if (metric == LuminaVectorMetric.COSINE) {
            // Cosine distance is in [0, 2]; convert to similarity in [-1, 1]
            return 1.0f - distance;
        } else {
            // Inner product is already a similarity
            return distance;
        }
    }

    private void validateSearchVector(Object vector, LuminaIndexMeta[] loadedMetas) {
        if (!(vector instanceof float[])) {
            throw new IllegalArgumentException(
                    "Expected float[] vector but got: " + vector.getClass());
        }
        if (!(fieldType instanceof ArrayType)
                || !(((ArrayType) fieldType).getElementType() instanceof FloatType)) {
            throw new IllegalArgumentException(
                    "Lumina currently only supports float arrays, but field type is: " + fieldType);
        }
        int queryDim = ((float[]) vector).length;
        for (LuminaIndexMeta meta : loadedMetas) {
            if (meta != null && queryDim != meta.dim()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Query vector dimension mismatch: index expects %d, but got %d",
                                meta.dim(), queryDim));
            }
        }
    }

    private void ensureLoadMetas() throws IOException {
        if (!metasLoaded) {
            synchronized (this) {
                if (!metasLoaded) {
                    for (int i = 0; i < ioMetas.size(); i++) {
                        byte[] metaBytes = ioMetas.get(i).metadata();
                        indexMetas[i] = LuminaIndexMeta.deserialize(metaBytes);
                    }
                    metasLoaded = true;
                }
            }
        }
    }

    private void ensureLoadAllIndices() throws IOException {
        if (!indicesLoaded) {
            synchronized (this) {
                if (!indicesLoaded) {
                    for (int i = 0; i < ioMetas.size(); i++) {
                        if (indices[i] == null) {
                            loadIndexAt(i);
                        }
                    }
                    indicesLoaded = true;
                }
            }
        }
    }

    private void ensureLoadIndices(List<Integer> positions) throws IOException {
        synchronized (this) {
            for (int pos : positions) {
                if (indices[pos] == null) {
                    loadIndexAt(pos);
                }
            }
            // Check if all indices are now loaded.
            if (!indicesLoaded) {
                boolean allLoaded = true;
                for (LuminaIndex idx : indices) {
                    if (idx == null) {
                        allLoaded = false;
                        break;
                    }
                }
                if (allLoaded) {
                    indicesLoaded = true;
                }
            }
        }
    }

    private void loadIndexAt(int position) throws IOException {
        GlobalIndexIOMeta ioMeta = ioMetas.get(position);
        SeekableInputStream in = fileReader.getInputStream(ioMeta);
        LuminaIndex index = null;
        try {
            index = loadIndex(in, position, ioMeta.fileSize());
            openStreams.add(in);
            indices[position] = index;
        } catch (Exception e) {
            IOUtils.closeQuietly(index);
            IOUtils.closeQuietly(in);
            throw e;
        }
    }

    private LuminaIndex loadIndex(SeekableInputStream in, int position, long fileSize)
            throws IOException {
        LuminaIndexMeta meta = indexMetas[position];
        LuminaVectorMetric metric = LuminaVectorMetric.fromValue(meta.metricValue());

        LuminaFileInput fileInput = new InputStreamFileInput(in);

        Map<String, String> extraOptions = new LinkedHashMap<>();
        extraOptions.put("diskann.search.list_size", String.valueOf(options.searchListSize()));
        return LuminaIndex.fromStream(fileInput, fileSize, meta.dim(), metric, extraOptions);
    }

    @Override
    public void close() throws IOException {
        Throwable firstException = null;

        // Close all indices first (releases native FileReader → JNI global refs).
        for (int i = 0; i < indices.length; i++) {
            LuminaIndex index = indices[i];
            if (index == null) {
                continue;
            }
            try {
                index.close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
            indices[i] = null;
        }

        // Then close underlying streams.
        List<SeekableInputStream> streamsSnapshot;
        synchronized (openStreams) {
            if (openStreams.isEmpty()) {
                streamsSnapshot = null;
            } else {
                streamsSnapshot = new ArrayList<>(openStreams);
                openStreams.clear();
            }
        }
        if (streamsSnapshot != null) {
            for (SeekableInputStream stream : streamsSnapshot) {
                try {
                    if (stream != null) {
                        stream.close();
                    }
                } catch (Throwable t) {
                    if (firstException == null) {
                        firstException = t;
                    } else {
                        firstException.addSuppressed(t);
                    }
                }
            }
        }

        if (firstException != null) {
            if (firstException instanceof IOException) {
                throw (IOException) firstException;
            } else if (firstException instanceof RuntimeException) {
                throw (RuntimeException) firstException;
            } else {
                throw new RuntimeException(
                        "Failed to close Lumina vector global index reader", firstException);
            }
        }
    }

    // =================== unsupported =====================

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    /**
     * Adapts a {@link SeekableInputStream} to the {@link LuminaFileInput} JNI callback API.
     *
     * <p>This mirrors the C++ {@code LuminaFileReader} adapter that bridges Paimon's {@code
     * InputStream} to Lumina's {@code FileReader} interface. The stream lifecycle is managed by the
     * enclosing reader, not by this adapter.
     */
    static class InputStreamFileInput implements LuminaFileInput {
        private final SeekableInputStream in;

        InputStreamFileInput(SeekableInputStream in) {
            this.in = in;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return in.read(b, off, len);
        }

        @Override
        public void seek(long position) throws IOException {
            in.seek(position);
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
        }

        @Override
        public void close() {
            // Stream lifecycle is managed by the enclosing Reader.
        }
    }

    /** A row ID paired with its similarity score, used in the top-k min-heap. */
    private static class ScoredRow {
        final long rowId;
        final float score;

        ScoredRow(long rowId, float score) {
            this.rowId = rowId;
            this.score = score;
        }
    }
}
