################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Tests for global index functionality."""

import unittest
import numpy as np

from pypaimon.globalindex.roaring_bitmap import RoaringBitmap64
from pypaimon.globalindex.range import Range
from pypaimon.globalindex.global_index_result import GlobalIndexResult, LazyGlobalIndexResult
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.vector_search_result import DictBasedVectorSearchResult
from pypaimon.globalindex.faiss.faiss_options import (
    FaissVectorIndexOptions,
    FaissVectorMetric,
    FaissIndexType,
)
from pypaimon.globalindex.faiss.faiss_index_meta import FaissIndexMeta


class TestRoaringBitmap64(unittest.TestCase):
    """Tests for RoaringBitmap64."""

    def test_add_and_contains(self):
        bitmap = RoaringBitmap64()
        bitmap.add(1)
        bitmap.add(5)
        bitmap.add(10)

        self.assertTrue(bitmap.contains(1))
        self.assertTrue(bitmap.contains(5))
        self.assertTrue(bitmap.contains(10))
        self.assertFalse(bitmap.contains(2))
        self.assertFalse(bitmap.contains(100))

    def test_add_range(self):
        bitmap = RoaringBitmap64()
        bitmap.add_range(1, 5)

        self.assertTrue(bitmap.contains(1))
        self.assertTrue(bitmap.contains(3))
        self.assertTrue(bitmap.contains(5))
        self.assertFalse(bitmap.contains(0))
        self.assertFalse(bitmap.contains(6))

    def test_and_operation(self):
        a = RoaringBitmap64()
        a.add_range(1, 10)

        b = RoaringBitmap64()
        b.add_range(5, 15)

        result = RoaringBitmap64.and_(a, b)
        
        self.assertEqual(result.cardinality(), 6)  # 5, 6, 7, 8, 9, 10
        self.assertTrue(result.contains(5))
        self.assertTrue(result.contains(10))
        self.assertFalse(result.contains(4))
        self.assertFalse(result.contains(11))

    def test_or_operation(self):
        a = RoaringBitmap64()
        a.add_range(1, 5)

        b = RoaringBitmap64()
        b.add_range(3, 8)

        result = RoaringBitmap64.or_(a, b)
        
        self.assertEqual(result.cardinality(), 8)  # 1-8
        self.assertTrue(result.contains(1))
        self.assertTrue(result.contains(8))
        self.assertFalse(result.contains(0))
        self.assertFalse(result.contains(9))

    def test_serialize_deserialize(self):
        bitmap = RoaringBitmap64()
        bitmap.add_range(1, 100)

        data = bitmap.serialize()
        restored = RoaringBitmap64.deserialize(data)

        self.assertEqual(bitmap, restored)


class TestRange(unittest.TestCase):
    """Tests for Range."""

    def test_contains(self):
        r = Range(5, 10)
        
        self.assertTrue(r.contains(5))
        self.assertTrue(r.contains(7))
        self.assertTrue(r.contains(10))
        self.assertFalse(r.contains(4))
        self.assertFalse(r.contains(11))

    def test_overlaps(self):
        r1 = Range(1, 5)
        r2 = Range(3, 8)
        r3 = Range(6, 10)

        self.assertTrue(r1.overlaps(r2))
        self.assertFalse(r1.overlaps(r3))
        self.assertTrue(r2.overlaps(r3))

    def test_exclude(self):
        r = Range(1, 10)
        excluded = r.exclude([Range(3, 5)])

        self.assertEqual(len(excluded), 2)
        self.assertEqual(excluded[0], Range(1, 2))
        self.assertEqual(excluded[1], Range(6, 10))

    def test_sort_and_merge(self):
        ranges = [Range(1, 5), Range(7, 10), Range(4, 8)]
        merged = Range.sort_and_merge_overlap(ranges)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0], Range(1, 10))


class TestGlobalIndexResult(unittest.TestCase):
    """Tests for GlobalIndexResult."""

    def test_create_empty(self):
        result = GlobalIndexResult.create_empty()
        self.assertTrue(result.is_empty())

    def test_from_range(self):
        result = GlobalIndexResult.from_range(Range(1, 5))
        
        self.assertFalse(result.is_empty())
        self.assertEqual(result.results().cardinality(), 5)
        self.assertTrue(1 in result.results())
        self.assertTrue(5 in result.results())

    def test_and_operation(self):
        r1 = GlobalIndexResult.from_range(Range(1, 10))
        r2 = GlobalIndexResult.from_range(Range(5, 15))

        result = r1.and_(r2)
        self.assertEqual(result.results().cardinality(), 6)

    def test_or_operation(self):
        r1 = GlobalIndexResult.from_range(Range(1, 5))
        r2 = GlobalIndexResult.from_range(Range(10, 15))

        result = r1.or_(r2)
        self.assertEqual(result.results().cardinality(), 11)  # 1-5 + 10-15

    def test_offset(self):
        r = GlobalIndexResult.from_range(Range(1, 5))
        offset_r = r.offset(10)

        self.assertTrue(11 in offset_r.results())
        self.assertTrue(15 in offset_r.results())
        self.assertFalse(1 in offset_r.results())


class TestVectorSearch(unittest.TestCase):
    """Tests for VectorSearch."""

    def test_create_vector_search(self):
        vector = [1.0, 2.0, 3.0]
        search = VectorSearch(vector=vector, limit=10, field_name="embedding")

        self.assertEqual(search.limit, 10)
        self.assertEqual(search.field_name, "embedding")
        self.assertEqual(len(search.vector), 3)

    def test_vector_search_with_numpy(self):
        vector = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        search = VectorSearch(vector=vector, limit=5, field_name="vec")

        self.assertEqual(search.limit, 5)
        self.assertEqual(search.field_name, "vec")

    def test_invalid_limit(self):
        with self.assertRaises(ValueError):
            VectorSearch(vector=[1.0, 2.0], limit=0, field_name="vec")

    def test_empty_field_name(self):
        with self.assertRaises(ValueError):
            VectorSearch(vector=[1.0, 2.0], limit=10, field_name="")


class TestVectorSearchResult(unittest.TestCase):
    """Tests for VectorSearchGlobalIndexResult."""

    def test_dict_based_result(self):
        scores = {1: 0.9, 2: 0.8, 3: 0.7}
        result = DictBasedVectorSearchResult(scores)

        self.assertEqual(result.results().cardinality(), 3)
        self.assertEqual(result.score_getter()(1), 0.9)
        self.assertEqual(result.score_getter()(2), 0.8)
        self.assertEqual(result.score_getter()(3), 0.7)
        self.assertIsNone(result.score_getter()(100))


class TestFaissOptions(unittest.TestCase):
    """Tests for FAISS vector index options."""

    def test_default_options(self):
        options = FaissVectorIndexOptions()

        self.assertEqual(options.dimension, 128)
        self.assertEqual(options.metric, FaissVectorMetric.L2)
        self.assertEqual(options.index_type, FaissIndexType.IVF_SQ8)
        self.assertEqual(options.m, 32)
        self.assertEqual(options.ef_construction, 40)
        self.assertEqual(options.ef_search, 16)
        self.assertEqual(options.nlist, 100)
        self.assertEqual(options.nprobe, 64)
        self.assertFalse(options.normalize)

    def test_from_dict(self):
        options_dict = {
            'vector.dim': 256,
            'vector.metric': 'INNER_PRODUCT',
            'vector.index-type': 'HNSW',
            'vector.normalize': True,
        }
        options = FaissVectorIndexOptions.from_options(options_dict)

        self.assertEqual(options.dimension, 256)
        self.assertEqual(options.metric, FaissVectorMetric.INNER_PRODUCT)
        self.assertEqual(options.index_type, FaissIndexType.HNSW)
        self.assertTrue(options.normalize)

    def test_to_dict(self):
        options = FaissVectorIndexOptions(dimension=512, normalize=True)
        d = options.to_dict()

        self.assertEqual(d['vector.dim'], 512)
        self.assertTrue(d['vector.normalize'])


class TestFaissIndexMeta(unittest.TestCase):
    """Tests for FaissIndexMeta."""

    def test_serialize_deserialize(self):
        meta = FaissIndexMeta(
            dim=128,
            metric_value=0,
            index_type_ordinal=4,
            num_vectors=1000,
            min_id=0,
            max_id=999
        )

        data = meta.serialize()
        restored = FaissIndexMeta.deserialize(data)

        self.assertEqual(meta.dim, restored.dim)
        self.assertEqual(meta.metric_value, restored.metric_value)
        self.assertEqual(meta.index_type_ordinal, restored.index_type_ordinal)
        self.assertEqual(meta.num_vectors, restored.num_vectors)
        self.assertEqual(meta.min_id, restored.min_id)
        self.assertEqual(meta.max_id, restored.max_id)


# Check if FAISS is available
try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False


@unittest.skipUnless(FAISS_AVAILABLE, "FAISS not installed")
class TestFaissVectorSearchE2E(unittest.TestCase):
    """
    End-to-end tests for vector search using FAISS index.
    
    These tests demonstrate the complete flow:
    1. Create FAISS index
    2. Add vectors with IDs
    3. Save index to file
    4. Load via FaissVectorGlobalIndexReader
    5. Perform vector search
    6. Verify results with scores
    """

    def setUp(self):
        """Set up test fixtures."""
        import tempfile
        import os
        
        self.temp_dir = tempfile.mkdtemp()
        self.dimension = 64
        self.n_vectors = 1000
        
        # Generate random vectors
        np.random.seed(42)
        self.vectors = np.random.random((self.n_vectors, self.dimension)).astype(np.float32)
        self.ids = np.arange(self.n_vectors, dtype=np.int64)

    def tearDown(self):
        """Clean up temp files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_e2e_ivf_sq8_vector_search(self):
        """
        E2E test: Create IVF_SQ8 index, add vectors, train, search.
        
        IVF_SQ8 is a quantized index that requires training before use.
        This test verifies the complete workflow including training.
        """
        import os
        from unittest.mock import MagicMock
        from pypaimon.globalindex.faiss.faiss_index import FaissIndex
        from pypaimon.globalindex.faiss.faiss_vector_reader import FaissVectorGlobalIndexReader
        from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
        
        # Step 1: Create IVF_SQ8 index
        nlist = 10  # Number of clusters for IVF
        index = FaissIndex.create_ivf_sq8_index(
            self.dimension, 
            nlist, 
            FaissVectorMetric.L2
        )
        
        # Step 2: Train index with vectors (required for IVF)
        training_vectors = np.ascontiguousarray(self.vectors)
        index.train(training_vectors)
        self.assertTrue(index.is_trained())
        
        # Step 3: Add vectors with IDs
        index.add_with_ids(
            np.ascontiguousarray(self.vectors),
            np.ascontiguousarray(self.ids)
        )
        self.assertEqual(index.size(), self.n_vectors)
        
        # Step 4: Save index to file
        index_path = os.path.join(self.temp_dir, 'ivf_sq8.faiss')
        faiss.write_index(index._index, index_path)
        index.close()
        
        # Step 5: Create FaissVectorGlobalIndexReader
        meta = FaissIndexMeta(
            dim=self.dimension,
            metric_value=0,  # L2
            index_type_ordinal=4,  # IVF_SQ8
            num_vectors=self.n_vectors,
            min_id=0,
            max_id=self.n_vectors - 1
        )
        
        io_meta = GlobalIndexIOMeta(
            file_name='ivf_sq8.faiss',
            file_size=os.path.getsize(index_path),
            metadata=meta.serialize()
        )
        
        # Mock file_io to read from temp directory
        mock_file_io = MagicMock()
        mock_input_stream = MagicMock()
        with open(index_path, 'rb') as f:
            index_data = f.read()
        mock_input_stream.read.return_value = index_data
        mock_input_stream.__enter__ = MagicMock(return_value=mock_input_stream)
        mock_input_stream.__exit__ = MagicMock(return_value=False)
        mock_file_io.new_input_stream.return_value = mock_input_stream
        
        options = FaissVectorIndexOptions(
            dimension=self.dimension,
            metric=FaissVectorMetric.L2,
            index_type=FaissIndexType.IVF_SQ8,
            nlist=nlist,
            nprobe=nlist  # Search all clusters for high recall
        )
        
        reader = FaissVectorGlobalIndexReader(
            file_io=mock_file_io,
            index_path=self.temp_dir,
            io_metas=[io_meta],
            options=options
        )
        
        try:
            # Step 6: Perform vector search
            query_idx = 42
            query_vector = self.vectors[query_idx]
            vector_search = VectorSearch(
                vector=query_vector,
                limit=10,
                field_name='embedding'
            )
            
            result = reader.visit_vector_search(vector_search)
            
            # Step 7: Verify results
            self.assertIsNotNone(result)
            self.assertFalse(result.is_empty())
            
            # Get result row IDs
            result_ids = list(result.results())
            self.assertLessEqual(len(result_ids), 10)
            
            # Query vector itself should be in results (exact match)
            self.assertIn(query_idx, result_ids)
            
            # Check scores - exact match should have high score
            score_getter = result.score_getter()
            score = score_getter(query_idx)
            self.assertIsNotNone(score)
            # For L2 distance, exact match has distance ~0, score ~1.0
            self.assertGreater(score, 0.9)
            
            # Compute ground truth with numpy for recall check
            distances = np.linalg.norm(self.vectors - query_vector, axis=1)
            ground_truth = set(np.argsort(distances)[:10].tolist())
            
            # Check recall (IVF_SQ8 is approximate, but with nprobe=nlist should be high)
            result_set = set(result_ids)
            recall = len(result_set & ground_truth) / len(ground_truth)
            self.assertGreaterEqual(recall, 0.8, 
                f"Expected recall >= 80%, got {recall*100}%")
            
        finally:
            reader.close()


if __name__ == '__main__':
    unittest.main()
