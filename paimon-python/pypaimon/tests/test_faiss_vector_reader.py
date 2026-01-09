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

"""Tests for FAISS vector global index reader."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import numpy as np

# Import test dependencies
try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False

from pypaimon.globalindex.roaring_bitmap import RoaringBitmap64
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.faiss.faiss_options import (
    FaissVectorIndexOptions,
    FaissVectorMetric,
    FaissIndexType,
)
from pypaimon.globalindex.faiss.faiss_index_meta import FaissIndexMeta
from pypaimon.globalindex.faiss.faiss_index import FaissIndex, FAISS_AVAILABLE as FAISS_MODULE_AVAILABLE


class TestFaissIndex(unittest.TestCase):
    """Tests for FaissIndex wrapper."""

    @unittest.skipUnless(FAISS_AVAILABLE, "FAISS not installed")
    def test_create_flat_index(self):
        """Test creating a flat index."""
        index = FaissIndex.create_flat_index(128, FaissVectorMetric.L2)
        
        self.assertEqual(index.dimension, 128)
        self.assertEqual(index.metric, FaissVectorMetric.L2)
        self.assertEqual(index.index_type, FaissIndexType.FLAT)
        self.assertEqual(index.size(), 0)
        
        index.close()

    @unittest.skipUnless(FAISS_AVAILABLE, "FAISS not installed")
    def test_create_hnsw_index(self):
        """Test creating an HNSW index."""
        index = FaissIndex.create_hnsw_index(
            dimension=64,
            m=16,
            ef_construction=32,
            metric=FaissVectorMetric.L2
        )
        
        self.assertEqual(index.dimension, 64)
        self.assertEqual(index.index_type, FaissIndexType.HNSW)
        
        index.close()

    @unittest.skipUnless(FAISS_AVAILABLE, "FAISS not installed")
    def test_add_and_search(self):
        """Test adding vectors and searching."""
        dimension = 32
        index = FaissIndex.create_flat_index(dimension, FaissVectorMetric.L2)
        
        # Create test vectors
        n_vectors = 100
        vectors = np.random.random((n_vectors, dimension)).astype(np.float32)
        ids = np.arange(n_vectors, dtype=np.int64)
        
        # Add vectors
        index.add_with_ids(
            np.ascontiguousarray(vectors),
            np.ascontiguousarray(ids)
        )
        
        self.assertEqual(index.size(), n_vectors)
        
        # Search
        query = vectors[0:1]  # Search for first vector
        distances, labels = index.search(query, k=5)
        
        # First result should be the exact match
        self.assertEqual(labels[0, 0], 0)
        self.assertAlmostEqual(distances[0, 0], 0.0, places=5)
        
        index.close()

    @unittest.skipUnless(FAISS_AVAILABLE, "FAISS not installed")
    def test_save_and_load(self):
        """Test saving and loading an index."""
        dimension = 16
        index = FaissIndex.create_flat_index(dimension, FaissVectorMetric.L2)
        
        # Add some vectors
        n_vectors = 50
        vectors = np.random.random((n_vectors, dimension)).astype(np.float32)
        ids = np.arange(n_vectors, dtype=np.int64)
        index.add_with_ids(
            np.ascontiguousarray(vectors),
            np.ascontiguousarray(ids)
        )
        
        # Save to temp file
        with tempfile.NamedTemporaryFile(suffix='.faiss', delete=False) as f:
            temp_path = f.name
        
        try:
            # Save using faiss directly
            faiss.write_index(index._index, temp_path)
            
            # Load
            loaded_index = FaissIndex.from_file(temp_path)
            
            self.assertEqual(loaded_index.size(), n_vectors)
            
            # Search should work
            query = vectors[0:1]
            distances, labels = loaded_index.search(query, k=1)
            self.assertEqual(labels[0, 0], 0)
            
            loaded_index.close()
        finally:
            os.unlink(temp_path)
        
        index.close()

    @unittest.skipUnless(FAISS_AVAILABLE, "FAISS not installed")
    def test_inner_product_metric(self):
        """Test inner product metric."""
        dimension = 32
        index = FaissIndex.create_flat_index(dimension, FaissVectorMetric.INNER_PRODUCT)
        
        self.assertEqual(index.metric, FaissVectorMetric.INNER_PRODUCT)
        
        # Add normalized vectors
        n_vectors = 10
        vectors = np.random.random((n_vectors, dimension)).astype(np.float32)
        # Normalize
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        vectors = vectors / norms
        ids = np.arange(n_vectors, dtype=np.int64)
        
        index.add_with_ids(
            np.ascontiguousarray(vectors),
            np.ascontiguousarray(ids)
        )
        
        # Search
        query = vectors[0:1]
        distances, labels = index.search(query, k=1)
        
        # Should find itself with IP close to 1.0
        self.assertEqual(labels[0, 0], 0)
        self.assertGreater(distances[0, 0], 0.99)
        
        index.close()


class TestFaissVectorGlobalIndexReader(unittest.TestCase):
    """Tests for FaissVectorGlobalIndexReader."""

    def setUp(self):
        """Set up test fixtures."""
        self.options = FaissVectorIndexOptions(
            dimension=32,
            metric=FaissVectorMetric.L2,
            index_type=FaissIndexType.FLAT,
            search_factor=10,
            normalize=False
        )

    @unittest.skipUnless(FAISS_AVAILABLE, "FAISS not installed")
    def test_vector_search_basic(self):
        """Test basic vector search functionality."""
        from pypaimon.globalindex.faiss.faiss_vector_reader import FaissVectorGlobalIndexReader
        
        dimension = 32
        n_vectors = 100
        
        # Create and save a test index
        index = FaissIndex.create_flat_index(dimension, FaissVectorMetric.L2)
        vectors = np.random.random((n_vectors, dimension)).astype(np.float32)
        ids = np.arange(n_vectors, dtype=np.int64)
        index.add_with_ids(
            np.ascontiguousarray(vectors),
            np.ascontiguousarray(ids)
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            index_path = os.path.join(temp_dir, 'test.faiss')
            faiss.write_index(index._index, index_path)
            
            # Create metadata
            meta = FaissIndexMeta(
                dim=dimension,
                metric_value=0,
                index_type_ordinal=0,
                num_vectors=n_vectors,
                min_id=0,
                max_id=n_vectors - 1
            )
            
            io_meta = GlobalIndexIOMeta(
                file_name='test.faiss',
                file_size=os.path.getsize(index_path),
                metadata=meta.serialize()
            )
            
            # Create mock file_io
            mock_file_io = MagicMock()
            mock_input_stream = MagicMock()
            with open(index_path, 'rb') as f:
                index_data = f.read()
            mock_input_stream.read.return_value = index_data
            mock_input_stream.__enter__ = MagicMock(return_value=mock_input_stream)
            mock_input_stream.__exit__ = MagicMock(return_value=False)
            mock_file_io.new_input_stream.return_value = mock_input_stream
            
            # Create reader
            reader = FaissVectorGlobalIndexReader(
                file_io=mock_file_io,
                index_path=temp_dir,
                io_metas=[io_meta],
                options=self.options
            )
            
            try:
                # Perform search
                query_vector = vectors[0]
                vector_search = VectorSearch(
                    vector=query_vector,
                    limit=5,
                    field_name='embedding'
                )
                
                result = reader.visit_vector_search(vector_search)
                
                self.assertIsNotNone(result)
                self.assertFalse(result.is_empty())
                
                # Should find at least the exact match
                row_ids = list(result.results())
                self.assertIn(0, row_ids)
                
            finally:
                reader.close()
        
        index.close()

    @unittest.skipUnless(FAISS_AVAILABLE, "FAISS not installed")
    def test_vector_search_with_filter(self):
        """Test vector search with row ID filter."""
        from pypaimon.globalindex.faiss.faiss_vector_reader import FaissVectorGlobalIndexReader
        
        dimension = 32
        n_vectors = 100
        
        # Create and save a test index
        index = FaissIndex.create_flat_index(dimension, FaissVectorMetric.L2)
        vectors = np.random.random((n_vectors, dimension)).astype(np.float32)
        ids = np.arange(n_vectors, dtype=np.int64)
        index.add_with_ids(
            np.ascontiguousarray(vectors),
            np.ascontiguousarray(ids)
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            index_path = os.path.join(temp_dir, 'test.faiss')
            faiss.write_index(index._index, index_path)
            
            meta = FaissIndexMeta(
                dim=dimension,
                metric_value=0,
                index_type_ordinal=0,
                num_vectors=n_vectors,
                min_id=0,
                max_id=n_vectors - 1
            )
            
            io_meta = GlobalIndexIOMeta(
                file_name='test.faiss',
                file_size=os.path.getsize(index_path),
                metadata=meta.serialize()
            )
            
            mock_file_io = MagicMock()
            mock_input_stream = MagicMock()
            with open(index_path, 'rb') as f:
                index_data = f.read()
            mock_input_stream.read.return_value = index_data
            mock_input_stream.__enter__ = MagicMock(return_value=mock_input_stream)
            mock_input_stream.__exit__ = MagicMock(return_value=False)
            mock_file_io.new_input_stream.return_value = mock_input_stream
            
            reader = FaissVectorGlobalIndexReader(
                file_io=mock_file_io,
                index_path=temp_dir,
                io_metas=[io_meta],
                options=self.options
            )
            
            try:
                # Create filter to only include IDs 10-20
                include_ids = RoaringBitmap64()
                include_ids.add_range(10, 20)
                
                query_vector = vectors[15]  # Search for vector at ID 15
                vector_search = VectorSearch(
                    vector=query_vector,
                    limit=5,
                    field_name='embedding',
                    include_row_ids=include_ids
                )
                
                result = reader.visit_vector_search(vector_search)
                
                self.assertIsNotNone(result)
                
                # All results should be within 10-20
                for row_id in result.results():
                    self.assertGreaterEqual(row_id, 10)
                    self.assertLessEqual(row_id, 20)
                
                # Should include 15 (exact match)
                self.assertIn(15, list(result.results()))
                
            finally:
                reader.close()
        
        index.close()

    @unittest.skipUnless(FAISS_AVAILABLE, "FAISS not installed")
    def test_score_getter(self):
        """Test that scores are properly returned."""
        from pypaimon.globalindex.faiss.faiss_vector_reader import FaissVectorGlobalIndexReader
        from pypaimon.globalindex.vector_search_result import VectorSearchGlobalIndexResult
        
        dimension = 32
        n_vectors = 50
        
        index = FaissIndex.create_flat_index(dimension, FaissVectorMetric.L2)
        vectors = np.random.random((n_vectors, dimension)).astype(np.float32)
        ids = np.arange(n_vectors, dtype=np.int64)
        index.add_with_ids(
            np.ascontiguousarray(vectors),
            np.ascontiguousarray(ids)
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            index_path = os.path.join(temp_dir, 'test.faiss')
            faiss.write_index(index._index, index_path)
            
            meta = FaissIndexMeta(
                dim=dimension,
                metric_value=0,
                index_type_ordinal=0,
                num_vectors=n_vectors,
                min_id=0,
                max_id=n_vectors - 1
            )
            
            io_meta = GlobalIndexIOMeta(
                file_name='test.faiss',
                file_size=os.path.getsize(index_path),
                metadata=meta.serialize()
            )
            
            mock_file_io = MagicMock()
            mock_input_stream = MagicMock()
            with open(index_path, 'rb') as f:
                index_data = f.read()
            mock_input_stream.read.return_value = index_data
            mock_input_stream.__enter__ = MagicMock(return_value=mock_input_stream)
            mock_input_stream.__exit__ = MagicMock(return_value=False)
            mock_file_io.new_input_stream.return_value = mock_input_stream
            
            reader = FaissVectorGlobalIndexReader(
                file_io=mock_file_io,
                index_path=temp_dir,
                io_metas=[io_meta],
                options=self.options
            )
            
            try:
                query_vector = vectors[0]
                vector_search = VectorSearch(
                    vector=query_vector,
                    limit=5,
                    field_name='embedding'
                )
                
                result = reader.visit_vector_search(vector_search)
                
                self.assertIsNotNone(result)
                self.assertIsInstance(result, VectorSearchGlobalIndexResult)
                
                # Get scores
                score_getter = result.score_getter()
                
                # First result (exact match) should have highest score
                score_0 = score_getter(0)
                self.assertIsNotNone(score_0)
                self.assertGreater(score_0, 0)
                
            finally:
                reader.close()
        
        index.close()


class TestFaissIndexMeta(unittest.TestCase):
    """Tests for FaissIndexMeta serialization."""

    def test_serialize_deserialize(self):
        """Test serialization and deserialization."""
        meta = FaissIndexMeta(
            dim=128,
            metric_value=0,
            index_type_ordinal=4,
            num_vectors=1000000,
            min_id=0,
            max_id=999999
        )
        
        data = meta.serialize()
        restored = FaissIndexMeta.deserialize(data)
        
        self.assertEqual(meta.dim, restored.dim)
        self.assertEqual(meta.metric_value, restored.metric_value)
        self.assertEqual(meta.index_type_ordinal, restored.index_type_ordinal)
        self.assertEqual(meta.num_vectors, restored.num_vectors)
        self.assertEqual(meta.min_id, restored.min_id)
        self.assertEqual(meta.max_id, restored.max_id)

    def test_large_values(self):
        """Test with large values."""
        meta = FaissIndexMeta(
            dim=1024,
            metric_value=1,
            index_type_ordinal=2,
            num_vectors=10000000000,  # 10 billion
            min_id=0,
            max_id=9999999999
        )
        
        data = meta.serialize()
        restored = FaissIndexMeta.deserialize(data)
        
        self.assertEqual(meta.num_vectors, restored.num_vectors)
        self.assertEqual(meta.max_id, restored.max_id)


class TestVectorSearchIntegration(unittest.TestCase):
    """Integration tests for vector search."""

    def test_vector_search_creation(self):
        """Test VectorSearch creation and properties."""
        vector = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        search = VectorSearch(vector=vector, limit=10, field_name='embedding')
        
        self.assertEqual(search.limit, 10)
        self.assertEqual(search.field_name, 'embedding')
        np.testing.assert_array_equal(search.vector, vector)

    def test_vector_search_with_include_ids(self):
        """Test VectorSearch with include_row_ids."""
        vector = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        include_ids = RoaringBitmap64()
        include_ids.add_range(0, 100)
        
        search = VectorSearch(
            vector=vector,
            limit=10,
            field_name='embedding',
            include_row_ids=include_ids
        )
        
        self.assertIsNotNone(search.include_row_ids)
        self.assertEqual(search.include_row_ids.cardinality(), 101)

    def test_vector_search_offset_range(self):
        """Test VectorSearch offset_range method."""
        vector = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        include_ids = RoaringBitmap64()
        include_ids.add_range(10, 20)
        
        search = VectorSearch(
            vector=vector,
            limit=10,
            field_name='embedding',
            include_row_ids=include_ids
        )
        
        # Offset by range [5, 25]
        offset_search = search.offset_range(5, 25)
        
        # Original IDs 10-20 should become 5-15 after offsetting by -5
        for row_id in offset_search.include_row_ids:
            self.assertGreaterEqual(row_id, 5)
            self.assertLessEqual(row_id, 15)


class TestFaissVectorIndexOptions(unittest.TestCase):
    """Tests for FaissVectorIndexOptions."""

    def test_default_values(self):
        """Test default option values."""
        options = FaissVectorIndexOptions()
        
        self.assertEqual(options.dimension, 128)
        self.assertEqual(options.metric, FaissVectorMetric.L2)
        self.assertEqual(options.index_type, FaissIndexType.IVF_SQ8)
        self.assertEqual(options.m, 32)
        self.assertEqual(options.ef_construction, 40)
        self.assertEqual(options.ef_search, 16)
        self.assertEqual(options.nlist, 100)
        self.assertEqual(options.nprobe, 64)
        self.assertEqual(options.pq_m, 8)
        self.assertEqual(options.pq_nbits, 8)
        self.assertEqual(options.size_per_index, 2000000)
        self.assertEqual(options.training_size, 500000)
        self.assertEqual(options.search_factor, 10)
        self.assertFalse(options.normalize)

    def test_from_dict(self):
        """Test creating options from dictionary."""
        options_dict = {
            'vector.dim': 256,
            'vector.metric': 'INNER_PRODUCT',
            'vector.index-type': 'HNSW',
            'vector.m': 64,
            'vector.ef-search': 32,
            'vector.normalize': True,
        }
        
        options = FaissVectorIndexOptions.from_options(options_dict)
        
        self.assertEqual(options.dimension, 256)
        self.assertEqual(options.metric, FaissVectorMetric.INNER_PRODUCT)
        self.assertEqual(options.index_type, FaissIndexType.HNSW)
        self.assertEqual(options.m, 64)
        self.assertEqual(options.ef_search, 32)
        self.assertTrue(options.normalize)

    def test_to_dict(self):
        """Test converting options to dictionary."""
        options = FaissVectorIndexOptions(
            dimension=512,
            metric=FaissVectorMetric.INNER_PRODUCT,
            normalize=True
        )
        
        d = options.to_dict()
        
        self.assertEqual(d['vector.dim'], 512)
        self.assertEqual(d['vector.metric'], 'INNER_PRODUCT')
        self.assertTrue(d['vector.normalize'])


if __name__ == '__main__':
    unittest.main()
