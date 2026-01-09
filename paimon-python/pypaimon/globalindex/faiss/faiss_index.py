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

"""
FAISS Index wrapper for Python.

This module provides a Python wrapper for FAISS indices, supporting efficient
approximate nearest neighbor search.
"""

from typing import Tuple
import numpy as np

from pypaimon.globalindex.faiss.faiss_options import FaissVectorMetric, FaissIndexType

# Try to import faiss, but make it optional
try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    faiss = None


class FaissIndex:
    """
    A wrapper class for FAISS index.
    
    This class provides a safe Python API for interacting with FAISS indices.
    """

    def __init__(
        self,
        index,
        dimension: int,
        metric: FaissVectorMetric,
        index_type: FaissIndexType
    ):
        self._index = index
        self._dimension = dimension
        self._metric = metric
        self._index_type = index_type
        self._closed = False

    @staticmethod
    def _ensure_faiss_available():
        """Ensure FAISS is available."""
        if not FAISS_AVAILABLE:
            raise ImportError(
                "FAISS is not installed. Please install it with: "
                "pip install faiss-cpu or pip install faiss-gpu"
            )

    @classmethod
    def create_flat_index(cls, dimension: int, metric: FaissVectorMetric) -> 'FaissIndex':
        """Create a flat index (exact search)."""
        cls._ensure_faiss_available()
        
        metric_type = faiss.METRIC_L2 if metric == FaissVectorMetric.L2 else faiss.METRIC_INNER_PRODUCT
        index = faiss.IndexFlatL2(dimension) if metric == FaissVectorMetric.L2 else faiss.IndexFlatIP(dimension)
        index = faiss.IndexIDMap(index)
        
        return cls(index, dimension, metric, FaissIndexType.FLAT)

    @classmethod
    def create_hnsw_index(
        cls,
        dimension: int,
        m: int,
        ef_construction: int,
        metric: FaissVectorMetric
    ) -> 'FaissIndex':
        """Create an HNSW index."""
        cls._ensure_faiss_available()
        
        metric_type = faiss.METRIC_L2 if metric == FaissVectorMetric.L2 else faiss.METRIC_INNER_PRODUCT
        index = faiss.IndexHNSWFlat(dimension, m, metric_type)
        index.hnsw.efConstruction = ef_construction
        index = faiss.IndexIDMap2(index)
        
        return cls(index, dimension, metric, FaissIndexType.HNSW)

    @classmethod
    def create_ivf_index(
        cls,
        dimension: int,
        nlist: int,
        metric: FaissVectorMetric
    ) -> 'FaissIndex':
        """Create an IVF index."""
        cls._ensure_faiss_available()
        
        metric_type = faiss.METRIC_L2 if metric == FaissVectorMetric.L2 else faiss.METRIC_INNER_PRODUCT
        quantizer = faiss.IndexFlatL2(dimension) if metric == FaissVectorMetric.L2 else faiss.IndexFlatIP(dimension)
        index = faiss.IndexIVFFlat(quantizer, dimension, nlist, metric_type)
        index = faiss.IndexIDMap(index)
        
        return cls(index, dimension, metric, FaissIndexType.IVF)

    @classmethod
    def create_ivf_pq_index(
        cls,
        dimension: int,
        nlist: int,
        m: int,
        nbits: int,
        metric: FaissVectorMetric
    ) -> 'FaissIndex':
        """Create an IVF-PQ index."""
        cls._ensure_faiss_available()
        
        metric_type = faiss.METRIC_L2 if metric == FaissVectorMetric.L2 else faiss.METRIC_INNER_PRODUCT
        quantizer = faiss.IndexFlatL2(dimension) if metric == FaissVectorMetric.L2 else faiss.IndexFlatIP(dimension)
        index = faiss.IndexIVFPQ(quantizer, dimension, nlist, m, nbits)
        index = faiss.IndexIDMap(index)
        
        return cls(index, dimension, metric, FaissIndexType.IVF_PQ)

    @classmethod
    def create_ivf_sq8_index(
        cls,
        dimension: int,
        nlist: int,
        metric: FaissVectorMetric
    ) -> 'FaissIndex':
        """Create an IVF-SQ8 index."""
        cls._ensure_faiss_available()
        
        metric_type = faiss.METRIC_L2 if metric == FaissVectorMetric.L2 else faiss.METRIC_INNER_PRODUCT
        quantizer = faiss.IndexFlatL2(dimension) if metric == FaissVectorMetric.L2 else faiss.IndexFlatIP(dimension)
        index = faiss.IndexIVFScalarQuantizer(
            quantizer, dimension, nlist,
            faiss.ScalarQuantizer.QT_8bit, metric_type
        )
        index = faiss.IndexIDMap(index)
        
        return cls(index, dimension, metric, FaissIndexType.IVF_SQ8)

    @classmethod
    def from_file(cls, file_path: str) -> 'FaissIndex':
        """Load an index from a local file."""
        cls._ensure_faiss_available()
        
        index = faiss.read_index(file_path)
        dimension = index.d
        
        # Detect index type and metric from loaded index
        index_type, metric = cls._detect_index_type(index)
        
        return cls(index, dimension, metric, index_type)
    
    @classmethod
    def _detect_index_type(cls, index) -> Tuple[FaissIndexType, FaissVectorMetric]:
        """Detect the index type and metric from a FAISS index."""
        # Unwrap IndexIDMap if present and downcast to get the correct type
        inner_index = index
        if hasattr(index, 'index'):
            inner_index = faiss.downcast_index(index.index)
        
        # Detect metric
        metric = FaissVectorMetric.L2
        if hasattr(inner_index, 'metric_type'):
            if inner_index.metric_type == faiss.METRIC_INNER_PRODUCT:
                metric = FaissVectorMetric.INNER_PRODUCT
        
        # Detect index type
        index_type = FaissIndexType.UNKNOWN
        type_name = type(inner_index).__name__
        
        if 'Flat' in type_name and 'IVF' not in type_name:
            index_type = FaissIndexType.FLAT
        elif 'HNSW' in type_name:
            index_type = FaissIndexType.HNSW
        elif 'IVFScalarQuantizer' in type_name:
            index_type = FaissIndexType.IVF_SQ8
        elif 'IVFPQ' in type_name:
            index_type = FaissIndexType.IVF_PQ
        elif 'IVF' in type_name:
            index_type = FaissIndexType.IVF
        
        return index_type, metric

    @classmethod
    def from_bytes(cls, data: bytes) -> 'FaissIndex':
        """Load an index from bytes."""
        cls._ensure_faiss_available()
        
        # Write bytes to a temporary file and read
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.faiss') as f:
            f.write(data)
            temp_path = f.name
        
        try:
            index = faiss.read_index(temp_path)
            dimension = index.d
            return cls(index, dimension, FaissVectorMetric.L2, FaissIndexType.UNKNOWN)
        finally:
            os.unlink(temp_path)

    def search(
        self,
        query_vectors: np.ndarray,
        k: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Search for k nearest neighbors.
        
        Args:
            query_vectors: Query vectors of shape (n, dimension)
            k: Number of nearest neighbors
            
        Returns:
            Tuple of (distances, labels) arrays
        """
        self._ensure_open()
        
        if len(query_vectors.shape) == 1:
            query_vectors = query_vectors.reshape(1, -1)
        
        # Ensure float32
        query_vectors = np.ascontiguousarray(query_vectors, dtype=np.float32)
        
        distances, labels = self._index.search(query_vectors, k)
        return distances, labels

    def add_with_ids(self, vectors: np.ndarray, ids: np.ndarray) -> None:
        """Add vectors with IDs to the index."""
        self._ensure_open()
        
        vectors = np.ascontiguousarray(vectors, dtype=np.float32)
        ids = np.ascontiguousarray(ids, dtype=np.int64)
        
        self._index.add_with_ids(vectors, ids)

    def train(self, vectors: np.ndarray) -> None:
        """Train the index (required for IVF-based indices)."""
        self._ensure_open()
        
        vectors = np.ascontiguousarray(vectors, dtype=np.float32)
        self._index.train(vectors)

    def is_trained(self) -> bool:
        """Check if the index is trained."""
        self._ensure_open()
        return self._index.is_trained

    def size(self) -> int:
        """Get the number of vectors in the index."""
        self._ensure_open()
        return self._index.ntotal

    def reset(self) -> None:
        """Reset the index (remove all vectors)."""
        self._ensure_open()
        self._index.reset()

    def set_hnsw_ef_search(self, ef_search: int) -> None:
        """Set HNSW search parameter efSearch."""
        self._ensure_open()
        
        # Try directly
        if hasattr(self._index, 'hnsw'):
            self._index.hnsw.efSearch = ef_search
            return
        
        # Unwrap IndexIDMap if present and downcast
        if hasattr(self._index, 'index'):
            inner = faiss.downcast_index(self._index.index)
            if hasattr(inner, 'hnsw'):
                inner.hnsw.efSearch = ef_search

    def set_ivf_nprobe(self, nprobe: int) -> None:
        """Set IVF search parameter nprobe."""
        self._ensure_open()
        
        # Try to set nprobe directly
        if hasattr(self._index, 'nprobe'):
            self._index.nprobe = nprobe
            return
        
        # Unwrap IndexIDMap if present and downcast to get the correct type
        if hasattr(self._index, 'index'):
            inner = faiss.downcast_index(self._index.index)
            if hasattr(inner, 'nprobe'):
                inner.nprobe = nprobe

    @property
    def dimension(self) -> int:
        """Get the dimension of vectors in the index."""
        return self._dimension

    @property
    def metric(self) -> FaissVectorMetric:
        """Get the metric used by this index."""
        return self._metric

    @property
    def index_type(self) -> FaissIndexType:
        """Get the type of this index."""
        return self._index_type

    def _ensure_open(self) -> None:
        """Ensure the index is not closed."""
        if self._closed:
            raise RuntimeError("Index has been closed")

    def close(self) -> None:
        """Close the index and release resources."""
        if not self._closed:
            self._index = None
            self._closed = True

    def __enter__(self) -> 'FaissIndex':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
