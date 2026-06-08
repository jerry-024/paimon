# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Lumina vector search benchmark: Local vs Jindo-Cache vs OSS.

Measures search latency across different storage backends using the
lumina-data SDK. Reports RPS, min/avg/p50/p90/p95/p99/max latencies.

Usage:
    # All params via CLI:
    python -m pypaimon.benchmark.lumina_search_bench \
        --index-path oss://bucket/path/to/index.lmi \
        --local-path /tmp/index.lmi \
        --dim 128 --metric inner_product --encoding pq --index-type diskann \
        --oss-access-key-id XXXX --oss-access-key-secret YYYY \
        --oss-endpoint oss-cn-hangzhou-internal.aliyuncs.com \
        --queries 20 --topk 10 --list-size 15 \
        --backends local,jindo-cache,oss

    # Local only (index already downloaded):
    python -m pypaimon.benchmark.lumina_search_bench \
        --local-path /tmp/index.lmi \
        --dim 128 --metric inner_product --encoding pq \
        --queries 20 --backends local

    # Via options files:
    python -m pypaimon.benchmark.lumina_search_bench \
        --index-path oss://bucket/path/to/index.lmi \
        --options-file searcher_opts.json \
        --catalog-options-file catalog_opts.json \
        --backends jindo-cache,oss
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


def _percentile(sorted_values, p):
    """Compute percentile from pre-sorted list."""
    if not sorted_values:
        return 0.0
    idx = (p / 100.0) * (len(sorted_values) - 1)
    lower = int(idx)
    upper = min(lower + 1, len(sorted_values) - 1)
    frac = idx - lower
    return sorted_values[lower] * (1 - frac) + sorted_values[upper] * frac


def _format_size(size_bytes):
    """Format byte size to human readable string."""
    mb = size_bytes / (1024 * 1024)
    return f"{size_bytes:,} bytes ({mb:,.2f} MB)"


def _generate_random_queries(dim, num_queries, seed=42):
    """Generate random query vectors for benchmarking."""
    rng = np.random.default_rng(seed)
    queries = rng.standard_normal((num_queries, dim)).astype(np.float32)
    norms = np.linalg.norm(queries, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    queries = queries / norms
    return queries


class BenchmarkResult:
    """Holds benchmark results for one storage backend."""

    def __init__(self, storage_name, num_queries, latencies_ms):
        self.storage = storage_name
        self.queries = num_queries
        self.latencies_ms = sorted(latencies_ms)
        self.total_time_s = sum(latencies_ms) / 1000.0
        self.rps = num_queries / self.total_time_s if self.total_time_s > 0 else 0

    @property
    def min_ms(self):
        return self.latencies_ms[0] if self.latencies_ms else 0

    @property
    def max_ms(self):
        return self.latencies_ms[-1] if self.latencies_ms else 0

    @property
    def avg_ms(self):
        return sum(self.latencies_ms) / len(self.latencies_ms) if self.latencies_ms else 0

    @property
    def p50_ms(self):
        return _percentile(self.latencies_ms, 50)

    @property
    def p90_ms(self):
        return _percentile(self.latencies_ms, 90)

    @property
    def p95_ms(self):
        return _percentile(self.latencies_ms, 95)

    @property
    def p99_ms(self):
        return _percentile(self.latencies_ms, 99)


def _open_searcher_local(searcher_options, local_path):
    """Open searcher from local file path."""
    from lumina_data import LuminaSearcher
    searcher = LuminaSearcher(searcher_options)
    searcher.open(local_path)
    return searcher, None


def _open_searcher_stream(searcher_options, file_io, index_path):
    """Open searcher from a file IO stream (OSS or Jindo-Cache).

    Falls back to downloading to a temp file if open_stream is not available.
    """
    import tempfile
    from lumina_data import LuminaSearcher

    file_size = file_io.get_file_size(index_path)
    stream = file_io.new_input_stream(index_path)
    searcher = LuminaSearcher(searcher_options)

    if hasattr(searcher, 'open_stream'):
        searcher.open_stream(stream, file_size)
        return searcher, stream
    else:
        tmp = tempfile.NamedTemporaryFile(suffix='.lmi', delete=False)
        try:
            while True:
                chunk = stream.read(8 * 1024 * 1024)
                if not chunk:
                    break
                tmp.write(chunk)
            tmp.close()
            stream.close()
            searcher.open(tmp.name)
            return searcher, tmp.name
        except Exception:
            tmp.close()
            os.unlink(tmp.name)
            raise


def _build_oss_file_io(index_path, catalog_options=None):
    """Build FileIO for direct OSS access."""
    from pypaimon.common.file_io import FileIO
    from pypaimon.common.options import Options
    opts = Options(catalog_options) if catalog_options else None
    return FileIO.get(index_path, opts)


def _build_jindo_cache_file_io(index_path, catalog_options=None):
    """Build FileIO with Jindo/local disk caching layer."""
    from pypaimon.common.file_io import FileIO
    from pypaimon.common.options import Options
    from pypaimon.filesystem.caching_file_io import (
        CachingFileIO, LocalDiskCacheManager,
    )

    opts = Options(catalog_options) if catalog_options else None
    delegate = FileIO.get(index_path, opts)

    cache_dir = (catalog_options or {}).get("cache.dir", "/tmp/lumina_cache")
    cache_size = int((catalog_options or {}).get(
        "cache.max-size-bytes", str(64 * 1024 * 1024 * 1024)))

    cache_manager = LocalDiskCacheManager(cache_dir, cache_size)
    return CachingFileIO(delegate=delegate, cache=cache_manager)


def _search_one(searcher, query_vector, topk, search_options):
    """Search with a single query vector, adapting to API version."""
    if hasattr(searcher, 'search_numpy'):
        query = np.ascontiguousarray(query_vector.reshape(1, -1), dtype=np.float32)
        searcher.search_numpy(query, topk, search_options)
    else:
        searcher.search_list(query_vector.tolist(), 1, topk, search_options)


def run_benchmark(searcher, queries, topk, search_options):
    """Run search queries and return latencies in milliseconds."""
    latencies = []
    dim = searcher.get_dimension()
    assert queries.shape[1] == dim, (
        f"Query dim {queries.shape[1]} != index dim {dim}")

    for i in range(queries.shape[0]):
        query = queries[i]
        t0 = time.perf_counter()
        _search_one(searcher, query, topk, search_options)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        latencies.append(elapsed_ms)

    return latencies


def warmup_searcher(searcher, dim, topk, search_options, warmup_count=3):
    """Warmup the searcher with a few dummy queries."""
    dummy = np.random.randn(dim).astype(np.float32)
    for _ in range(warmup_count):
        _search_one(searcher, dummy, topk, search_options)


def print_results(index_size, metric, encoding, topk, list_size, results):
    """Print benchmark results in tabular format."""
    print(f"\nIndex size: {_format_size(index_size)} | "
          f"Metric: {metric} | Encoding: {encoding} | "
          f"TopK: {topk} | list_size: {list_size}")
    print("-" * 120)

    header = (f"{'Storage':<15} {'Queries':>7} {'Total Time (s)':>14} "
              f"{'RPS':>8} {'Min (ms)':>10} {'Avg (ms)':>10} "
              f"{'P50 (ms)':>10} {'P90 (ms)':>10} {'P95 (ms)':>10} "
              f"{'P99 (ms)':>10} {'Max (ms)':>10}")
    print(header)
    print("-" * 120)

    for r in results:
        row = (f"{r.storage:<15} {r.queries:>7} {r.total_time_s:>14.2f} "
               f"{r.rps:>8.2f} {r.min_ms:>10.3f} {r.avg_ms:>10.3f} "
               f"{r.p50_ms:>10.3f} {r.p90_ms:>10.3f} {r.p95_ms:>10.3f} "
               f"{r.p99_ms:>10.3f} {r.max_ms:>10.3f}")
        print(row)

    print("-" * 120)


def main():
    parser = argparse.ArgumentParser(
        description="Lumina vector search benchmark across storage backends")
    parser.add_argument("--index-path", type=str, default=None,
                        help="Remote index path (oss://bucket/path/to/index.lmi)")
    parser.add_argument("--local-path", type=str, default=None,
                        help="Local index file path")
    parser.add_argument("--queries", type=int, default=20,
                        help="Number of search queries (default: 20)")
    parser.add_argument("--topk", type=int, default=10,
                        help="Top-K results per query (default: 10)")
    parser.add_argument("--list-size", type=int, default=15,
                        help="DiskANN search list size (default: 15)")
    parser.add_argument("--backends", type=str, default="local,jindo-cache,oss",
                        help="Comma-separated backends: local,jindo-cache,oss")
    parser.add_argument("--options-file", type=str, default=None,
                        help="JSON file with lumina searcher options")
    parser.add_argument("--catalog-options-file", type=str, default=None,
                        help="JSON file with catalog/IO options (OSS credentials etc)")
    # Direct searcher options (override options-file)
    parser.add_argument("--dim", type=int, default=None,
                        help="Vector dimension (index.dimension)")
    parser.add_argument("--metric", type=str, default=None,
                        help="Distance metric: l2, cosine, inner_product")
    parser.add_argument("--encoding", type=str, default=None,
                        help="Encoding type: pq, sq8, rabitq, rawf32")
    parser.add_argument("--index-type", type=str, default=None,
                        help="Index type: diskann, ivf, bruteforce")
    # Direct catalog/IO options (override catalog-options-file)
    parser.add_argument("--oss-access-key-id", type=str, default=None,
                        help="OSS access key ID")
    parser.add_argument("--oss-access-key-secret", type=str, default=None,
                        help="OSS access key secret")
    parser.add_argument("--oss-endpoint", type=str, default=None,
                        help="OSS endpoint")
    parser.add_argument("--cache-dir", type=str, default=None,
                        help="Local cache directory for jindo-cache backend")
    parser.add_argument("--warmup", type=int, default=3,
                        help="Number of warmup queries (default: 3)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for query generation")
    args = parser.parse_args()

    backends = [b.strip() for b in args.backends.split(",")]

    if not args.local_path and "local" in backends:
        parser.error("--local-path required for 'local' backend")
    if not args.index_path and any(b in backends for b in ("jindo-cache", "oss")):
        parser.error("--index-path required for 'jindo-cache' or 'oss' backends")

    # Load searcher options
    if args.options_file:
        with open(args.options_file) as f:
            searcher_options = json.load(f)
    else:
        searcher_options = {}

    # CLI overrides for searcher options
    if args.dim is not None:
        searcher_options["index.dimension"] = str(args.dim)
    if args.metric is not None:
        searcher_options["distance.metric"] = args.metric
    if args.encoding is not None:
        searcher_options["encoding.type"] = args.encoding
    if args.index_type is not None:
        searcher_options["index.type"] = args.index_type

    # Load catalog options (OSS credentials, cache config, etc.)
    catalog_options = {}
    if args.catalog_options_file:
        with open(args.catalog_options_file) as f:
            catalog_options = json.load(f)

    # CLI overrides for catalog options
    if args.oss_access_key_id is not None:
        catalog_options["fs.oss.accessKeyId"] = args.oss_access_key_id
    if args.oss_access_key_secret is not None:
        catalog_options["fs.oss.accessKeySecret"] = args.oss_access_key_secret
    if args.oss_endpoint is not None:
        catalog_options["fs.oss.endpoint"] = args.oss_endpoint
    if args.cache_dir is not None:
        catalog_options["cache.dir"] = args.cache_dir

    # Determine index size and metadata from the first available source
    index_size = 0
    if args.local_path and os.path.isfile(args.local_path):
        index_size = os.path.getsize(args.local_path)
    elif args.index_path:
        try:
            file_io = _build_oss_file_io(args.index_path, catalog_options)
            index_size = file_io.get_file_size(args.index_path)
        except Exception as e:
            print(f"Warning: could not determine remote index size: {e}")

    metric = searcher_options.get("distance.metric", "unknown")
    encoding = searcher_options.get("encoding.type", "unknown")
    list_size = args.list_size

    search_options = {
        "search.topk": str(args.topk),
        "diskann.search.list_size": str(list_size),
    }

    # Determine dimension from options or by opening index
    dim = int(searcher_options.get("index.dimension", 0))
    if dim == 0:
        if args.local_path and os.path.isfile(args.local_path):
            from lumina_data import LuminaSearcher
            tmp = LuminaSearcher(searcher_options)
            tmp.open(args.local_path)
            dim = tmp.get_dimension()
            tmp.close()
        else:
            parser.error("Cannot determine dimension. Set index.dimension in options file.")

    queries = _generate_random_queries(dim, args.queries, seed=args.seed)
    results = []

    for backend in backends:
        print(f"\nRunning benchmark: {backend} ...")
        try:
            if backend == "local":
                searcher, stream = _open_searcher_local(
                    searcher_options, args.local_path)
            elif backend == "oss":
                file_io = _build_oss_file_io(args.index_path, catalog_options)
                searcher, stream = _open_searcher_stream(
                    searcher_options, file_io, args.index_path)
            elif backend == "jindo-cache":
                file_io = _build_jindo_cache_file_io(
                    args.index_path, catalog_options)
                searcher, stream = _open_searcher_stream(
                    searcher_options, file_io, args.index_path)
            else:
                print(f"  Unknown backend: {backend}, skipping")
                continue

            warmup_searcher(searcher, dim, args.topk, search_options, args.warmup)
            latencies = run_benchmark(searcher, queries, args.topk, search_options)

            display_name = {
                "local": "Local",
                "jindo-cache": "Jindo-Cache",
                "oss": "OSS",
            }.get(backend, backend)
            results.append(BenchmarkResult(display_name, args.queries, latencies))

            searcher.close()
            if stream is not None:
                if isinstance(stream, str):
                    os.unlink(stream)
                else:
                    stream.close()

        except Exception as e:
            print(f"  {backend} failed: {e}")
            import traceback
            traceback.print_exc()

    if results:
        print_results(index_size, metric, encoding, args.topk, list_size, results)


if __name__ == "__main__":
    main()
