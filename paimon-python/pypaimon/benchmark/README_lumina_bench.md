# Lumina Vector Search Benchmark

Benchmark Lumina vector search latency across different storage backends (Local / Jindo-Cache / OSS).

## Dependencies

```bash
# Core (required)
pip install numpy

# lumina-data SDK (required) — internal package, install from wheel
pip install lumina_data-0.1.0-py3-none-manylinux2014_x86_64.whl

# For OSS / Jindo-Cache backends (optional)
pip install pyarrow
pip install pypaimon         # paimon python SDK, provides FileIO abstraction
pip install pyjindosdk       # Jindo filesystem handler (jindo-cache backend only)
```

### Runtime: LD_LIBRARY_PATH

`lumina_data` depends on `liblumina.so`. If you see `liblumina.so: cannot open shared object file`, set:

```bash
export LD_LIBRARY_PATH=$(python3 -c "import lumina_data, os; print(os.path.join(os.path.dirname(lumina_data.__file__), 'lib'))"):$LD_LIBRARY_PATH
```

## Usage

### Local only

```bash
python3 lumina_search_bench.py \
    --local-path /data/index.lmi \
    --dim 128 --metric inner_product --encoding pq --index-type diskann \
    --queries 20 --topk 10 --list-size 15 \
    --backends local
```

### All backends (Local + Jindo-Cache + OSS)

```bash
python3 lumina_search_bench.py \
    --index-path oss://bucket/path/to/index.lmi \
    --local-path /data/index.lmi \
    --dim 128 --metric inner_product --encoding pq --index-type diskann \
    --oss-access-key-id XXXX \
    --oss-access-key-secret YYYY \
    --oss-endpoint oss-cn-hangzhou-internal.aliyuncs.com \
    --cache-dir /tmp/lumina_cache \
    --queries 20 --topk 10 --list-size 15 \
    --backends local,jindo-cache,oss
```

### Via JSON config files

```bash
python3 lumina_search_bench.py \
    --index-path oss://bucket/path/to/index.lmi \
    --local-path /data/index.lmi \
    --options-file searcher_opts.json \
    --catalog-options-file catalog_opts.json \
    --queries 20 --topk 10 --list-size 15 \
    --backends local,jindo-cache,oss
```

`searcher_opts.json`:
```json
{
    "index.dimension": "128",
    "index.type": "diskann",
    "distance.metric": "inner_product",
    "encoding.type": "pq"
}
```

`catalog_opts.json`:
```json
{
    "fs.oss.accessKeyId": "XXXX",
    "fs.oss.accessKeySecret": "YYYY",
    "fs.oss.endpoint": "oss-cn-hangzhou-internal.aliyuncs.com",
    "cache.dir": "/tmp/lumina_cache",
    "cache.max-size-bytes": "68719476736"
}
```

## CLI Arguments

| Argument | Description |
|----------|-------------|
| `--index-path` | Remote index path (`oss://...`), required for oss/jindo-cache |
| `--local-path` | Local index file path, required for local backend |
| `--dim` | Vector dimension |
| `--metric` | Distance metric: `l2`, `cosine`, `inner_product` |
| `--encoding` | Encoding: `pq`, `sq8`, `rabitq`, `rawf32` |
| `--index-type` | Index type: `diskann`, `ivf`, `bruteforce` |
| `--topk` | Top-K results per query (default: 10) |
| `--list-size` | DiskANN search list size (default: 15) |
| `--queries` | Number of search queries (default: 20) |
| `--backends` | Comma-separated: `local`, `jindo-cache`, `oss` |
| `--oss-access-key-id` | OSS access key ID |
| `--oss-access-key-secret` | OSS access key secret |
| `--oss-endpoint` | OSS endpoint |
| `--cache-dir` | Local cache directory for jindo-cache |
| `--options-file` | JSON file with searcher options |
| `--catalog-options-file` | JSON file with catalog/IO options |
| `--warmup` | Warmup queries before benchmark (default: 3) |
| `--seed` | Random seed for query generation (default: 42) |

## Output Example

```
Index size: 55,054,393,838 bytes (52,503.96 MB) | Metric: inner_product | Encoding: pq | TopK: 10 | list_size: 15
------------------------------------------------------------------------------------------------------------------------
Storage         Queries Total Time (s)      RPS   Min (ms)   Avg (ms)   P50 (ms)   P90 (ms)   P95 (ms)   P99 (ms)   Max (ms)
------------------------------------------------------------------------------------------------------------------------
Local                20           6.28     3.19    308.520    313.952    311.440    317.737    362.884    362.884    362.884
Jindo-Cache          20          35.05     0.57   1589.823   1752.483   1786.880   1909.417   1927.954   1927.954   1927.954
OSS                  20          53.95     0.37   2445.015   2697.298   2676.452   2943.058   3184.557   3184.557   3184.557
------------------------------------------------------------------------------------------------------------------------
```

## Backend Details

| Backend | Description | Requires |
|---------|-------------|----------|
| **Local** | Direct file read via `searcher.open(path)` | Index file on local disk |
| **Jindo-Cache** | Read through Jindo caching layer (DLF mode) | `pypaimon`, `pyarrow`, `pyjindosdk` |
| **OSS** | Direct read from Alibaba Cloud OSS | `pypaimon`, `pyarrow`, OSS credentials |
