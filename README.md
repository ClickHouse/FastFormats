# FastFormats: A Benchmark for ClickHouse Input Formats

## Overview

FastFormats is a benchmark designed to compare the ingestion performance of different data formats in ClickHouse. It measures **server-side insert speed**, **CPU and memory efficiency**, and the impact of **compression, pre-sorting, and batching** on ingestion performance.

ClickHouse supports [70+ input formats](https://clickhouse.com/docs/en/interfaces/formats), but choosing the right one can significantly impact performance. FastFormats systematically evaluates these formats to identify the most efficient options for high-throughput ingestion.

## Dataset

FastFormats uses the **real-world web analytics dataset** from [ClickBench](https://github.com/ClickHouse/ClickBench). 

## Benchmark Scope

FastFormats evaluates:
- **Insert speed**: How quickly the server processes data in different formats.
- **Resource efficiency**: CPU and memory usage per insert.
- **Compression impact**: How different compression methods affect insert performance.
- **Pre-sorting benefits**: Whether sorting data before sending improves ingestion speed.
- **Batching efficiency**: The effect of varying batch sizes on performance.

The benchmark includes **60+ formats**, covering:
- **Binary columnar formats** (e.g., Native, Parquet, Arrow)
- **Binary row-oriented formats** (e.g., RowBinary, Avro, Protobuf)
- **Text-based formats** (e.g., CSV, TSV, JSONEachRow)

## How It Works

FastFormats runs a **nested loop** over multiple test dimensions:
- **Batch sizes**: 10k, 100k, and 1M rows per insert.
- **Formats**: CSV, TSV, JSONEachRow, Native, Parquet, Arrow, and more.
- **Pre-sorting**: Yes/No.
- **Compression methods**: Uncompressed, LZ4, ZSTD.

The benchmark follows these steps:
1. **Convert data** into different formats using `clickhouse-local`.
2. **Send data** to ClickHouse using the **HTTP interface**.
3. **Track ingestion performance**, including **server-side processing time, CPU, and memory usage**.
4. **Store results** in JSON format for easy analysis.

## Blog Post & Results

We wrote a detailed **[blog post](https://clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient)** explaining FastFormats, how it works, and showcasing benchmark results for different formats.

📊 **For full test results, check out the FastFormats online dashboard**: [🔗 Link] (if applicable)


## Getting Started

FastFormats requires:
- **A client machine** to run the benchmark.
- **A ClickHouse test system (server)** where ClickHouse is running and accessible via **HTTP and the native interface**.

- `curl` and `clickhouse-client` for data ingestion
- `clickhouse-local` for format conversion

### 1. Set Up the Client Machine

#### Clone the Repository
```bash
git clone https://github.com/ClickHouse/FastFormats.git
cd FastFormats
```

#### Install ClickHouse client tools

FastFormats requires `clickhouse-local` and `clickhouse-client` on the client machine.

```bash
./install-clickhouse_client.sh
```

### 2. Configure ClickHouse Credentials

Set up ClickHouse connection credentials as environment variables:

```bash
export CLICKHOUSE_USER="your_user"
export CLICKHOUSE_PASSWORD="your_password"
export CLICKHOUSE_HOST="your_clickhouse_host"
```

### 3. Run the Benchmark

After setting up credentials, start the benchmark:

```bash
./main.sh
```

### 4. Analyze results

Once the benchmark completes, results are stored in the `results` folder. 


## Avoiding Artificial Insert Slowdowns

ClickHouse includes built-in safeguards to prevent excessive resource usage when creating and [merging](https://clickhouse.com/docs/en/merges) parts. If the number of active parts in a single [partition](partition) exceeds the [parts_to_delay_insert](https://clickhouse.com/docs/en/operations/settings/merge-tree-settings#parts-to-delay-insert) threshold, ClickHouse artificially slows down inserts to allow background merges to keep up. If the number of parts surpasses [parts_to_throw_insert](https://clickhouse.com/docs/en/operations/settings/merge-tree-settings#parts-to-throw-insert), INSERTs fail with a `Too many parts` error.

Our benchmark evaluates each combination of **batch size, format, pre-sorting, and compression** using an **isolated sequence of inserts** into ClickHouse. We perform up to **1000 sequential inserts** for the smallest batch size (1k rows per insert), each creating a single part, scaling by **N** when measuring the impact of **N parallel clients**. No extra workload is generated on ClickHouse, aside from automatic background merges of inserted parts.

To prevent artificial slowdowns and failed inserts:
- We [increase](https://github.com/ClickHouse/FastFormats/blob/c6457ff17be6016d7b59543b62ad332b6f382858/ddl-hits.sql#L113) both the `parts_to_delay_insert` and `parts_to_throw_insert` thresholds.
- The target table is [dropped](https://github.com/ClickHouse/FastFormats/blob/c6457ff17be6016d7b59543b62ad332b6f382858/main.sh#L178) after each run to minimize the number of parts on the server.
- Automatic insert deduplication is [disabled](https://github.com/ClickHouse/FastFormats/blob/c6457ff17be6016d7b59543b62ad332b6f382858/ddl-hits.sql#L116) to allow parallel inserts with identical data when testing input format performance with concurrent clients.
- We [track](https://github.com/ClickHouse/FastFormats/blob/c6457ff17be6016d7b59543b62ad332b6f382858/metrics.sql#L33) occurrences of artificial slowdowns in the test results but did not observe any in our benchmarks.

> ⚠️ **Note:** These threshold modifications and settings are applied **only for benchmarking purposes** on a dedicated ClickHouse system. We do **not recommend adjusting them in production environments.**

## ⚠️ Disclaimer: Designed for ClickHouse Cloud  

FastFormats is currently optimized to run against a **ClickHouse Cloud** service.  

- Benchmark SQL queries for fetching **performance metrics** rely on `clusterAllReplicas('default')`, assuming a **default cluster setup**.
- If running FastFormats **on a self-hosted ClickHouse instance**, you may need to **adjust queries** in [`metrics.sql`](https://github.com/ClickHouse/FastFormats/blob/main/metrics.sql) and other scripts that use cluster-level functions.

> **Future versions** will include support for self-hosted ClickHouse setups with configurable cluster settings.


