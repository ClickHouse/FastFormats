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