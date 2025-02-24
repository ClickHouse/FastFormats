#!/bin/bash

# General information variables
SERVER_SYSTEM="ClickHouse Cloud 24.10 30 vCPU and 120 GiB per replica / 3 replicas"
DATE_TODAY=$(date +"%Y-%m-%d")

# Read ClickHouse credentials from environment variables
CLICKHOUSE_USER="${CLICKHOUSE_USER:?Environment variable CLICKHOUSE_USER is not set}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:?Environment variable CLICKHOUSE_PASSWORD is not set}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:?Environment variable CLICKHOUSE_HOST is not set}"

RESULTS_DIR="results/multi_threaded"
mkdir -p "$RESULTS_DIR"

# New JSON filename format: multi_threaded-N_THREADS-INTERFACE-FORMAT-BATCH_SIZE-SORTED-COMPRESSOR.json
BASELINE_JSON="$RESULTS_DIR/_baseline.json"

echo -e "\nSleeping for 5 minutes before running baseline metrics query..."
sleep 300

echo -e "\nRunning baseline metrics query..."
BASELINE_QUERY=$(<cpu_memory_usage-whole_service-baseline.sql)  # Read SQL file content
BASELINE_RAW=$(curl --silent --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "$BASELINE_QUERY" "$CLICKHOUSE_HOST/?database=system&query=")

BASELINE_CPU_MEMORY_USAGE=$(echo "$BASELINE_RAW" | awk -F': ' '
    /^cpu_usage_50th:/ {print "    \"cpu_usage_50th\": " $2 ","}
    /^cpu_usage_95th:/ {print "    \"cpu_usage_95th\": " $2 ","}
    /^cpu_usage_99th:/ {print "    \"cpu_usage_99th\": " $2 ","}
    /^memory_usage_50th:/ {print "    \"memory_usage_50th\": " $2 ","}
    /^memory_usage_95th:/ {print "    \"memory_usage_95th\": " $2 ","}
    /^memory_usage_99th:/ {print "    \"memory_usage_99th\": " $2 ","}
    /^memory_usage_50th_readable:/ {print "    \"memory_usage_50th_readable\": \"" $2 "\","}
    /^memory_usage_95th_readable:/ {print "    \"memory_usage_95th_readable\": \"" $2 "\","}
    /^memory_usage_99th_readable:/ {print "    \"memory_usage_99th_readable\": \"" $2 "\""}' )

# Save the baseline JSON
echo -e "{\n  \"server_system\": \"$SERVER_SYSTEM\",\n  \"date\": \"$DATE_TODAY\",\n  \"cpu_memory_usage_baseline\": {\n$BASELINE_CPU_MEMORY_USAGE\n  }\n}" > "$BASELINE_JSON"
echo "Baseline results saved to $BASELINE_JSON"

# Starting ingestion process
echo -e "\nStarting ingestion process...\n"

ingest_and_wait() {
    local format="$1"
    local directory="$2"
    local compressor="$3"
    local format_lower=$(echo "$format" | tr '[:upper:]' '[:lower:]')

    TIMESTAMP=$(date +"%Y_%m_%d_%H_%M")

    echo -e "\nStarting ingestion for format: $format..."

    ./multi_threaded_ingest.sh 1  "multi_threaded_ingest_${format_lower}_001_${TIMESTAMP}"  "$directory" http "$format" 10000 false "$compressor" 200
    ./multi_threaded_ingest.sh 5  "multi_threaded_ingest_${format_lower}_005_${TIMESTAMP}"  "$directory" http "$format" 10000 false "$compressor" 200
    ./multi_threaded_ingest.sh 10 "multi_threaded_ingest_${format_lower}_010_${TIMESTAMP}"    "$directory" http "$format" 10000 false "$compressor" 200

    echo -e "Completed ingestion for format: $format\n"
}

# Ingest each format with appropriate directories and compression settings
ingest_and_wait "Native"        "/home/ubuntu/data/hits/split/native_10000"             "lz4"
ingest_and_wait "ArrowStream"   "/home/ubuntu/data/hits/split/arrowstream_10000_lz4"    "none"
ingest_and_wait "RowBinary"     "/home/ubuntu/data/hits/split/rowbinary_10000"          "lz4"
ingest_and_wait "TSV"           "/home/ubuntu/data/hits/split/tsv_10000"                "lz4"
ingest_and_wait "Parquet"       "/home/ubuntu/data/hits/split/parquet_10000_lz4"        "none"
ingest_and_wait "CSV"           "/home/ubuntu/data/hits/split/csv_10000"                "lz4"
ingest_and_wait "BSONEachRow"   "/home/ubuntu/data/hits/split/bsoneachrow_10000"        "lz4"
ingest_and_wait "JSONEachRow"   "/home/ubuntu/data/hits/split/jsoneachrow_10000"        "lz4"

echo -e "\nAll ingestion tasks completed successfully."