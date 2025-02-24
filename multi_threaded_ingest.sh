#!/bin/bash

# General information variables
CLIENT_MACHINE="m6i.8xlarge, 10000gib gp3"
SERVER_SYSTEM="ClickHouse Cloud 24.10 30 vCPU and 120 GiB per replica / 3 replicas"
DATE_TODAY=$(date +"%Y-%m-%d")
TOTAL_NUM_ROWS=10000000


# Read ClickHouse credentials from environment variables
CLICKHOUSE_USER="${CLICKHOUSE_USER:?Environment variable CLICKHOUSE_USER is not set}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:?Environment variable CLICKHOUSE_PASSWORD is not set}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:?Environment variable CLICKHOUSE_HOST is not set}"

# Ensure at least eleven parameters are provided
if [ $# -lt 8 ]; then
    echo -e "\nUsage: $0 N_THREADS DATABASE_NAME DIRECTORY INTERFACE FORMAT BATCH_SIZE SORTED COMPRESSOR\n"
    echo "Example: $0 4 my_database /data/hits http TabSeparated 100000 true none"
    exit 1
fi

# Assign parameters
N_THREADS="$1"
DATABASE_NAME="$2"
DIRECTORY="$3"
INTERFACE="$4"
FORMAT="$5"
BATCH_SIZE="$6"
SORTED="$7"
COMPRESSOR="$8"
LIMIT="${9:-0}"  # Default to 0 (no limit)

ERROR_LOG="multi_ingest_errors.log"
RESULTS_DIR="results/multi_threaded"

# Determine compressor label
COMPRESSOR_LABEL="_$( [ "$COMPRESSOR" == "none" ] && echo "no_compression" || echo "$COMPRESSOR" )"

# New JSON filename format: multi_threaded-N_THREADS-INTERFACE-FORMAT-BATCH_SIZE-SORTED-COMPRESSOR.json
RESULT_JSON="$RESULTS_DIR/multi_threaded-${N_THREADS}_${INTERFACE}_${FORMAT}_${BATCH_SIZE}_$( [ "$SORTED" == "true" ] && echo "sorted" || echo "unsorted" )${COMPRESSOR_LABEL}.json"

mkdir -p "$RESULTS_DIR"

# Ensure directory exists
if [ ! -d "$DIRECTORY" ]; then
    echo "Error: Directory '$DIRECTORY' not found!" | tee -a "$ERROR_LOG"
    exit 1
fi

echo -e "\n===================================="
echo "  Multi-threaded Ingest into ClickHouse"
echo "  Client Machine: $CLIENT_MACHINE"
echo "  Server System: $SERVER_SYSTEM"
echo "  Date: $DATE_TODAY"
echo "  Database: $DATABASE_NAME"
echo "  Interface: $INTERFACE"
echo "  Format: $FORMAT"
echo "  Directory: $DIRECTORY"
echo "  Batch Size: $BATCH_SIZE"
echo "  Sorted: $SORTED"
echo "  Compressor: $COMPRESSOR"
echo "  Parallel Threads: $N_THREADS"
echo "  Error Log: $ERROR_LOG"
echo "===================================="

# Create database and table
echo -e "\nCreating database and table..."
./create_db_and_table.sh "$DATABASE_NAME" "ddl-hits.sql" "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD" "$CLICKHOUSE_HOST"

# Launch N_THREADS instances of ingest_thread.sh in parallel with thread ID
for ((i=1; i<=N_THREADS; i++)); do
    ./ingest_thread.sh "$i" "$DATABASE_NAME" "$DIRECTORY" "$FORMAT" "$COMPRESSOR" "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD" "$CLICKHOUSE_HOST" "$LIMIT" &
done

# Wait for all background jobs to finish
wait

echo -e "\nAll ingestion processes have completed. Check '$ERROR_LOG' for any errors."

echo -e "\nFlushing system logs before running metrics query..."
curl --silent --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "SYSTEM FLUSH LOGS" "$CLICKHOUSE_HOST"

echo -e "\nSleeping for 30 seconds before running metrics query..."
sleep 30

echo -e "\nRunning metrics query for database: $DATABASE_NAME"
METRICS_QUERY=$(sed "s/{db_name}/$DATABASE_NAME/g; s/{table_name}/hits/g" metrics.sql)
SERVER_METRICS_RAW=$(curl --silent --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "$METRICS_QUERY" "$CLICKHOUSE_HOST/?database=system&query=")

# Process server metrics
SERVER_METRICS=$(echo "$SERVER_METRICS_RAW" | grep -v -E "^Row 1:|^──────" | awk '{
        key=$1;
        value=substr($0, index($0, $2));
        if (value ~ /[A-Za-z]/) {
            print "    \"" key "\": \"" value "\",";
        } else {
            print "    \"" key "\": " value ",";
        }
    }' | sed '$s/,$//')

echo -e "\nRunning CPU and memory usage query for database: $DATABASE_NAME"
CPU_MEMORY_QUERY=$(sed "s/{db_name}/$DATABASE_NAME/g; s/{table_name}/hits/g" cpu_memory_usage-whole_service.sql)

# Extract CPU & memory usage directly from the query response
CPU_MEMORY_USAGE_RAW=$(curl --silent --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "$CPU_MEMORY_QUERY" "$CLICKHOUSE_HOST/?database=system&query=")

CPU_MEMORY_USAGE=$(echo "$CPU_MEMORY_USAGE_RAW" | awk -F': ' '
    /^cpu_usage_50th:/ {print "    \"cpu_usage_50th\": " $2 ","}
    /^cpu_usage_95th:/ {print "    \"cpu_usage_95th\": " $2 ","}
    /^cpu_usage_99th:/ {print "    \"cpu_usage_99th\": " $2 ","}
    /^memory_usage_50th:/ {print "    \"memory_usage_50th\": " $2 ","}
    /^memory_usage_95th:/ {print "    \"memory_usage_95th\": " $2 ","}
    /^memory_usage_99th:/ {print "    \"memory_usage_99th\": " $2 ","}
    /^memory_usage_50th_readable:/ {print "    \"memory_usage_50th_readable\": \"" $2 "\","}
    /^memory_usage_95th_readable:/ {print "    \"memory_usage_95th_readable\": \"" $2 "\","}
    /^memory_usage_99th_readable:/ {print "    \"memory_usage_99th_readable\": \"" $2 "\""}' )

# Drop database and table at the end
echo -e "\nDropping database and table..."
./drop_db.sh "$DATABASE_NAME" "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD" "$CLICKHOUSE_HOST"

# Generate JSON result
echo -e "{\n  \"client_machine\": \"$CLIENT_MACHINE\",\n  \"server_system\": \"$SERVER_SYSTEM\",\n  \"date\": \"$DATE_TODAY\",\n  \"threads\": $N_THREADS,\n  \"interface\": \"$INTERFACE\",\n  \"format\": \"$FORMAT\",\n  \"total_num_rows\": $TOTAL_NUM_ROWS,\n  \"batch_size\": \"$BATCH_SIZE\",\n  \"sorted\": \"$SORTED\",\n  \"compressor\": \"$COMPRESSOR\",\n  \"server_metrics\": {\n$SERVER_METRICS\n  },\n  \"cpu_memory_usage_multi_threaded\": {\n$CPU_MEMORY_USAGE\n  }\n}" > "$RESULT_JSON"
echo "Results saved to $RESULT_JSON"