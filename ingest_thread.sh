#!/bin/bash

# Ensure required parameters are provided
if [ $# -lt 8 ]; then
    echo -e "\nUsage: $0 THREAD_ID DATABASE_NAME DIRECTORY FORMAT COMPRESSOR CLICKHOUSE_USER CLICKHOUSE_PASSWORD CLICKHOUSE_HOST\n"
    echo "Example (no compression): $0 1 my_database /path/to/files TabSeparated none user pass https://clickhouse-host:8443"
    exit 1
fi

# Assign required parameters
THREAD_ID="$1"
DATABASE_NAME="$2"
DIRECTORY="$3"
FORMAT="$4"
COMPRESSOR="$5"
CLICKHOUSE_USER="$6"
CLICKHOUSE_PASSWORD="$7"
CLICKHOUSE_HOST="$8"

# Table name (assumed to be 'hits')
TABLE_NAME="hits"

# Error log file
ERROR_LOG="ingest_errors_thread_${THREAD_ID}.log"

# Create a unique temp directory for this thread
TEMP_DIR=$(mktemp -d)

# Ensure directory exists
if [ ! -d "$DIRECTORY" ]; then
    echo "[Thread $THREAD_ID] Error: Directory '$DIRECTORY' not found!" | tee -a "$ERROR_LOG"
    exit 1
fi

# Function to ingest a file via HTTP interface
ingest_file_via_http_interface() {
    local file_path="$1"

    # Encode the query properly
    local query="INSERT INTO $TABLE_NAME FORMAT $FORMAT"
    local query_encoded
    query_encoded=$(echo "$query" | sed 's/ /%20/g')

    # Determine compression command
    local compress_cmd=""
    local header=""
    local temp_file="$TEMP_DIR/$(basename "$file_path").compressed"

    case "$COMPRESSOR" in
        gzip)
            compress_cmd="gzip -c"
            header="Content-Encoding: gzip"
            ;;
        lz4)
            compress_cmd="lz4 -c"
            header="Content-Encoding: lz4"
            ;;
        zstd)
            compress_cmd="zstd -c"
            header="Content-Encoding: zstd"
            ;;
        none)
            temp_file="$file_path"  # No compression, use original file
            header=""
            ;;
        *)
            echo "[Thread $THREAD_ID] Invalid compression algorithm: $COMPRESSOR. Valid options: none, gzip, lz4, zstd." | tee -a "$ERROR_LOG"
            exit 1
            ;;
    esac

    echo -e "[Thread $THREAD_ID] Ingesting: $file_path into $DATABASE_NAME.$TABLE_NAME as $FORMAT via HTTP Interface"

    # Compress the file if needed (without streaming)
    if [ -n "$compress_cmd" ]; then
        $compress_cmd "$file_path" > "$temp_file"
    fi

    # Build curl command dynamically
    local curl_cmd=(
        curl --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD"
        --data-binary @"$temp_file"
        --url "$CLICKHOUSE_HOST/?database=$DATABASE_NAME&async_insert=0&query=$query_encoded"
    )

    if [ -n "$compress_cmd" ]; then
        curl_cmd+=(--header "$header")
    fi

    # Execute ingestion
    local response
    response=$("${curl_cmd[@]}" 2>&1)


    # Check for "out of memory" error and retry
    if echo "$response" | grep -q "out of memory"; then
        echo "[Thread $THREAD_ID] Memory issue detected. Retrying with streaming mode (-T)..." | tee -a "$ERROR_LOG"

        response=$(curl --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
            -T "$file_path" -X POST \
            --url "$CLICKHOUSE_HOST/?database=$DATABASE_NAME&async_insert=0&query=$query_encoded" 2>&1)

        # If streaming also fails, log the error
        if echo "$response" | grep -q "Exception"; then
            echo "[Thread $THREAD_ID] $(date +"%Y-%m-%d %H:%M:%S") - Error ingesting file: $file_path" >> "$ERROR_LOG"
            echo "[Thread $THREAD_ID] $response" >> "$ERROR_LOG"
            echo "[Thread $THREAD_ID] Error logged to $ERROR_LOG"
            return
        fi
    fi

    echo "[Thread $THREAD_ID] Successfully ingested: $file_path"
}

# Get list of files, sorted
FILES=$(ls -1 "$DIRECTORY" | sort)

# Iterate over the list of files
for file in $FILES; do
    FILE_PATH="$DIRECTORY/$file"

    # Ensure it's a regular file
    if [ -f "$FILE_PATH" ]; then
        ingest_file_via_http_interface "$FILE_PATH"
    fi
done

echo -e "[Thread $THREAD_ID] All files have been processed. Check '$ERROR_LOG' for any errors."

# Cleanup temp directory
rm -rf "$TEMP_DIR"