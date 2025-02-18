#!/bin/bash

# Ensure all required parameters are provided
if [ $# -ne 8 ]; then
    echo -e "\nUsage: $0 INTERFACE FORMAT DIRECTORY DATABASE_NAME COMPRESSOR CLICKHOUSE_USER CLICKHOUSE_PASSWORD CLICKHOUSE_HOST\n"
    echo "Example: $0 http Parquet /path/to/files my_database none user password https://clickhouse-host:8443"
    echo "Example: $0 native Parquet /path/to/files my_database gzip user password https://clickhouse-host:8443"
    exit 1
fi

# Assign required parameters
INTERFACE="$1"
FORMAT="$2"
DIRECTORY="$3"
DATABASE_NAME="$4"
COMPRESSOR="$5"
CLICKHOUSE_USER="$6"
CLICKHOUSE_PASSWORD="$7"
CLICKHOUSE_HOST="$8"

# Table name (always 'hits')
TABLE_NAME="hits"

# Error log file
ERROR_LOG="ingest_errors.log"

# Ensure directory exists
if [ ! -d "$DIRECTORY" ]; then
    echo "Error: Directory '$DIRECTORY' not found!" | tee -a "$ERROR_LOG"
    exit 1
fi

# Create a temporary directory for storing compressed files
TMP_DIR=$(mktemp -d)

echo -e "\n===================================="
echo "  Ingesting Files into ClickHouse"
echo "  Database: $DATABASE_NAME"
echo "  Table: $TABLE_NAME"
echo "  Interface: $INTERFACE"
echo "  Format: $FORMAT"
echo "  Directory: $DIRECTORY"
echo "  Compressor: $COMPRESSOR"
echo "  Temporary Directory: $TMP_DIR"
echo "  Error Log: $ERROR_LOG"
echo "===================================="

ingest_file_via_http_interface() {
    local file_path="$1"

    # Encode the query properly
    local query="INSERT INTO $TABLE_NAME FORMAT $FORMAT"
    local query_encoded
    query_encoded=$(echo "$query" | sed 's/ /%20/g')

    # Determine compression command
    local compress_cmd=""
    local header=""
    local temp_file="$file_path"  # Default to original file if no compression

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
            compress_cmd=""
            header=""
            ;;
        *)
            echo "Invalid compression algorithm: $COMPRESSOR. Valid options: none, gzip, lz4, zstd."
            exit 1
            ;;
    esac

    echo -e "\nIngesting: $file_path into $DATABASE_NAME.$TABLE_NAME as $FORMAT via HTTP Interface"

    # If compression is enabled, create a compressed file inside the temporary directory
    if [ -n "$compress_cmd" ]; then
        temp_file="$TMP_DIR/$(basename "$file_path").compressed"
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

    # Remove temp compressed file if it was created
    if [ -n "$compress_cmd" ]; then
        rm -f "$temp_file"
    fi

    # Check for "out of memory" error and retry with -T if NOT compressed
    if echo "$response" | grep -q "out of memory"; then
        echo "Memory issue detected."

        if [ "$COMPRESSOR" != "none" ]; then
            echo "Compression enabled. Cannot retry with -T. Logging error..."
            echo "$(date +"%Y-%m-%d %H:%M:%S") - Error ingesting file (compression mode: $COMPRESSOR): $file_path" >> "$ERROR_LOG"
            echo "$response" >> "$ERROR_LOG"
            return
        fi

        echo "Retrying with streaming mode (-T)..."

        response=$(curl --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
            -T "$file_path" -X POST \
            --url "$CLICKHOUSE_HOST/?database=$DATABASE_NAME&async_insert=0&query=$query_encoded" 2>&1)

        # If streaming also fails, log the error
        if echo "$response" | grep -q "Exception"; then
            echo "$(date +"%Y-%m-%d %H:%M:%S") - Error ingesting file: $file_path" >> "$ERROR_LOG"
            echo "$response" >> "$ERROR_LOG"
            echo "Error logged to $ERROR_LOG"
            return
        fi
    fi

    echo "Successfully ingested: $file_path"
}

ingest_file_via_native_interface() {
    local file_path="$1"

    # Determine the network compression method based on the compressor
    local network_compression=""
    case "$COMPRESSOR" in
        lz4|zstd)
            network_compression="--network_compression_method=$COMPRESSOR"
            ;;
        *)
            echo "Invalid compression algorithm: $COMPRESSOR. Valid options: lz4, zstd."
            exit 1
            ;;
    esac

    echo -e "\nIngesting: $file_path into $DATABASE_NAME.$TABLE_NAME as $FORMAT via native Interface"

    # Extract hostname only (removes 'https://' and port like ':8443')
    CLICKHOUSE_HOSTNAME=$(echo "$CLICKHOUSE_HOST" | sed -E 's|https?://||;s|:[0-9]+$||')

    # Execute ingestion using clickhouse-client
    clickhouse-client --host="$CLICKHOUSE_HOSTNAME" \
        --secure \
        --user="$CLICKHOUSE_USER" \
        --password="$CLICKHOUSE_PASSWORD" \
        $network_compression \
        --async_insert=0 \
        --query="INSERT INTO ${DATABASE_NAME}.${TABLE_NAME} FORMAT ${FORMAT}" < "$file_path" 2>&1 | tee -a "$ERROR_LOG"

    if [ $? -eq 0 ]; then
        echo "Successfully ingested: $file_path"
    else
        echo "Failed to ingest: $file_path. Error logged to $ERROR_LOG"
    fi
}

# Get list of files, sorted
FILES=$(ls -1 "$DIRECTORY" | sort)

# Iterate over the list of files
for file in $FILES; do
    FILE_PATH="$DIRECTORY/$file"

    # Ensure it's a regular file
    if [ -f "$FILE_PATH" ]; then
        if [[ "$INTERFACE" == "http" ]]; then
            ingest_file_via_http_interface "$FILE_PATH"
        elif [[ "$INTERFACE" == "native" ]]; then
            ingest_file_via_native_interface "$FILE_PATH"
        else
            echo "Invalid interface: $INTERFACE. Valid options: 'http' or 'native'."
            exit 1
        fi
    fi
done

# Cleanup: Remove temporary directory
rm -rf "$TMP_DIR"

echo -e "\nAll files have been processed. Check '$ERROR_LOG' for any errors."