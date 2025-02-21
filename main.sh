#!/bin/bash

TEST_MODE="false"
# TEST_MODE="true"





# Read ClickHouse credentials from environment variables
CLICKHOUSE_USER="${CLICKHOUSE_USER:?Environment variable CLICKHOUSE_USER is not set}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:?Environment variable CLICKHOUSE_PASSWORD is not set}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:?Environment variable CLICKHOUSE_HOST is not set}"


NATIVE_INTERFACE_COMPRESSION_LIST=("zstd" "lz4")

# List of binary formats with built-in compression
declare -A FORMATS_WITH_BUILT_IN_COMPRESSION_MAP=(
    ["Parquet"]="zstd lz4 snappy brotli gzip none"
    ["Arrow"]="zstd lz4 none"
    ["ArrowStream"]="zstd lz4 none"
    ["Avro"]="zstd snappy deflate none"
)

declare -A FORMATS_WITH_BUILT_IN_COMPRESSION_SETTINGS_MAP=(
    ["Parquet_zstd"]="output_format_parquet_compression_method='zstd'"
    ["Parquet_lz4"]="output_format_parquet_compression_method='lz4'"
    ["Parquet_snappy"]="output_format_parquet_compression_method='snappy'"
    ["Parquet_brotli"]="output_format_parquet_compression_method='brotli'"
    ["Parquet_gzip"]="output_format_parquet_compression_method='gzip'"
    ["Parquet_none"]="output_format_parquet_compression_method='none'"
#
    ["Arrow_zstd"]="output_format_arrow_compression_method='zstd'"
    ["Arrow_lz4"]="output_format_arrow_compression_method='lz4_frame'"
    ["Arrow_none"]="output_format_arrow_compression_method='none'"
#
    ["ArrowStream_zstd"]="output_format_arrow_compression_method='zstd'"
    ["ArrowStream_lz4"]="output_format_arrow_compression_method='lz4_frame'"
    ["ArrowStream_none"]="output_format_arrow_compression_method='none'"
#
    ["Avro_zstd"]="output_format_avro_codec='zstd'"
    ["Avro_snappy"]="output_format_avro_codec='snappy'"
    ["Avro_deflate"]="output_format_avro_codec='deflate'"
    ["Avro_none"]="output_format_avro_codec='null'"
)





# Default values
OUTPUT_PREFIX="_cloud"
RESULT_SNIPPETS_DIR="result_snippets"

# Ensure results directories exist
mkdir -p results
mkdir -p "$RESULT_SNIPPETS_DIR"

# General information variables
CLIENT_MACHINE="m6i.8xlarge, 10000gib gp3"
SERVER_SYSTEM="ClickHouse Cloud 24.10 30 vCPU and 120 GiB per replica / 3 replicas"
DATE_TODAY=$(date +"%Y-%m-%d")
TOTAL_NUM_ROWS=10000000

get_server_metrics_file() {
    local interface="$1"
    local format="$2"
    local sorted="$3"
    local compressor="$4"
    local batch_size="$5"
    local format_lower=$(echo "$format" | tr '[:upper:]' '[:lower:]')

    local compressor_label=$( [ "$compressor" == "none" ] && echo "no_compression" || echo "$compressor" )

    echo "$RESULT_SNIPPETS_DIR/${OUTPUT_PREFIX}_${interface}_${format_lower}_${batch_size}_$( [ "$sorted" == "true" ] && echo "sorted" || echo "unsorted" )_${compressor_label}.server_metrics"
}

get_server_metrics_query_file() {
    local interface="$1"
    local format="$2"
    local sorted="$3"
    local compressor="$4"
    local batch_size="$5"
    local format_lower=$(echo "$format" | tr '[:upper:]' '[:lower:]')

    local compressor_label=$( [ "$compressor" == "none" ] && echo "no_compression" || echo "$compressor" )

    echo "$RESULT_SNIPPETS_DIR/${OUTPUT_PREFIX}_${interface}_${format_lower}_${batch_size}_$( [ "$sorted" == "true" ] && echo "sorted" || echo "unsorted" )_${compressor_label}.server_metrics.sql"
}

get_cpu_memory_file() {
    local interface="$1"
    local format="$2"
    local sorted="$3"
    local compressor="$4"
    local batch_size="$5"
    local format_lower=$(echo "$format" | tr '[:upper:]' '[:lower:]')

    local compressor_label=$( [ "$compressor" == "none" ] && echo "no_compression" || echo "$compressor" )

    echo "$RESULT_SNIPPETS_DIR/${OUTPUT_PREFIX}_${interface}_${format_lower}_${batch_size}_$( [ "$sorted" == "true" ] && echo "sorted" || echo "unsorted" )_${compressor_label}.cpu_memory"
}

convert() {
    local batch_size="$1"
    local format="$2"
    local sorted="$3"
    local compressor="$4"
    local input_dir="/home/ubuntu/data/hits/split/tabseparatedwithnames_${batch_size}"
    local format_lower=$(echo "$format" | tr '[:upper:]' '[:lower:]')

    if [ "$sorted" == "true" ]; then
        output_dir="/home/ubuntu/data/hits/split/${format_lower}_${batch_size}_sorted"
    else
        output_dir="/home/ubuntu/data/hits/split/${format_lower}_${batch_size}"
    fi

    if [[ -n "${FORMATS_WITH_BUILT_IN_COMPRESSION_SETTINGS_MAP[${format}_${compressor}]}" ]]; then
        output_dir="${output_dir}_${compressor}"
        extra_settings="${FORMATS_WITH_BUILT_IN_COMPRESSION_SETTINGS_MAP[${format}_${compressor}]}"
    else
        extra_settings=""
    fi

    echo -e "\nStarting conversion..."
    echo "  Format: $format"
    echo "  Sorting: $sorted"
    echo "  Compressor: $compressor"
    echo "  Input Directory: $input_dir"
    echo "  Output Directory: $output_dir"
    echo "  Extra Settings: $extra_settings"

    ./convert_tsv-chunks.sh "$format" "$sorted" "$input_dir" "$output_dir" "$extra_settings" "$TEST_MODE"

    echo "Conversion complete."
}

ingest() {
    local interface="$1"
    local batch_size="$2"
    local format="$3"
    local sorted="$4"
    local compressor="$5"
    local format_lower=$(echo "$format" | tr '[:upper:]' '[:lower:]')

    if [ "$sorted" == "true" ]; then
        output_dir="/home/ubuntu/data/hits/split/${format_lower}_${batch_size}_sorted"
    else
        output_dir="/home/ubuntu/data/hits/split/${format_lower}_${batch_size}"
    fi

    if [[ -n "${FORMATS_WITH_BUILT_IN_COMPRESSION_SETTINGS_MAP[${format}_${compressor}]}" ]]; then
        output_dir="${output_dir}_${compressor}"
        actual_compressor="none"
        compressor_label=""
    else
        actual_compressor="$compressor"
        compressor_label="_$( [ "$compressor" == "none" ] && echo "no_compression" || echo "$compressor" )"
    fi


    local server_metrics_file=$(get_server_metrics_file "$interface" "$format" "$sorted" "$compressor" "$batch_size")
    local server_metrics_query_file=$(get_server_metrics_query_file "$interface" "$format" "$sorted" "$compressor" "$batch_size")
    local cpu_memory_file=$(get_cpu_memory_file "$interface" "$format" "$sorted" "$compressor" "$batch_size")

    TABLE_NAME="hits"
    TIMESTAMP=$(date +"%Y_%m_%d_%H_%M")
    DATABASE_NAME="${TABLE_NAME}_${interface}_$(basename "$output_dir")${compressor_label}_${TIMESTAMP}"
    ./create_db_and_table.sh "$DATABASE_NAME" "ddl-hits.sql" "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD" "$CLICKHOUSE_HOST"

    echo -e "\nStarting ingestion..."
    echo "  Interface: $interface"
    echo "  Format: $format"
    echo "  Sorting: $sorted"
    echo "  Actual_Compressor: $actual_compressor"
    echo "  Directory: $output_dir"
    echo "  Database Name: $DATABASE_NAME"

    ./ingest.sh "$interface" "$format" "$output_dir" "$DATABASE_NAME" "$actual_compressor"  "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD" "$CLICKHOUSE_HOST"

    echo "Ingestion complete."

    echo -e "\nFlushing system logs before running metrics query..."
    curl --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "SYSTEM FLUSH LOGS" "$CLICKHOUSE_HOST"

    echo -e "\nSleeping for 30 seconds before running metrics query..."
    sleep 30

    echo -e "\nRunning metrics query for database: $DATABASE_NAME"
    METRICS_QUERY=$(sed "s/{db_name}/$DATABASE_NAME/g; s/{table_name}/$TABLE_NAME/g" metrics.sql)
    echo "$METRICS_QUERY" > "$server_metrics_query_file"
    curl --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "$METRICS_QUERY" "$CLICKHOUSE_HOST/?database=system&query=" > "$server_metrics_file"
    echo "Metrics collected. Results saved to $server_metrics_file"

    echo -e "\nRunning CPU and memory usage query for database: $DATABASE_NAME"
    CPU_MEMORY_QUERY=$(sed "s/{db_name}/$DATABASE_NAME/g; s/{table_name}/$TABLE_NAME/g" cpu_memory_usage_over_time-just_inserts.sql)
    curl --user "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" --data-binary "$CPU_MEMORY_QUERY" "$CLICKHOUSE_HOST/?database=system&query=" > "$cpu_memory_file"
    echo "CPU and memory usage collected. Results saved to $cpu_memory_file"

    ./drop_db.sh "$DATABASE_NAME" "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD" "$CLICKHOUSE_HOST"
}

save_json_metrics() {
    local interface="$1"
    local batch_size="$2"
    local format="$3"
    local sorted="$4"
    local compressor="$5"

    # Generate JSON filename
    json_filename="results/${interface}_${format,,}_${batch_size}_$( [ "$sorted" == "true" ] && echo "sorted" || echo "unsorted" )_$( [ "$compressor" == "none" ] && echo "no_compression" || echo "$compressor" ).json"

    # Get server metrics file and extract content
    metrics_file=$(get_server_metrics_file "$interface" "$format" "$sorted" "$compressor" "$batch_size")
    metrics_content=$(grep -v -E "^Row 1:|^──────" "$metrics_file" 2>/dev/null | awk '{
        key=$1;
        sub(/:$/, "", key);  # Remove trailing colon from key
        $1="";               # Remove the key from the line
        value=substr($0, 2); # Trim leading space
        if (value ~ /[A-Za-z]/) {
            print "    \"" key "\": \"" value "\",";
        } else {
            print "    \"" key "\": " value ",";
        }
    }' | sed '$s/,$//')

    # Get CPU memory usage
    cpu_memory_file=$(get_cpu_memory_file "$interface" "$format" "$sorted" "$compressor" "$batch_size")
    cpu_memory_usage=$(grep "cpu_memory:" "$cpu_memory_file" 2>/dev/null | sed 's/cpu_memory: //g' | tr -d '\n')

    # Determine compression type
    if [[ "$interface" == "native" ]]; then
        compression_type="network compression"
    elif [[ -n "${FORMATS_WITH_BUILT_IN_COMPRESSION_MAP[$format]}" ]]; then
        compression_type="built-in compression"
    else
        compression_type="http compression"
    fi

    # Create JSON file
    echo -e "{\n  \"client_machine\": \"$CLIENT_MACHINE\",\n  \"server_system\": \"$SERVER_SYSTEM\",\n  \"date\": \"$DATE_TODAY\",\n  \"interface\": \"$interface\",\n  \"format\": \"$format\",\n  \"total_num_rows\": $TOTAL_NUM_ROWS,\n  \"batch_size\": $batch_size,\n  \"sorted\": $sorted,\n  \"compressor\": \"$compressor\",\n  \"compression_type\": \"$compression_type\",\n  \"server_metrics\": {\n$metrics_content\n  },\n  \"cpu_memory_usage\": \"$cpu_memory_usage\"\n}" > "$json_filename"

    echo "JSON results saved to $json_filename"
}

convert_IF_format_without_built_in_compression() {
    local batch_size="$1"
    local format="$2"
    local sorted="$3"

    # Call convert only once per (batch_size, format, sorted) if it has no built-in compression
    if [[ "$format" != "TabSeparatedWithNames" || "$sorted" == "true" ]] && [[ -z "${FORMATS_WITH_BUILT_IN_COMPRESSION_MAP[$format]}" ]]; then
        echo -e "\nConverting format without built-in compression."
        convert "$batch_size" "$format" "$sorted" "none"
    fi
}


convert_IF_format_with_built_in_compression_and_compressor_is_supported() {
    local batch_size="$1"
    local format="$2"
    local sorted="$3"
    local compressor="$4"

    # Check if the format has built-in compression
    if [[ -n "${FORMATS_WITH_BUILT_IN_COMPRESSION_MAP[$format]}" ]]; then
        # Check if the compressor is in the supported list for this format
        if [[ " ${FORMATS_WITH_BUILT_IN_COMPRESSION_MAP[$format]} " =~ " $compressor " ]]; then
            echo -e "\nConverting format with built-in compression."
            convert "$batch_size" "$format" "$sorted" "$compressor"
        else
            echo -e "\nSkipping ingestion: $compressor is not a supported built-in compression for $format."
            return 1  # Return a non-zero exit code for unsupported compression
        fi
    fi
}

should_ingest() {
    local interface="$1"
    local format="$2"
    local compressor="$3"
    local native_formats=("${@:4}")  # Capture the remaining arguments as an array

    if [[ "$interface" == "http" ]]; then
        return 0  # Allow ingestion for HTTP interface
    elif [[ "$interface" == "native" ]]; then
        if [[ " ${native_formats[*]} " =~ " $format " &&
              " ${NATIVE_INTERFACE_COMPRESSION_LIST[*]} " =~ " $compressor " ]]; then
            return 0  # Allow ingestion for Native interface if conditions are met
        fi
    fi
    return 1  # Otherwise, do not ingest
}

batch_sizes=("10000" "100000" "500000" "1000000")

formats=(
    "Arrow"
    "ArrowStream"
    "Avro"
    "BSONEachRow"
    "CSV"
    "CSVWithNames"
    "CSVWithNamesAndTypes"
    "CapnProto"
    "CustomSeparated"
    "CustomSeparatedWithNames"
    "CustomSeparatedWithNamesAndTypes"
    "JSON"
    "JSONColumns"
    "JSONColumnsWithMetadata"
    "JSONCompact"
    "JSONCompactColumns"
    "JSONCompactEachRow"
    "JSONCompactEachRowWithNames"
    "JSONCompactEachRowWithNamesAndTypes"
    "JSONCompactStringsEachRow"
    "JSONCompactStringsEachRowWithNames"
    "JSONCompactStringsEachRowWithNamesAndTypes"
    "JSONEachRow"
    "JSONLines"
    "JSONObjectEachRow"
    "JSONStringsEachRow"
#     "LineAsString"
    "MsgPack"
    "NDJSON"
    "Native"
#     "Npy"
    "ORC"
    "Parquet"
    "Protobuf"
#     "ProtobufList"
#     "ProtobufSingle"
#     "Raw"
#     "RawBLOB"
#     "RawWithNames"
#     "RawWithNamesAndTypes"
    "RowBinary"
    "RowBinaryWithNames"
    "RowBinaryWithNamesAndTypes"
    "TSKV"
    "TSV"
#     "TSVRaw"
#     "TSVRawWithNames"
#     "TSVRawWithNamesAndTypes"
    "TSVWithNames"
    "TSVWithNamesAndTypes"
    "TabSeparated"
#     "TabSeparatedRaw"
#     "TabSeparatedRawWithNames"
#     "TabSeparatedRawWithNamesAndTypes"
    "TabSeparatedWithNames"
    "TabSeparatedWithNamesAndTypes"
#     "Template"
    "Values"
)
input_format_for_native_interface=("TabSeparatedWithNames" "JSONEachRow" "Native")

sortings=("true" "false")

compressors=("lz4" "zstd" "none")

interfaces=("http" "native")

for batch_size in "${batch_sizes[@]}"; do

    echo -e "\nGenerating TSV chunks for batch size: $batch_size..."
    ./hits_to_tsv-chunks.sh "$batch_size"

    for format in "${formats[@]}"; do

        for sorted in "${sortings[@]}"; do

            convert_IF_format_without_built_in_compression "$batch_size" "$format" "$sorted"

            for compressor in "${compressors[@]}"; do

               convert_IF_format_with_built_in_compression_and_compressor_is_supported "$batch_size" "$format" "$sorted" "$compressor" || continue

                for interface in "${interfaces[@]}"; do
                    if should_ingest "$interface" "$format" "$compressor" "${input_format_for_native_interface[@]}"; then
                        ingest "$interface" "$batch_size" "$format" "$sorted" "$compressor"
                        save_json_metrics "$interface" "$batch_size" "$format" "$sorted" "$compressor"
                    fi
                done
            done
        done
    done
done