WITH
    '{db_name}' AS db_name,
    '{table_name}' AS table_name
SELECT
    db_name,
    table_name,

    if(any(interface) = 1, 'native', if(any(interface) = 2, 'http', 'unknown')) AS used_interface,
    if(used_interface = 'native', 'Native', any(used_formats)[1]) AS used_transport_format,

    count() AS inserts,
    avg(read_rows) AS rows_per_insert,
    sum(read_rows) AS total_inserted_rows,

    dateDiff('second', min(query_start_time_microseconds), max(event_time_microseconds)) AS total_time_including_client_loop_s,

    sum(ProfileEvents['NetworkReceiveBytes']) AS total_received_bytes,
    round(total_received_bytes / inserts) AS received_bytes_per_insert,
    formatReadableSize(total_received_bytes) AS total_received_bytes_readable,
    formatReadableSize(received_bytes_per_insert) AS received_bytes_per_insert_readable,

    sum(ProfileEvents['MergeTreeDataWriterCompressedBytes']) AS total_written_bytes_compressed,
    round(total_written_bytes_compressed / inserts) AS written_bytes_compressed_per_insert,
    formatReadableSize(total_written_bytes_compressed) AS total_written_bytes_compressed_readable,
    formatReadableSize(written_bytes_compressed_per_insert) AS written_bytes_compressed_per_insert_readable,

    round(sum(query_duration_ms) / 1000, 2) AS total_insert_duration_s,
    round(sum(ProfileEvents['NetworkReceiveElapsedMicroseconds']) / 1000 / 1000, 2) AS total_network_receive_duration_s,
    round(sum(ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds']) / 1000 / 1000, 2) AS total_block_sort_duration_s,
    round(sum(ProfileEvents['WriteBufferFromS3Microseconds']) / 1000 / 1000, 2) AS total_storage_write_duration_s,
    round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000 / 1000, 2) AS total_cpu_s,

    sum(ProfileEvents['DelayedInserts']) AS num_delayed_inserts,
    sum(ProfileEvents['DelayedInsertsMilliseconds']) AS total_delayed_inserts_ms,

    round(quantiles(0.50)(query_duration_ms)[1], 2) AS insert_duration_ms_50th,
    round(quantiles(0.95)(query_duration_ms)[1], 2) AS insert_duration_ms_95th,
    round(quantiles(0.99)(query_duration_ms)[1], 2) AS insert_duration_ms_99th,

    round(quantiles(0.50)(ProfileEvents['NetworkReceiveElapsedMicroseconds'])[1] / 1000, 2) AS network_receive_duration_ms_50th,
    round(quantiles(0.95)(ProfileEvents['NetworkReceiveElapsedMicroseconds'])[1] / 1000, 2) AS network_receive_duration_ms_95th,
    round(quantiles(0.99)(ProfileEvents['NetworkReceiveElapsedMicroseconds'])[1] / 1000, 2) AS network_receive_duration_ms_99th,

    round(quantiles(0.50)(ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds'])[1] / 1000, 2) AS block_sort_duration_ms_50th,
    round(quantiles(0.95)(ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds'])[1] / 1000, 2) AS block_sort_duration_ms_95th,
    round(quantiles(0.99)(ProfileEvents['MergeTreeDataWriterSortingBlocksMicroseconds'])[1] / 1000, 2) AS block_sort_duration_ms_99th,

    round(quantiles(0.50)(ProfileEvents['WriteBufferFromS3Microseconds'])[1] / 1000, 2) AS storage_write_duration_ms_50th,
    round(quantiles(0.95)(ProfileEvents['WriteBufferFromS3Microseconds'])[1] / 1000, 2) AS storage_write_duration_ms_95th,
    round(quantiles(0.99)(ProfileEvents['WriteBufferFromS3Microseconds'])[1] / 1000, 2) AS storage_write_duration_ms_99th,


    round(quantiles(0.50)(ProfileEvents['NetworkReceiveBytes'])[1]) AS network_received_bytes_50th,
    round(quantiles(0.95)(ProfileEvents['NetworkReceiveBytes'])[1]) AS network_received_bytes_95th,
    round(quantiles(0.99)(ProfileEvents['NetworkReceiveBytes'])[1]) AS network_received_bytes_99th,
    formatReadableSize(network_received_bytes_50th) AS network_received_bytes_50th_readable,
    formatReadableSize(network_received_bytes_95th) AS network_received_bytes_95th_readable,
    formatReadableSize(network_received_bytes_99th) AS network_received_bytes_99th_readable,

    round(quantiles(0.50)(ProfileEvents['MergeTreeDataWriterUncompressedBytes'])[1]) AS mergetree_written_uncompressed_bytes_50th,
    round(quantiles(0.95)(ProfileEvents['MergeTreeDataWriterUncompressedBytes'])[1]) AS mergetree_written_uncompressed_bytes_95th,
    round(quantiles(0.99)(ProfileEvents['MergeTreeDataWriterUncompressedBytes'])[1]) AS mergetree_written_uncompressed_bytes_99th,
    formatReadableSize(mergetree_written_uncompressed_bytes_50th) AS mergetree_written_uncompressed_bytes_50th_readable,
    formatReadableSize(mergetree_written_uncompressed_bytes_95th) AS mergetree_written_uncompressed_bytes_95th_readable,
    formatReadableSize(mergetree_written_uncompressed_bytes_99th) AS mergetree_written_uncompressed_bytes_99th_readable,

    round(quantiles(0.50)(ProfileEvents['MergeTreeDataWriterCompressedBytes'])[1]) AS mergetree_written_compressed_bytes_50th,
    round(quantiles(0.95)(ProfileEvents['MergeTreeDataWriterCompressedBytes'])[1]) AS mergetree_written_compressed_bytes_95th,
    round(quantiles(0.99)(ProfileEvents['MergeTreeDataWriterCompressedBytes'])[1]) AS mergetree_written_compressed_bytes_99th,
    formatReadableSize(mergetree_written_compressed_bytes_50th) AS mergetree_written_compressed_bytes_50th_readable,
    formatReadableSize(mergetree_written_compressed_bytes_95th) AS mergetree_written_compressed_bytes_95th_readable,
    formatReadableSize(mergetree_written_compressed_bytes_99th) AS mergetree_written_compressed_bytes_99th_readable,


    round(quantiles(0.50)(memory_usage)[1]) AS memory_usage_bytes_50th,
    round(quantiles(0.95)(memory_usage)[1]) AS memory_usage_bytes_95th,
    round(quantiles(0.99)(memory_usage)[1]) AS memory_usage_bytes_99th,
    formatReadableSize(memory_usage_bytes_50th) AS memory_usage_50th_readable,
    formatReadableSize(memory_usage_bytes_95th) AS memory_usage_95th_readable,
    formatReadableSize(memory_usage_bytes_99th) AS memory_usage_99th_readable,

    round(quantiles(0.50)(ProfileEvents['OSCPUVirtualTimeMicroseconds'])[1] / 1000, 2) AS cpu_ms_50th,
    round(quantiles(0.95)(ProfileEvents['OSCPUVirtualTimeMicroseconds'])[1] / 1000, 2) AS cpu_ms_95th,
    round(quantiles(0.99)(ProfileEvents['OSCPUVirtualTimeMicroseconds'])[1] / 1000, 2) AS cpu_ms_99th,

    quantiles(0.50)(length(thread_ids))[1] AS num_used_threads_50th,
    quantiles(0.95)(length(thread_ids))[1] AS num_used_threads_95th,
    quantiles(0.99)(length(thread_ids))[1] AS num_used_threads_99th,

    quantiles(0.50)(peak_threads_usage)[1] AS num_used_threads_concurrently_50th,
    quantiles(0.95)(peak_threads_usage)[1] AS num_used_threads_concurrently_95th,
    quantiles(0.99)(peak_threads_usage)[1] AS num_used_threads_concurrently_99th,

    any(ProfileEvents) AS profileevents_sample

FROM
    clusterAllReplicas(default, system.query_log)
WHERE
    has(tables, db_name || '.' || table_name)
    AND length(tables) = 1
    AND is_initial_query
    AND query_kind = 'Insert'
    AND type = 'QueryFinish'
FORMAT Vertical;