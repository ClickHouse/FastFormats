WITH
    '{db_name}' AS db_name,
    '{table_name}' AS table_name,
    T1 AS (
        SELECT *
        FROM
            clusterAllReplicas(default, system.query_log)
        WHERE
            has(tables, db_name || '.' || table_name)
            AND length(tables) = 1
            AND is_initial_query
            AND query_kind = 'Insert'
            AND type = 'QueryFinish'
        ORDER BY event_time ASC),
    T2 AS (
        SELECT
            groupArray(round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1_000_000, 2)) AS cpu_usage_pct,
            groupArray(memory_usage) AS memory_usage_bytes,
            groupArray(formatReadableSize(memory_usage)) AS memory_usage_readable,
            arrayEnumerate(cpu_usage_pct) AS num
        FROM
            T1)
SELECT
    arrayZip(num, cpu_usage_pct, memory_usage_bytes, memory_usage_readable) AS cpu_memory
FROM T2
FORMAT Vertical
SETTINGS skip_unavailable_shards = 1;

