WITH
    '{db_name}' AS db_name,
    '{table_name}' AS table_name,
     10 AS interval_seconds,
    (SELECT
        min(event_time)
    FROM
        clusterAllReplicas(default, system.query_log)
    WHERE
        has(tables, db_name || '.' || table_name)
        AND length(tables) = 1
        AND is_initial_query
        AND query_kind = 'Insert'
        AND type = 'QueryFinish') AS start_time,
    T1 AS (
        SELECT
            toStartOfInterval(event_time, toIntervalSecond(interval_seconds)) AS t,
            dateDiff('second', toStartOfInterval(start_time, toIntervalSecond(interval_seconds)), t) AS seconds,
            round(avg(metric_cpu) / 1_000_000, 2) AS cpu_usage,
            round(avg(metric_memory))  AS memory_usage,
            formatReadableSize(memory_usage) AS memory_usage_readable
        FROM (
            SELECT event_time, sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS metric_cpu, sum(memory_usage) AS metric_memory
              FROM clusterAllReplicas(default, system.query_log)
            WHERE
                has(tables, db_name || '.' || table_name)
                AND length(tables) = 1
                AND is_initial_query
                AND query_kind = 'Insert'
                AND type = 'QueryFinish'
            GROUP BY event_time)
        GROUP BY t
        ORDER BY t ASC)
SELECT
    arrayZip(
        groupArray(seconds) ,
        groupArray(cpu_usage),
        groupArray(memory_usage),
        groupArray(memory_usage_readable)) AS cpu_memory
FROM T1
FORMAT Vertical
SETTINGS skip_unavailable_shards = 1;