SELECT
    round(quantiles(0.50)(ProfileEvent_OSCPUVirtualTimeMicroseconds)[1] / 1_000_000,4) AS cpu_usage_50th,
    round(quantiles(0.95)(ProfileEvent_OSCPUVirtualTimeMicroseconds)[1] / 1_000_000,4) AS cpu_usage_95th,
    round(quantiles(0.99)(ProfileEvent_OSCPUVirtualTimeMicroseconds)[1] / 1_000_000,4) AS cpu_usage_99th,

    round(quantiles(0.50)(CurrentMetric_MemoryTracking)[1]) AS memory_usage_50th,
    round(quantiles(0.95)(CurrentMetric_MemoryTracking)[1]) AS memory_usage_95th,
    round(quantiles(0.99)(CurrentMetric_MemoryTracking)[1]) AS memory_usage_99th,

    formatReadableSize(memory_usage_50th) AS memory_usage_50th_readable,
    formatReadableSize(memory_usage_95th) AS memory_usage_95th_readable,
    formatReadableSize(memory_usage_99th) AS memory_usage_99th_readable
FROM clusterAllReplicas(default, system.metric_log)
WHERE event_time >= now() - INTERVAL 5 MINUTE
FORMAT Vertical;