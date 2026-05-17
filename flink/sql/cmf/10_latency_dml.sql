-- Latency report — per-hop latency from origin, aggregated by origin
-- service and current hop topic, in 1-minute tumbling windows.
--
-- The header decode is in a WITH CTE because CMF requires TVF TUMBLE
-- syntax, the TVF doesn't propagate METADATA VIRTUAL columns through
-- itself, and CMF doesn't support CREATE VIEW. The CTE works because
-- CMF accepts `TABLE <cte_name>` as the TVF data argument.
INSERT INTO `isotope-report-latency-1m`
WITH `iso_decoded` AS (
    SELECT
        CAST(`headers`['x-isotope-trace-id']        AS STRING) AS `trace_id`,
        CAST(`headers`['x-isotope-origin-service']  AS STRING) AS `origin_service`,
        CAST(`headers`['x-isotope-this-topic']      AS STRING) AS `this_topic`,
        (TIMESTAMPDIFF(
            MICROSECOND,
            TO_TIMESTAMP_LTZ(
                CAST(CAST(`headers`['x-isotope-origin-ts'] AS STRING) AS BIGINT), 3),
            `$rowtime`) / 1000)                                AS `latency_ms`,
        `$rowtime`                                             AS `event_time`
    FROM `iso-start`
    WHERE `headers`['x-isotope-trace-id'] IS NOT NULL
)
SELECT
    UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
    UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
    `origin_service`,
    `this_topic`,
    COUNT(*)                                              AS `sample_count`,
    COUNT(DISTINCT `trace_id`)                            AS `distinct_traces`,
    AVG(CAST(`latency_ms` AS DOUBLE))                     AS `avg_latency_ms`,
    CAST(MIN(`latency_ms`) AS BIGINT)                     AS `min_latency_ms`,
    CAST(MAX(`latency_ms`) AS BIGINT)                     AS `max_latency_ms`
FROM TABLE(TUMBLE(TABLE `iso_decoded`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE))
GROUP BY
    `window_start`,
    `window_end`,
    `origin_service`,
    `this_topic`;
