-- Coverage report — distinct trace IDs seen at each (topic, origin)
-- per 1-minute tumbling window. Coarse funnel signal: a drop in
-- distinct_traces between successive topics in the same pipeline
-- suggests traces being lost between stages.
INSERT INTO `isotope-report-coverage-1m`
WITH `iso_decoded` AS (
    SELECT
        CAST(`headers`['x-isotope-trace-id']       AS STRING) AS `trace_id`,
        CAST(`headers`['x-isotope-this-topic']     AS STRING) AS `this_topic`,
        CAST(`headers`['x-isotope-origin-service'] AS STRING) AS `origin_service`,
        `$rowtime` AS `event_time`
    FROM `iso-start`
    WHERE `headers`['x-isotope-trace-id'] IS NOT NULL
)
SELECT
    UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
    UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
    `this_topic`,
    `origin_service`,
    COUNT(DISTINCT `trace_id`) AS `distinct_traces`,
    COUNT(*)                   AS `records`
FROM TABLE(TUMBLE(TABLE `iso_decoded`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE))
GROUP BY
    `window_start`,
    `window_end`,
    `this_topic`,
    `origin_service`;
