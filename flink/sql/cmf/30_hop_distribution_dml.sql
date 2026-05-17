-- Hop distribution report — counts of records per (topic, hop_count)
-- bucket in 1-minute tumbling windows. Surfaces unexpectedly-long
-- chains, re-emission loops, and hop_list truncation.
INSERT INTO `isotope-report-hop-distribution-1m`
WITH `iso_decoded` AS (
    SELECT
        CAST(`headers`['x-isotope-trace-id'] AS STRING) AS `trace_id`,
        CAST(`headers`['x-isotope-this-topic'] AS STRING) AS `this_topic`,
        CAST(CAST(`headers`['x-isotope-hop-count'] AS STRING) AS INT) AS `hop_count`,
        `$rowtime` AS `event_time`
    FROM `iso-start`
    WHERE `headers`['x-isotope-trace-id'] IS NOT NULL
)
SELECT
    UNIX_TIMESTAMP(CAST(`window_start` AS STRING)) * 1000 AS `window_start`,
    UNIX_TIMESTAMP(CAST(`window_end`   AS STRING)) * 1000 AS `window_end`,
    `this_topic`,
    `hop_count`,
    COUNT(*)                  AS `records`,
    COUNT(DISTINCT `trace_id`) AS `distinct_traces`
FROM TABLE(TUMBLE(TABLE `iso_decoded`, DESCRIPTOR(`event_time`), INTERVAL '1' MINUTE))
GROUP BY
    `window_start`,
    `window_end`,
    `this_topic`,
    `hop_count`;
