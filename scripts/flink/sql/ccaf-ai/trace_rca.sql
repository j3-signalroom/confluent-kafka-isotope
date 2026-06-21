-- =============================================================================
-- PoC: AI-enriched root-cause analysis for Isotope's stuck-trace report (CCAF)
-- =============================================================================
-- Adds an 8th, AI-generated "report" that turns each deterministic stuck-trace
-- alert into a natural-language root-cause hypothesis + suggested remediation.
--
-- DESIGN RULE — call the model on ALERTS, never per record. The input is
-- `stuck_trace_alerts_1m` (one row per stuck trace per 1-min window), which is
-- low-volume and high-signal. NEVER point inference at orders.placed / the
-- source stream. This mirrors isotope's metrics-native-vs-Flink split.
--
-- The AI output is a HYPOTHESIS, not ground truth — it never overwrites the
-- deterministic latency/coverage numbers; it's a separate enrichment topic.
--
-- Status caveats (verify against current docs before production):
--   • CREATE MODEL + ML_PREDICT ....... GA
--   • AI_COMPLETE built-in ............ GA-ish (confirm exact signature)
--   • CREATE AGENT / AI_RUN_AGENT ..... newer (2025-Q3/Q4); preview/early-GA
--   • AI_TOOL_INVOKE .................. Preview
-- Sources: docs.confluent.io/cloud/current/flink/reference/statements/create-model.html
--          docs.confluent.io/cloud/current/ai/builtin-functions/overview.html
--          docs.confluent.io/cloud/current/ai/streaming-agents/overview.html
--
-- Input schema (from scripts/flink/sql/cp/05_report_sinks.fql):
--   stuck_trace_alerts_1m(trace_id, origin_service, pipeline, last_service,
--                         last_topic, last_hop_count, last_seen_ts_ms, stuck_for_ms)
-- Pipeline shape: orders.placed -> orders.enriched -> orders.fulfilled -> shipping
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- OPTION A — CREATE MODEL + ML_PREDICT  (most control; pick your provider)
-- ─────────────────────────────────────────────────────────────────────────────
-- NOTE: Anthropic is NOT a direct CREATE MODEL provider. To use Claude, go via
-- AWS Bedrock ('provider'='bedrock'). Supported providers: bedrock, sagemaker,
-- azureml, azureopenai, googleai, openai, vertexai (+ 'confluent' managed).
-- The WITH keys below follow the verified OpenAI example shape; swap the
-- provider prefix + auth for Bedrock/Claude (AWS creds via a CREATE CONNECTION).

CREATE MODEL trace_rca
INPUT  (prompt STRING)
OUTPUT (analysis STRING)
WITH (
    'provider'             = 'openai',                 -- or 'bedrock' for Claude
    'task'                 = 'text_generation',
    'openai.model_version' = 'gpt-4o',                 -- e.g. a Bedrock Claude model id under bedrock.*
    'openai.endpoint'      = 'https://api.openai.com/v1/chat/completions',
    'openai.api_key'       = '{{sessionconfig/sql.secrets.rca_api_key}}'
);

-- Enrichment sink (one row per alert, carrying the AI hypothesis alongside the facts).
CREATE TABLE IF NOT EXISTS trace_rca_1m (
    `trace_id`     STRING,
    `pipeline`     STRING,
    `last_service` STRING,
    `last_topic`   STRING,
    `stuck_for_ms` BIGINT,
    `root_cause`   STRING
) WITH (
    'connector'    = 'kafka',
    'topic'        = 'isotope_report_trace_rca_1m',
    'value.format' = 'avro-confluent'
    -- ...bootstrap/SR/partitioner WITH options as in cp/05_report_sinks.fql
);

-- One model call per stuck-trace alert. The prompt embeds the deterministic
-- facts + the expected topology so the model can reason about WHERE it stalled.
INSERT INTO trace_rca_1m
SELECT
    a.trace_id,
    a.pipeline,
    a.last_service,
    a.last_topic,
    a.stuck_for_ms,
    p.analysis AS root_cause
FROM stuck_trace_alerts_1m AS a,
     LATERAL TABLE(ML_PREDICT('trace_rca',
         'You are an SRE analyzing a Kafka pipeline. A trace is STUCK.' ||
         ' pipeline=' || a.pipeline ||
         ' trace_id=' || a.trace_id ||
         ' last_seen_at=' || a.last_service || ' -> ' || a.last_topic ||
         ' hop_count=' || CAST(a.last_hop_count AS STRING) ||
         ' idle_ms=' || CAST(a.stuck_for_ms AS STRING) ||
         '. Expected path: orders.placed -> orders.enriched -> orders.fulfilled' ||
         ' -> shipping-notification-service.' ||
         ' In 2 sentences: most likely root cause, and one concrete remediation.'
     )) AS p;
-- Cost/throughput control on ML_PREDICT (optional 4th map arg):
--   { 'max_parallelism' = '10', 'retry_count' = '3', 'async_enabled' = 'true' }


-- ─────────────────────────────────────────────────────────────────────────────
-- OPTION B — built-in AI_COMPLETE  (zero model setup; Confluent-managed)
-- ─────────────────────────────────────────────────────────────────────────────
-- Simplest start: no CREATE MODEL, no provider keys. Confirm the exact arg list
-- against docs.confluent.io/cloud/current/ai/builtin-functions/overview.html —
-- some versions take AI_COMPLETE(prompt[, settings_map]).
--
-- INSERT INTO trace_rca_1m
-- SELECT a.trace_id, a.pipeline, a.last_service, a.last_topic, a.stuck_for_ms,
--        c.response AS root_cause
-- FROM stuck_trace_alerts_1m AS a,
--      LATERAL TABLE(AI_COMPLETE(
--          'A stuck trace: ' || a.last_service || ' -> ' || a.last_topic ||
--          ' idle ' || CAST(a.stuck_for_ms AS STRING) || 'ms on pipeline ' ||
--          a.pipeline || '. Likely root cause + one fix, 2 sentences.'
--      )) AS c;


-- ─────────────────────────────────────────────────────────────────────────────
-- OPTION C — agentic remediation  (Streaming Agents; close the loop, advanced)
-- ─────────────────────────────────────────────────────────────────────────────
-- Beyond explaining the alert, an agent can ACT: page on-call, open a ticket,
-- look up a runbook — via MCP tools. Tool calling uses Anthropic's MCP protocol.
--
-- 1) Connect to an MCP server (your runbook/incident tools):
-- CREATE CONNECTION ops_mcp WITH (
--     'type'           = 'mcp_server',
--     'endpoint'       = 'https://mcp.internal.example.com',
--     'api-key'        = '{{sessionconfig/sql.secrets.mcp_key}}',
--     'transport-type' = 'STREAMABLE_HTTP'
-- );
--
-- 2) Expose its tools to Flink:
-- CREATE TOOL runbook_tool USING CONNECTION ops_mcp
--   WITH ('type' = 'mcp', 'allowed_tools' = 'lookup_runbook,open_incident');
--
-- 3) Define the agent (role + tools + model):
-- CREATE AGENT rca_agent
--   USING MODEL trace_rca
--   USING PROMPT 'You are an SRE agent. For each stuck trace, diagnose the
--                 likely cause, fetch the matching runbook, and open an
--                 incident if idle time exceeds 5 minutes.'
--   USING TOOLS runbook_tool
--   WITH ('max_iterations' = '5');
--
-- 4) Run it over the alert stream (AI_RUN_AGENT automates the reason→tool loop):
-- INSERT INTO trace_rca_1m
-- SELECT a.trace_id, a.pipeline, a.last_service, a.last_topic, a.stuck_for_ms,
--        r.agent_output AS root_cause
-- FROM stuck_trace_alerts_1m AS a,
--      LATERAL TABLE(AI_RUN_AGENT('rca_agent',
--          a.last_service || '->' || a.last_topic || ' idle ' ||
--          CAST(a.stuck_for_ms AS STRING) || 'ms', a.trace_id)) AS r;
