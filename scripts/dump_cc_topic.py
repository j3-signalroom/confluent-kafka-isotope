#!/usr/bin/env python3
"""
Downloads up to N records from a Confluent Cloud Kafka topic into a sample data file.

Captures a real production topic as a local sample dataset for offline
development, calibration, or replay against a different deployment path.

Output format is inferred from the --output extension:
  .csv    — flattened columns; nested objects become dotted keys (a.b.c);
            lists are json.dumps'd into a single CSV cell. The header is the
            sorted union of all flattened keys observed across the run.
  .jsonl  — one JSON record per line, preserving nested structure verbatim.

Schema handling (--key-format / --value-format):
  avro    — Confluent SR-framed Avro (magic byte 0x00 + 4-byte schema ID +
            Avro-encoded payload). Writer schema is auto-fetched from SR by ID;
            no reader schema is supplied. Logical types like timestamp-millis
            deserialize to Python datetime, which is rendered as ISO-8601 by
            the JSONL/CSV writers via default=str.
  json    — Confluent SR-framed JSON if `schema.registry.url` is in
            client.properties; otherwise raw JSON bytes.
  string  — UTF-8 string passthrough (sensible default for string-keyed topics).
  raw     — base64-encoded bytes, for diagnostic dumps.

Per-record handling:
  * The deserialized key is promoted into the record under a `_key.` prefix
    (or, for `--key-format string`, into a single `_kafka_key` field). This
    preserves the producer's partition strategy in the captured sample.
  * Optional `--include-kafka-metadata` adds `_kafka_partition`,
    `_kafka_offset`, and `_kafka_timestamp_ms` so downstream replay can
    reproduce ordering.

Stop conditions (whichever fires first):
  * --max-records reached (default 1,000,000)
  * No record arrives within --poll-timeout (default 5s, treated as
    end-of-topic)
  * SIGINT / Ctrl-C — partial output is still flushed

Examples:
  # Avro topic with both key and value Avro-encoded:
  python3 scripts/dump_cc_topic.py \\
      --config client.properties \\
      --topic my_source_topic \\
      --key-format avro --value-format avro \\
      --output sample_data/my_source_topic.csv

  # JSON-registry topic, string key:
  python3 scripts/dump_cc_topic.py \\
      --config client.properties \\
      --topic my_other_topic \\
      --key-format string --value-format json \\
      --output sample_data/my_other_topic.csv

Dependencies:
    pip install confluent-kafka                # raw / string mode
    pip install 'confluent-kafka[json]'        # value-format json with SR
    pip install 'confluent-kafka[avro]'        # value-format / key-format avro
"""
import argparse
import base64
import csv
import json
import signal
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "sample_data"

SR_CONFIG_KEYS = {
    "schema.registry.url",
    "basic.auth.user.info",
    "basic.auth.credentials.source",
}

DEFAULT_MAX_RECORDS = 1_000_000
DEFAULT_REPORT_EVERY = 10_000
DEFAULT_POLL_TIMEOUT_S = 5.0
DEFAULT_GROUP_ID_PREFIX = "cc-topic-dump-"

VALUE_FORMATS = ("avro", "json", "raw")
KEY_FORMATS = ("avro", "json", "string", "raw")


def load_kafka_config(path: Path) -> dict:
    cfg = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip()
    return cfg


def flatten_for_csv(obj, prefix: str = "") -> dict:
    """Flatten dicts to dotted keys; lists/tuples → JSON-string cells; scalars passthrough."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else k
            out.update(flatten_for_csv(v, key))
        return out
    if isinstance(obj, (list, tuple)):
        return {prefix: json.dumps(list(obj), separators=(",", ":"), default=str)}
    return {prefix: obj}


def _build_sr_client(sr_cfg: dict):
    if "schema.registry.url" not in sr_cfg:
        sys.stderr.write(
            "Schema Registry URL is required for avro/json formats. "
            "Add `schema.registry.url=...` (and `basic.auth.user.info=...` for CC) "
            "to your client.properties.\n"
        )
        sys.exit(2)
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
    except ImportError:
        sys.stderr.write("pip install confluent-kafka  # required\n")
        sys.exit(1)
    sr_client_conf = {"url": sr_cfg["schema.registry.url"]}
    if "basic.auth.user.info" in sr_cfg:
        sr_client_conf["basic.auth.user.info"] = sr_cfg["basic.auth.user.info"]
    return SchemaRegistryClient(sr_client_conf)


def build_value_deserializer(value_format: str, sr_cfg: dict, topic: str):
    """Returns a callable: bytes → Python object (dict, scalar, or base64 str)."""
    if value_format == "avro":
        try:
            from confluent_kafka.schema_registry.avro import AvroDeserializer
            from confluent_kafka.serialization import (
                MessageField, SerializationContext,
            )
        except ImportError:
            sys.stderr.write("pip install 'confluent-kafka[avro]'  # required for avro\n")
            sys.exit(1)
        sr_client = _build_sr_client(sr_cfg)
        deser = AvroDeserializer(sr_client)

        def _avro(raw: bytes):
            return deser(raw, SerializationContext(topic, MessageField.VALUE))

        return _avro

    if value_format == "json":
        if "schema.registry.url" in sr_cfg:
            try:
                from confluent_kafka.schema_registry.json_schema import JSONDeserializer
                from confluent_kafka.serialization import (
                    MessageField, SerializationContext,
                )
            except ImportError:
                sys.stderr.write("pip install 'confluent-kafka[json]'  # required for json+SR\n")
                sys.exit(1)
            sr_client = _build_sr_client(sr_cfg)
            json_deser = JSONDeserializer(schema_str=None, schema_registry_client=sr_client)

            def _json_sr(raw: bytes):
                return json_deser(raw, SerializationContext(topic, MessageField.VALUE))

            return _json_sr

        # Plain JSON (no SR framing).
        def _json_plain(raw: bytes):
            return json.loads(raw.decode("utf-8"))

        return _json_plain

    if value_format == "raw":
        def _raw(raw: bytes):
            return base64.b64encode(raw).decode("ascii") if raw is not None else None

        return _raw

    raise ValueError(f"unknown value format: {value_format}")


def build_key_deserializer(key_format: str, sr_cfg: dict, topic: str):
    """Returns a callable: bytes → Python object."""
    if key_format == "avro":
        try:
            from confluent_kafka.schema_registry.avro import AvroDeserializer
            from confluent_kafka.serialization import (
                MessageField, SerializationContext,
            )
        except ImportError:
            sys.stderr.write("pip install 'confluent-kafka[avro]'  # required for avro\n")
            sys.exit(1)
        sr_client = _build_sr_client(sr_cfg)
        deser = AvroDeserializer(sr_client)

        def _avro(raw: bytes):
            return deser(raw, SerializationContext(topic, MessageField.KEY))

        return _avro

    if key_format == "json":
        if "schema.registry.url" in sr_cfg:
            try:
                from confluent_kafka.schema_registry.json_schema import JSONDeserializer
                from confluent_kafka.serialization import (
                    MessageField, SerializationContext,
                )
            except ImportError:
                sys.stderr.write("pip install 'confluent-kafka[json]'  # required for json+SR\n")
                sys.exit(1)
            sr_client = _build_sr_client(sr_cfg)
            json_deser = JSONDeserializer(schema_str=None, schema_registry_client=sr_client)

            def _json_sr(raw: bytes):
                return json_deser(raw, SerializationContext(topic, MessageField.KEY))

            return _json_sr

        def _json_plain(raw: bytes):
            return json.loads(raw.decode("utf-8"))

        return _json_plain

    if key_format == "string":
        def _string(raw: bytes):
            return raw.decode("utf-8") if raw is not None else None

        return _string

    if key_format == "raw":
        def _raw(raw: bytes):
            return base64.b64encode(raw).decode("ascii") if raw is not None else None

        return _raw

    raise ValueError(f"unknown key format: {key_format}")


def merge_key_into_record(record, deserialized_key, key_format: str) -> dict:
    """Promote the (deserialized) key into the record so partition info survives."""
    if not isinstance(record, dict):
        record = {"_value": record}
    if isinstance(deserialized_key, dict):
        for k, v in deserialized_key.items():
            record[f"_key.{k}"] = v
    else:
        # string / raw / scalar JSON → single column
        record["_kafka_key"] = deserialized_key
    return record


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--config", type=Path, required=True,
                    help="librdkafka client config file (CC bootstrap + SASL; SR keys required for avro/json)")
    ap.add_argument("--topic", required=True,
                    help="Source topic name on Confluent Cloud")
    ap.add_argument("--output", type=Path, default=None,
                    help=f"Output path; format from extension (.csv|.jsonl). "
                         f"Default: {DEFAULT_OUTPUT_DIR}/<topic>.csv")
    ap.add_argument("--value-format", choices=VALUE_FORMATS, default="json",
                    help="How to deserialize the record value (default: json)")
    ap.add_argument("--key-format", choices=KEY_FORMATS, default="string",
                    help="How to deserialize the record key (default: string)")
    ap.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS,
                    help=f"Stop after N records (default: {DEFAULT_MAX_RECORDS:,})")
    ap.add_argument("--from-offset", choices=("earliest", "latest"), default="earliest",
                    help="Where to start reading (default: earliest)")
    ap.add_argument("--include-kafka-metadata", action="store_true",
                    help="Add _kafka_partition / _kafka_offset / _kafka_timestamp_ms columns")
    ap.add_argument("--group-id", default=None,
                    help="Consumer group id (default: cc-topic-dump-<epoch_ms>, ephemeral)")
    ap.add_argument("--poll-timeout", type=float, default=DEFAULT_POLL_TIMEOUT_S,
                    help=f"Seconds with no new records before declaring end-of-topic "
                         f"(default: {DEFAULT_POLL_TIMEOUT_S})")
    ap.add_argument("--report-every", type=int, default=DEFAULT_REPORT_EVERY,
                    help=f"Print progress every N records (default: {DEFAULT_REPORT_EVERY:,})")
    args = ap.parse_args()

    # Expand ~ in config / output paths — passing them through "$(VAR)" in a
    # Makefile preserves the literal tilde, which Path doesn't auto-expand.
    args.config = args.config.expanduser()
    if args.output is not None:
        args.output = args.output.expanduser()

    if not args.config.exists():
        sys.stderr.write(f"Config not found: {args.config}\n")
        return 2

    output: Path = args.output or (DEFAULT_OUTPUT_DIR / f"{args.topic}.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    fmt = output.suffix.lower()
    if fmt not in (".csv", ".jsonl"):
        sys.stderr.write(f"Unsupported --output extension: {fmt} (expected .csv or .jsonl)\n")
        return 2

    raw_cfg = load_kafka_config(args.config)
    if "bootstrap.servers" not in raw_cfg:
        sys.stderr.write("config must include bootstrap.servers\n")
        return 2

    sr_cfg = {k: v for k, v in raw_cfg.items() if k in SR_CONFIG_KEYS}
    consumer_only_cfg = {k: v for k, v in raw_cfg.items() if k not in SR_CONFIG_KEYS}

    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        sys.stderr.write("pip install confluent-kafka  # required\n")
        return 1

    deserialize_value = build_value_deserializer(args.value_format, sr_cfg, args.topic)
    deserialize_key = build_key_deserializer(args.key_format, sr_cfg, args.topic)
    sys.stderr.write(
        f"key-format={args.key_format} value-format={args.value_format} "
        f"output={output} max-records={args.max_records:,}\n"
    )

    group_id = args.group_id or f"{DEFAULT_GROUP_ID_PREFIX}{int(time.time() * 1000)}"
    consumer = Consumer({
        **consumer_only_cfg,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": args.from_offset,
    })
    consumer.subscribe([args.topic])

    stop = False

    def handle_sigint(_sig, _frame):
        nonlocal stop
        sys.stderr.write("\ninterrupt — flushing partial output\n")
        stop = True

    signal.signal(signal.SIGINT, handle_sigint)

    consumed = 0
    deserialize_failures = 0
    last_report_t = time.monotonic()

    if fmt == ".csv":
        # Buffer in memory so we can compute the column union before writing the
        # header. At 1M records of the streaming__timeseries POC topic (~40 cols
        # of mostly small strings/numbers) this is a few hundred MB — workable
        # but memory-bound. For larger dumps prefer .jsonl, which streams.
        buffered: list[dict] = []
        column_union: set[str] = set()
    else:
        jsonl_fh = output.open("w", encoding="utf-8")

    try:
        while not stop and consumed < args.max_records:
            msg = consumer.poll(args.poll_timeout)
            if msg is None:
                sys.stderr.write(
                    f"no records in {args.poll_timeout:.1f}s — assuming end-of-topic\n"
                )
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                sys.stderr.write(f"consumer error: {msg.error()}\n")
                continue

            raw_value = msg.value()
            raw_key = msg.key()
            if raw_value is None:
                continue
            try:
                value = deserialize_value(raw_value)
            except Exception as e:
                deserialize_failures += 1
                if deserialize_failures <= 5:
                    sys.stderr.write(
                        f"value deserialize failure (offset={msg.offset()}): {e}\n"
                    )
                continue
            try:
                key = deserialize_key(raw_key) if raw_key is not None else None
            except Exception as e:
                deserialize_failures += 1
                if deserialize_failures <= 5:
                    sys.stderr.write(
                        f"key deserialize failure (offset={msg.offset()}): {e}\n"
                    )
                key = None

            record = merge_key_into_record(value, key, args.key_format)

            if args.include_kafka_metadata:
                record["_kafka_partition"] = msg.partition()
                record["_kafka_offset"] = msg.offset()
                record["_kafka_timestamp_ms"] = msg.timestamp()[1]

            if fmt == ".csv":
                flat = flatten_for_csv(record)
                column_union.update(flat.keys())
                buffered.append(flat)
            else:
                jsonl_fh.write(
                    json.dumps(record, separators=(",", ":"), default=str) + "\n"
                )

            consumed += 1
            if consumed % args.report_every == 0:
                now = time.monotonic()
                rate = args.report_every / max(now - last_report_t, 1e-6)
                last_report_t = now
                sys.stderr.write(
                    f"consumed={consumed:,} ({rate:,.0f}/s) "
                    f"deserialize_failures={deserialize_failures}\n"
                )

        if fmt == ".csv":
            header = sorted(column_union)
            with output.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=header, extrasaction="ignore")
                writer.writeheader()
                for row in buffered:
                    # Render datetimes / non-CSV-friendly scalars as strings.
                    writer.writerow({
                        k: ("" if v is None else (v if isinstance(v, (str, int, float, bool)) else str(v)))
                        for k, v in row.items()
                    })
        else:
            jsonl_fh.close()

    finally:
        try:
            consumer.close()
        except Exception:
            pass

    sys.stderr.write(
        f"done: wrote {consumed:,} records → {output} "
        f"(deserialize_failures={deserialize_failures})\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
