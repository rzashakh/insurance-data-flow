"""
Kafka consumer for Debezium CDC events from the insurance Postgres source.

Connects to Redpanda and subscribes to the policies and claims topics.
Each CDC event is decoded from the Debezium envelope JSON and printed with:
  table, operation (INSERT/UPDATE/DELETE), primary key, and key payload fields.

Run with:
    # Port-forward Redpanda first (in a separate terminal):
    #   kubectl port-forward -n data-lab svc/redpanda 9093:9093
    python kafka/consumer.py
"""

import json
import logging
import os
import signal
import sys

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("consumer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda-0.redpanda.data-lab.svc.cluster.local:9093")
TOPICS = ["insurance.public.policies", "insurance.public.claims"]
GROUP_ID = "lab-consumer"

OP_LABELS = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "READ"}

FIELD_MAP = {
    "policies": ["id", "product_line", "region", "premium"],
    "claims": ["id", "policy_id", "amount", "status"],
}


def extract(envelope: dict) -> dict | None:
    payload = envelope.get("payload")
    if not payload:
        return None

    op = payload.get("op", "?")
    table = payload.get("source", {}).get("table", "unknown")
    row = payload.get("after") or payload.get("before") or {}
    pk = row.get("id", "?")
    fields = {k: row.get(k) for k in FIELD_MAP.get(table, [])}

    return {"table": table, "op": op, "pk": pk, "fields": fields}


def print_event(event: dict) -> None:
    op_label = OP_LABELS.get(event["op"], event["op"])
    fields_str = "  ".join(f"{k}={v}" for k, v in event["fields"].items())
    log.info(
        "%-8s  table=%-10s  pk=%.8s...  %s",
        op_label,
        event["table"],
        event["pk"],
        fields_str,
    )


def build_consumer() -> KafkaConsumer:
    log.info("Connecting to Redpanda at %s ...", BOOTSTRAP)
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        consumer_timeout_ms=-1,
    )


def main() -> None:
    consumer = build_consumer()
    log.info("Subscribed to: %s", TOPICS)
    log.info("Consumer group: %s  (Ctrl-C to stop)", GROUP_ID)

    def shutdown(sig, frame):
        log.info("Shutting down ...")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for msg in consumer:
        if msg.value is None:
            continue
        event = extract(msg.value)
        if event:
            print_event(event)


if __name__ == "__main__":
    main()
