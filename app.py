import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


WDFW_RAW_PAGE_SCHEMA = """
{
  "type": "record",
  "namespace": "com.wdfw.regulations",
  "name": "WdfwRawPageEvent",
  "doc": "Represents a fetched WDFW regulation page when a content change is detected.",
  "fields": [
    {
      "name": "page_id",
      "type": "string",
      "doc": "Logical identifier for the page."
    },
    {
      "name": "source_type",
      "type": {
        "type": "enum",
        "name": "SourceType",
        "symbols": ["pamphlet", "emergency_rules"]
      },
      "doc": "Type of regulation source."
    },
    {
      "name": "source_url",
      "type": "string",
      "doc": "URL of the WDFW page."
    },
    {
      "name": "fetched_at",
      "type": "string",
      "doc": "ISO-8601 timestamp when the page was fetched."
    },
    {
      "name": "content_hash",
      "type": "string",
      "doc": "SHA-256 hash of normalized page content."
    },
    {
      "name": "etag",
      "type": ["null", "string"],
      "default": null,
      "doc": "Optional HTTP ETag value."
    },
    {
      "name": "text",
      "type": "string",
      "doc": "Normalized extracted text content of the page."
    },
    {
      "name": "html",
      "type": "string",
      "doc": "Raw HTML content of the page."
    }
  ]
}
"""

PAGES = [
    {
        "page_id": "fishing_regulations",
        "url": "https://wdfw.wa.gov/fishing/regulations",
        "source_type": "pamphlet",
    },
    {
        "page_id": "emergency_rules_current",
        "url": "https://wdfw.wa.gov/fishing/regulations/emergency-rules",
        "source_type": "emergency_rules",
    },
]

STATE_DIR = Path("state")
SNAPSHOT_DIR = Path("snapshots")
STATE_FILE = STATE_DIR / "page_state.json"

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "900"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wdfw.raw_pages")


def ensure_dirs() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}

    with STATE_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict[str, Any]) -> None:
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def fetch_page(url: str) -> tuple[str, str | None]:
    response = requests.get(
        url,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={
            "User-Agent": "wdfw-regs-poller/1.0",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    response.raise_for_status()
    return response.text, response.headers.get("ETag")


def extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript", "svg", "img"]):
        tag.decompose()

    main = soup.find("main")
    container = main if main else (soup.body if soup.body else soup)

    lines = [
        line.strip()
        for line in container.get_text(separator="\n", strip=True).splitlines()
        if line.strip()
    ]
    return "\n".join(lines)


def compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def safe_timestamp_for_filename(iso_ts: str) -> str:
    return (
        iso_ts.replace(":", "")
        .replace("-", "")
        .replace("+00:00", "Z")
        .replace(".", "_")
    )


def save_snapshot(page_id: str, fetched_at: str, html: str, text: str) -> None:
    ts = safe_timestamp_for_filename(fetched_at)
    base = SNAPSHOT_DIR / f"{page_id}_{ts}"

    with open(f"{base}.html", "w", encoding="utf-8") as f:
        f.write(html)

    with open(f"{base}.txt", "w", encoding="utf-8") as f:
        f.write(text)


def make_schema_registry_client() -> SchemaRegistryClient:
    return SchemaRegistryClient(
        {
            "url": os.environ["SCHEMA_REGISTRY_URL"],
            "basic.auth.user.info": (
                f'{os.environ["SCHEMA_REGISTRY_API_KEY"]}:'
                f'{os.environ["SCHEMA_REGISTRY_API_SECRET"]}'
            ),
        }
    )


def dict_to_avro(obj: dict[str, Any], ctx: Any) -> dict[str, Any]:
    return obj


def make_producer() -> SerializingProducer:
    schema_registry_client = make_schema_registry_client()

    value_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=WDFW_RAW_PAGE_SCHEMA,
        to_dict=dict_to_avro,
        conf={"auto.register.schemas": True},
    )

    producer_conf = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": value_serializer,
    }

    return SerializingProducer(producer_conf)


def delivery_report(err: Exception | None, msg: Any) -> None:
    if err is not None:
        print(f"Delivery failed for key={msg.key()!r}: {err}")
    else:
        print(
            f"Delivered key={msg.key()!r} "
            f"to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def build_event(
    page: dict[str, str],
    fetched_at: str,
    html: str,
    text: str,
    etag: str | None,
    content_hash: str,
) -> dict[str, Any]:
    return {
        "page_id": page["page_id"],
        "source_type": page["source_type"],
        "source_url": page["url"],
        "fetched_at": fetched_at,
        "content_hash": content_hash,
        "etag": etag,
        "text": text,
        "html": html,
    }


def publish_event(producer: SerializingProducer, event: dict[str, Any]) -> None:
    producer.produce(
        topic=KAFKA_TOPIC,
        key=event["page_id"],
        value=event,
        on_delivery=delivery_report,
    )
    producer.poll(0)


def poll_once(state: dict[str, Any], producer: SerializingProducer) -> dict[str, Any]:
    for page in PAGES:
        fetched_at = datetime.now(timezone.utc).isoformat()

        try:
            html, etag = fetch_page(page["url"])
            text = extract_text(html)
            content_hash = compute_hash(text)

            previous_hash = state.get(page["page_id"], {}).get("content_hash")

            if previous_hash != content_hash:
                print(f"[CHANGED] {page['page_id']}")

                save_snapshot(page["page_id"], fetched_at, html, text)

                event = build_event(
                    page=page,
                    fetched_at=fetched_at,
                    html=html,
                    text=text,
                    etag=etag,
                    content_hash=content_hash,
                )
                publish_event(producer, event)

                state[page["page_id"]] = {
                    "content_hash": content_hash,
                    "fetched_at": fetched_at,
                    "etag": etag,
                    "source_url": page["url"],
                }
            else:
                print(f"[NO CHANGE] {page['page_id']}")

        except Exception as exc:
            print(f"[ERROR] Failed polling {page['url']}: {exc}")

    save_state(state)
    producer.flush()
    return state


def main() -> None:
    ensure_dirs()
    state = load_state()
    producer = make_producer()

    print("Starting WDFW poller...")
    print(f"Polling every {POLL_INTERVAL_SECONDS} seconds")
    print(f"Kafka topic: {KAFKA_TOPIC}")

    while True:
        state = poll_once(state, producer)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()