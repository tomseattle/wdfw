import hashlib
import os
import re
from typing import Any

from bs4 import BeautifulSoup
from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer


RAW_PAGE_SCHEMA = """
{
  "type": "record",
  "namespace": "com.wdfw.regulations",
  "name": "WdfwRawPageEvent",
  "fields": [
    { "name": "page_id", "type": "string" },
    {
      "name": "source_type",
      "type": {
        "type": "enum",
        "name": "SourceType",
        "symbols": ["pamphlet", "emergency_rules"]
      }
    },
    { "name": "source_url", "type": "string" },
    { "name": "fetched_at", "type": "string" },
    { "name": "content_hash", "type": "string" },
    { "name": "etag", "type": ["null", "string"], "default": null },
    { "name": "text", "type": "string" },
    { "name": "html", "type": "string" }
  ]
}
"""

RULE_EXTRACTED_SCHEMA = """
{
  "type": "record",
  "namespace": "com.wdfw.regulations",
  "name": "WdfwRuleExtracted",
  "fields": [
    { "name": "rule_id", "type": "string" },
    { "name": "page_id", "type": "string" },
    { "name": "source_type", "type": "string" },
    { "name": "source_url", "type": "string" },
    { "name": "fetched_at", "type": "string" },
    { "name": "content_hash", "type": "string" },
    { "name": "title", "type": ["null", "string"], "default": null },
    { "name": "effective_start", "type": ["null", "string"], "default": null },
    { "name": "effective_end", "type": ["null", "string"], "default": null },
    { "name": "water_body", "type": ["null", "string"], "default": null },
    { "name": "county", "type": ["null", "string"], "default": null },
    { "name": "species", "type": { "type": "array", "items": "string" } },
    { "name": "rule_text", "type": "string" },
    { "name": "is_emergency_rule", "type": "boolean" }
  ]
}
"""

RAW_TOPIC = os.getenv("RAW_TOPIC", "wdfw.raw_pages")
RULES_TOPIC = os.getenv("RULES_TOPIC", "wdfw.rules.extracted")
GROUP_ID = os.getenv("GROUP_ID", "wdfw-rule-parser-v1")


SPECIES_KEYWORDS = [
    "salmon",
    "steelhead",
    "trout",
    "sturgeon",
    "halibut",
    "lingcod",
    "bass",
    "perch",
    "walleye",
    "pike",
    "crab",
    "shrimp",
    "clam",
    "oyster",
    "mussel",
    "shellfish",
]

COUNTY_KEYWORDS = [
    "Whatcom County",
    "Skagit County",
    "Snohomish County",
    "King County",
    "Pierce County",
    "Thurston County",
    "Clark County",
    "Cowlitz County",
    "Yakima County",
    "Chelan County",
    "Grant County",
    "Spokane County",
]


def dict_identity(obj: dict[str, Any], ctx: Any) -> dict[str, Any]:
    return obj


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


def make_consumer() -> DeserializingConsumer:
    schema_registry_client = make_schema_registry_client()

    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=RAW_PAGE_SCHEMA,
        from_dict=lambda obj, ctx: obj,
    )

    return DeserializingConsumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"],
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def make_producer() -> SerializingProducer:
    schema_registry_client = make_schema_registry_client()

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=RULE_EXTRACTED_SCHEMA,
        to_dict=dict_identity,
        conf={"auto.register.schemas": True},
    )

    return SerializingProducer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"],
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed for key={msg.key()!r}: {err}")
    else:
        print(
            f"Delivered key={msg.key()!r} "
            f"to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
        )


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def detect_species(text: str) -> list[str]:
    found = []
    lower_text = text.lower()
    for species in SPECIES_KEYWORDS:
        if species in lower_text:
            found.append(species)
    return sorted(set(found))


def detect_county(text: str) -> str | None:
    for county in COUNTY_KEYWORDS:
        if county.lower() in text.lower():
            return county
    return None


def detect_water_body(text: str) -> str | None:
    patterns = [
        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+River)\b",
        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+Creek)\b",
        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+Lake)\b",
        r"\b(Marine Area\s+\d+[A-Z]?)\b",
        r"\b(Puget Sound)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def detect_dates(text: str) -> tuple[str | None, str | None]:
    # MVP heuristic only
    patterns = [
        r"(\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})\s+(?:through|to|-)\s+(\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})",
        r"effective\s+(\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            if len(match.groups()) == 2:
                return match.group(1), match.group(2)
            return match.group(1), None

    return None, None


def split_emergency_rule_blocks(html: str) -> list[dict[str, str | None]]:
    """
    Heuristic parser:
    - find main content
    - collect candidate headings and text blocks
    - return rule-like chunks
    """
    soup = BeautifulSoup(html, "lxml")
    main = soup.find("main")
    container = main if main else (soup.body if soup.body else soup)

    blocks: list[dict[str, str | None]] = []

    current_title = None
    current_lines: list[str] = []

    for node in container.find_all(["h1", "h2", "h3", "h4", "p", "li", "div"], recursive=True):
        text = normalize_whitespace(node.get_text(" ", strip=True))
        if not text:
            continue

        if node.name in {"h1", "h2", "h3", "h4"}:
            if current_lines:
                blocks.append(
                    {
                        "title": current_title,
                        "text": "\n".join(current_lines).strip(),
                    }
                )
                current_lines = []

            current_title = text
        else:
            # avoid adding overly tiny/noisy fragments
            if len(text) >= 30:
                current_lines.append(text)

    if current_lines:
        blocks.append(
            {
                "title": current_title,
                "text": "\n".join(current_lines).strip(),
            }
        )

    # Filter out obvious boilerplate-ish blocks
    filtered = []
    for block in blocks:
        block_text = block["text"] or ""
        if len(block_text) < 80:
            continue
        filtered.append(block)

    return filtered


def parse_emergency_rules_event(event: dict[str, Any]) -> list[dict[str, Any]]:
    html = event["html"]
    chunks = split_emergency_rule_blocks(html)

    extracted_rules: list[dict[str, Any]] = []

    for chunk in chunks:
        title = chunk.get("title")
        rule_text = chunk.get("text") or ""
        combined_text = f"{title or ''}\n{rule_text}".strip()

        effective_start, effective_end = detect_dates(combined_text)
        county = detect_county(combined_text)
        water_body = detect_water_body(combined_text)
        species = detect_species(combined_text)

        # stable deterministic id from the raw page hash + block text
        rule_id_seed = f'{event["page_id"]}|{event["content_hash"]}|{combined_text}'
        rule_id = hashlib.sha256(rule_id_seed.encode("utf-8")).hexdigest()

        extracted_rules.append(
            {
                "rule_id": rule_id,
                "page_id": event["page_id"],
                "source_type": event["source_type"],
                "source_url": event["source_url"],
                "fetched_at": event["fetched_at"],
                "content_hash": event["content_hash"],
                "title": title,
                "effective_start": effective_start,
                "effective_end": effective_end,
                "water_body": water_body,
                "county": county,
                "species": species,
                "rule_text": rule_text,
                "is_emergency_rule": True,
            }
        )

    return extracted_rules


def parse_pamphlet_event(event: dict[str, Any]) -> list[dict[str, Any]]:
    """
    MVP behavior for the main regulations page:
    emit one coarse record so downstream systems at least have searchable text.
    Later you can make this much more granular.
    """
    text = event["text"]
    title = "Main fishing regulations page"

    rule_id_seed = f'{event["page_id"]}|{event["content_hash"]}|{title}'
    rule_id = hashlib.sha256(rule_id_seed.encode("utf-8")).hexdigest()

    return [
        {
            "rule_id": rule_id,
            "page_id": event["page_id"],
            "source_type": event["source_type"],
            "source_url": event["source_url"],
            "fetched_at": event["fetched_at"],
            "content_hash": event["content_hash"],
            "title": title,
            "effective_start": None,
            "effective_end": None,
            "water_body": None,
            "county": None,
            "species": detect_species(text),
            "rule_text": text,
            "is_emergency_rule": False,
        }
    ]


def parse_raw_page_event(event: dict[str, Any]) -> list[dict[str, Any]]:
    if event["page_id"] == "emergency_rules_current":
        return parse_emergency_rules_event(event)

    if event["page_id"] == "fishing_regulations":
        return parse_pamphlet_event(event)

    return []


def publish_rules(producer: SerializingProducer, rules: list[dict[str, Any]]) -> None:
    for rule in rules:
        producer.produce(
            topic=RULES_TOPIC,
            key=rule["rule_id"],
            value=rule,
            on_delivery=delivery_report,
        )
        producer.poll(0)

    producer.flush()


def main() -> None:
    consumer = make_consumer()
    producer = make_producer()

    consumer.subscribe([RAW_TOPIC])
    print(f"Listening on {RAW_TOPIC}, publishing to {RULES_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = msg.value()
            if event is None:
                print("Skipping empty message value")
                consumer.commit(message=msg)
                continue

            try:
                rules = parse_raw_page_event(event)
                print(f"Parsed {len(rules)} rules from page_id={event['page_id']}")

                if rules:
                    publish_rules(producer, rules)

                consumer.commit(message=msg)
            except Exception as exc:
                print(f"Failed to parse message for page_id={event.get('page_id')}: {exc}")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()