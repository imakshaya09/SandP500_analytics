import json
import os
import sqlite3
from pathlib import Path

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "stocks.db"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_BUCKETS = [
    (2000, 2004),
    (2005, 2009),
    (2010, 2014),
    (2015, 2019),
    (2020, 2025),
]


def build_topic_name(start, end):
    return f"stock_stream_{start}_{end}"


TOPIC_NAMES = [build_topic_name(start, end) for start, end in TOPIC_BUCKETS]


def year_to_topic(year: int):
    for start, end in TOPIC_BUCKETS:
        if start <= year <= end:
            return build_topic_name(start, end)
    return build_topic_name(TOPIC_BUCKETS[-1][0], TOPIC_BUCKETS[-1][1])


def reset_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="stock-producer-admin")
    existing = admin.list_topics()
    delete_topics = [topic for topic in TOPIC_NAMES if topic in existing]
    if delete_topics:
        try:
            admin.delete_topics(delete_topics)
            print(f"Deleting existing topics: {delete_topics}")
        except Exception as exc:
            print(f"Warning: could not delete topics {delete_topics}: {exc}")
        for _ in range(30):
            remaining = [topic for topic in delete_topics if topic in admin.list_topics()]
            if not remaining:
                break
            time.sleep(1)
    admin.close()


def ensure_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="stock-producer-admin")
    existing = admin.list_topics()
    new_topics = []
    for topic in TOPIC_NAMES:
        if topic not in existing:
            new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
    if new_topics:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        print(f"Created topics: {[t.name for t in new_topics]}")
    admin.close()


def read_rows():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT id, date, open, close, ticker FROM stocks ORDER BY id")
    rows = cursor.fetchall()
    conn.close()
    return rows


def produce():
    reset_topics()
    ensure_topics()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    total = 0
    for row in read_rows():
        if not row["date"] or row["open"] is None or row["close"] is None:
            continue
        year = int(row["date"][:4])
        topic = year_to_topic(year)
        payload = {
            "record_id": row["id"],
            "Date": row["date"],
            "Ticker": row["ticker"],
            "diff": row["open"] - row["close"],
        }
        producer.send(topic, value=payload)
        total += 1

    producer.flush()
    producer.close()
    print(f"Produced {total} messages across {len(TOPIC_NAMES)} topics")


if __name__ == "__main__":
    produce()
