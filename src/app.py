import json
import os
import sqlite3
import threading
import time
from collections import defaultdict, OrderedDict
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template, request
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "data" / "stocks.db"
TEMPLATE_DIR = ROOT_DIR / "templates"
STATIC_DIR = ROOT_DIR / "static"
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
SPECIAL_TOPIC = "recent_comments"

app = Flask(
    __name__,
    template_folder=str(TEMPLATE_DIR),
    static_folder=str(STATIC_DIR),
)


topic_messages = defaultdict(lambda: OrderedDict())
messages_lock = threading.Lock()


def get_topic_definitions():
    topic_defs = [
        {
            "topic": build_topic_name(start, end),
            "label": f"stock stream from {start} to {end}",
        }
        for start, end in TOPIC_BUCKETS
    ]
    topic_defs.append({"topic": SPECIAL_TOPIC, "label": "Recent comments"})
    return topic_defs


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def refresh_comment_field(message):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_comment FROM stocks WHERE id = ?", (message["record_id"],))
    row = cursor.fetchone()
    conn.close()
    message["user_comment"] = row["user_comment"] if row else ""
    return message


def record_exists(record_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM stocks WHERE id = ?", (record_id,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists


def kafka_consumer_loop():
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id=None,
                value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            )
            consumer.subscribe(TOPIC_NAMES)

            # Wait until partitions are assigned, then rewind to the earliest offsets.
            while not consumer.assignment():
                consumer.poll(timeout_ms=100)
            consumer.seek_to_beginning(*consumer.assignment())

            for record in consumer:
                with messages_lock:
                    topic = record.topic
                    payload = record.value
                    payload["topic"] = topic
                    topic_messages[topic][payload["record_id"]] = payload
        except NoBrokersAvailable:
            time.sleep(2)
        except Exception:
            time.sleep(2)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/topics")
def api_topics():
    return jsonify(get_topic_definitions())


@app.route("/api/messages/<topic_name>")
def api_messages(topic_name):
    if topic_name == SPECIAL_TOPIC:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id AS record_id, date AS Date, ticker AS Ticker, open - close AS diff, user_comment, commented_at "
            "FROM stocks "
            "WHERE user_comment IS NOT NULL AND user_comment != '' "
            "ORDER BY commented_at DESC, id DESC"
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify(rows)

    if topic_name not in TOPIC_NAMES:
        return jsonify({"error": "unknown topic"}), 404

    with messages_lock:
        queue = topic_messages[topic_name]
        while queue:
            message = next(iter(queue.values()))
            if record_exists(message["record_id"]):
                break
            queue.pop(next(iter(queue.keys())), None)
        else:
            return jsonify([])

    message = refresh_comment_field(message.copy())
    return jsonify([message])


@app.route("/api/comment", methods=["POST"])
def api_comment():
    data = request.get_json(force=True)
    record_id = data.get("record_id")
    comment = data.get("comment", "")
    if not record_id:
        return jsonify({"error": "record_id is required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE stocks SET user_comment = ?, commented_at = ? WHERE id = ?",
        (comment, datetime.utcnow().isoformat(), record_id),
    )
    updated = cursor.rowcount
    conn.commit()
    conn.close()
    if updated == 0:
        with messages_lock:
            for queue in topic_messages.values():
                queue.pop(record_id, None)
        return jsonify({"error": "record not found"}), 404

    with messages_lock:
        for queue in topic_messages.values():
            queue.pop(record_id, None)

    return jsonify({"success": True, "record_id": record_id})


def start_consumer_thread():
    thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    thread.start()


if __name__ == "__main__":
    start_consumer_thread()
    port = int(os.getenv("APP_PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
