# SandP500_analytics
Analytics of S & P 500 stocks for the past 25 years

## What this project does

This project helps demonstrate a streaming workflow for stock data:

- Load raw stock data from a CSV file into a SQLite database.
- Send stock records from SQLite into real Kafka topics.
- Provide a web app with tabs for each Kafka topic bucket.
- Allow users to add comments to streamed stock records.
- Save comments back into SQLite and show recently commented records.

## How it works

1. **CSV → SQLite**
   - `src/setup_db.py` reads `data/World-Stock-Prices-Dataset.csv`.
   - It stores each row in a `stocks` table and adds a `user_comment` field.

2. **SQLite → Kafka**
   - `src/kafka_producer.py` reads rows from the SQLite table.
   - It assigns each row to one of five Kafka topics based on the year in the `date` field.
   - Each produced Kafka message includes `Date`, `Ticker`, and `diff` = `Open - Close`.

3. **Kafka → Browser**
   - `src/app.py` starts a Flask UI and consumes messages from the five Kafka topics.
   - The browser UI has one tab per topic bucket and a final tab for recent comments.
   - Each topic tab shows one stream message at a time, like a real consumer.

4. **Comment flow**
   - User enters a comment for the currently displayed streaming record.
   - The comment is saved to the corresponding SQLite row.
   - The consumed message is removed from the active tab display.
   - The `Recent comments` tab lists records from SQLite that have comments.

## Simple example for non-technical viewers

Imagine a product manager watching stock updates in five windows:

- Tab 1: stocks from 2000 to 2004
- Tab 2: stocks from 2005 to 2009
- Tab 3: stocks from 2010 to 2014
- Tab 4: stocks from 2015 to 2019
- Tab 5: stocks from 2020 to 2025

When a new message appears in a tab, the user can write a short note and save it.
The note is stored in the database, the message is marked as processed, and the latest commented records appear in the last tab.

This is similar to a workflow where data enters a live stream, a user reviews one event at a time, and later the system keeps a history of reviewed items.

## Setup and Run

### 1. Install Python dependencies

```bash
python3 -m pip install -r requirements.txt
```

### 2. Start Kafka and Zookeeper using Docker

If you have Docker installed, launch both services with:

```bash
docker compose up -d
```

If `docker compose` is not available, use:

```bash
docker-compose up -d
```

Wait until the containers are running.

### 3. Verify Kafka is available

```bash
nc -vz localhost 9092
```

Or, if you have Kafka CLI tools installed:

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

### 4. Import CSV into SQLite

```bash
python3 src/setup_db.py
```

### 5. Publish records into Kafka

```bash
python3 src/kafka_producer.py
```

### 6. Start the web app

```bash
APP_PORT=8080 python3 src/app.py
```

Then open your browser at:

```bash
http://localhost:8080
```

## Docker installation on macOS

If Docker is not installed, install Docker Desktop:

```bash
brew install --cask docker
```

Then open Docker Desktop and ensure it is running.

## Project files

- `src/setup_db.py` - loads CSV into SQLite and adds the comment field.
- `src/kafka_producer.py` - reads SQLite rows and publishes them into Kafka topics.
- `src/app.py` - runs a Flask UI and consumes Kafka messages.
- `templates/index.html` and `static/app.js` - browser interface for tabs and comments.

## Notes

- `Recent comments` is a database-driven view, not a Kafka consumer.
- Kafka must be running on `localhost:9092` or use `KAFKA_BOOTSTRAP_SERVERS` to point to another broker.
- The producer resets the five Kafka topics before publishing fresh messages.
