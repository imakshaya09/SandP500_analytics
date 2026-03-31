import csv
import sqlite3
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT_DIR / "data" / "World-Stock-Prices-Dataset.csv"
DB_PATH = ROOT_DIR / "data" / "stocks.db"
TABLE_NAME = "stocks"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    brand_name TEXT,
    ticker TEXT,
    industry_tag TEXT,
    country TEXT,
    dividends REAL,
    stock_splits REAL,
    capital_gains REAL,
    user_comment TEXT DEFAULT '',
    commented_at TEXT DEFAULT NULL
);
"""

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    date, open, high, low, close, volume,
    brand_name, ticker, industry_tag, country,
    dividends, stock_splits, capital_gains, user_comment, commented_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_database():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_SQL)
    existing_columns = [row[1] for row in cursor.execute(f"PRAGMA table_info({TABLE_NAME});")]
    if "commented_at" not in existing_columns:
        cursor.execute("ALTER TABLE stocks ADD COLUMN commented_at TEXT DEFAULT NULL")
    cursor.execute(f"DELETE FROM {TABLE_NAME};")

    with CSV_PATH.open(mode="r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = []
        for row in reader:
            rows.append(
                (
                    row.get("Date", ""),
                    safe_float(row.get("Open")),
                    safe_float(row.get("High")),
                    safe_float(row.get("Low")),
                    safe_float(row.get("Close")),
                    safe_float(row.get("Volume")),
                    row.get("Brand_Name"),
                    row.get("Ticker"),
                    row.get("Industry_Tag"),
                    row.get("Country"),
                    safe_float(row.get("Dividends")),
                    safe_float(row.get("Stock Splits")),
                    safe_float(row.get("Capital Gains")),
                    "",
                    None,
                )
            )

    cursor.executemany(INSERT_SQL, rows)
    conn.commit()
    conn.close()
    print(f"Imported {len(rows)} rows into {DB_PATH}")


if __name__ == "__main__":
    build_database()
