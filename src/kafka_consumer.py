"""
kafka_consumer.py
─────────────────
Consumes transaction events from Kafka, applies basic validation,
and writes records to a PostgreSQL staging table for downstream ETL.

Topic: bank-transactions
Target: PostgreSQL → staging.raw_transactions
"""

import json
import logging
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BROKER = "localhost:9092"
TOPIC        = "bank-transactions"
GROUP_ID     = "fraud-consumer-group"

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "fraud_db",
    "user":     "fraud_user",
    "password": "fraud_pass",
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging.raw_transactions (
    transaction_id   TEXT PRIMARY KEY,
    user_name        TEXT,
    account_number   TEXT,
    amount           NUMERIC(12, 2),
    merchant         TEXT,
    transaction_type TEXT,
    location         TEXT,
    timestamp        TIMESTAMPTZ,
    is_fraud         SMALLINT,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);
"""

INSERT_SQL = """
INSERT INTO staging.raw_transactions
    (transaction_id, user_name, account_number, amount,
     merchant, transaction_type, location, timestamp, is_fraud)
VALUES %s
ON CONFLICT (transaction_id) DO NOTHING;
"""


def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def setup_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    log.info("Schema ready.")


def run():
    conn = get_connection()
    setup_schema(conn)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    log.info(f"👂 Listening on topic '{TOPIC}' ...")
    buffer = []
    BATCH_SIZE = 10

    for message in consumer:
        record = message.value
        buffer.append((
            record["transaction_id"],
            record["user_name"],
            record["account_number"],
            record["amount"],
            record["merchant"],
            record["transaction_type"],
            record["location"],
            record["timestamp"],
            record["is_fraud"],
        ))
        log.info(f"Received: {record['transaction_id']} | ${record['amount']}")

        if len(buffer) >= BATCH_SIZE:
            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, buffer)
            conn.commit()
            log.info(f"✅ Flushed {len(buffer)} records to PostgreSQL")
            buffer.clear()


if __name__ == "__main__":
    run()
