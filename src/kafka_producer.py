"""
kafka_producer.py
─────────────────
Simulates a bank's real-time transaction stream by publishing
events to a Kafka topic every 2 seconds.

Topic: bank-transactions
"""

import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

KAFKA_BROKER = "localhost:9092"
TOPIC        = "bank-transactions"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",          # wait for all replicas to acknowledge
        retries=3,
    )


def generate_event() -> dict:
    amount = round(random.uniform(1, 9999), 2)
    return {
        "transaction_id":   str(uuid.uuid4()),
        "user_name":        fake.name(),
        "account_number":   fake.bban(),
        "amount":           amount,
        "merchant":         fake.company(),
        "transaction_type": random.choice(["purchase", "transfer", "withdrawal"]),
        "location":         fake.city(),
        "timestamp":        datetime.utcnow().isoformat(),
        # Higher amounts skewed toward fraud for realism
        "is_fraud": int(random.random() < (0.30 if amount > 5000 else 0.03)),
    }


def run():
    producer = create_producer()
    print(f"🚀 Producer started — publishing to topic '{TOPIC}' ...")

    sent = 0
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, value=event)
            sent += 1
            label = "🚨 FRAUD" if event["is_fraud"] else "✅ OK   "
            print(f"{label} | ${event['amount']:>9,.2f} | {event['merchant'][:30]:<30} | {event['transaction_id']}")
            time.sleep(2)
    except KeyboardInterrupt:
        print(f"\n⛔ Producer stopped. Total sent: {sent}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()
