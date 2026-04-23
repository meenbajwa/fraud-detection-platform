import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# ----------------------------
# Generate fake transactions
# ----------------------------
def generate_transactions(n=100):
    records = []
    start_date = datetime.now() - timedelta(days=30)

    for _ in range(n):
        amount = round(random.uniform(1, 5000), 2)
        is_fraud = random.choices([0, 1], weights=[95, 5])[0]  # 5% fraud rate

        # High amounts are more likely to be fraud
        if amount > 3000:
            is_fraud = random.choices([0, 1], weights=[70, 30])[0]

        records.append({
            "transaction_id":   str(uuid.uuid4()),
            "user_name":        fake.name(),                          # PII - will mask later
            "account_number":   fake.bban(),                          # PII - will mask later
            "amount":           amount,
            "merchant":         fake.company(),
            "transaction_type": random.choice(["purchase", "transfer", "withdrawal"]),
            "location":         fake.city(),
            "timestamp":        (start_date + timedelta(
                                    minutes=random.randint(0, 43200)
                                )).isoformat(),
            "is_fraud":         is_fraud
        })

    return pd.DataFrame(records)


# ----------------------------
# Run and save to CSV
# ----------------------------
if __name__ == "__main__":
    df = generate_transactions(500)

    # Save to CSV
    df.to_csv("data/transactions.csv", index=False)

    # Print summary
    print(f"✅ Generated {len(df)} transactions")
    print(f"   Fraud transactions : {df['is_fraud'].sum()} ({df['is_fraud'].mean()*100:.1f}%)")
    print(f"   Total amount       : ${df['amount'].sum():,.2f}")
    print(f"   Avg amount         : ${df['amount'].mean():,.2f}")
    print(f"\nFirst 5 rows:")
    print(df.head())
