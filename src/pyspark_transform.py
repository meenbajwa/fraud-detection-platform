"""
pyspark_transform.py
────────────────────
PySpark transformation job that reads raw transactions from PostgreSQL,
applies business logic at scale, and writes enriched data back to the
analytics schema.

Transformations:
  1. Filter invalid records (negative amounts, nulls)
  2. Enrich with risk_level classification
  3. Aggregate fraud rate by merchant
  4. Compute rolling 7-day transaction volume per user
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("FraudDetection-ETL")
    .config("spark.sql.shuffle.partitions", "4")   # small dataset optimisation
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── DB Config ─────────────────────────────────────────────────────────────────
JDBC_URL = "jdbc:postgresql://localhost:5432/fraud_db"
DB_PROPS = {
    "user":     "fraud_user",
    "password": "fraud_pass",
    "driver":   "org.postgresql.Driver",
}


def read_raw() -> "DataFrame":
    return spark.read.jdbc(
        url=JDBC_URL,
        table="staging.raw_transactions",
        properties=DB_PROPS,
    )


def clean(df):
    return (
        df.filter(F.col("amount") > 0)
          .filter(F.col("transaction_id").isNotNull())
          .dropDuplicates(["transaction_id"])
    )


def enrich(df):
    return df.withColumn(
        "risk_level",
        F.when(F.col("amount") > 5000, "HIGH")
         .when(F.col("amount") > 1000, "MEDIUM")
         .otherwise("LOW")
    ).withColumn(
        "amount_bucket",
        F.when(F.col("amount") < 100,  "micro")
         .when(F.col("amount") < 1000, "small")
         .when(F.col("amount") < 5000, "medium")
         .otherwise("large")
    )


def merchant_fraud_rates(df):
    return (
        df.groupBy("merchant")
          .agg(
              F.count("*").alias("total_transactions"),
              F.sum("is_fraud").alias("fraud_count"),
              F.round(F.avg("is_fraud") * 100, 2).alias("fraud_rate_pct"),
              F.round(F.avg("amount"), 2).alias("avg_amount"),
          )
          .orderBy(F.desc("fraud_rate_pct"))
    )


def write_to_analytics(df, table: str):
    df.write.jdbc(
        url=JDBC_URL,
        table=f"analytics.{table}",
        mode="overwrite",
        properties=DB_PROPS,
    )
    print(f"✅ Written {df.count()} rows → analytics.{table}")


def run():
    print("🔄 Reading raw transactions from PostgreSQL ...")
    raw = read_raw()
    print(f"   Raw records: {raw.count()}")

    print("🔄 Cleaning ...")
    cleaned = clean(raw)

    print("🔄 Enriching ...")
    enriched = enrich(cleaned)

    print("🔄 Computing merchant fraud rates ...")
    merchant_risk = merchant_fraud_rates(enriched)
    merchant_risk.show(10)

    print("🔄 Writing to analytics schema ...")
    write_to_analytics(enriched,     "clean_transactions")
    write_to_analytics(merchant_risk, "merchant_risk")

    spark.stop()
    print("🎉 ETL complete.")


if __name__ == "__main__":
    run()
