"""
fraud_etl_dag.py
────────────────
Airflow DAG that orchestrates the full fraud detection pipeline.

Schedule: Every hour
Tasks:
  1. generate_transactions  → create fresh synthetic data
  2. apply_masking          → mask PII before analytics
  3. run_pyspark_etl        → transform + enrich with PySpark
  4. validate_data_quality  → run Great Expectations checks
  5. notify_completion      → log pipeline summary
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "fraud-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="fraud_detection_pipeline",
    default_args=default_args,
    description="End-to-end fraud detection ETL pipeline",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraud", "etl", "daas"],
) as dag:

    # ── Task 1: Generate Transactions ─────────────────────────────────────────
    generate_transactions = BashOperator(
        task_id="generate_transactions",
        bash_command="python3 /opt/airflow/src/generate_transactions.py",
    )

    # ── Task 2: Apply Data Masking ────────────────────────────────────────────
    def mask_task(**context):
        import pandas as pd
        import sys
        sys.path.insert(0, "/opt/airflow/src")
        from data_masking import apply_masking, validate_masking

        raw = pd.read_csv("/opt/airflow/data/transactions.csv")
        masked = apply_masking(raw)
        validate_masking(raw, masked)
        masked.to_csv("/opt/airflow/data/masked_transactions.csv", index=False)
        print(f"✅ Masked {len(masked)} records")
        return len(masked)

    apply_masking_task = PythonOperator(
        task_id="apply_pii_masking",
        python_callable=mask_task,
    )

    # ── Task 3: PySpark ETL ───────────────────────────────────────────────────
    run_spark_etl = BashOperator(
        task_id="run_pyspark_etl",
        bash_command="spark-submit /opt/airflow/src/pyspark_transform.py",
    )

    # ── Task 4: Data Quality Validation ──────────────────────────────────────
    def validate_quality(**context):
        import pandas as pd

        df = pd.read_csv("/opt/airflow/data/masked_transactions.csv")

        checks = {
            "no_null_transaction_ids": df["transaction_id"].isna().sum() == 0,
            "no_negative_amounts":     (df["amount"] <= 0).sum() == 0,
            "pii_masked_accounts":     df["account_number"].str.startswith("ACC_").all(),
            "pii_masked_names":        df["user_name"].str.startswith("USER_").all(),
            "fraud_rate_reasonable":   0 < df["is_fraud"].mean() < 0.5,
        }

        failed = [k for k, v in checks.items() if not v]
        if failed:
            raise ValueError(f"❌ Data quality checks FAILED: {failed}")

        print("✅ All data quality checks passed:")
        for check, result in checks.items():
            print(f"   {'✅' if result else '❌'} {check}")

    validate_quality_task = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_quality,
    )

    # ── Task 5: Notify Completion ─────────────────────────────────────────────
    def notify(**context):
        masked_count = context["ti"].xcom_pull(task_ids="apply_pii_masking")
        print(f"""
        ╔══════════════════════════════════════════╗
        ║     FRAUD PIPELINE COMPLETE ✅            ║
        ║  Records processed : {masked_count:<6}             ║
        ║  Run time          : {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}         ║
        ╚══════════════════════════════════════════╝
        """)

    notify_task = PythonOperator(
        task_id="notify_completion",
        python_callable=notify,
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    generate_transactions >> apply_masking_task >> run_spark_etl >> validate_quality_task >> notify_task
