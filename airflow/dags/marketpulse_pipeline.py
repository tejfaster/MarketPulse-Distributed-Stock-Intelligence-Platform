from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ── constants ────────────────────────────────────────────────
MAC_HOST = "tejfaster@172.23.181.20"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
PROJECT = "/Users/tejfaster/Developer/Python/MarketPulse"
DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"
PG_JAR = "/opt/spark/jars/postgresql-42.7.3.jar"
SPARK_MASTER = "spark://172.23.181.20:7077"

SSH = f"ssh -o StrictHostKeyChecking=no {MAC_HOST}"

# ── default args ─────────────────────────────────────────────
default_args = {
    "owner": "tejfaster",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── DAG definition ───────────────────────────────────────────
with DAG(
    dag_id="marketpulse_silver_gold",
    description="Daily Silver → Gold pipeline after market close",
    schedule_interval="0 17 * * 1-5",   # 17:00 Mon–Fri (UTC)
    start_date=datetime(2026, 3, 17),
    catchup=False,
    default_args=default_args,
    tags=["marketpulse", "spark", "delta"],
) as dag:

    run_silver = BashOperator(
        task_id="run_silver",
        bash_command=(
            f"{SSH} "
            f"'source /opt/anaconda3/etc/profile.d/conda.sh && "
            f"conda activate base && "
            f"{SPARK_SUBMIT} "
            f"--packages {DELTA_PKG} "
            f"{PROJECT}/pipelines/silver/bronze_to_silver.py'"
        ),
        execution_timeout=timedelta(hours=1),
    )

    run_gold = BashOperator(
        task_id="run_gold",
        bash_command=(
            f"{SSH} "
            f"'source /opt/anaconda3/etc/profile.d/conda.sh && "
            f"conda activate base && "
            f"{SPARK_SUBMIT} "
            f"--master {SPARK_MASTER} "
            f"--jars {PG_JAR} "
            f"--packages {DELTA_PKG} "
            f"{PROJECT}/pipelines/gold/silver_to_gold.py'"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # Silver must succeed before Gold starts
    run_silver >> run_gold
