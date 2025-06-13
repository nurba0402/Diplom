from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

def run_quality_checks():
    engine = create_engine("postgresql+psycopg2://postgres:123@host.docker.internal:5437/postgres")
    create_table = """
        set search_path to normalized_data;
        CREATE TABLE IF NOT EXISTS quality_check (
            check_id serial primary key,
            check_name varchar(100),
            check_description text,
            check_result varchar(20),
            row_count int,
            check_timestamp timestamp default current_timestamp
        );
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table))

        def insert_check(name, description, result, row_count):
            insert = """
                INSERT INTO normalized_data.quality_check (
                    check_name, check_description, check_result, row_count
                )
                VALUES (:check_name, :check_description, :check_result, :row_count)
            """
            conn.execute(text(insert), {
                "check_name": name,
                "check_description": description,
                "check_result": result,
                "row_count": row_count
            })

        # Check 1: NULL invoice_id
        nulls = conn.execute(text("select count(*) from normalized_data.sales where invoice_id is NULL")).scalar()
        insert_check(
            name="Check NULL invoice_id",
            description="Проверка на NULL в invoice_id",
            result="Fail" if nulls > 0 else "Pass",
            row_count=nulls
        )

        # Check 2: Дубликаты invoice_id
        duplicates = conn.execute(text("""
            select count(*) from (
                select invoice_id from normalized_data.sales
                group by invoice_id
                having count(*) > 1
            ) as dups
        """)).scalar()
        insert_check(
            name="Check duplicate invoice_id",
            description="Проверка на дубликаты в invoice_id",
            result="Fail" if duplicates > 0 else "Pass",
            row_count=duplicates
        )

        # Check 3: Отрицательные total
        negative_totals = conn.execute(text("select count(*) from normalized_data.sales where total < 0")).scalar()
        insert_check(
            name="Check negative total",
            description="Проверка на отрицательные значения в total",
            result="Fail" if negative_totals > 0 else "Pass",
            row_count=negative_totals
        )

with DAG(
    dag_id="quality_check_dag",
    default_args={'owner': 'airflow', 'retries': 1},
    description="DAG для проверок качества данных в нормализованной схеме",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality_check"]
) as dag:
    
    quality_check_task = PythonOperator(
        task_id="quality_check_task",
        python_callable=run_quality_checks
    )

    quality_check_task
