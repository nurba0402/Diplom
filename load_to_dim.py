from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
import logging

# Подключение к базе
def get_engine():
    return create_engine("postgresql+psycopg2://postgres:123@host.docker.internal:5437/postgres")

# Создание таблиц
def create_dimension_tables():
    engine = get_engine()
    ddl = """
    drop schema if exists dimensions cascade;

    create schema if not exists dimensions;

    set search_path to dimensions;

    create table if not exists dim_product (
        product_id serial primary key,
        product_name varchar(100),
        unit_price numeric(10, 2),
        unique(product_name, unit_price)
    );

    create table if not exists dim_customer (
        customer_id serial primary key,
        customer_type varchar(255),
        gender varchar(10),
        unique(customer_type, gender)
    );

    create table if not exists dim_branch (
        branch_id serial primary key,
        branch_code char(5),
        city varchar(10),
        unique(branch_code, city)
    );

    create table if not exists dim_date (
        date_id serial primary key,
        date date,
        day int,
        month int,
        year int,
        weekday varchar(10),
        time time
    );

    create table if not exists fact_sales (
        invoice_id varchar(50) primary key,
        product_id int references dim_product(product_id),
        customer_id int references dim_customer(customer_id),
        branch_id int references dim_branch(branch_id),
        date_id int references dim_date(date_id),
        payment_method varchar(50),
        cogs numeric(10, 2),
        tax_5_percent numeric(10, 2),
        total numeric(10, 2) not null,
        gross_margin_percentage numeric(10, 2),
        gross_income numeric(10, 2),
        rating numeric(3, 1)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logging.info("✅ Таблицы успешно созданы в схеме dimensions.")

# Универсальная функция загрузки
def load_data(table_name, delete_sql, insert_sql):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(delete_sql))
        conn.execute(text(insert_sql))
    logging.info(f"✅ Данные в {table_name} успешно загружены.")

# Загрузки
def load_dim_customer():
    load_data(
        "dim_customer",
        "delete from dimensions.dim_customer",
        """
        insert into dimensions.dim_customer (customer_type, gender)
        select distinct customer_type, gender
        from normalized_data.customer
        """
    )

def load_dim_product():
    load_data(
        "dim_product",
        "delete from dimensions.dim_product",
        """
        insert into dimensions.dim_product (product_name, unit_price)
        select distinct product_line, unit_price
        from normalized_data.products
        """
    )

def load_dim_branch():
    load_data(
        "dim_branch",
        "delete from dimensions.dim_branch",
        """
        insert into dimensions.dim_branch (branch_code, city)
        select distinct branch, city
        from normalized_data.branches
        """
    )

def load_dim_date():
    load_data(
        "dim_date",
        "delete from dimensions.dim_date",
        """
        insert into dimensions.dim_date (date, day, month, year, weekday, time)
        select distinct full_date, day, month, year, weekday, time
        from normalized_data.dates
        """
    )

def load_fact_sales():
    load_data(
        "fact_sales",
        "delete from dimensions.fact_sales",
        """
        insert into dimensions.fact_sales (
            invoice_id,
            product_id, customer_id, branch_id, date_id,
            payment_method, cogs, tax_5_percent, total,
            gross_margin_percentage, gross_income, rating
        )
        select distinct
            invoice_id,
            p.product_id,
            c.customer_id,
            br.branch_id,
            d.date_id,
            s.payment_method,
            s.cogs,
            s.tax_5_percent,
            s.total,
            s.gross_margin_percentage,
            s.gross_income,
            s.rating
        from normalized_data.sales s
        join normalized_data.products pl on s.product_id = pl.product_id
        join dimensions.dim_product p on p.product_name = pl.product_line and p.unit_price = pl.unit_price
        join normalized_data.customer cst on s.customer_id = cst.customer_id
        join dimensions.dim_customer c on c.customer_type = cst.customer_type and c.gender = cst.gender
        join normalized_data.branches br on s.branch_id = br.branch_id
        join dimensions.dim_branch b on b.branch_code = br.branch and b.city = br.city
        join normalized_data.dates nd on s.date_id = nd.date_id
        join dimensions.dim_date d on d.date = nd.full_date and d.time = nd.time
        """
    )

# DAG
with DAG(
    dag_id="dimension_data_dag",
    start_date=datetime(2023, 10, 1),
    schedule_interval=None,
    catchup=False,
    description="ETL из нормализованной схемы в витрину данных"
) as dag:

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_dimension_tables
    )

    load_product = PythonOperator(
        task_id="load_dim_product",
        python_callable=load_dim_product
    )

    load_customer = PythonOperator(
        task_id="load_dim_customer",
        python_callable=load_dim_customer
    )

    load_branch = PythonOperator(
        task_id="load_dim_branch",
        python_callable=load_dim_branch
    )

    load_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date
    )

    load_sales = PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_sales
    )

    create_tables >> [load_product, load_customer, load_branch, load_date] >> load_sales
