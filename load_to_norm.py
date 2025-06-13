from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

def create_load_data():
    engine = create_engine("postgresql+psycopg2://postgres:123@host.docker.internal:5437/postgres")

    ddl = """
    drop schema if exists normalized_data cascade;

    create schema if not exists normalized_data;
    
    set search_path to normalized_data;

    create table if not exists normalized_data.customer (
        customer_id serial primary key,
        customer_type varchar(20),
        gender varchar(10),
        unique(customer_type, gender)
    );

    create table if not exists normalized_data.products (
        product_id serial primary key,
        product_line varchar(100),
        unit_price numeric(10, 2),
        unique(product_line, unit_price)
    );

    create table if not exists normalized_data.branches (
        branch_id serial primary key,
        branch varchar(5),
        city varchar(50),
        unique(branch, city)
    );

    create table if not exists normalized_data.dates (
        date_id serial primary key,
        full_date date,
        day int,
        month int,
        year int,
        weekday varchar(10),
        time time
    );

    create table if not exists normalized_data.sales (
        invoice_id varchar(50) primary key,
        customer_id int not null, 
        product_id int not null,
        branch_id int not null,
        date_id int not null,
        payment_method varchar(50),
        cogs numeric,
        tax_5_percent numeric,
        total numeric,
        gross_margin_percentage numeric,
        gross_income numeric,
        rating numeric,
        CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
        CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(product_id),
        CONSTRAINT fk_branch FOREIGN KEY (branch_id) REFERENCES branches(branch_id),
        CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dates(date_id)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("✅ Таблицы успешно созданы в нормализованной схеме PostgreSQL.")


def load_data_to_normalized():
    engine = create_engine("postgresql+psycopg2://postgres:123@host.docker.internal:5437/postgres")
    data = pd.read_csv('/opt/airflow/dags/files/sales.csv')

    # Переименование столбцов
    data.columns = [
        'invoice_id', 'branch', 'city', 'customer_type', 'gender', 'product_line',
        'unit_price', 'quantity', 'tax_5_percent', 'total', 'date', 'time',
        'payment_method', 'cogs', 'gross_margin_percentage', 'gross_income', 'rating'
    ] 

    # Удаление строк с пропусками и дубликатами
    data = data.dropna().drop_duplicates()

    # Приведение типов
    data['date'] = pd.to_datetime(data['date'], format='%m/%d/%Y')
    data['time'] = pd.to_datetime(data['time'], format='%H:%M').dt.time

    # Customer
    customer_df = data[['customer_type', 'gender']].drop_duplicates().reset_index(drop=True)
    customer_df.to_sql('customer', engine, schema='normalized_data', if_exists='append', index=False)

    # Product
    product_df = data[['product_line', 'unit_price']].drop_duplicates().reset_index(drop=True)
    product_df.to_sql('products', engine, schema='normalized_data', if_exists='append', index=False)

    # Branches
    branch_df = data[['branch', 'city']].drop_duplicates().reset_index(drop=True)
    branch_df.to_sql('branches', engine, schema='normalized_data', if_exists='append', index=False)

    # Dates
    dates_df = data[['date', 'time']].drop_duplicates()
    dates_df.rename(columns={'date': 'full_date'}, inplace=True)
    dates_df['day'] = dates_df['full_date'].dt.day
    dates_df['month'] = dates_df['full_date'].dt.month
    dates_df['year'] = dates_df['full_date'].dt.year
    dates_df['weekday'] = dates_df['full_date'].dt.day_name()
    dates_df.to_sql('dates', engine, schema='normalized_data', if_exists='append', index=False)

    # Считываем таблицы обратно, чтобы получить surrogate keys
    customer_df = pd.read_sql("SELECT * FROM normalized_data.customer", engine)
    product_df = pd.read_sql("SELECT * FROM normalized_data.products", engine)
    branch_df = pd.read_sql("SELECT * FROM normalized_data.branches", engine)
    dates_df = pd.read_sql("SELECT * FROM normalized_data.dates", engine)

    # Приводим даты снова
    data['date'] = pd.to_datetime(data['date'])
    dates_df['full_date'] = pd.to_datetime(dates_df['full_date'])

    # Merge ключей
    data = data.merge(customer_df, on=['customer_type', 'gender'], how='left')
    data = data.merge(product_df, on=['product_line', 'unit_price'], how='left')
    data = data.merge(branch_df, on=['branch', 'city'], how='left')
    data = data.merge(dates_df, left_on=['date', 'time'], right_on=['full_date', 'time'], how='left')

    # Подготовка sales
    sales_df = data[[
        'invoice_id', 'customer_id', 'product_id', 'branch_id', 'date_id',
        'payment_method', 'cogs', 'tax_5_percent', 'total',
        'gross_margin_percentage', 'gross_income', 'rating'
    ]].drop_duplicates()

    sales_df.to_sql('sales', engine, schema='normalized_data', if_exists='append', index=False)

    print("✅ Данные успешно загружены в нормализованную схему PostgreSQL.")

with DAG(
    'load_to_normalized_data',
    default_args={'owner': 'airflow', 'retries': 1},
    description='DAG для создания и загрузки данных в нормализованную схему',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
              
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_load_data
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_normalized
    )

    create_tables_task >> load_data_task
