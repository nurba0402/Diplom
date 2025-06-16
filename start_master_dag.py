from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='start_master_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    load_nds = TriggerDagRunOperator(
        task_id = 'load_to_normalirized_data_store',
        trigger_dag_id = 'load_to_normalized_data')
    
    load_quality = TriggerDagRunOperator(
        task_id = 'load_quality_checks',
        trigger_dag_id = 'quality_check_dag')

    load_dds = TriggerDagRunOperator(
        task_id = 'load_to_dimensional_data_store',
        trigger_dag_id = 'dimension_data_dag')

    load_nds >> load_quality >> load_dds