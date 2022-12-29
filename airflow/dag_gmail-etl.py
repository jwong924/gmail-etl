import etl_gmail
import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
#from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(dag_id='gmail_etl_dag', schedule='@daily', start_date=datetime(2022,12,1),catchup=False) as dag:
    with TaskGroup("extract_gmail_write", tooltip="Extract raw gmail data") as extract_load_raw:
        raw_emails = etl_gmail.extract()
        load_raw_emails = etl_gmail.write_raw(raw_emails)

        raw_emails >> load_raw_emails
    
    with TaskGroup("transform_raw") as format_load_raw:
        format_emails = etl_gmail.transform_raw(raw_emails)
        load_format_emails = etl_gmail.write_stage_1(format_emails)

        format_emails >> load_format_emails

    extract_load_raw >> format_load_raw