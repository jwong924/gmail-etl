from resources.gmail_functions import extract, transform_load_raw
import datetime
from airflow.models.dag import DAG

with DAG('gmail_etl_dag', schedule_interval='@daily', start_date=datetime(2022,12,1),catchup=False) as dag:
    
    raw_emails = extract()

    format_emails = transform_load_raw

    raw_emails >> format_emails