from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.emr_plugin import EMRRunStepOperator, EMRCheckAndTerminateClusterOperator

DEFAULT_ARGS = {
    'owner': 'Airflow'
}

with DAG(
        dag_id='emr_plugin_example',
        default_args=DEFAULT_ARGS,
        start_date=days_ago(1),
        schedule_interval='@once'
) as dag:
    pass