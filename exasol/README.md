# Exasol plugin for AWS Managed Airflow (MWAA)
(Airflow 1.10.12)

## Requirements
1. Create Exasol connection in Airflow UI with a connection type of MySQL (as we don't have Exasol by default)
2. Clone repository
3. Create zip of plugin code:

  * Change directories to your `$AIRFLOW_HOME/plugins` folder:
    `myproject$ cd $AIRFLOW_HOME/plugins`
  * Run the following command to ensure that the contents have executable permissions (macOS and Linux only).
    `$AIRFLOW_HOME/plugins$ chmod -R 755 .`
  * Zip the contents within your `$AIRFLOW_HOME/plugins` folder.
    `$AIRFLOW_HOME/plugins$ zip -r plugins.zip .`

4. Upload zip to s3 bucket and restart MWAA with picking up the version of just uploaded plugin.

## Usage example 
```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.exasol_plugin import ExasolSensor
from airflow.operators.exasol_plugin import ExasolOperator

DEFAULT_ARGS = {
    'owner': 'Airflow'
}

with DAG(
        dag_id='exasol_plugin_example',
        default_args=DEFAULT_ARGS,
        start_date=days_ago(1),
        schedule_interval='@once'
) as dag:

    # Set path to your SQL files which will be executed by operator
    project_path = '/usr/local/airflow/dags/projects/exasol_plugin_template'

    ###########################
    # ExasolOperator Examples #
    ###########################

    # Get SQL from file in your project
    with open(f'{project_path}/query_example.sql', 'r') as f:
        sql = f.read()

    # Run operator to execute SQL on Exasol with exasol_default connection
    run_sql1 = ExasolOperator(
        task_id='sql_example1',
        exasol_conn_id='exasol_default',
        autocommit=True,
        sql=sql,
        dag=dag
    )

    # Run operator to execute SQL statement on Exasol with exasol_default connection
    run_sql2 = ExasolOperator(
        task_id='sql_example2',
        exasol_conn_id='exasol_default',
        autocommit=True,
        sql='select 1',
        dag=dag
    )

    #########################
    # ExasolSensor Examples #
    #########################

    # Set SQL sensor query
    # If query returns 0, '0' or None then we assume criteria in not met and wait <poke_interval> seconds till <timeout>
    # otherwise assume criteria is met stop sensor and continue DAG execution
    # Default values:
    # poke_interval=60
    # timeout=60 * 60 * 24 * 7

    sensor_query = '''
    SELECT count(*) from test_schema.test_table
    '''

    # Define sensor task with default poke interval and timeout
    run_sensor1 = ExasolSensor(
        task_id='sensor1',
        conn_id='exasol_default',
        sql=sensor_query,
        dag=dag
    )

    # Define sensor task with custom poke interval and timeout and query from file
    with open(f'{project_path}/sensor_query_example.sql', 'r') as f:
        sql = f.read()

    run_sensor2 = ExasolSensor(
        task_id='sensor2',
        conn_id='exasol_default',
        sql=sql,
        poke_interval=60,
        timeout=60*5,
        dag=dag
    )

    (
        run_sensor1 >>
        run_sql1 >>
        run_sql2 >>
        run_sensor2
    )
```
