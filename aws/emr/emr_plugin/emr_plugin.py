from airflow.plugins_manager import AirflowPlugin
from emr_plugin.operators.emr_run_step_operator import EMRRunStepOperator
from emr_plugin.operators.emr_check_and_terminate_cluster_operator import EMRCheckAndTerminateClusterOperator

class EMRPlugin(AirflowPlugin):

    name = 'emr_plugin'

    operators = [EMRRunStepOperator, EMRCheckAndTerminateClusterOperator]
