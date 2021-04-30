from time import sleep
from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.utils import timezone


class EMRCheckAndTerminateJobFlowOperator(BaseOperator):
    """
    Operator check if any active step is running and terminate EMR JobFlows if not.

    :param job_flow_id: id of the JobFlow to terminate. (templated)
    :type job_flow_id: str
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    """
    template_fields = ['job_flow_id']
    template_ext = ()
    ui_color = '#f9c915'

    TIMEOUT = 5 * 60 * 60
    POKE_INTERVAL = 5 * 60
    MAX_INACTIVE_TIME = 5 * 60

    @apply_defaults
    def __init__(
            self,
            job_flow_id: str = None,
            check_for_active_steps: bool = True,
            aws_conn_id: str = 'aws_default',
            timeout: int = TIMEOUT,
            poke_interval: int = POKE_INTERVAL,
            max_inactive_time: int = MAX_INACTIVE_TIME,
            *args, **kwargs):
        super(EMRCheckAndTerminateJobFlowOperator, self).__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id
        self.check_for_active_steps = check_for_active_steps
        self.aws_conn_id = aws_conn_id
        self.timeout = timeout
        self.poke_interval = poke_interval
        self.max_inactive_time = max_inactive_time
        self.emr = None

    @staticmethod
    def get_inactive_time(steps):
        last_step_end_time = max([s["Status"]["Timeline"]["EndDateTime"] for s in steps])
        print(f'The latest step at cluster was finished at {last_step_end_time.strftime("%Y-%m-%d %H:%M:%S")} UTC')

        cluster_inactive_time = (datetime.now(timezone.utc) - last_step_end_time).seconds
        print(f'Time passed from latest finished step: {cluster_inactive_time} seconds')
        return cluster_inactive_time

    def get_cluster_steps(self):
        # Get EMR cluster steps
        response = self.emr.list_steps(ClusterId=self.job_flow_id)
        marker = response.get("Marker")
        steps = response.get("Steps")
        while marker:
            response = self.emr.list_steps(ClusterId=self.job_flow_id, Marker=marker)
            marker = response.get("Marker")
            steps.extend(response.get("Steps"))
        return steps

    def poke_job_flow(self):
        steps = self.get_cluster_steps()
        if steps:
            states = [s["Status"]["State"] for s in steps]

            # Check for running step
            if 'RUNNING' in states or 'PENDING' in states:
                self.log.info(f"There is an active step on cluster {self.job_flow_id}")
                return True
            # Check if MAX_INACTIVE_TIME reached
            else:
                inactive_time = self.get_inactive_time(steps)
                if inactive_time >= self.max_inactive_time:
                    self.log.info(f'MAX_INACTIVE_TIME is reached. Marking cluster to terminate')
                    return False
                else:
                    self.log.info(f'MAX_INACTIVE_TIME is not reached')
                    return True
        return False

    def execute(self, context):
        self.emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()
        started_at = timezone.utcnow()

        # Wait for all active steps to complete
        while self.poke_job_flow():
            if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                raise AirflowSensorTimeout('Snap. Time is OUT.')
            self.log.info(f'Sleeping for {self.poke_interval} seconds...')
            sleep(self.poke_interval)

        # Terminate cluster
        self.log.info('Terminating JobFlow %s', self.job_flow_id)
        response = self.emr.terminate_job_flows(JobFlowIds=[self.job_flow_id])

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('JobFlow termination failed: %s' % response)
        else:
            self.log.info('JobFlow with id %s terminated', self.job_flow_id)
