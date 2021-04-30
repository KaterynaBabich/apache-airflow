from time import sleep

from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class EmrRunStepOperator(EmrAddStepsOperator):
    """
     An operator that adds one step to an existing EMR job_flow and then monitors it's execution.

     :param job_flow_id: id of the JobFlow to add step to (templated)
     :type job_flow_id: str
     :param aws_conn_id: aws connection to use
     :type aws_conn_id: str
     :param step: EMR step description (templated)
     :type step: dict
     :param poke_interval: interval to check EMR step status
     :type poke_interval: int
     :param timeout: timeout when to stop poking the step
     :type timeout: int
     """

    template_fields = ['job_flow_id', 'step']

    NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE', 'CANCEL_PENDING']
    FAILED_STATE = ['CANCELLED', 'FAILED', 'INTERRUPTED']
    POKE_INTERVAL = 15 # 15 seconds
    TIMEOUT = 60 * 60 * 5  # 5 hours

    @apply_defaults
    def __init__(
            self,
            job_flow_id: str,
            aws_conn_id: str = 'aws_default',
            step: dict = None,
            poke_interval: int = POKE_INTERVAL,
            timeout: int = TIMEOUT,
            *args, **kwargs):
        super(EmrAddStepsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.job_flow_id = job_flow_id
        if step:
            self.step = step
        else:
            raise AirflowException('EMR step has to be provided')
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.emr = None
        self.step_id = None

    def add_emr_step(self):
        self.log.info(f'Adding step to cluster {self.job_flow_id}')
        response = self.emr.add_job_flow_steps(JobFlowId=self.job_flow_id, Steps=self.step)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('Adding step failed: %s' % response)

        self.log.info(f'Step {response["StepIds"]} added to JobFlow')

        return response['StepIds'][0]

    def poke_emr_step(self):
        self.log.info(f'Poking step {self.step_id} on EMR cluster {self.job_flow_id}')
        response = self.emr.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info(f'Bad HTTP response: {response}')
            return False

        state = response['Step']['Status']['State']
        self.log.info(f'Step currently is in {state} state')

        if state in self.NON_TERMINAL_STATES:
            return True

        if state in self.FAILED_STATE:
            failure_message = self.failure_message_from_response(response)
            raise AirflowException('EMR step failed ' + failure_message)

        return False

    @staticmethod
    def failure_message_from_response(response):
        failure_details = response['Step']['Status'].get('FailureDetails')
        if failure_details:
            return f'for reason {failure_details.get("Reason")} ' \
                   f'with message {failure_details.get("Message")} ' \
                   f'and log file {failure_details.get("LogFile")}'
        return ''

    def execute(self, context):
        self.emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        # Add step to EMR cluster
        self.step_id = self.add_emr_step()

        # Monitor EMR step
        self.log.info(f'Start watching step [{self.step_id}]')
        started_at = timezone.utcnow()

        while self.poke_emr_step():
            if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                raise AirflowSensorTimeout('Snap. Time is OUT.')
            self.log.info(f'Sleeping for {self.poke_interval} seconds...')
            sleep(self.poke_interval)

        self.log.info(f"Step [{self.step_id}] completed successfully. Exiting...")
