from airflow.plugins_manager import AirflowPlugin
from exasol_plugin.hooks.exasol_hook import ExasolHook
from exasol_plugin.sensors.exasol_sensor import ExasolSensor
from exasol_plugin.operators.exasol_operator import ExasolOperator


class ExasolPlugin(AirflowPlugin):

    name = 'exasol_plugin'

    hooks = [ExasolHook]
    sensors = [ExasolSensor]
    operators = [ExasolOperator]
