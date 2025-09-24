from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime
import numpy as np


class CustomSensor(BaseSensorOperator):

    def poke(self, context):
        return_value = np.random.binomial(1, 0.3)
        return bool(return_value)


dag_default_args = {
    'start_date': datetime(2021, 1, 1)
}
# Здесь и далее код создание задачи-сенсора

# dag = DAG(...)
dag = DAG('AirFlow_5_4_1', schedule_interval='@daily', default_args=dag_default_args)

sensor_default_args = {
    "poke_interval": 4,
    "timeout": 50,
    "mode": "reschedule",
    "soft_fail": True
}
for sensor_number in range(0, 3):
    sensor_id = f'sensor_{sensor_number}'

    globals()[sensor_id] = CustomSensor(dag=dag, task_id=sensor_id, default_args=sensor_default_args)
