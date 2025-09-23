from airflow import DAG
from datetime import datetime
from airflow.models import BaseOperator

import random


class DummyOperator(BaseOperator):
    ui_color = '#e8f7e4'

    # inherits_from_dummy_operator = True

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        random_number = random.randint(0, 9)
        context['task_instance'].xcom_push(key='key', value=random_number)


dag = DAG('AirFlow_6_2', schedule='@daily', start_date=datetime(2024, 1, 1))
t1 = DummyOperator(task_id='task_1', dag=dag)
t2 = DummyOperator(task_id='task_2', dag=dag)

t1 >> t2
