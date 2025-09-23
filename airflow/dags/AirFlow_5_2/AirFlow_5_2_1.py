from airflow import DAG
from airflow.sensors.python import PythonSensor
from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}


def true():
    return False


dag = DAG('AirFlow_5_2_1', schedule_interval='@daily', default_args=default_args)

false_op_1 = PythonSensor(
    task_id='reschedule',
    poke_interval=5,
    timeout=10,
    mode="reschedule",
    python_callable=true,
    soft_fail=True,
    dag=dag
)

false_op_2 = PythonSensor(
    task_id='poke',
    poke_interval=5,
    timeout=10,
    mode="poke",
    python_callable=true,
    dag=dag
)
