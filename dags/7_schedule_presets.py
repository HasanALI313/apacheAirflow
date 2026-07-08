from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="schedule_presets_dag",
    start_date=datetime(2026, 7, 7, tz="UTC"),
    schedule="@daily",
    is_paused_upon_creation=False
)
def first_schedule_presets_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


dag = first_schedule_presets_dag()