from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="decider_task_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def extract_data():
        # Automatically pushed to XCom
        return {"score": 85}

    def decider(**kwargs):
        ti = kwargs["ti"]

        data = ti.xcom_pull(task_ids="extract_data")
        score = data["score"]

        if score >= 50:
            return "pass_task"
        else:
            return "fail_task"

    decide = BranchPythonOperator(
        task_id="decider_task",
        python_callable=decider,
    )

    @task
    def pass_task():
        print("Student Passed!")

    @task
    def fail_task():
        print("Student Failed!")

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    extract = extract_data()
    passed = pass_task()
    failed = fail_task()

    extract >> decide
    decide >> passed >> end
    decide >> failed >> end