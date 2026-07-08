from airflow.sdk import dag, task


@dag(
    dag_id="xcom_parallel_dag",
    schedule=None,
    catchup=False,
)
def xcom_parallel_dag():

    @task.python
    def first_task(**kwargs):
        ti = kwargs["ti"]

        print("Extracting data... This is the first task")

        fetched_data = {"data": [1, 2, 3, 4, 5]}

        ti.xcom_push(
            key="return_result",
            value=fetched_data
        )

    @task.python
    def second_task(**kwargs):
        ti = kwargs["ti"]

        print("Transforming data... This is the second task")

        fetched_data = ti.xcom_pull(
            task_ids="first_task",
            key="return_result"
        )

        transformed_data = [x * 2 for x in fetched_data["data"]]

        transformed_data_dict = {
            "trans_data": transformed_data
        }

        ti.xcom_push(
            key="transformed_result",
            value=transformed_data_dict
        )

    @task.python
    def second_parallel_task(**kwargs):
        ti = kwargs["ti"]

        print("Parallel data... This is the second task")

        fetched_data = ti.xcom_pull(
            task_ids="first_task",
            key="return_result"
        )

        transformed_data = [x * 6 for x in fetched_data["data"]]

        transformed_data_dict = {
            "trans_data": transformed_data
        }

        ti.xcom_push(
            key="transformed_result",
            value=transformed_data_dict
        )

    @task.python
    def third_task(**kwargs):
        ti = kwargs["ti"]

        print("Loading data... This is the third task")

        transformed_data = ti.xcom_pull(
            task_ids="second_task",
            key="transformed_result"
        )

        print("Loaded Data:", transformed_data)


    first = first_task()
    second = second_task()
    second_parallel = second_parallel_task()
    third = third_task()

    first >> [second, second_parallel] >> third


dag = xcom_parallel_dag()
