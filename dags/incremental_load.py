from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="incremental_dag",
    start_date=datetime(2026, 7, 7, tz="UTC"),
    end_date=datetime(2026, 7, 12, tz="UTC"),
    schedule="30 * * * *",  # Every minute
    catchup=True,
    is_paused_upon_creation=False,
)
def incremental_dag():

    @task
    def incremental_data_fetch(**kwargs):
        data_interval_start = kwargs["data_interval_start"]
        data_interval_end = kwargs["data_interval_end"]

        print(
            f"Fetching incremental data..."
            f"\nStart: {data_interval_start}"
            f"\nEnd: {data_interval_end}"
        )

    @task.bash
    def incremental_data_process():
        return (
            "echo 'Processing incremental data "
            "from {{ data_interval_start }} "
            "to {{ data_interval_end }}'"
        )
    
    fetch_task = incremental_data_fetch()
    process_task = incremental_data_process()


    fetch_task >> process_task


dag = incremental_dag()