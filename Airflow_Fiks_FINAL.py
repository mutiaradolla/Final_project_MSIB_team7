from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator


# Default arguments untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Mendefinisikan DAG
with DAG(
    'coba_read_hasil_keuntungan_postgres_to_olap',
    default_args=default_args,
    description='Read tabel keuntungan from PostgreSQL and write to OLAP',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    # dummy start
    start = DummyOperator(
        task_id='start',
        dag=dag
    )
    # Task untuk membaca data hasil_keuntungan dan menulis ke database OLAP
    read_write_data_task = BashOperator(
        task_id = 'staging',
        bash_command = 'python3 /home/mutiara/spark_script/spark_Fiks_FINAL.py',
        dag = dag
    )
    # dummy end
    end = DummyOperator(
        task_id = 'end',
        dag = dag
    )

# Menentukan urutan tugas
start >> read_write_data_task >> end