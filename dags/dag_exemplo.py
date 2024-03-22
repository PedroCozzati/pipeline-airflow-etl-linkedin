from datetime import datetime, timedelta
from airflow import DAG
from cloud_con.postgres_cloud_con import export_data_aiven_cloud_postgres
from github_con.push_to_github import add_or_update_in_git
from scrap.main import web_scrap_linkedin
from etl.extract import get_all_source
from etl.load import load_data_analitico
from etl.load import load_data_transacional
from etl.transform import transform_data
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()

dag = DAG(
    "Extraçao_de_dados_do_linkedin",
    description="Processo ETL de extração de dados do linkedin",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 3, 4),
    catchup=False,
)

task1 = PythonOperator(
    task_id="Extraçao_dos_dados",
    dag=dag,
    python_callable=web_scrap_linkedin,
    retries= 3, 
    retry_delay= timedelta(minutes=1),
)

task2 = PythonOperator(
    task_id="Carregamento_dos_dados_RAW",
    dag=dag,
    python_callable=get_all_source,
)

task3 = PythonOperator(
    task_id="Transformaçao_dos_dados",
    dag=dag,
    python_callable=transform_data,
)

task4 = PythonOperator(
    task_id="Carregamento_dos_dados_transacional",
    dag=dag,
    python_callable=load_data_transacional,
)

task5 = PythonOperator(
    task_id="Carregamento_dos_dados_analitico",
    dag=dag,
    python_callable=load_data_analitico,
)

task6 = PythonOperator(
    task_id="Exportar_para_CLOUD_POSTGRESQL",
    dag=dag,
    python_callable=export_data_aiven_cloud_postgres,
)

task7 = PythonOperator(
    task_id="Enviar_dados_analiticos_streamlit",
    dag=dag,
    python_callable=add_or_update_in_git,
)

task1>>task2>>task3>>[task4,task5] >> task6>>task7

