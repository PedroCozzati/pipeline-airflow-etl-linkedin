from datetime import datetime
from airflow import DAG
import pandas
import fastapi
import fastapi
from airflow.operators.python import PythonOperator

app = fastapi.FastAPI()
dag = DAG(
    "Extraçao_de_dados_do_linkedin",
    description="Processo ETL de extração de dados do linkedin",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 3, 4),
    catchup=False,
)

def executar_tarefa():
    # Aqui você pode acessar a instância do FastAPI 'app' e fazer o que precisar
    print("Executando a tarefa...")
    print("Acesso à instância do FastAPI 'app':")


with open("/opt/airflow/dags/req.txt") as f:
    requisitos = f.read().splitlines()
    
# def executar_comando():
#     comando = "../scrap-proj/uvicorn src.app:app --reload"
#     subprocess.run(comando, shell=True)

task1 = PythonOperator(
    task_id="Extraçao_dos_dados",
    dag=dag,
    python_callable=executar_tarefa,
    # requirements=requisitos+['fastapi'],
    # system_site_packages=False,
    # provide_context=False,

)
