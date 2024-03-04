from collections import Counter
from datetime import datetime
import os
import re
import subprocess
from airflow import DAG
import pandas as pd
import sqlite3
from sqlalchemy import create_engine

# import fastapi
from airflow.operators.python import PythonOperator


dag = DAG(
    "Extraçao_de_dados_do_linkedin",
    description="Processo ETL de extração de dados do linkedin",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 3, 4),
    catchup=False,
)


def extract_and_save_data():
    source_file = "source/database6.db"
    destination_file = "results/destino.csv"

    current_directory = os.path.dirname(__file__)

    source_path = os.path.join(current_directory, source_file)
    destination_path = os.path.join(current_directory, destination_file)

    conn = sqlite3.connect(source_path)

    query = "SELECT * FROM vaga;"

    dados = pd.read_sql_query(query, conn)
    conn.close()

    dados.to_csv(destination_path, index=False)


def transform_data():
    source_file = "results/destino.csv"

    current_directory = os.path.dirname(__file__)

    source_path = os.path.join(current_directory, source_file)

    dados = pd.read_csv(source_path)
    dados = dados.reset_index(drop=True)
    dados.set_index("id", inplace=True)
    dados_tec = dados[
        dados["title"].str.contains(
            "QA|Implantação|Programação|Desenvolvedor|Programador|Developer|Analista|Desenvolvimento|Engenheiro|Software|Estágio|Tecnologicas|Tecnologia|Computação|Tech|stack|Dev|Data|Desenvolver|TI",
            case=False,
            regex=True,
        )
    ]
    dados_tec.drop_duplicates(inplace=True)
    dados_tec[dados_tec.duplicated()]
    dados_tec.drop_duplicates(subset=["job_id"], inplace=True)

    dados_tec.applications.fillna("Hidden", inplace=True)
    dados_tec.loc[
        dados["applications"] == "Seja um dos 25 primeiros a se candidatar",
        "applications",
    ] = "<=25"
    dados_tec.loc[
        dados["applications"] == "Mais de 200 candidaturas", "applications"
    ] = ">25"

    dados_tec.experience_level.fillna("Assistente", inplace=True)

    dados_tec.job_type.fillna("Tempo integral", inplace=True)

    dados_tec.role.fillna("Engenharia e Tecnologia da informação", inplace=True)
    dados_tec.sectors.fillna(
        "Atividades dos serviços de tecnologia da informação", inplace=True
    )

    dados_tec.description.fillna("Sem descrição", inplace=True)

    dados_nao_nulos = dados_tec
    dados_nao_nulos.isnull().sum()

    regex_str = r"Excel|Windows|Linux|\\bC\\b|\\bC\+\+\\b|\bGIT|ASSEMBLY|GRAPHQL|DELPHI|PL/SQL|Node\.?JS|CSS|HTML|Wordpress|Angular(?:JS)?|Airflow|NOSQL|Spark|Power BI|Salesforce|DotNet|PASCAL|COBOL|ABAP|JAVASCRIPT|JAVA|SQL|Python|Angular|\.NET|AWS|GCP|Azure|Cloud|C#|Flutter|React|REACT\.JS|Ruby|Rails|Bootstrap|jQuery|Vue\.js|Express|Django|Spring|MVC|Android|Kotlin|GOLANG|\bGO\b|Swift|Objective-C|PHP|Laravel"

    def busca_tecnologia(title):
        tecnologia = []
        matches = re.findall(regex_str, title, re.IGNORECASE)
        if matches:
            for match in matches:
                cleaned_match = match.upper().replace(" ", "").replace("-", "")
                tecnologia.append(cleaned_match)

            tecnologia = list(set(tecnologia))

            for i, item in enumerate(tecnologia):
                if item == "GOLANG":
                    tecnologia[i] = "GO"
                elif item == "DOTNET" or item == "NET":
                    tecnologia[i] = ".NET"
                elif item == "NODEJS":
                    tecnologia[i] = "NODE.JS"
                elif item == "REACTJS":
                    tecnologia[i] = "REACT"
                elif item == "REACT.JS":
                    tecnologia[i] = "REACT"
        else:
            tecnologia = ["Não especificado"]

        return ", ".join(map(str, tecnologia))

    # Buscar tecnologia no titulo da vaga
    def busca_tecnologia_grafico(title):
        tecnologia = []
        matches = re.findall(regex_str, title, re.IGNORECASE)
        if matches:
            for match in matches:
                cleaned_match = match.upper().replace(" ", "").replace("-", "")
                tecnologia.append(cleaned_match)

            tecnologia = list(set(tecnologia))

            for i, item in enumerate(tecnologia):
                if item == "GOLANG":
                    tecnologia[i] = "GO"
                elif item == "DOTNET" or item == "NET":
                    tecnologia[i] = ".NET"
                elif item == "NODEJS":
                    tecnologia[i] = "NODE.JS"
        #
        else:
            tecnologia = "Não especificado"

        return tecnologia

    dados_nao_nulos["temp"] = (
        dados_nao_nulos["title"] + " " + dados_nao_nulos["description"]
    )
    dados_nao_nulos["requisitos"] = dados_nao_nulos.temp.apply(
        lambda x: busca_tecnologia(x)
    )
    dados_nao_nulos["lista"] = dados_nao_nulos.temp.apply(
        lambda x: busca_tecnologia_grafico(x)
    )

    dados_nao_nulos = dados_nao_nulos.drop(columns=["temp"])

    regex_str_posicao = "Estagiário|Estagiario|Junior|Júnior|JR|Nivel 1|Nivel I|Nível 1|Nível I|Pleno/Sênior|Senior|Sênior|SR|Pleno|Tech Lead|Tech-lead|Diretor|Coordenador|Gerente"

    # Buscar posicao no titulo da vaga
    def busca_posicao(title):
        tecnologia = ""
        if re.findall(regex_str_posicao, title, re.IGNORECASE) != []:
            tecnologia = (
                re.findall(regex_str_posicao, title, re.IGNORECASE)[0]
                .upper()
                .replace(" ", "")
                .replace("-", "")
            )
        else:
            tecnologia = "Não especificado"

        return tecnologia

    def busca_posicao_detalhe(title):
        tecnologia = ""
        tecnologias_encontradas = re.findall(regex_str_posicao, title, re.IGNORECASE)

        if tecnologias_encontradas:
            # Conta a frequência das tecnologias encontradas
            contador = Counter(tecnologias_encontradas)

            # Escolhe a tecnologia mais frequente
            tecnologia_mais_frequente = (
                contador.most_common(1)[0][0].upper().replace(" ", "").replace("-", "")
            )

            tecnologia = tecnologia_mais_frequente
        else:
            tecnologia = "Não especificado"

        return tecnologia

    def busca_posicao_descricao(title):
        tecnologia = ""
        tecnologias_encontradas = re.findall(regex_str_posicao, title, re.IGNORECASE)

        if tecnologias_encontradas:
            # Conta a frequência das tecnologias encontradas
            contador = Counter(tecnologias_encontradas)

            # Escolhe a tecnologia mais frequente
            tecnologia_mais_frequente = (
                contador.most_common(1)[0][0].upper().replace(" ", "").replace("-", "")
            )

            tecnologia = tecnologia_mais_frequente
        else:
            tecnologia = "Não especificado"

        return tecnologia

    dados_nao_nulos["posicao"] = dados_nao_nulos.title.apply(lambda x: busca_posicao(x))
    dados_nao_nulos.loc[dados_nao_nulos["posicao"] == "Não especificado", "posicao"] = (
        dados_nao_nulos.experience_level.apply(lambda x: busca_posicao_detalhe(x))
    )
    dados_nao_nulos.loc[dados_nao_nulos["posicao"] == "Não especificado", "posicao"] = (
        dados_nao_nulos.description.apply(lambda x: busca_posicao_descricao(x))
    )

    dados_nao_nulos.loc[
        dados_nao_nulos["posicao"].isin(["SR", "SENIOR"]), "posicao"
    ] = "SÊNIOR"
    dados_nao_nulos.loc[dados_nao_nulos["posicao"].isin(["JR"]), "posicao"] = "JUNIOR"
    dados_nao_nulos.loc[dados_nao_nulos["posicao"].isin(["JÚNIOR"]), "posicao"] = (
        "JUNIOR"
    )
    dados_nao_nulos.loc[
        dados_nao_nulos["posicao"].isin(["NÍVELI", "NÍVEL1"]), "posicao"
    ] = "JUNIOR"
    dados_nao_nulos.loc[
        dados_nao_nulos["posicao"].isin(["TECH LEAD,TECH-LEAD"]), "posicao"
    ] = "TECH-LEAD"
    dados_nao_nulos.loc[dados_nao_nulos["posicao"].isin(["ESTAGIÁRIO"]), "posicao"] = (
        "ESTAGIARIO"
    )

    dados_nao_nulos.drop(columns=["lista"], inplace=True)

    destination_path = os.path.join(current_directory, "results/dados_transformados.csv")
    dados_nao_nulos.to_csv(destination_path, index=False)


def load_data():
    source_file = "results/dados_transformados.csv"
    destination_file = "prod/dados_final.db"

    current_directory = os.path.dirname(__file__)

    source_path = os.path.join(current_directory, source_file)
    destination_path = os.path.join(current_directory, destination_file)

    dados = pd.read_csv(source_path)
    
    disk_engine = create_engine(f'sqlite:///{destination_path}')
    dados.to_sql('vagas', disk_engine)



task1 = PythonOperator(
    task_id="Extraçao_dos_dados",
    dag=dag,
    python_callable=extract_and_save_data,
    # requirements=requisitos+['fastapi'],
    # system_site_packages=False,
    # provide_context=False,
)

task2 = PythonOperator(
    task_id="Transformaçao_dos_dados",
    dag=dag,
    python_callable=transform_data,
    # requirements=requisitos+['fastapi'],
    # system_site_packages=False,
    # provide_context=False,
)

task3 = PythonOperator(
    task_id="Carregamento_dos_dados",
    dag=dag,
    python_callable=load_data,
    # requirements=requisitos+['fastapi'],
    # system_site_packages=False,
    # provide_context=False,
)

task1 >> task2 >> task3
