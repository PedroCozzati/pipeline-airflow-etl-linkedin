from collections import Counter
import os
import re
import pandas as pd

def transform_data():
    source_file = "../results/full_data.csv"

    current_directory = os.path.dirname(__file__)

    source_path = os.path.join(current_directory, source_file)
    
    dados = pd.read_csv(source_path)
    dados = dados.reset_index(drop=True)
    # dados.set_index("id", inplace=True)
    dados_tec = dados[
        dados["title"].str.contains(
            "QA|Implantação|Programação|Desenvolvedor|Programador|Developer|Analista|Desenvolvimento|Engenheiro|Software|Estágio|Tecnologicas|Tecnologia|Computação|Tech|stack|Dev|Data|Desenvolver|TI",
            # "Desenvolvedor|Engenheiro de Dados|DATA ENGINEER|DATA ENGINEERING",
            case=False,
            regex=True,
        )
    ]
    # dados_tec.drop_duplicates(inplace=True)
    # dados_tec[dados_tec.duplicated()]
    dados_tec.drop_duplicates(subset=["job_id"], inplace=True)

    dados_tec.applications.fillna("Hidden", inplace=True)
    dados_tec.loc[
        dados_tec["applications"] == "Seja um dos 25 primeiros a se candidatar",
        "applications",
    ] = "<=25"
    dados_tec.loc[:, "applications"] = dados_tec.loc[:, "applications"].str.replace(r'[^0-9]', '', regex=True)

    dados_tec.experience_level.fillna("Assistente", inplace=True)

    dados_tec.job_type.fillna("Tempo integral", inplace=True)

    dados_tec.role.fillna("Engenharia e Tecnologia da informação", inplace=True)
    dados_tec.sectors.fillna(
        "Atividades dos serviços de tecnologia da informação", inplace=True
    )

    dados_tec.description.fillna("Sem descrição", inplace=True)

    dados_nao_nulos = dados_tec
    dados_nao_nulos.isnull().sum()

    regex_str = r"RUST|Excel|Windows|Linux|\\bC\\b|\\bC\+\+\\b|\bGIT|ASSEMBLY|GRAPHQL|DELPHI|PL/SQL|Node\.?JS|CSS|HTML|Wordpress|Angular(?:JS)?|Airflow|NOSQL|Spark|Power BI|Salesforce|DotNet|PASCAL|COBOL|ABAP|JAVASCRIPT|JAVA|SQL|Python|Angular|\.NET|AWS|GCP|Azure|Cloud|C#|Flutter|React|REACT\.JS|Ruby|Rails|Bootstrap|jQuery|Vue\.js|Express|Django|Spring|MVC|Android|Kotlin|GOLANG|\bGO\b|Swift|Objective-C|PHP|Laravel"

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

    regex_str_posicao = r"\b(Estagiário|Estagiario|Junior|Júnior|Nivel\s?1|Nivel\s?I|Nível\s?1|Nível\s?I|Pleno/Sênior|Senior|Sênior|Pleno|Tech\s?Lead|Tech-lead|Coordenador|Gerente|JR|SR)\b"


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
            print(tecnologia)
        else:
            tecnologia = "Não especificado"

        return tecnologia

    # Aplicar busca_posicao na coluna "title"
    dados_nao_nulos["posicao"] = dados_nao_nulos.title.apply(lambda x: busca_posicao(x))

    # Identificar índices onde "posicao" ainda é "Não especificado"
    indices_sem_posicao = dados_nao_nulos[dados_nao_nulos["posicao"] == "Não especificado"].index

    # Aplicar busca_posicao_detalhe nos índices identificados
    dados_nao_nulos.loc[indices_sem_posicao, "posicao"] = dados_nao_nulos.loc[indices_sem_posicao, "description"].apply(lambda row: busca_posicao_detalhe(row))
    
    indices_sem_posicao = dados_nao_nulos[dados_nao_nulos["posicao"] == "Não especificado"].index
    
    dados_nao_nulos.loc[indices_sem_posicao, "posicao"] = dados_nao_nulos.loc[indices_sem_posicao, "experience_level"].apply(lambda row: busca_posicao_detalhe(row))

    # dados_nao_nulos["posicao"] = dados_nao_nulos.apply(lambda row: busca_posicao_descricao(row['description']))
    
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
    
    regex_str_tipo_vaga = r"\b(HomeOffice|Home-Office|Home Office|Híbrido|Hibrido|Hibrida|Híbrida|Remoto|Remota|Presencial)\b"


    # Buscar posicao no titulo da vaga
    def busca_tipo(title):
        tecnologia = ""
        if re.findall(regex_str_tipo_vaga, title, re.IGNORECASE) != []:
            tecnologia = (
                re.findall(regex_str_tipo_vaga, title, re.IGNORECASE)[0]
                .upper()
                .replace(" ", "")
                .replace("-", "")
            )
        else:
            tecnologia = "Não especificado"

        return tecnologia

   
    # Aplicar busca_posicao na coluna "title"
    dados_nao_nulos["tipo_vaga"] = dados_nao_nulos.description.apply(lambda x: busca_tipo(x))
    
    dados_nao_nulos.loc[
        dados_nao_nulos["tipo_vaga"].isin(["HÍBRIDO", "HIBRIDO","HIBRIDA"]), "tipo_vaga"
    ] = "HÍBRIDA"
    
    dados_nao_nulos.loc[
        dados_nao_nulos["tipo_vaga"].isin(["HOME OFFICE", "HOMEOFFICE","REMOTO"]), "tipo_vaga"
    ] = "REMOTA"
    
    regex_str_beneficios = r"Vale Refeição|Vale-Refeição|Vale Alimentação|Vale-Alimentação|Vale Transporte|Vale-Transporte|VR|VA|VT|Convênio|Convenio|CONVÊNIO|CONVENIO|GYMPASS|Gympass|Gym-pass|GymPass|Gym Pass|GYM-PASS|GYM PASS"


    # Buscar posicao no titulo da vaga
    def busca_beneficios(title):
        tecnologia = []
        matches = re.findall(regex_str_beneficios, title)
        if matches:
            for match in matches:
                cleaned_match = match.upper().replace(" ", "").replace("-", "")
                tecnologia.append(cleaned_match)

            tecnologia = list(set(tecnologia))
            
            for i, item in enumerate(tecnologia):
                if item == "CONVENIO":
                    tecnologia[i] = "CONVÊNIO"
                elif item == "VALEREFEIÇÃO":
                    tecnologia[i] = "VR"
                elif item == "VALEALIMENTAÇÃO":
                    tecnologia[i] = "VA"
                elif item == "VALETRANSPORTE":
                    tecnologia[i] = "VT"
        else:
            tecnologia = ["Não especificado"]

        return ", ".join(map(str, list(set(tecnologia))))

   
    # Aplicar busca_posicao na coluna "title"
    dados_nao_nulos["beneficios"] = dados_nao_nulos.description.apply(lambda x: busca_beneficios(x))
    
    destination_path = os.path.join(current_directory, "../results/dados_transformados.csv")
    dados_nao_nulos.to_csv(destination_path, index=False)
    
transform_data()