import os
import pandas as pd
from sqlalchemy import create_engine

#No banco de dados transacional, apenas as vagas que foram extraidas no dia serão inseridas.
#O banco será recriado com apenas essas vagas
def load_data_transacional():
    source_file = "../results/dados_transformados.csv"
    destination_file = "../prod/dados_transacional.db"

    current_directory = os.path.dirname(__file__)

    source_path = os.path.join(current_directory, source_file)
    destination_path = os.path.join(current_directory, destination_file)

    dados = pd.read_csv(source_path)
    
    dados.drop(columns=["lista"], inplace=True)
    
    disk_engine = create_engine(f'sqlite:///{destination_path}')
    dados.to_sql('vagas', disk_engine,if_exists='replace')
    
#No banco analítico, todo dia serão inseridas novas vagas e elas irão permanecer para consulta
def load_data_analitico():
    source_file = "../results/dados_transformados.csv"
    destination_file = "../prod/dados_analitico.db"

    current_directory = os.path.dirname(__file__)

    source_path = os.path.join(current_directory, source_file)
    destination_path = os.path.join(current_directory, destination_file)

    dados = pd.read_csv(source_path)
    dados.drop(columns=["lista"], inplace=True)
    
    dados = dados.drop_duplicates(subset=['job_id'])
    
    disk_engine = create_engine(f'sqlite:///{destination_path}')
 
    dados.to_sql('vagas', disk_engine, if_exists='append', index=False)