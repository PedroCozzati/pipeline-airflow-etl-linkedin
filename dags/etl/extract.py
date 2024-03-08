import os
import pandas as pd
import sqlite3


def get_all_source():

    destination_file = "../results/full_data.csv"

    source_file = "../source/dados_raw.db"

    current_directory = os.path.dirname(__file__)

    source_path = os.path.join(current_directory, source_file)
            
    conn = sqlite3.connect(source_path)
    query = "SELECT * FROM vaga;"
    data = pd.read_sql_query(query, conn)
    conn.close()

    destination_path = os.path.join(current_directory, destination_file)

    data.to_csv(destination_path, index=False)   