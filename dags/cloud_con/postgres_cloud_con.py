import os
import sqlite3
from dotenv import load_dotenv
import psycopg2
from cloud_con.conect_file import con_string

def export_data_aiven_cloud_postgres():
    load_dotenv()
    
    #SQLITE DATABASE LOADING
    
    db_file = "../prod/dados_transacional.db"
    db_directory = os.path.dirname(__file__)
    db_path = os.path.join(db_directory, db_file)
    sqlite_conn = sqlite3.connect(db_path) 
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute("SELECT * FROM vagas")  
    sqlite_data = sqlite_cursor.fetchall()
    
    #SEE THE "connect_file.py" and change the connect string with your auth data
    postgre_con = psycopg2.connect(con_string())
    
    query_sql = 'SELECT VERSION()'
    postgre_cursor = postgre_con.cursor()
    postgre_cursor.execute(query_sql)
    version = postgre_cursor.fetchone()[0]
    print(version)
    postgre_cursor.execute("DELETE FROM vagas")
    
    
    #EXPORT SQLITE DATABASE TO REMOTE POSTGRES ON AIVEN
    try:
        postgre_cursor.execute('''CREATE TABLE IF NOT EXISTS vagas (
            job_id BIGINT PRIMARY KEY,
            title VARCHAR(200),
            register_date VARCHAR(200),
            company VARCHAR(100),
            location VARCHAR(200),
            time_opened VARCHAR(200),
            link VARCHAR(900),
            applications VARCHAR(100),
            experience_level VARCHAR(100),
            job_type VARCHAR(100),
            role VARCHAR(140),
            sectors VARCHAR(140),
            description VARCHAR(10000),
            requirements VARCHAR(200),
            position VARCHAR(50)
            )''')

        for row in sqlite_data:
            postgre_cursor.execute('''
            INSERT INTO vagas (
                job_id,
                title,
                register_date,
                company,
                location,
                time_opened,
                link,
                applications,
                experience_level,
                job_type,
                role,
                sectors,
                description,
                requirements,
                position
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (job_id) DO UPDATE
            SET
                title = EXCLUDED.title,
                register_date = EXCLUDED.register_date,
                company = EXCLUDED.company,
                location = EXCLUDED.location,
                time_opened = EXCLUDED.time_opened,
                link = EXCLUDED.link,
                applications = EXCLUDED.applications,
                experience_level = EXCLUDED.experience_level,
                job_type = EXCLUDED.job_type,
                role = EXCLUDED.role,
                sectors = EXCLUDED.sectors,
                description = EXCLUDED.description,
                requirements = EXCLUDED.requirements,
                position = EXCLUDED.position
            ''', (
                row[3],  # job_id
                row[2],  # title
                row[4],  # register_date
                row[5],  # company
                row[6],  # location
                row[7],  # time_opened
                row[8],  # link
                row[9],  # applications
                row[10],  # experience_level
                row[11],  # job_type
                row[12],  # role
                row[13],  # sectors
                row[14],  # description
                row[15],  # requirements
                row[16]  # position
            ))

        postgre_con.commit()

        postgre_cursor.execute("SELECT * FROM vagas")
        print(postgre_cursor.fetchone()[1])

    finally:
        postgre_con.close()
        sqlite_conn.close()

