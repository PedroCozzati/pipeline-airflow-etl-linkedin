Projeto que contém um ETL básico onde:<br><br>

- A extração de dados é feita com um web-scrap de vagas do linkedin<br>

- A transformação de dados trata dados nulos e melhora a qualidade de algumas colunas, inclusive criando colunas com informações importantes como requisitos da vaga, cargo (em breve será adicionado se a vaga é remota, hibrida ou presencial)<br>

- O carregamento desses dados tratados é feito em um banco de dados sql (sqlite), onde temos o banco analítico e transacional.<br>

- No banco transacional, apenas os registros do dia são gravados, já no analítico, ele mantém dados de outros dias para questões de análise.<br>

![image](https://github.com/PedroCozzati/pipeline-airflow-etl-linkedin/assets/80106385/60e6c975-23dc-44a0-a05a-132cd6d6fca7)
![image](https://github.com/PedroCozzati/pipeline-airflow-etl-linkedin/assets/80106385/bea71478-86a4-43de-929b-45a4f936ff47)
![image](https://github.com/PedroCozzati/pipeline-airflow-etl-linkedin/assets/80106385/e3161672-1e81-4df0-b21c-90b9bcd68db6)
![image](https://github.com/PedroCozzati/pipeline-airflow-etl-linkedin/assets/80106385/6cf3ed61-e20a-4fd6-9726-ff15fc191f2c)
![image](https://github.com/PedroCozzati/pipeline-airflow-etl-linkedin/assets/80106385/ccc36e6e-f757-4013-8494-be834aa601d6)

