# ETL-AIRFLOW

Um processo de ETL orquestrado com Airflow.



## Como Funciona?

A extração de dados é feita com um web-scrap de vagas do linkedin

A transformação de dados trata dados nulos e melhora os dados de algumas colunas, inclusive criando colunas com informações importantes como requisitos da vaga, cargo (em breve será adicionado se a vaga é remota, hibrida ou presencial)

O carregamento desses dados tratados é feito temporariamente em um banco de dados sql (sqlite), onde temos o banco analítico e transacional.

No banco transacional, apenas os registros do dia são gravados, já no analítico, ele mantém dados de outros dias para questões de análise histórica.

Após esses dados serem armazenados, os dados do banco de dados analitico são enviados para a cloud (https://aiven.io/) em um banco de dados POSTGRES. 

Já o banco de dados analitico é enviado para um repositório no GITHUB (https://github.com/PedroCozzati/streamlit-test), onde o STREAMLIT está observando para gerar alguns gráficos no site hospedado (https://relatorios-vagas.streamlit.app/?utm_medium=oembed)
## Documentação técnica

Para o projeto, foi utilizada a imagem do Airflow v2.8.1 no Docker.

Após isso, para implementar o web-scrap, foi necessário inserir uma imagem do selenium no docker-compose do airflow, para ser possível ter uma sessão remota do selenium e conseguir capturar os dados do Linkedin.

**OBS: Os dados são capturados em uma sessão offline para evitar problemas com a conta pessoal, então menos informações estão acessíveis.**

Para criar uma conexão com bancos de dados, foi utilizado SQLAlchemy e Pandas para ler esses dados. Com isso foi possível enviar dados para o Aiven Cloud. 

Para enviar dados para o repositório onde o Streamlit faz o deploy, foi utilizada a lib PyGithub, onde com o token da conta é possível fazer commit, push e muito mais em repositórios.



## Autores

- [@PedroCozzati](https://www.github.com/PedroCozzati)

## Colaboradores

- [@GuiTadeuS](https://github.com/GuiTadeuS) - Contribuiu com o web-scrap, configuração do Docker e testou o projeto na máquina dele. Também sugeriu funcionalidades.


## Deploy

Links 

```bash
  Site de vagas que consome os dados: *EM BREVE*
  Relatório Streamlit: https://relatorio-linkedin-vagas.streamlit.app/
```


## Observações

Tive alguns problemas em fazer o Airflow reconhecer algumas bibliotecas como o selenium, caso queira instalar o projeto localmente irá notar que existem pastas de libs dentro do projeto (além da pasta libs no ambiente). 
## Rodando localmente
**Primeiramente, tenha o ambiente do docker instalado e configurado na sua máquina, e verifique as portas de rede sendo utilizadas**

Clone o projeto

```bash
  git clone https://github.com/PedroCozzati/pipeline-airflow-etl-linkedin
```

Instale as dependências

```bash
  pip install -r requirements.txt
```

Identifique o diretório com o arquivo docker-compose e rode o comando:

```bash
  docker-compose up -d
```

Verifique se os containeres foram inicializados e vá na url http://localhost:8080/

Se aparecer a área de login e a DAG foi importada corretamente, então está tudo rodando corretamente.

## Stack utilizada

Docker, Python, Airflow, SQL


## Screenshots


DAG
![image](https://github.com/PedroCozzati/pipeline-airflow-etl-linkedin/assets/80106385/60e6c975-23dc-44a0-a05a-132cd6d6fca7)

Estrutura do projeto
![image](https://github.com/PedroCozzati/pipeline-airflow-etl-linkedin/assets/80106385/d476f47a-f4ef-47c5-97d9-473055690f75)

Gráfico utilizando os dados obtidos (MATPLOTLIB)
![image](https://github.com/PedroCozzati/pipeline-airflow-etl-linkedin/assets/80106385/bea71478-86a4-43de-929b-45a4f936ff47)
